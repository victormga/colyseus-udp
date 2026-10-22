using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Colyseus.Schema.Utils;
using GameDevWare.Serialization;

namespace Colyseus.Udp {
	public sealed class UdpChannel : IDisposable {
		const string TICKET = "__udp";

		const byte HELLO = 0x01;
		const byte ACK = 0x02;
		const byte DATA = 0x03;
		const byte UNRELIABLE_PATCH = 0x0F | 0x40;
		const int HANDSHAKE_INTERVAL_MS = 250;
		const int KEEPALIVE_INTERVAL_MS = 5000;

		internal class Ticket {
#pragma warning disable CS0649 // assigned by MsgPack through reflection
			public string token;
			public int port;
			public string host;
#pragma warning restore CS0649

			[Preserve]
			public Ticket() { }
		}

		public bool IsOpen { get; private set; }
		public event Action OnOpen;
		public event Action OnClose;

		readonly CancellationTokenSource lifetime = new CancellationTokenSource();
		readonly TaskCompletionSource<bool> opened = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		readonly SynchronizationContext mainThread;
		readonly Action<byte[]> dispatch;
		readonly Func<Task<Ticket>> requestTicket;
		UdpClient socket;
		Action unsubscribe;
		byte[] hello;
		ushort? lastSequence;

		internal UdpChannel(SynchronizationContext mainThread, Action<byte[]> dispatch, Func<Task<Ticket>> requestTicket) {
			this.mainThread = mainThread;
			this.dispatch = dispatch;
			this.requestTicket = requestTicket;
		}

		/// <param name="host">The host the Colyseus client connects to; the server's ticket can override it.</param>
		/// <exception cref="TimeoutException">The server never answered over UDP, most likely because UDP is blocked.</exception>
		/// <exception cref="SocketException">The host didn't resolve, or there's no route to it.</exception>
		/// <exception cref="OperationCanceledException">The room was left after the ticket arrived, before the channel opened.</exception>
		/// <remarks>Leaving while the ticket is still being requested surfaces Room.Request's own error.</remarks>
		public static async Task<UdpChannel> Connect<T>(Room<T> room, string host, int timeoutMs = 3000) where T : Schema.Schema {
			SynchronizationContext mainThread = SynchronizationContext.Current
				?? throw new InvalidOperationException("UdpChannel.Connect must be called from Unity's main thread.");

			UdpChannel channel = new UdpChannel(
				mainThread,
				frame => room.Connection.Dispatch(ConnectionEvent.Received(frame)),
				() => room.Request<Ticket>(TICKET)
			);

			CloseWithCodeEventHandler onLeave = _ => channel.Dispose();
			CloseWithCodeEventHandler onDrop = _ => channel.Drop();
			NoArgsEventHandler onReconnect = channel.Rebind;
			room.OnLeave += onLeave;
			room.OnDrop += onDrop;
			room.OnReconnect += onReconnect;
			channel.unsubscribe = () => {
				room.OnLeave -= onLeave;
				room.OnDrop -= onDrop;
				room.OnReconnect -= onReconnect;
			};

			await channel.Start(host, timeoutMs);
			return channel;
		}

		public void Send(string type, object message = null) {
			List<byte> packet = new List<byte> { DATA, Protocol.ROOM_DATA };
			Encode.String(packet, type);
			this.Transmit(packet, message);
		}

		public void Send(byte type, object message = null) {
			List<byte> packet = new List<byte> { DATA, Protocol.ROOM_DATA };
			packet.AddRange(Encode.EncodeUintNumber(type));
			this.Transmit(packet, message);
		}

		public void Dispose() {
			if (this.lifetime.IsCancellationRequested) return;
			this.lifetime.Cancel();
			this.opened.TrySetCanceled();
			this.unsubscribe?.Invoke();
			this.socket?.Dispose();
			this.Drop();
		}

		internal async Task Start(string host, int timeoutMs) {
			try {
				Ticket ticket = await this.requestTicket();
				this.lifetime.Token.ThrowIfCancellationRequested();

				host = ticket.host ?? host;
				// synchronous on purpose: an await here would let Rebind send before the socket exists
				IPAddress[] addresses = Dns.GetHostAddresses(host);
				if (addresses.Length == 0) throw new SocketException((int)SocketError.HostNotFound);

				// the server listens on IPv4 only; an IPv6-only network's sole address is a NAT64 one that still reaches it
				IPAddress address = Array.Find(addresses, candidate => candidate.AddressFamily == AddressFamily.InterNetwork) ?? addresses[0];
				this.socket = new UdpClient(address.AddressFamily);
				this.socket.Connect(address, ticket.port);

				this.hello = HelloPacket(ticket.token);
				_ = this.Receive();
				_ = this.Handshake();

				if (await Task.WhenAny(this.opened.Task, Task.Delay(timeoutMs)) != this.opened.Task) {
					throw new TimeoutException($"No UDP answer from {host}:{ticket.port} after {timeoutMs}ms.");
				}
				await this.opened.Task;
			} catch {
				this.Dispose();
				throw;
			}
		}

		internal void Drop() {
			// the server may still ACK the old token until it notices the drop; only Rebind's new one may reopen
			this.hello = null;
			if (!this.IsOpen) return;

			this.IsOpen = false;
			this.OnClose?.Invoke();
		}

		internal async void Rebind() {
			Ticket ticket = await this.requestTicket();
			if (this.lifetime.IsCancellationRequested) return;

			this.hello = HelloPacket(ticket.token);
			this.lastSequence = null; // the reconnect resent the full state, so patches start a new sequence
			this.SendHello();
		}

		static byte[] HelloPacket(string token) {
			byte[] packet = new byte[1 + token.Length];
			packet[0] = HELLO;
			Encoding.ASCII.GetBytes(token, 0, token.Length, packet, 1);
			return packet;
		}

		void Transmit(List<byte> packet, object message) {
			if (!this.IsOpen) return;

			if (message != null) {
				MemoryStream payload = new MemoryStream();
				MsgPack.Serialize(message, payload, SerializationOptions.SuppressTypeInformation);
				packet.AddRange(payload.ToArray());
			}

			this.Write(packet.ToArray());
		}

		void SendHello() {
			if (this.hello != null) this.Write(this.hello);
		}

		void Write(byte[] packet) {
			try {
				this.socket.Send(packet, packet.Length);
			} catch (SocketException) {
				// an ICMP "unreachable" or a lost network surfaces on sends; the datagram is just lost
			}
		}

		async Task Handshake() {
			// steps at the handshake rate even while open, so a drop or rebind never waits out a keepalive
			DateTime keepaliveAt = DateTime.MinValue;
			while (!this.lifetime.IsCancellationRequested) {
				if (!this.IsOpen || DateTime.UtcNow >= keepaliveAt) {
					this.SendHello();
					keepaliveAt = DateTime.UtcNow.AddMilliseconds(KEEPALIVE_INTERVAL_MS);
				}

				try {
					await Task.Delay(HANDSHAKE_INTERVAL_MS, this.lifetime.Token);
				} catch (OperationCanceledException) {
					return;
				}
			}
		}

		async Task Receive() {
			while (!this.lifetime.IsCancellationRequested) {
				byte[] packet;
				try {
					packet = (await this.socket.ReceiveAsync().ConfigureAwait(false)).Buffer;
				} catch (ObjectDisposedException) {
					return;
				} catch (SocketException) {
					// an ICMP "unreachable" from an earlier send surfaces here
					continue;
				}

				this.mainThread.Post(_ => this.Handle(packet), null);
			}
		}

		void Handle(byte[] packet) {
			if (this.lifetime.IsCancellationRequested || packet.Length == 0) return;

			switch (packet[0]) {
				case ACK:
					if (this.hello == null || this.IsOpen) return;
					this.IsOpen = true;
					this.OnOpen?.Invoke();
					this.opened.TrySetResult(true);
					break;
				case DATA:
					this.Deliver(packet);
					break;
			}
		}

		void Deliver(byte[] packet) {
			if (packet.Length < 2) return;

			if (packet[1] == Protocol.ROOM_DATA) {
				this.dispatch(packet[1..]);
				return;
			}

			// the server sends nothing else over UDP; a spoofed LEAVE_ROOM, say, must not reach the room
			if (packet[1] != UNRELIABLE_PATCH || packet.Length < 4) return;

			// the SDK doesn't know the UNRELIABLE modifier or its [uint16 LE seq] header
			ushort sequence = BinaryPrimitives.ReadUInt16LittleEndian(packet.AsSpan(2));
			bool stale = this.lastSequence is ushort last && (short)(sequence - last) <= 0;
			if (stale) return;
			this.lastSequence = sequence;

			byte[] frame = new byte[packet.Length - 3];
			frame[0] = Protocol.ROOM_STATE_PATCH;
			Buffer.BlockCopy(packet, 4, frame, 1, packet.Length - 4);
			this.dispatch(frame);
		}
	}
}
