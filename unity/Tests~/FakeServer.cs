using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Colyseus.Udp.Tests {
	/// <summary>A UDP peer that ACKs HELLOs, records what the channel sends, and collects what it dispatches to the room.</summary>
	sealed class FakeServer : IDisposable {
		public const byte HELLO = 0x01;
		public const byte ACK = 0x02;
		public const byte DATA = 0x03;

		public int acksPerHello = 1;
		public int ackDelayMs;
		public readonly List<byte[]> dispatched = new List<byte[]>();

		readonly ConcurrentQueue<byte[]> received = new ConcurrentQueue<byte[]>();
		readonly UdpClient socket;
		IPEndPoint peer;

		public FakeServer(IPAddress address = null) {
			this.socket = new UdpClient(new IPEndPoint(address ?? IPAddress.Loopback, 0));
			_ = this.Listen();
		}

		public int Port => ((IPEndPoint)this.socket.Client.LocalEndPoint).Port;

		public byte[][] Data => this.received.Where(packet => packet.Length > 0 && packet[0] == DATA).ToArray();

		public string LastHelloToken => this.received
			.Where(packet => packet.Length > 0 && packet[0] == HELLO)
			.Select(packet => Encoding.ASCII.GetString(packet, 1, packet.Length - 1))
			.LastOrDefault();

		/// <summary>A channel whose tickets point here, handing out <paramref name="tokens"/> in order (the last one repeats).</summary>
		public UdpChannel Channel(params string[] tokens) {
			int issued = 0;
			return new UdpChannel(SynchronizationContext.Current, this.dispatched.Add, () => {
				string token = tokens.Length == 0 ? "token" : tokens[Math.Min(issued++, tokens.Length - 1)];
				return Task.FromResult(new UdpChannel.Ticket { token = token, port = this.Port });
			});
		}

		public void Send(params byte[] packet) {
			this.socket.Send(packet, packet.Length, this.peer);
		}

		public void Dispose() {
			this.socket.Dispose();
		}

		async Task Listen() {
			while (true) {
				UdpReceiveResult result;
				try {
					result = await this.socket.ReceiveAsync().ConfigureAwait(false);
				} catch (ObjectDisposedException) {
					return;
				} catch (SocketException) {
					return;
				}

				this.peer = result.RemoteEndPoint;
				this.received.Enqueue(result.Buffer);
				if (result.Buffer.Length > 0 && result.Buffer[0] == HELLO) _ = this.Ack();
			}
		}

		async Task Ack() {
			if (this.ackDelayMs > 0) await Task.Delay(this.ackDelayMs).ConfigureAwait(false);
			for (int i = 0; i < this.acksPerHello; i++) this.Send(ACK);
		}
	}
}
