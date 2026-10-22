using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Colyseus.Udp.Tests {
	/// <summary>The channel on its own, against a FakeServer: no Colyseus server or room involved.</summary>
	public class UdpChannelTest {
		const byte UNRELIABLE_PATCH = 0x0F | 0x40;

		readonly MainThread mainThread = new MainThread();

		[TearDown]
		public void NothingThrewOnTheMainThread() {
			Exception[] errors = this.mainThread.errors.ToArray();
			this.mainThread.errors.Clear();
			Assert.That(errors, Is.Empty);
		}

		[Test]
		public async Task Acks_after_a_drop_do_not_reopen_the_channel_before_rebind() {
			(bool reopenedEarly, string reopenedWith) after = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer();
				using UdpChannel channel = server.Channel("first", "second");
				await channel.Start("127.0.0.1", 1000);

				// stands in for the server ACKing the old token until it notices the drop
				channel.Drop();
				server.Send(FakeServer.ACK);
				await Task.Delay(400);
				bool reopenedEarly = channel.IsOpen;

				channel.Rebind();
				await MainThread.Until(() => channel.IsOpen);
				return (reopenedEarly, server.LastHelloToken);
			});

			Assert.That(after, Is.EqualTo((false, "second")));
		}

		[Test]
		public async Task A_lost_hello_after_rebind_is_retried_at_the_handshake_rate() {
			bool reopened = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer();
				using UdpChannel channel = server.Channel("first", "second");
				await channel.Start("127.0.0.1", 1000);

				channel.Drop();
				server.acksPerHello = 0;
				channel.Rebind();
				await Task.Delay(50);
				server.acksPerHello = 1;

				await MainThread.Until(() => channel.IsOpen, 1);
				return channel.IsOpen;
			});

			Assert.That(reopened, Is.True);
		}

		[Test]
		public async Task A_rebind_finishing_after_dispose_is_ignored() {
			// checked by the teardown: a late rebind would throw on the disposed socket
			await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer();
				TaskCompletionSource<UdpChannel.Ticket> late = new TaskCompletionSource<UdpChannel.Ticket>();
				bool first = true;
				UdpChannel channel = new UdpChannel(SynchronizationContext.Current, _ => { }, () => {
					if (!first) return late.Task;
					first = false;
					return Task.FromResult(new UdpChannel.Ticket { token = "first", port = server.Port });
				});
				await channel.Start("127.0.0.1", 1000);

				channel.Rebind();
				channel.Dispose();
				late.SetResult(new UdpChannel.Ticket { token = "second", port = server.Port });
				await Task.Delay(50);
				return true;
			});
		}

		[Test]
		public async Task Duplicate_acks_open_the_channel_once() {
			int opens = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer { acksPerHello = 2 };
				using UdpChannel channel = server.Channel();
				int count = 0;
				channel.OnOpen += () => count++;

				await channel.Start("127.0.0.1", 1000);
				await Task.Delay(100);
				return count;
			});

			Assert.That(opens, Is.EqualTo(1));
		}

		[Test]
		public async Task The_round_trip_is_measured_from_keepalives_not_the_handshake() {
			(int? whileOpening, int? afterKeepalive) rtt = await this.mainThread.Run(async () => {
				// slower than the handshake's 250ms resend, so the opening ACK answers an older HELLO than the last one sent
				using FakeServer server = new FakeServer { ackDelayMs = 300 };
				using UdpChannel channel = server.Channel();
				await channel.Start("127.0.0.1", 1000);
				await Task.Delay(400); // the ACK for the handshake's second HELLO arrives meanwhile
				int? whileOpening = channel.RttMs;

				await MainThread.Until(() => channel.RttMs != null, 7);
				return (whileOpening, channel.RttMs);
			});

			Assert.That(rtt.whileOpening, Is.Null);
			Assert.That(rtt.afterKeepalive, Is.InRange(290, 1000));
		}

		[Test]
		public async Task Malformed_or_foreign_datagrams_never_reach_the_room() {
			int dispatched = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer();
				using UdpChannel channel = server.Channel();
				await channel.Start("127.0.0.1", 1000);

				server.Send();
				server.Send(FakeServer.DATA);
				server.Send(FakeServer.DATA, UNRELIABLE_PATCH, 0x01);
				server.Send(FakeServer.DATA, Protocol.LEAVE_ROOM);
				server.Send(FakeServer.DATA, Protocol.ROOM_STATE, 0x00);
				await Task.Delay(100);

				return server.dispatched.Count;
			});

			Assert.That(dispatched, Is.Zero);
		}

		[Test]
		public async Task Unreliable_patches_drop_stale_sequences_and_lose_their_header() {
			byte[][] frames = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer();
				using UdpChannel channel = server.Channel();
				await channel.Start("127.0.0.1", 1000);
				void SendPatch(ushort sequence, byte marker) => server.Send(FakeServer.DATA, UNRELIABLE_PATCH, (byte)sequence, (byte)(sequence >> 8), marker);

				SendPatch(65535, 0xA1);
				SendPatch(0, 0xA2);
				SendPatch(65534, 0xA3);
				SendPatch(1, 0xA4);
				await MainThread.Until(() => server.dispatched.Count == 3);

				// a rebind follows a reconnect, whose full state re-baselines the sequence
				channel.Rebind();
				SendPatch(60000, 0xA5);
				await MainThread.Until(() => server.dispatched.Count == 4);

				return server.dispatched.ToArray();
			});

			Assert.That(frames, Is.EqualTo(new[] {
				new byte[] { Protocol.ROOM_STATE_PATCH, 0xA1 },
				new byte[] { Protocol.ROOM_STATE_PATCH, 0xA2 },
				new byte[] { Protocol.ROOM_STATE_PATCH, 0xA4 },
				new byte[] { Protocol.ROOM_STATE_PATCH, 0xA5 },
			}));
		}

		[Test]
		public async Task Sends_never_throw_once_the_server_is_gone() {
			Exception thrown = await this.mainThread.Run(async () => {
				FakeServer server = new FakeServer();
				using UdpChannel channel = server.Channel();
				await channel.Start("127.0.0.1", 1000);
				server.Dispose();

				// a tight burst: with time between sends, the pending receive picks up the ICMP error instead
				try {
					for (int i = 0; i < 2000; i++) channel.Send("move");
					return null;
				} catch (Exception e) {
					return e;
				}
			});

			Assert.That(thrown, Is.Null);
		}

		[Test]
		public async Task Start_times_out_when_nobody_answers() {
			(Type error, bool open) after = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer { acksPerHello = 0 };
				using UdpChannel channel = server.Channel();
				try {
					await channel.Start("127.0.0.1", 300);
					return (null, channel.IsOpen);
				} catch (Exception e) {
					return (e.GetType(), channel.IsOpen);
				}
			});

			Assert.That(after, Is.EqualTo((typeof(TimeoutException), false)));
		}

		[Test]
		public async Task Disposing_during_the_handshake_fails_Start_at_once() {
			(bool canceled, long elapsedMs) after = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer { acksPerHello = 0 };
				UdpChannel channel = server.Channel();
				Stopwatch clock = Stopwatch.StartNew();
				Task start = channel.Start("127.0.0.1", 5000);

				await Task.Delay(100);
				channel.Dispose();
				try {
					await start;
					return (false, clock.ElapsedMilliseconds);
				} catch (OperationCanceledException) {
					return (true, clock.ElapsedMilliseconds);
				}
			});

			Assert.That(after.canceled, Is.True);
			Assert.That(after.elapsedMs, Is.LessThan(1000));
		}

		[Test]
		public async Task Hosts_with_only_an_IPv6_address_are_reached_over_IPv6() {
			bool open = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer(IPAddress.IPv6Loopback);
				using UdpChannel channel = server.Channel();
				await channel.Start("::1", 1000);
				return channel.IsOpen;
			});

			Assert.That(open, Is.True);
		}

		[Test]
		public async Task Hosts_with_both_IPv4_and_IPv6_addresses_are_reached_over_IPv4() {
			IPAddress[] localhost = Dns.GetHostAddresses("localhost");
			bool dualStack = localhost.Any(address => address.AddressFamily == AddressFamily.InterNetworkV6)
				&& localhost.Any(address => address.AddressFamily == AddressFamily.InterNetwork);
			Assume.That(dualStack, "needs a localhost that resolves to both IPv4 and IPv6, as on Windows");

			bool open = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer(IPAddress.Loopback);
				using UdpChannel channel = server.Channel();
				await channel.Start("localhost", 1000);
				return channel.IsOpen;
			});

			Assert.That(open, Is.True);
		}

		[Test]
		public async Task A_ticket_arriving_after_dispose_cancels_Start() {
			Type error = await this.mainThread.Run(async () => {
				using FakeServer server = new FakeServer();
				TaskCompletionSource<UdpChannel.Ticket> ticket = new TaskCompletionSource<UdpChannel.Ticket>();
				UdpChannel channel = new UdpChannel(SynchronizationContext.Current, _ => { }, () => ticket.Task);
				Task start = channel.Start("127.0.0.1", 1000);

				channel.Dispose();
				ticket.SetResult(new UdpChannel.Ticket { token = "token", port = server.Port });
				try {
					await start;
					return null;
				} catch (Exception e) {
					return e.GetType();
				}
			});

			Assert.That(error, Is.EqualTo(typeof(OperationCanceledException)));
		}
	}
}
