using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Colyseus.Udp.Samples;
using NUnit.Framework;

namespace Colyseus.Udp.Tests {
	/// <summary>The channel through a real room, against the example server.</summary>
	public class UdpChannelRoomTest {
		const int PORT = 2597;
		const byte UNRELIABLE = 0x40;

		class Move {
			public double x;
			public double z;
		}

		class Echo {
			public int n;
		}

		readonly MainThread mainThread = new MainThread();
		Process exampleServer;

		[OneTimeSetUp]
		public void StartExampleServer() {
			string root = TestContext.CurrentContext.TestDirectory;
			while (!File.Exists(Path.Combine(root, "example", "server.ts"))) root = Path.GetDirectoryName(root);

			this.exampleServer = Process.Start(new ProcessStartInfo("node", "--import tsx example/server.ts") {
				WorkingDirectory = root,
				RedirectStandardOutput = true,
				Environment = { ["PORT"] = PORT.ToString() },
			});

			TaskCompletionSource<bool> listening = new TaskCompletionSource<bool>();
			this.exampleServer.OutputDataReceived += (_, e) => {
				if (e.Data != null && e.Data.Contains("listening")) listening.TrySetResult(true);
			};
			this.exampleServer.BeginOutputReadLine();
			if (!listening.Task.Wait(TimeSpan.FromSeconds(30))) throw new TimeoutException("example server did not start");
		}

		[OneTimeTearDown]
		public void StopExampleServer() {
			this.exampleServer.Kill(true);
		}

		[TearDown]
		public void NothingThrewOnTheMainThread() {
			Exception[] errors = this.mainThread.errors.ToArray();
			this.mainThread.errors.Clear();
			Assert.That(errors, Is.Empty);
		}

		[Test]
		public async Task Connect_returns_an_open_channel() {
			(bool open, int lateOpens) after = await this.mainThread.Run(async () => {
				(Room<CapsulesState> room, UdpChannel udp) = await Join();
				bool open = udp.IsOpen;
				int lateOpens = 0;
				udp.OnOpen += () => lateOpens++;

				await Task.Delay(100);
				await room.Leave();
				return (open, lateOpens);
			});

			Assert.That(after, Is.EqualTo((true, 0)));
		}

		[Test]
		public async Task Unreliable_patches_update_the_room_state() {
			(double x, double z) position = await this.mainThread.Run(async () => {
				(Room<CapsulesState> room, UdpChannel udp) = await Join();

				// writes made before the capsule's ADD is flushed ride on that (reliable) ADD instead
				await Task.Delay(100);

				// x and z are @unreliable: both the move and the resulting patch only travel over UDP
				udp.Send("move", new Move { x = 3, z = 4 });
				Capsule capsule = room.State.capsules[room.SessionId];
				await MainThread.Until(() => capsule.x == 3);

				(double, double) result = (capsule.x, capsule.z);
				await room.Leave();
				return result;
			});

			Assert.That(position, Is.EqualTo((3d, 4d)));
		}

		[Test]
		public async Task The_room_never_sees_the_unreliable_modifier() {
			bool sawModifier = await this.mainThread.Run(async () => {
				(Room<CapsulesState> room, UdpChannel udp) = await Join();

				bool seen = false;
				Action<ConnectionEvent> inner = room.Connection.Dispatch;
				room.Connection.Dispatch = e => {
					bool modified = e.Type == ConnectionEvent.Kind.Message && (e.Data[0] & UNRELIABLE) != 0;
					seen |= modified;
					inner(e);
				};

				// writes made before the capsule's ADD is flushed ride on that (reliable) ADD instead
				await Task.Delay(100);
				Capsule capsule = room.State.capsules[room.SessionId];
				for (int x = 1; x <= 5; x++) {
					udp.Send("move", new Move { x = x, z = 0 });
					await MainThread.Until(() => capsule.x == x);
				}

				await room.Leave();
				return seen;
			});

			Assert.That(sawModifier, Is.False);
		}

		[Test]
		public async Task Unreliable_messages_reach_OnMessage_handlers() {
			int echoed = await this.mainThread.Run(async () => {
				(Room<CapsulesState> room, _) = await Join();

				TaskCompletionSource<int> echo = new TaskCompletionSource<int>();
				room.OnMessage<Echo>("echo", message => echo.TrySetResult(message.n));
				// sent reliably; the example server echoes over UDP only
				await room.Send("echo", new Echo { n = 7 });

				int n = await echo.Task.WaitAsync(TimeSpan.FromSeconds(2));
				await room.Leave();
				return n;
			});

			Assert.That(echoed, Is.EqualTo(7));
		}

		[Test]
		public async Task Sent_frames_match_the_SDKs_own_Room_Send() {
			(byte[][] sdk, byte[][] udp) frames = await this.mainThread.Run(async () => {
				Room<CapsulesState> room = await JoinRoom();

				List<byte[]> sdk = new List<byte[]>();
				Func<byte[], Task> transmit = room.Connection.Transmit;
				room.Connection.Transmit = data => {
					sdk.Add(data);
					return Task.CompletedTask;
				};
				await room.Send("move", new Move { x = 1, z = 2 });
				await room.Send((byte)7, new Move { x = 1, z = 2 });
				room.Connection.Transmit = transmit;

				using FakeServer server = new FakeServer();
				using UdpChannel channel = server.Channel();
				await channel.Start("127.0.0.1", 1000);
				channel.Send("move", new Move { x = 1, z = 2 });
				channel.Send((byte)7, new Move { x = 1, z = 2 });
				await MainThread.Until(() => server.Data.Length == 2);

				await room.Leave();
				return (sdk.ToArray(), server.Data.Select(packet => packet[1..]).ToArray());
			});

			Assert.That(frames.udp, Is.EqualTo(frames.sdk));
		}

		[Test]
		public async Task Leaving_the_room_closes_the_channel() {
			(bool closed, bool open) after = await this.mainThread.Run(async () => {
				(Room<CapsulesState> room, UdpChannel udp) = await Join();
				bool closed = false;
				udp.OnClose += () => closed = true;

				await room.Leave();
				await MainThread.Until(() => closed);
				// sends after the close are dropped, not thrown
				udp.Send("move", new Move { x = 1, z = 1 });

				return (closed, udp.IsOpen);
			});

			Assert.That(after, Is.EqualTo((true, false)));
		}

		[Test]
		public async Task Reconnecting_reopens_the_channel() {
			(bool closed, int opens, double x) after = await this.mainThread.Run(async () => {
				(Room<CapsulesState> room, UdpChannel udp) = await Join();
				bool closed = false;
				int opens = 0;
				udp.OnClose += () => closed = true;
				udp.OnOpen += () => opens++;

				// the SDK only auto-reconnects connections that stayed up for MinUptime
				room.Reconnection.MinUptime = 0;
				room.Connection.Drop();
				await MainThread.Until(() => closed);
				await MainThread.Until(() => udp.IsOpen, 10);

				udp.Send("move", new Move { x = 9, z = 0 });
				await MainThread.Until(() => room.State.capsules[room.SessionId].x == 9);

				double x = room.State.capsules[room.SessionId].x;
				await room.Leave();
				return (closed, opens, x);
			});

			Assert.That(after, Is.EqualTo((true, 1, 9d)));
		}

		static Task<Room<CapsulesState>> JoinRoom() {
			return new Client($"ws://127.0.0.1:{PORT}").JoinOrCreate<CapsulesState>("capsules");
		}

		static async Task<(Room<CapsulesState>, UdpChannel)> Join() {
			Room<CapsulesState> room = await JoinRoom();
			return (room, await UdpChannel.Connect(room, "127.0.0.1"));
		}
	}
}
