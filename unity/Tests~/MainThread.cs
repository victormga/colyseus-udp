using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Colyseus.Udp.Tests {
	/// <summary>
	/// Single-threaded SynchronizationContext standing in for Unity's main thread. Like Unity, it
	/// records exceptions thrown by posted work and keeps pumping.
	/// NUnit assertions don't report from this thread: bodies return values, tests assert on them.
	/// </summary>
	sealed class MainThread : SynchronizationContext {
		public readonly ConcurrentQueue<Exception> errors = new ConcurrentQueue<Exception>();

		readonly BlockingCollection<(SendOrPostCallback callback, object state)> queue = new BlockingCollection<(SendOrPostCallback, object)>();

		public MainThread() {
			new Thread(this.Pump) { IsBackground = true }.Start();
		}

		public override void Post(SendOrPostCallback callback, object state) {
			this.queue.Add((callback, state));
		}

		public Task<T> Run<T>(Func<Task<T>> body) {
			TaskCompletionSource<T> done = new TaskCompletionSource<T>();
			this.Post(async _ => {
				try {
					done.SetResult(await body());
				} catch (Exception e) {
					done.SetException(e);
				}
			}, null);
			return done.Task;
		}

		public static async Task Until(Func<bool> condition, int seconds = 2) {
			DateTime deadline = DateTime.UtcNow.AddSeconds(seconds);
			while (!condition()) {
				if (DateTime.UtcNow > deadline) throw new TimeoutException($"condition not met within {seconds}s");
				await Task.Delay(10);
			}
		}

		void Pump() {
			SetSynchronizationContext(this);
			foreach ((SendOrPostCallback callback, object state) in this.queue.GetConsumingEnumerable()) {
				try {
					callback(state);
				} catch (Exception e) {
					this.errors.Enqueue(e);
				}
			}
		}
	}
}
