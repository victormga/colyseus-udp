using System.Collections.Generic;
using UnityEngine;

namespace Colyseus.Udp.Samples {
	/// <summary>
	/// Drop on an empty GameObject and run the example server (npm run example). Even the local capsule is drawn from
	/// server state, so what moves on screen is what came back over UDP.
	/// </summary>
	public class CapsulesDemo : MonoBehaviour {
		const float SEND_INTERVAL = 1f / 30f;
		const float RADIUS = 3f;
		const float SMOOTHING = 15f;

		class Move {
			public float x;
			public float z;
		}

		[SerializeField] string host = "127.0.0.1";
		[SerializeField] int port = 2567;

		readonly Dictionary<string, Transform> views = new Dictionary<string, Transform>();
		Room<CapsulesState> room;
		UdpChannel udp;
		float phase;
		float nextSend;

		async void Start() {
			this.phase = Random.value * Mathf.PI * 2f;
			Room<CapsulesState> room = await new Client($"ws://{this.host}:{this.port}").JoinOrCreate<CapsulesState>("capsules");
			if (this == null) {
				// destroyed while joining, e.g. Play Mode was exited
				await room.Leave();
				return;
			}
			this.room = room;

			try {
				this.udp = await UdpChannel.Connect(room, this.host);
			} catch (System.TimeoutException) {
				Debug.LogError("No UDP answer from the server; UDP is probably blocked.");
				await room.Leave();
			} catch (System.OperationCanceledException) {
				// destroyed while connecting: OnDestroy already left the room
			}
		}

		void Update() {
			if (this.udp == null || !this.udp.IsOpen) return;

			if (Time.time >= this.nextSend) {
				this.nextSend = Time.time + SEND_INTERVAL;
				float angle = this.phase + Time.time;
				this.udp.Send("move", new Move { x = Mathf.Cos(angle) * RADIUS, z = Mathf.Sin(angle) * RADIUS });
			}

			foreach ((string id, Capsule capsule) in this.room.State.capsules) {
				if (!this.views.TryGetValue(id, out Transform view)) {
					view = GameObject.CreatePrimitive(PrimitiveType.Capsule).transform;
					this.views[id] = view;
				}
				Vector3 target = new Vector3((float)capsule.x, 1f, (float)capsule.z);
				view.position = Vector3.Lerp(view.position, target, SMOOTHING * Time.deltaTime);
			}

			foreach (string id in new List<string>(this.views.Keys)) {
				if (this.room.State.capsules.ContainsKey(id)) continue;
				Destroy(this.views[id].gameObject);
				this.views.Remove(id);
			}
		}

		void OnDestroy() {
			this.room?.Leave();
		}
	}
}
