using Colyseus.Schema;

// Mirrors example/capsules.ts, in the shape schema-codegen emits.
namespace Colyseus.Udp.Samples {
	public class Capsule : Schema.Schema {
		[Preserve]
		public Capsule() { }

		[Type(0, "number")]
		public double x = default(double);

		[Type(1, "number")]
		public double z = default(double);
	}

	public class CapsulesState : Schema.Schema {
		[Preserve]
		public CapsulesState() { }

		[Type(0, "map", typeof(MapSchema<Capsule>))]
		public MapSchema<Capsule> capsules = new MapSchema<Capsule>();
	}
}
