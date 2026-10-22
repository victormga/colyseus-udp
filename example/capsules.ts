import { definePlugins, Room, Server, type Client } from "@colyseus/core";
import { schema, t, type SchemaType } from "@colyseus/schema";
import { WebSocketTransport } from "@colyseus/ws-transport";
import { UdpPlugin, UdpServer } from "../src/index.ts";

const Capsule = schema({
	x: t.number().unreliable(),
	z: t.number().unreliable(),
}, "Capsule");

const CapsulesState = schema({
	capsules: t.map(Capsule),
}, "CapsulesState");

export const udp = new UdpServer();

export class CapsulesRoom extends Room<{ state: SchemaType<typeof CapsulesState> }> {
	plugins = definePlugins([new UdpPlugin(udp)]);

	messages = {
		move: (client: Client, position: { x: number, z: number }) => {
			const capsule = this.state.capsules.get(client.sessionId)!;
			capsule.x = position.x;
			capsule.z = position.z;
		},
		echo: (client: Client, message: unknown) => {
			this.plugins.udp.send(client, "echo", message);
		},
	};

	onCreate() {
		this.state = new CapsulesState();
	}

	onJoin(client: Client) {
		this.state.capsules.set(client.sessionId, new Capsule());
	}

	onDrop(client: Client) {
		this.allowReconnection(client, 10);
	}

	onLeave(client: Client) {
		this.state.capsules.delete(client.sessionId);
	}
}

export async function start(port: number) {
	const server = new Server({ transport: new WebSocketTransport() });
	server.define("capsules", CapsulesRoom);
	await udp.listen(port);
	await server.listen(port);

	return async () => {
		await server.gracefullyShutdown(false);
		await udp.close();
	};
}
