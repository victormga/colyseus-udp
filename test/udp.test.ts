import assert from "node:assert/strict";
import { createSocket, type Socket } from "node:dgram";
import { EventEmitter } from "node:events";
import { after, before, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { getMessageBytes, matchMaker, Protocol, ProtocolModifier, type Client as ServerClient } from "@colyseus/core";
import { Client, type Room } from "@colyseus/sdk";
import { TICKET, UdpServer, type UdpTicket } from "../src/index.ts";
import { start, udp as udpServer, type CapsulesRoom } from "../example/capsules.ts";

const PORT = 2599;
const SPARE_PORT = 2598;
const HELLO = 0x01;
const ACK = 0x02;
const DATA = 0x03;
const UNRELIABLE_PATCH = Protocol.ROOM_STATE_PATCH | ProtocolModifier.UNRELIABLE;

let stop: () => Promise<void>;
before(async () => { stop = await start(PORT); });
after(() => stop());

async function openSocket(port = PORT) {
	const socket = createSocket("udp4");
	await new Promise<void>((resolve) => socket.connect(port, "127.0.0.1", resolve));
	return socket;
}

function dataFrame(type: string, message: unknown) {
	return getMessageBytes.raw(Protocol.ROOM_DATA, type, message);
}

function helloPacket(token: string) {
	return Buffer.concat([Buffer.of(HELLO), Buffer.from(token, "latin1")]);
}

function sendData(socket: Socket, frame: Uint8Array) {
	socket.send(Buffer.concat([Buffer.of(DATA), frame]));
}

function isAck(packet: Buffer) {
	return packet[0] === ACK;
}

function isData(code: number) {
	return (packet: Buffer) => packet[0] === DATA && packet[1] === code;
}

function receive(socket: Socket, match: (packet: Buffer) => boolean, timeoutMs = 1000) {
	return new Promise<Buffer | undefined>((resolve) => {
		const finish = (packet: Buffer | undefined) => {
			clearTimeout(timer);
			socket.off("message", onMessage);
			resolve(packet);
		};
		const onMessage = (packet: Buffer) => {
			if (match(packet)) finish(packet);
		};
		const timer = setTimeout(() => finish(undefined), timeoutMs);

		socket.on("message", onMessage);
	});
}

async function hello(socket: Socket, token: string) {
	const ack = receive(socket, isAck, 300);
	socket.send(helloPacket(token));
	return (await ack) !== undefined;
}

async function connect(shared?: Socket) {
	const room = await new Client(`ws://127.0.0.1:${PORT}`).joinOrCreate("capsules");
	const ticket: UdpTicket = await room.request(TICKET);
	const socket = shared ?? await openSocket();
	assert.ok(await hello(socket, ticket.token), "HELLO was not acknowledged");

	return {
		room,
		ticket,
		socket,
		close: async () => {
			if (shared === undefined) socket.close();
			await room.leave().catch(() => {});
		},
	};
}

function roomOnServer(roomId: string) {
	return matchMaker.getLocalRoomById(roomId) as CapsulesRoom;
}

function capsuleOf(room: Room) {
	return roomOnServer(room.roomId).state.capsules.get(room.sessionId)!;
}

async function dropAndReconnect(room: Room) {
	// an abrupt drop, so the SDK reconnects; it skips rooms joined less than minUptime ago
	room.reconnection.minUptime = 0;
	const reconnected = new Promise<void>((resolve) => room.onReconnect(resolve));
	const client = roomOnServer(room.roomId).clients.get(room.sessionId)!;
	(client.ref as unknown as { terminate(): void }).terminate();
	await reconnected;
}

async function until(condition: () => boolean) {
	for (let i = 0; i < 50; i++) {
		if (condition()) return true;
		await sleep(20);
	}
	return condition();
}

function fakeClient() {
	return { sessionId: "fake", ref: new EventEmitter() } as unknown as ServerClient;
}

test("unknown tokens are ignored", async (t) => {
	const socket = await openSocket();
	t.after(() => socket.close());

	assert.equal(await hello(socket, "0".repeat(32)), false);
});

test("a HELLO from source port 0 doesn't throw", async (t) => {
	const { ticket, close } = await connect();
	t.after(close);

	// a real socket can't send from port 0, but a crafted packet can
	const packet = helloPacket(ticket.token);
	assert.doesNotThrow(() => udpServer.socket.emit("message", packet, { address: "127.0.0.1", port: 0, family: "IPv4", size: packet.length }));
});

test("socket errors after listen() don't throw", () => {
	assert.doesNotThrow(() => udpServer.socket.emit("error", new Error("ECONNREFUSED")));
});

test("tickets fail with a clear error while the UdpServer isn't listening", () => {
	assert.throws(() => new UdpServer().ticket(fakeClient()), /isn't listening/);
});

test("closing the UdpServer detaches its clients", async (t) => {
	const server = new UdpServer();
	await server.listen(SPARE_PORT);
	const socket = await openSocket(SPARE_PORT);
	t.after(() => socket.close());
	const client = fakeClient();

	assert.ok(await hello(socket, server.ticket(client).token));
	assert.equal(typeof client.rawUnreliable, "function");

	await server.close();
	assert.equal(client.rawUnreliable, undefined);
});

test("datagrams from a bound address reach the room's message handlers", async (t) => {
	const { room, socket, close } = await connect();
	t.after(close);

	sendData(socket, dataFrame("move", { x: 3, z: 4 }));

	assert.ok(await until(() => capsuleOf(room).x === 3));
	assert.equal(capsuleOf(room).z, 4);
});

test("@unreliable fields are patched over UDP", async (t) => {
	const { socket, close } = await connect();
	t.after(close);

	// writes made before the capsule's ADD is flushed ride on that (reliable) ADD instead
	await sleep(100);

	const patch = receive(socket, isData(UNRELIABLE_PATCH));
	sendData(socket, dataFrame("move", { x: 1, z: 2 }));
	assert.ok(await patch, "no unreliable patch arrived");
});

test("unreliable messages from the room arrive over UDP untouched", async (t) => {
	const { room, socket, close } = await connect();
	t.after(close);

	const echo = receive(socket, isData(Protocol.ROOM_DATA));
	room.send("echo", { n: 1 });
	assert.deepEqual((await echo)?.subarray(1), dataFrame("echo", { n: 1 }));
});

test("a new address presenting the token takes over the channel", async (t) => {
	const { room, ticket, socket: old, close } = await connect();
	const moved = await openSocket();
	t.after(async () => { moved.close(); await close(); });

	assert.ok(await hello(moved, ticket.token));
	const echo = receive(moved, isData(Protocol.ROOM_DATA));
	room.send("echo", { n: 2 });
	assert.ok(await echo, "data still goes to the old address");

	sendData(moved, dataFrame("move", { x: 6, z: 0 }));
	assert.ok(await until(() => capsuleOf(room).x === 6));

	sendData(old, dataFrame("move", { x: 5, z: 0 }));
	await sleep(100);
	assert.equal(capsuleOf(room).x, 6, "the old address is still accepted");
});

test("two clients on one address: the one that leaves doesn't take the other's binding with it", async (t) => {
	const first = await connect();
	const second = await connect(first.socket);
	t.after(async () => { await second.close(); first.socket.close(); });

	await first.room.leave();
	await sleep(50);

	assert.ok(await hello(first.socket, second.ticket.token));
	sendData(first.socket, dataFrame("move", { x: 7, z: 0 }));
	assert.ok(await until(() => capsuleOf(second.room).x === 7), "the remaining client's upstream is dead");
});

test("closing the WebSocket detaches the channel", async (t) => {
	const { room, ticket, socket } = await connect();
	t.after(() => socket.close());
	const client = roomOnServer(room.roomId).clients.get(room.sessionId)!;

	await room.leave();
	await sleep(50);

	assert.equal(client.rawUnreliable, undefined);
	assert.equal(await hello(socket, ticket.token), false);
});

test("a reconnect needs a new ticket: the old token dies with its socket", async (t) => {
	const { room, ticket, socket, close } = await connect();
	t.after(close);

	await dropAndReconnect(room);

	assert.equal(await hello(socket, ticket.token), false);

	const renewed: UdpTicket = await room.request(TICKET);
	assert.ok(await hello(socket, renewed.token));
});

test("after a reconnect, sends to the previous client object reach the new channel", async (t) => {
	const { room, socket, close } = await connect();
	t.after(close);
	const serverSide = roomOnServer(room.roomId);
	const previous = serverSide.clients.get(room.sessionId)!;

	await dropAndReconnect(room);
	const renewed: UdpTicket = await room.request(TICKET);
	assert.ok(await hello(socket, renewed.token));

	const echo = receive(socket, isData(Protocol.ROOM_DATA));
	serverSide.plugins.udp.send(previous, "echo", { n: 4 });
	assert.ok(await echo);
});

test("datagrams over maxDatagramSize are dropped, warning once per client", async (t) => {
	const { room, socket, close } = await connect();
	t.after(close);

	const warnings: unknown[] = [];
	const warn = console.warn;
	console.warn = (message: unknown) => {
		if (String(message).startsWith("colyseus-udp")) warnings.push(message);
	};
	t.after(() => { console.warn = warn; });

	const echo = receive(socket, isData(Protocol.ROOM_DATA), 300);
	for (let i = 0; i < 3; i++) room.send("echo", { blob: "x".repeat(2000) });
	assert.equal(await echo, undefined);
	assert.equal(warnings.length, 1);
});
