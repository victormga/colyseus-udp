import { randomBytes } from "node:crypto";
import { createSocket, type RemoteInfo } from "node:dgram";
import { getMessageBytes, logger, Protocol, RoomPlugin, type Client } from "@colyseus/core";

export const TICKET = "__udp";

const HELLO = 0x01;
const ACK_PACKET = Buffer.of(0x02);
const DATA = 0x03;

export interface UdpTicket {
	token: string;
	port: number;
	host?: string;
}

export interface UdpServerOptions {
	/** Host announced in tickets. Omit it and clients reuse the host of their WebSocket. */
	host?: string;
	/** Default 1200, including the 1-byte header. Larger datagrams are dropped, never fragmented. */
	maxDatagramSize?: number;
	/** Replaces the default warning, which is logged once per client. */
	onOversize?: (client: Client, size: number) => void;
}

interface Session {
	client: Client;
	token: string;
	address?: string;
}

export class UdpServer {
	/** The underlying dgram socket, e.g. to tune OS buffer sizes. */
	readonly socket = createSocket("udp4");

	readonly #host?: string;
	readonly #maxDatagramSize: number;
	readonly #onOversize: (client: Client, size: number) => void;
	#port?: number;
	readonly #byClient = new Map<Client, Session>();
	readonly #byToken = new Map<string, Session>();
	readonly #byAddress = new Map<string, Session>();

	constructor({ host, maxDatagramSize = 1200, onOversize = warnOversize }: UdpServerOptions = {}) {
		this.#host = host;
		this.#maxDatagramSize = maxDatagramSize;
		this.#onOversize = onOversize;
		this.socket.on("message", (packet, remote) => this.#receive(packet, remote));
	}

	listen(port: number, hostname?: string) {
		return new Promise<void>((resolve, reject) => {
			this.socket.once("error", reject);
			this.socket.bind(port, hostname, () => {
				this.socket.off("error", reject);
				// an unhandled 'error' event would throw, and core turns that into a process exit
				this.socket.on("error", (error) => logger.warn(`colyseus-udp: ${error.message}`));
				this.#port = this.socket.address().port;
				resolve();
			});
		});
	}

	close() {
		for (const session of this.#byClient.values()) this.#closeSession(session);
		this.#port = undefined;
		return new Promise<void>((resolve) => this.socket.close(() => resolve()));
	}

	ticket(client: Client): UdpTicket {
		if (this.#port === undefined) throw new Error("colyseus-udp: ticket requested while UdpServer isn't listening");

		const session = this.#byClient.get(client) ?? this.#openSession(client);
		const ticket: UdpTicket = { token: session.token, port: this.#port };
		if (this.#host !== undefined) ticket.host = this.#host;
		return ticket;
	}

	#openSession(client: Client) {
		const session: Session = { client, token: randomBytes(16).toString("hex") };
		this.#byClient.set(client, session);
		this.#byToken.set(session.token, session);
		client.ref.once("close", () => this.#closeSession(session));
		return session;
	}

	#closeSession(session: Session) {
		this.#byClient.delete(session.client);
		this.#byToken.delete(session.token);
		this.#unbind(session);
	}

	#unbind(session: Session) {
		if (session.address !== undefined) this.#byAddress.delete(session.address);
		session.address = undefined;
		// not `delete`: core does the same to keep clients in V8 fast mode
		session.client.rawUnreliable = undefined;
	}

	#receive(packet: Buffer, remote: RemoteInfo) {
		// replying to port 0 throws synchronously
		if (remote.port === 0) return;

		const address = `${remote.address}:${remote.port}`;
		switch (packet[0]) {
			case DATA:
				this.#byAddress.get(address)?.client.ref.emit("message", packet.subarray(1));
				break;
			case HELLO:
				this.#hello(packet.toString("latin1", 1), address, remote);
				break;
		}
	}

	#hello(token: string, address: string, remote: RemoteInfo) {
		const session = this.#byToken.get(token);
		if (session === undefined) return;

		const owner = this.#byAddress.get(address);
		if (owner !== session) {
			if (owner !== undefined) this.#unbind(owner);
			this.#unbind(session);
			session.address = address;
			session.client.rawUnreliable = (data) => this.#transmit(session.client, data, remote);
			this.#byAddress.set(address, session);
		}

		this.socket.send(ACK_PACKET, remote.port, remote.address);
	}

	#transmit(client: Client, data: Uint8Array, remote: RemoteInfo) {
		const size = data.byteLength + 1;
		if (size > this.#maxDatagramSize) {
			this.#onOversize(client, size);
			return;
		}

		// copy: core reuses its encode buffers on the next patch, and dgram sends asynchronously
		const packet = Buffer.allocUnsafe(size);
		packet[0] = DATA;
		packet.set(data, 1);
		this.socket.send(packet, remote.port, remote.address);
	}
}

export class UdpPlugin extends RoomPlugin {
	readonly pluginName = "udp" as const;

	readonly #server: UdpServer;

	protected messages = {
		[TICKET]: (client: Client) => this.#server.ticket(client),
	};

	constructor(server: UdpServer) {
		super();
		this.#server = server;
	}

	send(client: Client, type: string | number, message?: unknown) {
		const frame = getMessageBytes.raw(Protocol.ROOM_DATA, type, message);
		// after a reconnect, core re-points a previous client's ref but not its rawUnreliable
		this.room.clients.get(client.sessionId)?.rawUnreliable?.(frame);
	}

	broadcast(type: string | number, message?: unknown) {
		const frame = getMessageBytes.raw(Protocol.ROOM_DATA, type, message);
		for (const client of this.room.clients) client.rawUnreliable?.(frame);
	}
}

const warnedOversize = new WeakSet<Client>();

function warnOversize(client: Client, size: number) {
	if (warnedOversize.has(client)) return;
	warnedOversize.add(client);
	logger.warn(`colyseus-udp: dropping datagrams over maxDatagramSize for '${client.sessionId}' (first one: ${size} bytes)`);
}
