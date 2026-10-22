# colyseus-udp

An unreliable UDP channel for [Colyseus](https://colyseus.io) 0.18 rooms, with a Unity client.

The WebSocket stays **the connection**: join, auth, reliable messages, reconnection and lifecycle are stock Colyseus. UDP is only a second way of talking to that same connection. It carries `@unreliable` state patches and unreliable messages. Colyseus encodes every byte; this project only moves frames. No fork of Colyseus or of the Unity SDK.

| Folder | What |
|---|---|
| root (`src/`, `example/`, `test/`) | npm package `colyseus-udp`: `UdpServer` (one UDP socket per process) and `UdpPlugin` (a Colyseus `RoomPlugin`) |
| `unity/` | Unity package `com.victormga.colyseus-udp`: `UdpChannel` |

## How it works

```
Unity                                  Colyseus process
  |-- join (WebSocket) ------------------>|
  |-- request "__udp" (WebSocket) ------->|  UdpPlugin issues a per-socket token
  |<-- { token, port, host? } ------------|
  |-- HELLO [0x01][token] (UDP) --------->|  binds the sender's ip:port to that client
  |<-- ACK   [0x02] ----------------------|
  |<== DATA  [0x03][Colyseus frame] ======|  both ways, frames untouched
  |-- HELLO every 5s (keepalive, re-binds if the router changes the port)
```

- **Lifetime follows the WebSocket.** When the socket closes, the server forgets the address and the client stops receiving. After a reconnect, the Unity side requests a new token automatically.
- **Senders are identified by ip:port**, so DATA packets carry no id. The token is only good for this UDP channel and can't be used against the WebSocket or HTTP.
- **No encryption, no fallback, no ordering.** If UDP is blocked, `UdpChannel.Connect` throws `TimeoutException` and the game decides what to do. Colyseus already stamps a sequence number on `@unreliable` patches, and the client drops stale ones.
- **Server datagrams over `maxDatagramSize` (default 1200 bytes, header included) are dropped**, never fragmented, with one warning per client. Losing one IP fragment loses the whole datagram, and some networks drop fragments outright. `UdpChannel.Send` doesn't check sizes, so keep client messages small too.

## Server

Install by git URL in `package.json`. Bun runs `src/` directly; npm builds `build/` on install:

```json
"colyseus-udp": "github:victormga/colyseus-udp#v0.1.2"
```

```ts
import { definePlugins, Room } from "@colyseus/core";
import { UdpPlugin, UdpServer } from "colyseus-udp";

export const udp = new UdpServer();   // one per process
await udp.listen(2567);               // UDP port; can match the WebSocket port

class GameRoom extends Room {
	plugins = definePlugins([new UdpPlugin(udp)]);

	messages = {
		// arrives over UDP when the client sends it with UdpChannel.Send
		move: (client, position) => { /* ... */ },
		// answered over UDP, whichever way the ping came in
		ping: (client, message) => this.plugins.udp.send(client, "pong", message),
	};

	onCreate() {
		this.setSimulationInterval(() => {
			this.plugins.udp.broadcast("wind", { x: 1, z: 2 });   // unreliable message to every open channel
		});
	}
}
```

State fields marked `.unreliable()` (or `@unreliable`) go over UDP automatically, to every client whose channel is open.

Options: `host` (announced to clients when the room's process is reachable at a different address than the WebSocket), `maxDatagramSize`, `onOversize` (replaces the default once-per-client warning). `udp.socket` is the underlying dgram socket, e.g. to tune OS buffer sizes.

## Unity

Install both packages by git URL in `Packages/manifest.json`:

```json
"io.colyseus.sdk": "https://github.com/colyseus/colyseus-unity-sdk.git?path=Assets/Colyseus#0.18.8",
"com.victormga.colyseus-udp": "https://github.com/victormga/colyseus-udp.git?path=unity#v0.1.2"
```

```csharp
Room<GameState> room = await client.JoinOrCreate<GameState>("game");
UdpChannel udp = await UdpChannel.Connect(room, host);   // main thread; host is what the Colyseus client connects to

udp.Send("move", new Move { x = 1, z = 2 });             // dropped while !udp.IsOpen
udp.OnClose += () => { /* WebSocket dropped or room left */ };
udp.OnOpen += () => { /* open again after a reconnect */ };
```

`Connect` throws `TimeoutException` when the server never answers over UDP (UDP is blocked), `SocketException` when the host doesn't resolve or can't be reached, and `OperationCanceledException` when the room is left after the ticket arrived but before the channel opened. Leaving while the ticket is still being requested surfaces `Room.Request`'s own error.

Incoming UDP frames go through `room.Connection.Dispatch`, so `room.OnMessage` handlers and state callbacks work as usual. The Capsules sample (Package Manager → Samples) runs against the example server (`npm run example`).

## Things to know

- **A field's first value is reliable.** It rides on its object's ADD. Only writes made after that ADD has been flushed go over UDP.
- **Core may log "no connected client's transport provides an unreliable channel"** once per room. That happens when an `@unreliable` field changes before any client has sent its HELLO. It's harmless, despite suggesting `@colyseus/h3-transport`.
- **Colyseus internals in use:** `client.rawUnreliable`, `client.ref` (`emit("message")`, `once("close")`), `getMessageBytes` on the server, and `Connection.Dispatch` on the client. These are typed and public, but `rawUnreliable` sits next to `raw`/`enqueueRaw`, which core marks "move to ClientPrivate". Pin versions; the tests fail if any of them moves.
- **Unity duplicates two protocol details:** the unreliable patch header `[0x0F|0x40][uint16 LE seq]` and the message frame layout. Both are covered by `unity/Tests~`.
- **`Room.Input(mode: "unreliable")` is not supported.** The Unity SDK throws before anything can intercept it, so send inputs as your own messages with `UdpChannel.Send`. The server already accepts unreliable input over UDP. Enabling it on Unity would need an SDK change: a `TransmitUnreliable` hook on `Connection`, and `Room.Input` accepting the mode when a channel exists.
- **IPv6:** give game-server hostnames an A record only, and use hostnames, not IPv4 literals, for the server's `host` option and the Colyseus client's endpoint. The server listens on IPv4 only; the client picks IPv4 when the host has it and IPv6 only when that's the sole option. On IPv6-only mobile networks, DNS64 maps the hostname to an IPv6 address that NAT64 routes to the server, which an AAAA record would bypass. Native IPv6 would need a second, IPv6-only server socket, because a dual-stack one silently drops IPv4 on hosts that default to IPv6-only.

## Development

```sh
npm install && npm test                      # also: bun test
cd unity/Tests~ && dotnet test               # .NET 8; starts the example server itself
npm run example                              # for the Unity sample
```
