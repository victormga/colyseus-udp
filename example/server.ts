import { start } from "./capsules.ts";

const port = Number(process.env.PORT ?? 2567);
await start(port);
// unity/Tests~ waits for "listening" on stdout
console.log(`capsules: ws + udp listening on :${port}`);
