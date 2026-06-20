// Register cb-namespace channel schemas with a running broker.
// Usage:
//   node setup-schemas-broker.js              # defaults: localhost:8080, warn-only
//   STRICT=1 node setup-schemas-broker.js     # register as strict (reject invalid)
//   BROKER_URL=... SHARED_SECRET=... node setup-schemas-broker.js
//
// Idempotent: re-running replaces the schema for each channel.

import { readFileSync } from "fs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import "dotenv/config";

const BROKER_URL = process.env.BROKER_URL || "http://localhost:8080/mcp";
const SECRET     = process.env.SHARED_SECRET || "";
const STRICT     = process.env.STRICT === "1";

const REGISTRATIONS = [
  { channel: "cb-core",         file: "schemas/cb-worker-inbox.json" },
  { channel: "cb-protocol-qa",  file: "schemas/cb-worker-inbox.json" },
  { channel: "cb-orchestrator", file: "schemas/cb-orchestrator-inbox.json" },
  { channel: "cb-control",      file: "schemas/cb-control.json" },
  { channel: "cb-status",       file: "schemas/cb-status.json" },
  { channel: "cb-telemetry",    file: "schemas/cb-telemetry.json" },
  { channel: "cb-backlog",      file: "schemas/cb-backlog.json" },
];

async function main() {
  const transport = new StreamableHTTPClientTransport(new URL(BROKER_URL), {
    requestInit: SECRET ? { headers: { Authorization: `Bearer ${SECRET}` } } : {},
  });
  const client = new Client({ name: "setup-schemas-broker", version: "1.0.0" });
  await client.connect(transport);

  console.log(`[setup] broker: ${BROKER_URL}`);
  console.log(`[setup] mode:   ${STRICT ? "STRICT (reject invalid)" : "warn-only (log but allow)"}`);
  console.log();

  for (const { channel, file } of REGISTRATIONS) {
    const schema = readFileSync(file, "utf-8");
    const res = await client.callTool({
      name: "register_channel_schema",
      arguments: { channel, schema, strict: STRICT, version: "1.0" },
    });
    const text = res.content?.[0]?.text ?? JSON.stringify(res);
    const ok = !res.isError;
    console.log(`  ${ok ? "✓" : "✗"} ${channel.padEnd(20)} ← ${file}`);
    if (!ok) console.log(`      ERROR: ${text}`);
  }

  console.log();
  console.log("[setup] done.");
  await client.close();
}

main().catch((err) => { console.error(err); process.exit(1); });
