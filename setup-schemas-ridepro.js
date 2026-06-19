// Register Ridepro channel schemas with a running broker.
// Usage:
//   node setup-schemas-ridepro.js              # defaults: localhost:8080, warn-only
//   STRICT=1 node setup-schemas-ridepro.js     # register as strict (reject invalid)
//   BROKER_URL=... SHARED_SECRET=... node setup-schemas-ridepro.js
//
// Idempotent: re-running replaces the schema for each channel.

import { readFileSync } from "fs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import "dotenv/config";

const BROKER_URL = process.env.BROKER_URL || "http://localhost:8080/mcp";
const SECRET     = process.env.SHARED_SECRET || "";
const STRICT     = process.env.STRICT === "1";

// 6 worker inboxes share the worker-inbox schema; orchestrator, status, control, telemetry, backlog are distinct
const REGISTRATIONS = [
  { channel: "rp-api",          file: "schemas/rp-worker-inbox.json" },
  { channel: "rp-admin",        file: "schemas/rp-worker-inbox.json" },
  { channel: "rp-web",          file: "schemas/rp-worker-inbox.json" },
  { channel: "rp-android",      file: "schemas/rp-worker-inbox.json" },
  { channel: "rp-ios",          file: "schemas/rp-worker-inbox.json" },
  { channel: "rp-qa",           file: "schemas/rp-worker-inbox.json" },
  { channel: "rp-orchestrator", file: "schemas/rp-orchestrator-inbox.json" },
  { channel: "rp-status",       file: "schemas/rp-status.json" },
  { channel: "rp-control",      file: "schemas/rp-control.json" },
  { channel: "rp-telemetry",    file: "schemas/rp-telemetry.json" },
  { channel: "rp-backlog",      file: "schemas/rp-backlog.json" },
];

async function main() {
  const transport = new StreamableHTTPClientTransport(new URL(BROKER_URL), {
    requestInit: SECRET ? { headers: { Authorization: `Bearer ${SECRET}` } } : {},
  });
  const client = new Client({ name: "setup-schemas-ridepro", version: "1.0.0" });
  await client.connect(transport);

  console.log(`[setup-ridepro] broker: ${BROKER_URL}`);
  console.log(`[setup-ridepro] mode:   ${STRICT ? "STRICT (reject invalid)" : "warn-only (log but allow)"}`);
  console.log();

  for (const { channel, file } of REGISTRATIONS) {
    const schema = readFileSync(file, "utf-8");
    const res = await client.callTool({
      name: "register_channel_schema",
      arguments: { channel, schema, strict: STRICT },
    });
    const text = res.content?.[0]?.text ?? "(no response)";
    console.log(`  ${channel.padEnd(18)} ← ${file}`);
    console.log(`    ${text}`);
  }

  console.log();
  const list = await client.callTool({ name: "list_channel_schemas", arguments: { prefix: "rp-" } });
  console.log("[setup-ridepro] registered rp-* schemas:");
  console.log(list.content[0].text);

  await transport.close();
  console.log("\n[setup-ridepro] done");
}

main().catch(e => { console.error("[setup-ridepro] FAIL:", e); process.exit(1); });
