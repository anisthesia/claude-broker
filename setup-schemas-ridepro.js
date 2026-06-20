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
//
// strict column: per-channel flip status as of 2026-06-20.
//   STRICT=1 env var overrides all to strict.
//   Channels held warn-only due to known violations:
//     rp-admin      — task_id uppercase P3, missing date in mod-P3-001; baseline_task_id extra prop; depends_on items fail pattern
//     rp-ios        — same task_id/depends_on pattern failures + baseline_task_id extra prop
//     rp-qa         — qa-phase3-verify-2026-06-20 missing suffix after date (fails pattern)
//     rp-backlog    — deferred-resolved uses promoted_sprint/promoted_at instead of required resolved_in_sprint/outcome
//     rp-status     — most type:result missing consent_basis; results with commits missing affected_files
//     rp-telemetry  — activity.state "idle-exit" not in allowed enum
const REGISTRATIONS = [
  { channel: "rp-api",          file: "schemas/rp-worker-inbox.json",       strict: true  },
  { channel: "rp-admin",        file: "schemas/rp-worker-inbox.json",       strict: false },
  { channel: "rp-web",          file: "schemas/rp-worker-inbox.json",       strict: true  },
  { channel: "rp-android",      file: "schemas/rp-worker-inbox.json",       strict: true  },
  { channel: "rp-ios",          file: "schemas/rp-worker-inbox.json",       strict: false },
  { channel: "rp-qa",           file: "schemas/rp-worker-inbox.json",       strict: false },
  { channel: "rp-orchestrator", file: "schemas/rp-orchestrator-inbox.json", strict: true  },
  { channel: "rp-status",       file: "schemas/rp-status.json",             strict: false },
  { channel: "rp-control",      file: "schemas/rp-control.json",            strict: true  },
  { channel: "rp-telemetry",    file: "schemas/rp-telemetry.json",          strict: false },
  { channel: "rp-backlog",      file: "schemas/rp-backlog.json",            strict: false },
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

  for (const { channel, file, strict: perChannelStrict } of REGISTRATIONS) {
    const schema = readFileSync(file, "utf-8");
    const strictMode = STRICT || perChannelStrict;
    const res = await client.callTool({
      name: "register_channel_schema",
      arguments: { channel, schema, strict: strictMode },
    });
    const text = res.content?.[0]?.text ?? "(no response)";
    console.log(`  ${channel.padEnd(18)} [${strictMode ? "strict" : "warn  "}] ← ${file}`);
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
