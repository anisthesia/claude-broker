/**
 * Tests for GET /inbox endpoint — the on-demand watchdog pre-check.
 *
 * Sends messages via the MCP send_message tool, then verifies the REST
 * /inbox endpoint returns the correct pending/count/max_id response.
 *
 * Usage:
 *   node test-inbox.js
 *   BROKER_URL=http://localhost:8080 node test-inbox.js
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

import { readFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Load .env from broker directory so SECRET is available without manual export
function loadDotEnv(file) {
  try {
    return Object.fromEntries(
      readFileSync(file, "utf8").split("\n")
        .filter(l => l.includes("=") && !l.startsWith("#"))
        .map(l => { const i = l.indexOf("="); return [l.slice(0, i).trim(), l.slice(i + 1).trim()]; })
    );
  } catch { return {}; }
}
const env = loadDotEnv(resolve(__dirname, ".env"));

const BROKER_BASE = process.env.BROKER_URL  || env.BROKER_URL  || "http://localhost:8080";
const MCP_URL     = `${BROKER_BASE}/mcp`;
const SECRET      = process.env.SHARED_SECRET || env.SHARED_SECRET || "";

let passed = 0, failed = 0;

function ok(label) {
  console.log(`  ✓ ${label}`);
  passed++;
}
function fail(label, detail) {
  console.error(`  ✗ ${label}: ${detail}`);
  failed++;
}
function assert(cond, label, detail = "") {
  cond ? ok(label) : fail(label, detail || "assertion failed");
}

async function connect() {
  const headers = SECRET ? { Authorization: `Bearer ${SECRET}` } : {};
  const transport = new StreamableHTTPClientTransport(new URL(MCP_URL), { requestInit: { headers } });
  const client = new Client({ name: "inbox-tester", version: "1.0.0" });
  await client.connect(transport);
  return { client, transport };
}

async function mcpCall(client, tool, args) {
  const res = await client.callTool({ name: tool, arguments: args });
  return res.content[0].text;
}

async function inbox(channel, since_id = 0) {
  const url = `${BROKER_BASE}/inbox?channel=${encodeURIComponent(channel)}&since_id=${since_id}`;
  const r = await fetch(url);
  return { status: r.status, body: await r.json() };
}

async function send(client, channel, payload) {
  return mcpCall(client, "send_message", {
    channel,
    sender: "inbox-test",
    content: JSON.stringify(payload),
  });
}

async function run() {
  const ts   = Date.now();
  const ch   = `test-inbox-${ts}`;
  const ch2  = `test-inbox-other-${ts}`;

  console.log(`\n[broker /inbox tests]  url=${BROKER_BASE}  channel=${ch}\n`);

  const { client, transport } = await connect();

  // ── 1. Empty channel ──────────────────────────────────────────────────────
  console.log("1. Empty channel");
  {
    const { status, body } = await inbox(ch);
    assert(status === 200, "HTTP 200");
    assert(body.pending === false, "pending=false on empty channel");
    assert(body.count === 0, `count=0 (got ${body.count})`);
    assert(body.max_id === 0, `max_id=0 (got ${body.max_id})`);
    assert(body.channel === ch, "channel echoed back");
    assert(body.since_id === 0, "since_id echoed back");
  }

  // ── 2. One message → pending ──────────────────────────────────────────────
  console.log("\n2. Single message");
  {
    await send(client, ch, { type: "task", task_id: "T-1", body: "first task" });
    const { body } = await inbox(ch);
    assert(body.pending === true, "pending=true after 1 message");
    assert(body.count === 1, `count=1 (got ${body.count})`);
    assert(body.max_id > 0, `max_id=${body.max_id} > 0`);
  }

  // ── 3. Cursor at max_id → no more pending ─────────────────────────────────
  console.log("\n3. Cursor at max_id");
  {
    const first = (await inbox(ch)).body;
    const { body } = await inbox(ch, first.max_id);
    assert(body.pending === false, `since_id=${first.max_id}: pending=false (cursor caught up)`);
    assert(body.count === 0, "count=0 at cursor");
    assert(body.max_id === 0, "max_id=0 when no messages after cursor");
  }

  // ── 4. New message after cursor → pending again ───────────────────────────
  console.log("\n4. New message after cursor");
  {
    const before = (await inbox(ch)).body;
    await send(client, ch, { type: "result", task_id: "T-1", body: "done" });
    const after = await inbox(ch, before.max_id);
    assert(after.body.pending === true, "pending=true after new message past cursor");
    assert(after.body.count === 1, "count=1 (only new message counted past cursor)");
    assert(after.body.max_id > before.max_id, `max_id advanced: ${after.body.max_id} > ${before.max_id}`);
  }

  // ── 5. Multiple messages accumulate ──────────────────────────────────────
  console.log("\n5. Multiple messages");
  {
    const cursor = (await inbox(ch)).body.max_id;
    for (let i = 0; i < 5; i++) {
      await send(client, ch, { type: "note", i });
    }
    const { body } = await inbox(ch, cursor);
    assert(body.pending === true, "pending=true for 5 messages");
    assert(body.count === 5, `count=5 (got ${body.count})`);
  }

  // ── 6. since_id=0 always counts everything ────────────────────────────────
  console.log("\n6. since_id=0 counts all messages");
  {
    const all = (await inbox(ch, 0)).body;
    assert(all.pending === true, "pending=true from beginning");
    assert(all.count >= 7, `all messages visible (got ${all.count}, expected >=7)`);
  }

  // ── 7. Channel isolation ──────────────────────────────────────────────────
  console.log("\n7. Channel isolation");
  {
    const before = (await inbox(ch2)).body;
    assert(before.pending === false, "ch2 starts empty");
    await send(client, ch2, { type: "task", body: "ch2 task" });
    const ch2after = (await inbox(ch2)).body;
    const ch1after = (await inbox(ch)).body;
    assert(ch2after.pending === true, "ch2 has its own pending message");
    assert(ch2after.max_id !== ch1after.max_id || ch2after.count !== ch1after.count,
      "ch1 and ch2 have independent state");
  }

  // ── 8. Error cases ────────────────────────────────────────────────────────
  console.log("\n8. Error cases");
  {
    // Missing channel
    const r1 = await fetch(`${BROKER_BASE}/inbox`);
    assert(r1.status === 400, "missing channel → HTTP 400");
    const b1 = await r1.json();
    assert(typeof b1.error === "string", "missing channel → {error} in body");

    // Non-numeric since_id
    const r2 = await fetch(`${BROKER_BASE}/inbox?channel=${ch}&since_id=notanumber`);
    assert(r2.status === 400, "non-numeric since_id → HTTP 400");
    const b2 = await r2.json();
    assert(typeof b2.error === "string", "non-numeric since_id → {error} in body");

    // since_id default omitted → same as 0
    const r3 = await fetch(`${BROKER_BASE}/inbox?channel=${ch}`);
    assert(r3.status === 200, "omitting since_id → HTTP 200 (defaults to 0)");
    const b3 = await r3.json();
    assert(b3.since_id === 0, "omitted since_id echoed as 0");
  }

  // ── 9. Large since_id (beyond any message) ────────────────────────────────
  console.log("\n9. since_id beyond all messages");
  {
    const { body } = await inbox(ch, 999_999_999);
    assert(body.pending === false, "since_id beyond all messages → pending=false");
    assert(body.count === 0, "count=0 for future cursor");
  }

  await transport.close();

  console.log(`\n── ${passed} passed, ${failed} failed ──\n`);
  if (failed > 0) process.exit(1);
}

run().catch(err => { console.error("Fatal:", err.message); process.exit(1); });
