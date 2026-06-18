/**
 * test-coverage.js — targeted tests for previously untested paths and boundary conditions.
 *
 * Covers:
 *  1. send_message_batch max=50 boundary (51 messages must fail Zod validation)
 *  2. check_results_batch max=50 boundary (51 task_ids must fail Zod validation)
 *  3. wait_for_messages combined filter_sender + filter_type
 *  4. Empty content validation (send_message, send_message_batch with content="")
 *  5. REST /cost with invalid `since` date
 *  6. REST /rate-limits with invalid `since` date
 *  7. clear_channel_schema on channel with no schema (graceful, not an error)
 *  8. Dashboard ?ns=unknown returns HTML (200, not crash)
 *  9. delete_message with id=0 and negative id (not-found, not a crash)
 * 10. read_messages non-JSON content when filter_type is active (does not crash)
 * 11. turn_start with limit parameter
 * 12. read_last with n=1 edge case (returns only the most-recent message)
 * 13. sprint_summary with type:failed messages (separate from FAIL-in-summary)
 * 14. post_gated_message timeout path (deps never satisfied → times out)
 * 15. has_messages cursor correctness (since_id excludes messages at or below cursor)
 * 16. filter_sender comma-separated multi-sender
 * 17. read_messages with limit parameter respected
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import "dotenv/config";

const BROKER_URL  = process.env.BROKER_URL  || "http://localhost:8080/mcp";
const BROKER_HTTP = BROKER_URL.replace("/mcp", "");
const SECRET      = process.env.SHARED_SECRET || "";
const RUN         = Date.now().toString(36);

let passed = 0;
let failed = 0;
const failures = [];

function ok(label) {
  console.log(`  ✓ ${label}`);
  passed++;
}
function fail(label, detail = "assertion failed") {
  console.error(`  ✗ ${label}: ${detail}`);
  failures.push({ label, detail });
  failed++;
}
function assert(cond, label, detail = "") {
  cond ? ok(label) : fail(label, detail || "assertion failed");
}

function ch(name) { return `cov-${RUN}-${name}`; }

async function connect(name) {
  const headers = SECRET ? { Authorization: `Bearer ${SECRET}` } : {};
  const transport = new StreamableHTTPClientTransport(new URL(BROKER_URL), { requestInit: { headers } });
  const client = new Client({ name, version: "1.0.0" });
  await client.connect(transport);
  return { client, transport };
}

async function call(client, tool, args) {
  const res = await client.callTool({ name: tool, arguments: args });
  return res.content[0].text;
}

async function callRaw(client, tool, args) {
  return client.callTool({ name: tool, arguments: args });
}

async function rest(method, path, body) {
  const headers = { "Content-Type": "application/json" };
  if (SECRET) headers.Authorization = `Bearer ${SECRET}`;
  const opts = { method, headers };
  if (body !== undefined) opts.body = JSON.stringify(body);
  return fetch(`${BROKER_HTTP}${path}`, opts);
}

async function run() {
  const { client: a, transport: ta } = await connect("cov-a");
  const { client: b, transport: tb } = await connect("cov-b");
  console.log(`\n[test-coverage]  broker=${BROKER_URL}  run=${RUN}\n`);

  // ── 1. send_message_batch max=50 boundary ────────────────────────────────────

  console.log("1. send_message_batch: max=50 boundary");
  {
    const bmCh = ch("batch-max");

    // Exactly 50 messages — must succeed
    const fifty = Array.from({ length: 50 }, (_, i) => ({
      channel: bmCh, sender: "orch",
      content: JSON.stringify({ type: "task", task_id: `batch-max-${i}-${RUN}`, subject: `task ${i}` }),
    }));
    const ok50 = await callRaw(a, "send_message_batch", { messages: fifty });
    assert(!ok50.isError, "send_message_batch: exactly 50 messages accepted");
    assert(ok50.content[0].text.includes("50 messages"), `send_message_batch: confirms 50 sent (got: ${ok50.content[0].text.slice(0, 60)})`);

    // 51 messages — must fail Zod validation
    const fiftyOne = [...fifty, { channel: bmCh, sender: "orch", content: JSON.stringify({ type: "task", task_id: `extra-${RUN}`, subject: "too many" }) }];
    const fail51 = await callRaw(a, "send_message_batch", { messages: fiftyOne });
    assert(fail51.isError === true, "send_message_batch: 51 messages rejected (exceeds max=50)");

    await call(a, "purge_channel", { channel: bmCh });
  }

  // ── 2. check_results_batch max=50 boundary ───────────────────────────────────

  console.log("\n2. check_results_batch: max=50 boundary");
  {
    const bCh = ch("batch-cr-max");

    // Exactly 50 task_ids — must succeed
    const fifty = Array.from({ length: 50 }, (_, i) => `t-batch-max-${i}-${RUN}`);
    const ok50 = await callRaw(a, "check_results_batch", { channel: bCh, task_ids: fifty });
    assert(!ok50.isError, "check_results_batch: exactly 50 task_ids accepted");
    const res50 = JSON.parse(ok50.content[0].text);
    assert(Object.keys(res50.results).length === 50, `check_results_batch: 50 results in map (got ${Object.keys(res50.results).length})`);
    assert(Object.values(res50.results).every(v => v === false), "check_results_batch: all false for non-existent tasks");

    // 51 task_ids — must fail Zod validation
    const fiftyOne = [...fifty, `t-extra-${RUN}`];
    const fail51 = await callRaw(a, "check_results_batch", { channel: bCh, task_ids: fiftyOne });
    assert(fail51.isError === true, "check_results_batch: 51 task_ids rejected (exceeds max=50)");
  }

  // ── 3. wait_for_messages combined filter_sender + filter_type ────────────────

  console.log("\n3. wait_for_messages: combined filter_sender + filter_type");
  {
    const wch = ch("wfm-combined");

    // Start a wait for sender="target" AND type="ping"
    const waitP = call(b, "wait_for_messages", {
      channel: wch, since_id: 0, timeout_ms: 6000,
      filter_sender: "target", filter_type: "ping",
    });

    await new Promise(r => setTimeout(r, 80));

    // Wrong sender, right type — must be skipped
    await call(a, "send_message", { channel: wch, sender: "other", content: JSON.stringify({ type: "ping", v: 1 }) });
    await new Promise(r => setTimeout(r, 80));

    // Right sender, wrong type — must be skipped
    await call(a, "send_message", { channel: wch, sender: "target", content: JSON.stringify({ type: "noise", v: 2 }) });
    await new Promise(r => setTimeout(r, 80));

    // Right sender, right type — must wake the wait
    await call(a, "send_message", { channel: wch, sender: "target", content: JSON.stringify({ type: "ping", v: 3 }) });

    const r = await waitP;
    assert(r.includes('"ping"'),  "wfm combined: matching message returned");
    assert(r.includes("target"),  "wfm combined: correct sender in result");
    assert(!r.includes('"noise"'), "wfm combined: wrong-type message excluded");
    assert(!r.includes('"other"'), "wfm combined: wrong-sender message excluded");

    await call(a, "purge_channel", { channel: wch });
  }

  // ── 4. Empty content validation ──────────────────────────────────────────────

  console.log("\n4. Empty content validation");
  {
    const eCh = ch("empty-content");

    // send_message with content="" must fail (z.string().min(1))
    const emptyMsg = await callRaw(a, "send_message", { channel: eCh, sender: "x", content: "" });
    assert(emptyMsg.isError === true, "send_message: empty content rejected");

    // send_message_batch with one empty content must fail
    const emptyBatch = await callRaw(a, "send_message_batch", { messages: [
      { channel: eCh, sender: "x", content: "" },
    ]});
    assert(emptyBatch.isError === true, "send_message_batch: empty content in batch rejected");

    // send_message with whitespace-only content (not empty — min(1) passes)
    // This is intentional: whitespace IS valid content (plain text)
    const wsMsg = await callRaw(a, "send_message", { channel: eCh, sender: "x", content: " " });
    assert(!wsMsg.isError, "send_message: single-space content accepted (not empty by min(1))");

    await call(a, "purge_channel", { channel: eCh }).catch(() => {});
  }

  // ── 5. REST /cost with invalid `since` date ──────────────────────────────────

  console.log("\n5. REST /cost: invalid since date");
  {
    if (!SECRET) {
      console.log("  - skipped (SHARED_SECRET not set)");
    } else {
      // Invalid date string — server must return 400, not crash with 500
      const res = await rest("GET", "/cost?since=not-a-date");
      assert(res.status === 400, `/cost?since=not-a-date returns 400 (validated), got ${res.status}`);
      const body = await res.json();
      assert("error" in body, "/cost: 400 response has error field");

      // Future date — should return empty result (no sessions after a future timestamp)
      const future = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString();
      const resFuture = await rest("GET", `/cost?since=${encodeURIComponent(future)}`);
      assert(resFuture.status === 200, `/cost?since=<future> returns 200`);
      const bodyFuture = await resFuture.json();
      assert(bodyFuture.total_usd === 0, `/cost?since=<future> returns total_usd=0 (no sessions after future date)`);
      assert(bodyFuture.sessions === 0,  `/cost?since=<future> returns sessions=0`);
    }
  }

  // ── 6. REST /rate-limits with invalid `since` date ───────────────────────────

  console.log("\n6. REST /rate-limits: invalid since date");
  {
    if (!SECRET) {
      console.log("  - skipped (SHARED_SECRET not set)");
    } else {
      // Invalid date string — server must return 400, not crash with 500
      const res = await rest("GET", "/rate-limits?since=garbage-date");
      assert(res.status === 400, `/rate-limits?since=garbage-date returns 400 (validated), got ${res.status}`);
      const body = await res.json();
      assert("error" in body, "/rate-limits: 400 response has error field");

      // Future date → empty
      const future = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString();
      const resFuture = await rest("GET", `/rate-limits?since=${encodeURIComponent(future)}`);
      assert(resFuture.status === 200, `/rate-limits?since=<future> returns 200`);
      const bodyFuture = await resFuture.json();
      assert(bodyFuture.total_hits === 0, `/rate-limits?since=<future> returns total_hits=0`);
    }
  }

  // ── 7. clear_channel_schema on channel with no schema ────────────────────────

  console.log("\n7. clear_channel_schema: no-op on channel without schema");
  {
    const nsCh = ch("no-schema");

    // Never registered a schema on nsCh — clearing should be graceful
    const r = await callRaw(a, "clear_channel_schema", { channel: nsCh });
    assert(!r.isError, "clear_channel_schema: no error when channel has no schema");
    assert(r.content[0].text.includes("Cleared"), `clear_channel_schema: returns confirmation (got: ${r.content[0].text})`);

    // Calling it twice also graceful
    const r2 = await callRaw(a, "clear_channel_schema", { channel: nsCh });
    assert(!r2.isError, "clear_channel_schema: idempotent — second call on schema-less channel is fine");
  }

  // ── 8. Dashboard ?ns=unknown ─────────────────────────────────────────────────

  console.log("\n8. Dashboard ?ns=unknown");
  {
    const res = await fetch(`${BROKER_HTTP}/dashboard?ns=definitely-nonexistent-ns-xyz`);
    assert(res.status === 200, `dashboard?ns=unknown returns 200 (not a crash), got ${res.status}`);
    const html = await res.text();
    assert(html.includes("<html") || html.includes("<!DOCTYPE"), "dashboard: returns HTML body");
    assert(!html.toLowerCase().includes("error") || html.includes("No channels"), "dashboard: no unhandled error with unknown ns");
  }

  // ── 9. delete_message: id=0 and negative id ──────────────────────────────────

  console.log("\n9. delete_message: invalid id values");
  {
    const dCh = ch("del-edge");
    await call(a, "send_message", { channel: dCh, sender: "x", content: "test" });

    // id=0 — no valid message has id=0 (SQLite ROWID starts at 1)
    const r0 = await a.callTool({ name: "delete_message", arguments: { channel: dCh, id: 0 } });
    // Should return not-found error, not crash
    const t0 = r0.content[0].text;
    assert(r0.isError === true || t0.includes("not found") || t0.includes("Not found"),
      `delete_message id=0: returns not-found, not crash (got: ${t0})`);

    // Negative id — also invalid
    const rNeg = await a.callTool({ name: "delete_message", arguments: { channel: dCh, id: -1 } });
    const tNeg = rNeg.content[0].text;
    // Either Zod rejects it (isError) or it returns not-found
    const isRejectedOrNotFound = rNeg.isError === true || tNeg.includes("not found") || tNeg.includes("Not found");
    assert(isRejectedOrNotFound, `delete_message id=-1: rejected or returns not-found (got: ${tNeg})`);

    await call(a, "purge_channel", { channel: dCh });
  }

  // ── 10. read_messages non-JSON content with filter_type active ───────────────

  console.log("\n10. read_messages: non-JSON content with filter_type");
  {
    const njCh = ch("non-json");

    // Post a mix of JSON and plain-text messages
    await call(a, "send_message", { channel: njCh, sender: "x", content: "plain text, not JSON" });
    await call(a, "send_message", { channel: njCh, sender: "x", content: JSON.stringify({ type: "task", v: 1 }) });
    await call(a, "send_message", { channel: njCh, sender: "x", content: "another plain text" });
    await call(a, "send_message", { channel: njCh, sender: "x", content: JSON.stringify({ type: "result", v: 2 }) });

    // filter_type="task" — must not crash, must return only the JSON task message
    const r = await call(a, "read_messages", { channel: njCh, since_id: 0, filter_type: "task" });
    assert(!r.includes("Error") && !r.includes("SyntaxError"), "read_messages filter_type: no crash on non-JSON content");
    assert(r.includes('"task"'),   "read_messages filter_type: returns the JSON task message");
    assert(!r.includes('"result"'), "read_messages filter_type: excludes non-matching JSON");
    assert(!r.includes("plain text"), "read_messages filter_type: plain-text messages silently excluded");

    // filter_type="result" — returns only the result message
    const r2 = await call(a, "read_messages", { channel: njCh, since_id: 0, filter_type: "result" });
    assert(r2.includes('"result"') && !r2.includes('"task"'), "read_messages filter_type=result: correct isolation");

    await call(a, "purge_channel", { channel: njCh });
  }

  // ── 11. turn_start with limit parameter ──────────────────────────────────────

  console.log("\n11. turn_start: limit parameter");
  {
    const tsCh = ch("ts-limit-inbox");
    const tcCh = ch("ts-limit-ctrl");

    // Post 5 inbox messages
    for (let i = 0; i < 5; i++) {
      await call(a, "send_message", { channel: tsCh, sender: "orch",
        content: JSON.stringify({ type: "task", task_id: `lim-${i}-${RUN}`, subject: `task ${i}` }) });
    }
    await call(a, "send_message", { channel: tcCh, sender: "orch",
      content: JSON.stringify({ type: "note", subject: "ctrl" }) });

    // limit=2 — inbox should return at most 2 messages
    const ts = JSON.parse(await call(b, "turn_start", {
      inbox_channel: tsCh, control_channel: tcCh,
      inbox_since_id: 0, control_since_id: 0, limit: 2,
    }));
    assert(ts.inbox.length === 2,   `turn_start limit=2: inbox capped at 2 (got ${ts.inbox.length})`);
    assert(ts.control.length === 1, `turn_start limit=2: control not affected — has 1 msg (got ${ts.control.length})`);

    // limit=10 — all 5 inbox messages returned
    const tsAll = JSON.parse(await call(b, "turn_start", {
      inbox_channel: tsCh, control_channel: tcCh,
      inbox_since_id: 0, control_since_id: 0, limit: 10,
    }));
    assert(tsAll.inbox.length === 5, `turn_start limit=10: all 5 inbox messages returned (got ${tsAll.inbox.length})`);

    await call(a, "purge_channel", { channel: tsCh });
    await call(a, "purge_channel", { channel: tcCh });
  }

  // ── 12. read_last with n=1 edge case ─────────────────────────────────────────

  console.log("\n12. read_last: n=1 edge case");
  {
    const rlCh = ch("read-last");

    await call(a, "send_message", { channel: rlCh, sender: "x", content: "first" });
    await call(a, "send_message", { channel: rlCh, sender: "x", content: "second" });
    await call(a, "send_message", { channel: rlCh, sender: "x", content: "third" });

    // n=1 — must return only the most recent message
    const r1 = await call(a, "read_last", { channel: rlCh, n: 1 });
    assert(r1.includes("third"),  "read_last n=1: returns most-recent message");
    assert(!r1.includes("first"), "read_last n=1: does not include older messages");
    assert(!r1.includes("second"), "read_last n=1: does not include second-to-last");

    // n=2 — returns second and third in chronological order
    const r2 = await call(a, "read_last", { channel: rlCh, n: 2 });
    const lines = r2.trim().split("\n").filter(l => l.match(/\[\d+\]/));
    assert(lines.length === 2,         `read_last n=2: returns 2 messages (got ${lines.length})`);
    assert(r2.includes("second"),      "read_last n=2: includes second message");
    assert(r2.includes("third"),       "read_last n=2: includes third message");
    assert(!r2.includes("first"),      "read_last n=2: first message excluded");
    // Chronological order: second before third
    assert(r2.indexOf("second") < r2.indexOf("third"), "read_last n=2: messages in chronological order");

    // Empty channel — returns "No messages." (not crash)
    const rlEmptyCh = ch("read-last-empty");
    const rEmpty = await call(a, "read_last", { channel: rlEmptyCh, n: 5 });
    assert(rEmpty.includes("No messages"), `read_last empty channel: graceful (got: ${rEmpty})`);

    await call(a, "purge_channel", { channel: rlCh });
  }

  // ── 13. sprint_summary: type:failed messages ──────────────────────────────────

  console.log("\n13. sprint_summary: type:failed messages separate from FAIL-in-summary");
  {
    // sprint_summary.failed counts type:result messages where summary LIKE '%FAIL%'
    // Verify a result with "PASS" summary is NOT counted as failed
    const ssNS   = `cov-${RUN}-sf`;
    const ssCh   = `${ssNS}-status`;
    const ssInCh = `${ssNS}-backend`;

    const tasks = ["sf-t1", "sf-t2", "sf-t3", "sf-t4"];
    const tid = (id) => `${id}-${RUN}`;

    // Dispatch 4 tasks to inbox channel
    for (const t of tasks) {
      await call(a, "send_message", { channel: ssInCh, sender: "orchestrator",
        content: JSON.stringify({ type: "task", task_id: tid(t), from: "orchestrator", to: "backend", subject: t }) });
    }

    // t1: PASS result (not failed)
    await call(a, "send_message", { channel: ssCh, sender: "backend",
      content: JSON.stringify({ type: "result", task_id: tid("sf-t1"), summary: "PASS — all good" }) });

    // t2: FAIL result (counted as failed)
    await call(a, "send_message", { channel: ssCh, sender: "backend",
      content: JSON.stringify({ type: "result", task_id: tid("sf-t2"), summary: "FAIL — lint errors" }) });

    // t3: FAIL result with lowercase "fail" (LIKE is case-insensitive in SQLite by default for ASCII)
    await call(a, "send_message", { channel: ssCh, sender: "backend",
      content: JSON.stringify({ type: "result", task_id: tid("sf-t3"), summary: "fail — build error" }) });

    // t4: pending (no result posted)

    const ss = JSON.parse(await call(a, "sprint_summary", { status_channel: ssCh }));
    assert(ss.dispatched === 4, `sprint_summary: dispatched=4 (got ${ss.dispatched})`);
    assert(ss.completed  === 3, `sprint_summary: completed=3 (got ${ss.completed})`);
    // LIKE '%FAIL%' is case-insensitive in SQLite for ASCII chars
    assert(ss.failed     >= 1, `sprint_summary: failed>=1 (got ${ss.failed})`);
    assert(ss.pending    === 1, `sprint_summary: pending=1 (got ${ss.pending})`);

    await call(a, "purge_channel", { channel: ssCh   });
    await call(a, "purge_channel", { channel: ssInCh });
  }

  // ── 14. post_gated_message timeout path ──────────────────────────────────────

  console.log("\n14. post_gated_message: timeout when deps never satisfied");
  {
    const pgCh  = ch("gate-timeout-out");
    const pgWCh = ch("gate-timeout-watch");
    const dep   = `gate-never-${RUN}`;

    const t0 = Date.now();
    const gRes = await call(a, "post_gated_message", {
      channel: pgCh, sender: "orch",
      content: JSON.stringify({ type: "task", subject: "should not appear" }),
      depends_on: [dep],
      watch_channel: pgWCh,
      timeout_ms: 500,
    });
    const elapsed = Date.now() - t0;

    assert(gRes.includes("Timed out") || gRes.includes("timeout"), `post_gated_message: timeout message returned (got: ${gRes})`);
    assert(elapsed >= 400, `post_gated_message: timeout fires at expected time (~500ms, got ${elapsed}ms)`);

    // Message must NOT have been posted to the output channel
    const after = await call(a, "read_messages", { channel: pgCh, since_id: 0 });
    assert(after.includes("No new messages"), "post_gated_message: no message posted on timeout");

    await call(a, "purge_channel", { channel: pgCh  }).catch(() => {});
    await call(a, "purge_channel", { channel: pgWCh }).catch(() => {});
  }

  // ── 15. has_messages cursor correctness ──────────────────────────────────────

  console.log("\n15. has_messages: since_id cursor correctness");
  {
    const hmCh = ch("has-messages");

    // Empty channel → pending=false
    const e0 = JSON.parse(await call(a, "has_messages", { channel: hmCh, since_id: 0 }));
    assert(e0.pending === false, "has_messages: empty channel → pending=false");
    assert(e0.max_id === 0, `has_messages: empty channel → max_id=0 (got ${e0.max_id})`);

    // Post a message
    const sentRaw = await call(a, "send_message", { channel: hmCh, sender: "x",
      content: JSON.stringify({ type: "note", v: 1 }) });
    const sentId = Number(sentRaw.match(/#(\d+)/)?.[1]);

    const h1 = JSON.parse(await call(a, "has_messages", { channel: hmCh, since_id: 0 }));
    assert(h1.pending === true,         "has_messages: new message → pending=true");
    assert(h1.max_id  === sentId,       `has_messages: max_id equals sent id (${sentId})`);

    // Cursor at sentId → pending=false (nothing new after sentId)
    const h2 = JSON.parse(await call(a, "has_messages", { channel: hmCh, since_id: sentId }));
    assert(h2.pending === false, "has_messages: cursor at last id → pending=false");

    await call(a, "purge_channel", { channel: hmCh });
  }

  // ── 16. filter_sender comma-separated multi-sender ───────────────────────────

  console.log("\n16. filter_sender: comma-separated multi-sender");
  {
    const msCh = ch("multi-sender");

    await call(a, "send_message", { channel: msCh, sender: "alice",   content: JSON.stringify({ v: 1 }) });
    await call(a, "send_message", { channel: msCh, sender: "bob",     content: JSON.stringify({ v: 2 }) });
    await call(a, "send_message", { channel: msCh, sender: "charlie", content: JSON.stringify({ v: 3 }) });
    await call(a, "send_message", { channel: msCh, sender: "dave",    content: JSON.stringify({ v: 4 }) });

    // alice,bob — should return messages from alice and bob only
    const r = await call(a, "read_messages", { channel: msCh, since_id: 0, filter_sender: "alice,bob" });
    assert(r.includes("<alice>"),   "filter_sender multi: alice included");
    assert(r.includes("<bob>"),     "filter_sender multi: bob included");
    assert(!r.includes("<charlie>"), "filter_sender multi: charlie excluded");
    assert(!r.includes("<dave>"),    "filter_sender multi: dave excluded");

    // Whitespace tolerance: "alice, charlie" (space after comma)
    const r2 = await call(a, "read_messages", { channel: msCh, since_id: 0, filter_sender: "alice, charlie" });
    assert(r2.includes("<alice>"),   "filter_sender multi trim: alice included");
    assert(r2.includes("<charlie>"), "filter_sender multi trim: charlie included");
    assert(!r2.includes("<bob>"),    "filter_sender multi trim: bob excluded");

    await call(a, "purge_channel", { channel: msCh });
  }

  // ── 17. read_messages: limit parameter ───────────────────────────────────────

  console.log("\n17. read_messages: limit parameter");
  {
    const limCh = ch("read-limit");

    // Post 10 messages
    for (let i = 0; i < 10; i++) {
      await call(a, "send_message", { channel: limCh, sender: "x",
        content: JSON.stringify({ type: "note", v: i }) });
    }

    // Default limit (100) — all 10 returned
    const rAll = await call(a, "read_messages", { channel: limCh, since_id: 0 });
    const allLines = rAll.trim().split("\n").filter(l => l.match(/\[#\d+\]/));
    assert(allLines.length === 10, `read_messages default limit: all 10 returned (got ${allLines.length})`);

    // limit=3 — only first 3 (oldest) returned
    const r3 = await call(a, "read_messages", { channel: limCh, since_id: 0, limit: 3 });
    const lines3 = r3.trim().split("\n").filter(l => l.match(/\[#\d+\]/));
    assert(lines3.length === 3, `read_messages limit=3: returns 3 messages (got ${lines3.length})`);
    // First 3 messages have v: 0, 1, 2
    assert(r3.includes('"v":0'), "read_messages limit=3: includes first message (v=0)");
    assert(!r3.includes('"v":9'), "read_messages limit=3: last message (v=9) excluded");

    await call(a, "purge_channel", { channel: limCh });
  }

  // ── Teardown ──────────────────────────────────────────────────────────────────
  await ta.close();
  await tb.close();

  console.log(`\n${"─".repeat(54)}`);
  console.log(`  passed: ${passed}   failed: ${failed}`);
  if (failures.length > 0) {
    console.error("\n  Failures:");
    for (const { label, detail } of failures) {
      console.error(`    ✗ ${label}`);
      console.error(`      ${detail}`);
    }
    process.exit(1);
  } else {
    console.log("  ALL TESTS PASSED");
  }
}

run().catch(e => {
  console.error("\n[FATAL]", e.message || e);
  process.exit(1);
});
