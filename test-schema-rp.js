// Schema validation tests for rp-namespace channels.
// Exercises all rp-* channel schemas against a live broker.
// Safe to run multiple times — uses unique channel names per run.

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { readFileSync } from "fs";
import "dotenv/config";

const BROKER_URL = process.env.BROKER_URL || "http://localhost:8080/mcp";
const SECRET     = process.env.SHARED_SECRET || "";
const RUN_TAG    = Date.now().toString(36);

async function connect(name) {
  const transport = new StreamableHTTPClientTransport(new URL(BROKER_URL), {
    requestInit: SECRET ? { headers: { Authorization: `Bearer ${SECRET}` } } : {},
  });
  const client = new Client({ name, version: "1.0.0" });
  await client.connect(transport);
  return { client, transport };
}

let passed = 0;
let failed = 0;

function expect(cond, label, detail) {
  if (cond) {
    console.log(`  ✓ ${label}`);
    passed++;
    return true;
  }
  console.error(`  ✗ ${label}`);
  if (detail !== undefined) console.error(`     detail: ${JSON.stringify(detail)}`);
  failed++;
  process.exitCode = 1;
  return false;
}

async function send(client, channel, content) {
  const r = await client.callTool({
    name: "send_message",
    arguments: { channel, sender: "test-rp-schema", content: JSON.stringify(content) },
  });
  return r.content?.[0]?.text ?? "";
}

async function registerSchema(client, channel, file, strict) {
  const schema = readFileSync(file, "utf-8");
  const r = await client.callTool({
    name: "register_channel_schema",
    arguments: { channel, schema, strict },
  });
  return r;
}

async function clearSchema(client, channel) {
  await client.callTool({ name: "clear_channel_schema", arguments: { channel } });
  await client.callTool({ name: "purge_channel", arguments: { channel } });
}

// ─── rp-worker-inbox ─────────────────────────────────────────────────────────
// Shared by: rp-api, rp-admin, rp-web, rp-android, rp-ios, rp-qa

async function testWorkerInbox(client) {
  console.log("\n── rp-worker-inbox (rp-api / rp-admin / rp-web / rp-android / rp-ios / rp-qa) ──");
  const ch = `rp-wi-test-${RUN_TAG}`;
  const file = "schemas/rp-worker-inbox.json";

  let r = await registerSchema(client, ch, file, false);
  expect(/Registered schema/.test(r.content?.[0]?.text), "worker-inbox: schema registered warn-only");

  // 1. Valid task envelope
  const validTask = {
    type: "task",
    task_id: "rp-2026-06-19-test-wi-001",
    from: "orchestrator",
    to: "api",
    subject: "run the API test suite",
    body: "run node test-api.js",
    depends_on: [],
    required_checks: ["test"],
  };
  let t = await send(client, ch, validTask);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid task accepted, no warn", t);

  // 2. Valid rotate envelope
  const validRotate = {
    type: "rotate",
    task_id: "rp-2026-06-19-test-wi-rot",
    from: "orchestrator",
    to: "api",
    subject: "please rotate",
  };
  t = await send(client, ch, validRotate);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid rotate accepted, no warn", t);

  // 3. Missing required field (task_id) → warn in warn-only
  const missingTaskId = { type: "task", from: "orchestrator", to: "api", subject: "no task_id" };
  t = await send(client, ch, missingTaskId);
  expect(/WARN/.test(t), "worker-inbox: missing task_id → warn in warn-only mode", t);
  expect(/Sent #/.test(t), "worker-inbox: missing task_id still stored in warn-only", t);

  // 4. Missing subject → warn
  const missingSubject = { type: "task", task_id: "rp-2026-06-19-wi-nosub", from: "orchestrator", to: "web" };
  t = await send(client, ch, missingSubject);
  expect(/WARN/.test(t), "worker-inbox: missing subject → warn in warn-only mode", t);

  // 5. Unknown type → warn (enum violation)
  const unknownType = {
    type: "broadcast",
    task_id: "rp-2026-06-19-wi-unk",
    from: "orchestrator",
    to: "android",
    subject: "bad type",
  };
  t = await send(client, ch, unknownType);
  expect(/WARN/.test(t), "worker-inbox: unknown type → warn in warn-only mode", t);

  // 6. contract-change without wire_compat → warn
  const contractNoCompat = {
    type: "contract-change",
    task_id: "rp-2026-06-19-wi-cc1",
    from: "orchestrator",
    to: "qa",
    subject: "breaking change incoming",
    body: { before: {}, after: {} },
  };
  t = await send(client, ch, contractNoCompat);
  expect(/WARN/.test(t), "worker-inbox: contract-change without wire_compat → warn", t);

  // 7. Valid contract-change with wire_compat → no warn
  const validContract = { ...contractNoCompat, wire_compat: "additive" };
  t = await send(client, ch, validContract);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid contract-change with wire_compat accepted", t);

  // 8. approval-token with empty body (missing required fields) → warn
  const tokenMissingBody = {
    type: "approval-token",
    task_id: "rp-2026-06-19-wi-tok",
    from: "orchestrator",
    to: "api",
    subject: "approve this",
    body: {},
  };
  t = await send(client, ch, tokenMissingBody);
  expect(/WARN/.test(t), "worker-inbox: approval-token missing body → warn", t);

  // 9. Valid approval-token with all required body fields → accepted
  const validToken = {
    type: "approval-token",
    task_id: "rp-2026-06-19-wi-tok-ok",
    from: "orchestrator",
    to: "api",
    subject: "approve deploy",
    body: {
      authorized_actions: ["deploy:staging"],
      env: "staging",
      scope_workers: ["api"],
      expires_at: "2026-06-19T23:59:59Z",
    },
  };
  t = await send(client, ch, validToken);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid approval-token with full body accepted", t);

  // 10. Flip to strict; missing task_id → rejected
  r = await registerSchema(client, ch, file, true);
  expect(/Registered schema/.test(r.content?.[0]?.text), "worker-inbox: schema re-registered strict");

  t = await send(client, ch, missingTaskId);
  expect(/schema validation failed/.test(t), "worker-inbox strict: missing task_id rejected", t);

  // 11. Valid task still accepted in strict
  t = await send(client, ch, validTask);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox strict: valid task accepted", t);

  await clearSchema(client, ch);
}

// ─── rp-orchestrator-inbox ───────────────────────────────────────────────────

async function testOrchestratorInbox(client) {
  console.log("\n── rp-orchestrator-inbox ──");
  const ch = `rp-oi-test-${RUN_TAG}`;
  const file = "schemas/rp-orchestrator-inbox.json";

  let r = await registerSchema(client, ch, file, false);
  expect(/Registered schema/.test(r.content?.[0]?.text), "orch-inbox: schema registered warn-only");

  // 1. Valid question from a worker
  const validQuestion = {
    type: "question",
    task_id: "rp-2026-06-19-oi-q1",
    from: "api",
    to: "orchestrator",
    subject: "should I skip the migration?",
    body: { question: "Confirm skip?" },
  };
  let t = await send(client, ch, validQuestion);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "orch-inbox: valid question accepted, no warn", t);

  // 2. Valid note
  const validNote = {
    type: "note",
    task_id: "rp-2026-06-19-oi-n1",
    from: "qa",
    to: "orchestrator",
    subject: "baseline tests complete",
  };
  t = await send(client, ch, validNote);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "orch-inbox: valid note accepted, no warn", t);

  // 3. Valid result from worker
  const validResult = {
    type: "result",
    task_id: "rp-2026-06-19-oi-r1",
    from: "web",
    to: "orchestrator",
    subject: "task done",
    body: { outcome: "success" },
  };
  t = await send(client, ch, validResult);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "orch-inbox: valid result accepted, no warn", t);

  // 4. Missing task_id → warn
  const missingTaskId = { type: "note", from: "android", to: "orchestrator", subject: "no id" };
  t = await send(client, ch, missingTaskId);
  expect(/WARN/.test(t), "orch-inbox: missing task_id → warn in warn-only", t);

  // 5. Unknown type (task) → warn — workers don't dispatch tasks to orchestrator inbox
  const wrongType = {
    type: "task",
    task_id: "rp-2026-06-19-oi-unk",
    from: "ios",
    to: "orchestrator",
    subject: "workers cannot send tasks",
  };
  t = await send(client, ch, wrongType);
  expect(/WARN/.test(t), "orch-inbox: type:task (not allowed) → warn", t);

  // 6. Wrong from (orchestrator is not a worker) → warn
  const wrongFrom = {
    type: "note",
    task_id: "rp-2026-06-19-oi-wf",
    from: "orchestrator",
    to: "orchestrator",
    subject: "self-note",
  };
  t = await send(client, ch, wrongFrom);
  expect(/WARN/.test(t), "orch-inbox: from:orchestrator (not a worker) → warn", t);

  // 7. Strict: missing task_id → rejected
  r = await registerSchema(client, ch, file, true);
  expect(/Registered schema/.test(r.content?.[0]?.text), "orch-inbox: schema re-registered strict");

  t = await send(client, ch, missingTaskId);
  expect(/schema validation failed/.test(t), "orch-inbox strict: missing task_id rejected", t);

  // 8. Strict: valid question accepted
  t = await send(client, ch, validQuestion);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "orch-inbox strict: valid question accepted", t);

  await clearSchema(client, ch);
}

// ─── rp-status ───────────────────────────────────────────────────────────────

async function testStatus(client) {
  console.log("\n── rp-status ──");
  const ch = `rp-st-test-${RUN_TAG}`;
  const file = "schemas/rp-status.json";

  let r = await registerSchema(client, ch, file, false);
  expect(/Registered schema/.test(r.content?.[0]?.text), "status: schema registered warn-only");

  // 1. Valid status update
  const validStatus = {
    type: "status",
    task_id: "rp-2026-06-19-st-001",
    from: "api",
    to: "orchestrator",
    subject: "running migrations",
    body: { state: "working" },
  };
  let t = await send(client, ch, validStatus);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "status: valid type:status accepted, no warn", t);

  // 2. Valid result with summary (no production_touching) → no consent_basis needed
  const validResult = {
    type: "result",
    task_id: "rp-2026-06-19-st-r01",
    from: "qa",
    to: "orchestrator",
    subject: "tests done",
    summary: "PASS — all 42 tests pass",
    body: {},
  };
  t = await send(client, ch, validResult);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "status: valid type:result without production_touching accepted", t);

  // 3. type:result without summary → warn
  const resultNoSummary = {
    type: "result",
    task_id: "rp-2026-06-19-st-r02",
    from: "web",
    to: "orchestrator",
    subject: "done",
    body: {},
  };
  t = await send(client, ch, resultNoSummary);
  expect(/WARN/.test(t), "status: type:result without summary → warn in warn-only", t);
  expect(/Sent #/.test(t), "status: type:result without summary still stored in warn-only", t);

  // 4. type:result with production_touching=true but no consent_basis → warn
  const resultNoConsent = {
    type: "result",
    task_id: "rp-2026-06-19-st-r03",
    from: "api",
    to: "orchestrator",
    subject: "deployed",
    summary: "PASS — deployed to staging",
    body: { production_touching: true },
  };
  t = await send(client, ch, resultNoConsent);
  expect(/WARN/.test(t), "status: type:result production_touching=true without consent_basis → warn", t);

  // 5. type:result with production_touching=true AND consent_basis → no warn
  const resultWithConsent = {
    type: "result",
    task_id: "rp-2026-06-19-st-r04",
    from: "api",
    to: "orchestrator",
    subject: "deployed",
    summary: "PASS — deployed to staging",
    body: { production_touching: true, consent_basis: "orchestrator-dispatch-only" },
  };
  t = await send(client, ch, resultWithConsent);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "status: type:result production_touching=true with consent_basis accepted", t);

  // 6. Valid handoff → no warn
  const validHandoff = {
    type: "handoff",
    task_id: "rp-2026-06-19-st-h01",
    from: "api",
    to: "web",
    subject: "auth token ready for web to consume",
    body: { token_channel: "rp-api-tokens" },
  };
  t = await send(client, ch, validHandoff);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "status: valid type:handoff accepted, no warn", t);

  // 7. Missing task_id → warn
  const missingTaskId = { type: "status", from: "android", to: "orchestrator", subject: "no id" };
  t = await send(client, ch, missingTaskId);
  expect(/WARN/.test(t), "status: missing task_id → warn in warn-only", t);

  // 8. Strict: type:result without summary → rejected
  r = await registerSchema(client, ch, file, true);
  expect(/Registered schema/.test(r.content?.[0]?.text), "status: schema re-registered strict");

  t = await send(client, ch, resultNoSummary);
  expect(/schema validation failed/.test(t), "status strict: type:result without summary → rejected", t);

  // 9. Strict: type:result with production_touching=true but no consent_basis → rejected
  t = await send(client, ch, resultNoConsent);
  expect(/schema validation failed/.test(t), "status strict: type:result production_touching=true without consent_basis → rejected", t);

  // 10. Strict: type:status without summary passes (summary only required on result)
  t = await send(client, ch, validStatus);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "status strict: type:status without summary accepted", t);

  // 11. Strict: valid result accepted
  t = await send(client, ch, validResult);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "status strict: valid type:result accepted", t);

  await clearSchema(client, ch);
}

// ─── rp-control ──────────────────────────────────────────────────────────────

async function testControl(client) {
  console.log("\n── rp-control ──");
  const ch = `rp-ctrl-test-${RUN_TAG}`;
  const file = "schemas/rp-control.json";

  let r = await registerSchema(client, ch, file, false);
  expect(/Registered schema/.test(r.content?.[0]?.text), "control: schema registered warn-only");

  // 1. Valid sprint-start
  const validSprint = {
    type: "sprint-start",
    task_id: "rp-2026-06-19-ctrl-sp1",
    from: "orchestrator",
    to: "*",
    subject: "Sprint 1 — begin",
    body: { goal: "ship auth service" },
  };
  let t = await send(client, ch, validSprint);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "control: valid sprint-start accepted, no warn", t);

  // 2. Valid note
  const validNote = {
    type: "note",
    task_id: "rp-2026-06-19-ctrl-n1",
    from: "orchestrator",
    to: "*",
    subject: "reminder: use env staging only",
  };
  t = await send(client, ch, validNote);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "control: valid note accepted, no warn", t);

  // 3. Valid rotate
  const validRotate = {
    type: "rotate",
    task_id: "rp-2026-06-19-ctrl-rot",
    from: "orchestrator",
    to: "api",
    subject: "rotate now",
  };
  t = await send(client, ch, validRotate);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "control: valid rotate accepted, no warn", t);

  // 4. Missing task_id → warn (task_id is required on rp-control)
  const missingTaskId = { type: "note", from: "orchestrator", to: "*", subject: "no task_id" };
  t = await send(client, ch, missingTaskId);
  expect(/WARN/.test(t), "control: missing task_id → warn in warn-only", t);

  // 5. Unknown type → warn
  const unknownType = {
    type: "broadcast",
    task_id: "rp-2026-06-19-ctrl-unk",
    from: "orchestrator",
    to: "*",
    subject: "unknown type",
  };
  t = await send(client, ch, unknownType);
  expect(/WARN/.test(t), "control: unknown type → warn", t);

  // 6. contract-change without wire_compat → warn
  const contractNoCompat = {
    type: "contract-change",
    task_id: "rp-2026-06-19-ctrl-cc1",
    from: "orchestrator",
    to: "*",
    subject: "api shape changing",
    body: { before: { version: 1 }, after: { version: 2 }, affected_workers: ["api", "web"] },
  };
  t = await send(client, ch, contractNoCompat);
  expect(/WARN/.test(t), "control: contract-change without wire_compat → warn", t);

  // 7. Valid contract-change → no warn
  const validContract = { ...contractNoCompat, wire_compat: "breaking" };
  t = await send(client, ch, validContract);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "control: valid contract-change with wire_compat accepted", t);

  // 8. Valid approval-token → no warn
  const validToken = {
    type: "approval-token",
    task_id: "rp-2026-06-19-ctrl-tok",
    from: "orchestrator",
    to: "api",
    subject: "approve deploy",
    body: {
      authorized_actions: ["deploy:staging"],
      env: "staging",
      scope_workers: ["api"],
      expires_at: "2026-06-19T23:59:59Z",
    },
  };
  t = await send(client, ch, validToken);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "control: valid approval-token accepted", t);

  // 9. Valid approval-revoke → no warn
  const validRevoke = {
    type: "approval-revoke",
    task_id: "rp-2026-06-19-ctrl-rev",
    from: "orchestrator",
    to: "*",
    subject: "token revoked",
    body: { revokes_msg_id: 9999 },
  };
  t = await send(client, ch, validRevoke);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "control: valid approval-revoke accepted", t);

  // 10. Strict: missing task_id → rejected
  r = await registerSchema(client, ch, file, true);
  expect(/Registered schema/.test(r.content?.[0]?.text), "control: schema re-registered strict");

  t = await send(client, ch, missingTaskId);
  expect(/schema validation failed/.test(t), "control strict: missing task_id rejected", t);

  // 11. Strict: valid sprint-start accepted
  t = await send(client, ch, validSprint);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "control strict: valid sprint-start accepted", t);

  await clearSchema(client, ch);
}

// ─── rp-telemetry ────────────────────────────────────────────────────────────

async function testTelemetry(client) {
  console.log("\n── rp-telemetry ──");
  const ch = `rp-tel-test-${RUN_TAG}`;
  const file = "schemas/rp-telemetry.json";

  let r = await registerSchema(client, ch, file, false);
  expect(/Registered schema/.test(r.content?.[0]?.text), "telemetry: schema registered warn-only");

  // 1. Valid heartbeat
  const validHB = {
    type: "heartbeat",
    from: "api",
    ts: "2026-06-19T15:30:00Z",
    session_id: "sess-rp-abc123",
    model: "claude-sonnet-4-6",
    context: { size_tokens: 45000, tier_threshold_pct: 30.0, rotation_recommended: false },
    activity: { state: "working", current_task_id: "rp-2026-06-19-api-001" },
  };
  let t = await send(client, ch, validHB);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "telemetry: valid heartbeat accepted, no warn", t);

  // 2. All valid activity states
  for (const state of ["idle-polling", "blocked-on-question", "rotating"]) {
    const hb = { ...validHB, activity: { state } };
    t = await send(client, ch, hb);
    expect(/Sent #/.test(t) && !/WARN/.test(t), `telemetry: state="${state}" accepted`, t);
  }

  // 3. Invalid state → warn
  const badState = { ...validHB, activity: { state: "undefined-state" } };
  t = await send(client, ch, badState);
  expect(/WARN/.test(t), "telemetry: unknown state → warn in warn-only", t);

  // 4. Missing context → warn
  const missingContext = {
    type: "heartbeat",
    from: "qa",
    ts: "2026-06-19T15:31:00Z",
    activity: { state: "working" },
  };
  t = await send(client, ch, missingContext);
  expect(/WARN/.test(t), "telemetry: missing context → warn in warn-only", t);

  // 5. Missing activity → warn
  const missingActivity = {
    type: "heartbeat",
    from: "web",
    ts: "2026-06-19T15:32:00Z",
    context: { size_tokens: 1000, tier_threshold_pct: 10.0, rotation_recommended: false },
  };
  t = await send(client, ch, missingActivity);
  expect(/WARN/.test(t), "telemetry: missing activity → warn in warn-only", t);

  // 6. Wrong type (not 'heartbeat') → warn
  const wrongType = { ...validHB, type: "status" };
  t = await send(client, ch, wrongType);
  expect(/WARN/.test(t), "telemetry: type:status (not heartbeat) → warn", t);

  // 7. Strict: invalid state → rejected
  r = await registerSchema(client, ch, file, true);
  expect(/Registered schema/.test(r.content?.[0]?.text), "telemetry: schema re-registered strict");

  t = await send(client, ch, badState);
  expect(/schema validation failed/.test(t), "telemetry strict: unknown state → rejected", t);

  // 8. Strict: valid heartbeat accepted
  t = await send(client, ch, validHB);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "telemetry strict: valid heartbeat accepted", t);

  await clearSchema(client, ch);
}

// ─── rp-backlog ───────────────────────────────────────────────────────────────

async function testBacklog(client) {
  console.log("\n── rp-backlog ──");
  const ch = `rp-bl-test-${RUN_TAG}`;
  const file = "schemas/rp-backlog.json";

  let r = await registerSchema(client, ch, file, false);
  expect(/Registered schema/.test(r.content?.[0]?.text), "backlog: schema registered warn-only");

  // 1. Valid deferred entry
  const validDeferred = {
    type: "deferred",
    task_id: "rp-2026-06-19-bl-001",
    subject: "add rate-limit middleware to API",
    from: "api",
    to: "orchestrator",
    body: { priority: "medium" },
    deferred_reason: "scope too large for current sprint",
  };
  let t = await send(client, ch, validDeferred);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "backlog: valid deferred accepted, no warn", t);

  // 2. Valid deferred-resolved with full body
  const validResolved = {
    type: "deferred-resolved",
    task_id: "rp-2026-06-19-bl-001",
    subject: "add rate-limit middleware to API",
    from: "orchestrator",
    to: "orchestrator",
    body: { resolved_in_sprint: "rp-sprint-003", outcome: "promoted" },
  };
  t = await send(client, ch, validResolved);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "backlog: valid deferred-resolved with full body accepted", t);

  // 3. deferred-resolved without body.outcome → warn
  const resolvedNoOutcome = {
    type: "deferred-resolved",
    task_id: "rp-2026-06-19-bl-002",
    subject: "another deferred task",
    body: { resolved_in_sprint: "rp-sprint-003" },
  };
  t = await send(client, ch, resolvedNoOutcome);
  expect(/WARN/.test(t), "backlog: deferred-resolved without outcome → warn in warn-only", t);

  // 4. deferred-resolved without body.resolved_in_sprint → warn
  const resolvedNoSprint = {
    type: "deferred-resolved",
    task_id: "rp-2026-06-19-bl-003",
    subject: "another deferred task",
    body: { outcome: "cancelled" },
  };
  t = await send(client, ch, resolvedNoSprint);
  expect(/WARN/.test(t), "backlog: deferred-resolved without resolved_in_sprint → warn in warn-only", t);

  // 5. Unknown type → warn
  const unknownType = {
    type: "note",
    task_id: "rp-2026-06-19-bl-unk",
    subject: "type:note not allowed in backlog",
  };
  t = await send(client, ch, unknownType);
  expect(/WARN/.test(t), "backlog: unknown type:note → warn in warn-only", t);

  // 6. Missing task_id → warn
  const missingTaskId = { type: "deferred", subject: "no task_id here" };
  t = await send(client, ch, missingTaskId);
  expect(/WARN/.test(t), "backlog: missing task_id → warn in warn-only", t);

  // 7. Missing subject → warn
  const missingSubject = { type: "deferred", task_id: "rp-2026-06-19-bl-ns" };
  t = await send(client, ch, missingSubject);
  expect(/WARN/.test(t), "backlog: missing subject → warn in warn-only", t);

  // 8. Strict: deferred-resolved without outcome → rejected
  r = await registerSchema(client, ch, file, true);
  expect(/Registered schema/.test(r.content?.[0]?.text), "backlog: schema re-registered strict");

  t = await send(client, ch, resolvedNoOutcome);
  expect(/schema validation failed/.test(t), "backlog strict: deferred-resolved without outcome → rejected", t);

  // 9. Strict: valid deferred accepted
  t = await send(client, ch, validDeferred);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "backlog strict: valid deferred accepted", t);

  await clearSchema(client, ch);
}

// ─── setup-schemas-ridepro.js idempotent re-run ──────────────────────────────

async function testSetupIdempotency() {
  console.log("\n── setup-schemas-ridepro.js idempotency ──");
  const { execSync } = await import("child_process");
  let output, exitCode;
  try {
    output = execSync(`"${process.execPath}" setup-schemas-ridepro.js`, { encoding: "utf-8", env: process.env });
    exitCode = 0;
  } catch (e) {
    output = e.stdout + e.stderr;
    exitCode = e.status ?? 1;
  }
  expect(exitCode === 0, "setup-schemas-ridepro.js: first run exits 0", output);
  expect(/rp-api/.test(output), "setup-schemas-ridepro.js: rp-api registered", output);
  expect(/rp-admin/.test(output), "setup-schemas-ridepro.js: rp-admin registered", output);
  expect(/rp-web/.test(output), "setup-schemas-ridepro.js: rp-web registered", output);
  expect(/rp-android/.test(output), "setup-schemas-ridepro.js: rp-android registered", output);
  expect(/rp-ios/.test(output), "setup-schemas-ridepro.js: rp-ios registered", output);
  expect(/rp-qa/.test(output), "setup-schemas-ridepro.js: rp-qa registered", output);
  expect(/rp-orchestrator/.test(output), "setup-schemas-ridepro.js: rp-orchestrator registered", output);
  expect(/rp-status/.test(output), "setup-schemas-ridepro.js: rp-status registered", output);
  expect(/rp-control/.test(output), "setup-schemas-ridepro.js: rp-control registered", output);
  expect(/rp-telemetry/.test(output), "setup-schemas-ridepro.js: rp-telemetry registered", output);
  expect(/rp-backlog/.test(output), "setup-schemas-ridepro.js: rp-backlog registered", output);

  // Second run — must also exit 0 (idempotent)
  try {
    output = execSync(`"${process.execPath}" setup-schemas-ridepro.js`, { encoding: "utf-8", env: process.env });
    exitCode = 0;
  } catch (e) {
    output = e.stdout + e.stderr;
    exitCode = e.status ?? 1;
  }
  expect(exitCode === 0, "setup-schemas-ridepro.js: second run (idempotent re-run) exits 0", output);
  expect(!/ERROR/.test(output), "setup-schemas-ridepro.js: no ERROR on second run", output);
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[test-schema-rp]  broker=${BROKER_URL}  run=${RUN_TAG}`);
  const { client, transport } = await connect("test-schema-rp");

  await testWorkerInbox(client);
  await testOrchestratorInbox(client);
  await testStatus(client);
  await testControl(client);
  await testTelemetry(client);
  await testBacklog(client);

  await transport.close();

  await testSetupIdempotency();

  console.log(`\n──────────────────────────────────────────────────────`);
  console.log(`[test-schema-rp]  passed=${passed}  failed=${failed}`);
  if (failed === 0) console.log("  ALL TESTS PASSED");
  else { console.error("  SOME TESTS FAILED"); process.exitCode = 1; }
}

main().catch(e => { console.error("[test-schema-rp] FATAL:", e); process.exit(1); });
