// Schema validation tests for sm-namespace channels.
// Exercises all sm-* channel schemas against a live broker.
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
    arguments: { channel, sender: "test-sm-schema", content: JSON.stringify(content) },
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

// ─── sm-worker-inbox ─────────────────────────────────────────────────────────
// Shared by: sm-contracts, sm-backend, sm-web

async function testWorkerInbox(client) {
  console.log("\n── sm-worker-inbox (sm-contracts / sm-backend / sm-web) ──");
  const ch = `sm-wi-test-${RUN_TAG}`;
  const file = "schemas/sm-worker-inbox.json";

  let r = await registerSchema(client, ch, file, false);
  expect(/Registered schema/.test(r.content?.[0]?.text), "worker-inbox: schema registered warn-only");

  // 1. Valid task envelope
  const validTask = {
    type: "task",
    task_id: "sm-2026-06-22-test-wi-001",
    from: "orchestrator",
    to: "contracts",
    subject: "review partnership agreement",
    body: "review and approve the agreement",
    depends_on: [],
    required_checks: ["review"],
  };
  let t = await send(client, ch, validTask);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid task accepted, no warn", t);

  // 2. Valid rotate envelope
  const validRotate = {
    type: "rotate",
    task_id: "sm-2026-06-22-test-wi-rot",
    from: "orchestrator",
    to: "backend",
    subject: "please rotate",
  };
  t = await send(client, ch, validRotate);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid rotate accepted, no warn", t);

  // 3. Missing required field (task_id) → warn in warn-only
  const missingTaskId = { type: "task", from: "orchestrator", to: "web", subject: "no task_id" };
  t = await send(client, ch, missingTaskId);
  expect(/WARN/.test(t), "worker-inbox: missing task_id → warn in warn-only mode", t);
  expect(/Sent #/.test(t), "worker-inbox: missing task_id still stored in warn-only", t);

  // 4. Missing subject → warn
  const missingSubject = { type: "task", task_id: "sm-2026-06-22-wi-nosub", from: "orchestrator", to: "backend" };
  t = await send(client, ch, missingSubject);
  expect(/WARN/.test(t), "worker-inbox: missing subject → warn in warn-only mode", t);

  // 5. Unknown type → warn (enum violation)
  const unknownType = {
    type: "broadcast",
    task_id: "sm-2026-06-22-wi-unk",
    from: "orchestrator",
    to: "contracts",
    subject: "bad type",
  };
  t = await send(client, ch, unknownType);
  expect(/WARN/.test(t), "worker-inbox: unknown type → warn in warn-only mode", t);

  // 6. type:task missing body → warn
  const taskNoBody = {
    type: "task",
    task_id: "sm-2026-06-22-wi-nobody",
    from: "orchestrator",
    to: "backend",
    subject: "task without body",
  };
  t = await send(client, ch, taskNoBody);
  expect(/WARN/.test(t), "worker-inbox: type:task missing body → warn", t);

  // 7. Valid note envelope
  const validNote = {
    type: "note",
    task_id: "sm-2026-06-22-wi-note1",
    from: "orchestrator",
    to: "web",
    subject: "status update",
  };
  t = await send(client, ch, validNote);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid note accepted, no warn", t);

  // 8. Valid question envelope
  const validQuestion = {
    type: "question",
    task_id: "sm-2026-06-22-wi-q1",
    from: "orchestrator",
    to: "contracts",
    subject: "should we accept this rate?",
  };
  t = await send(client, ch, validQuestion);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: valid question accepted, no warn", t);

  // 9. task with acceptance_criteria → accepted, no warn
  const taskWithCriteria = {
    type: "task",
    task_id: "sm-2026-06-22-wi-ac1",
    from: "orchestrator",
    to: "contracts",
    subject: "audit compliance",
    body: "review contracts for compliance\n\nAcceptance criteria:\n- [ ] all contracts reviewed\n- [ ] report generated\n- [ ] committed",
    acceptance_criteria: ["all contracts reviewed", "report generated", "committed"],
    required_checks: ["review", "committed"],
  };
  t = await send(client, ch, taskWithCriteria);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox: task with acceptance_criteria accepted, no warn", t);

  // 10. task with context + files + checks (new fields) → accepted, no warn
  const taskWithNewFields = {
    type: "task",
    task_id: "sm-2026-06-22-wi-new-a",
    from: "orchestrator",
    to: "backend",
    subject: "implement payout processor",
    body: "add payout processing logic",
    context: "New envelope fields added in sprint-023: context, files, checks for enhanced task tracking.",
    files: { read: ["schemas/sm-worker-inbox.json"], write: ["test-schema-sm.js"] },
    checks: [{ name: "tests pass", run: "node test-schema-sm.js", pass_condition: "ALL TESTS PASSED" }],
  };
  t = await send(client, ch, taskWithNewFields);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox new-a: task with context+files+checks accepted, no warn", t);

  // 11. task without new fields → backward compat, should still PASS
  const taskWithoutNewFields = {
    type: "task",
    task_id: "sm-2026-06-22-wi-new-b",
    from: "orchestrator",
    to: "web",
    subject: "plain task no new fields",
    body: "run updates",
  };
  t = await send(client, ch, taskWithoutNewFields);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox new-b: task without new fields still accepted (backward compat)", t);

  // 12. checks item missing pass_condition → WARN (schema violation)
  const taskChecksMissingPassCond = {
    type: "task",
    task_id: "sm-2026-06-22-wi-new-c",
    from: "orchestrator",
    to: "contracts",
    subject: "checks without pass_condition",
    body: "do thing",
    checks: [{ name: "run tests", run: "node test-schema-sm.js" }],
  };
  t = await send(client, ch, taskChecksMissingPassCond);
  expect(/WARN/.test(t), "worker-inbox new-c: checks item missing pass_condition → warn in warn-only", t);

  // 13. files with extra property beyond read/write → WARN
  const taskFilesExtraProp = {
    type: "task",
    task_id: "sm-2026-06-22-wi-new-d",
    from: "orchestrator",
    to: "backend",
    subject: "files with extra property",
    body: "do thing",
    files: { read: [], write: [], execute: ["script.sh"] },
  };
  t = await send(client, ch, taskFilesExtraProp);
  expect(/WARN/.test(t), "worker-inbox new-d: files with extra property → warn in warn-only", t);

  // 14. context set to empty string → WARN (minLength:1)
  const taskEmptyContext = {
    type: "task",
    task_id: "sm-2026-06-22-wi-new-e",
    from: "orchestrator",
    to: "web",
    subject: "empty context string",
    body: "do thing",
    context: "",
  };
  t = await send(client, ch, taskEmptyContext);
  expect(/WARN/.test(t), "worker-inbox new-e: context set to empty string → warn (minLength:1)", t);

  // 15. Flip to strict; missing task_id → rejected
  r = await registerSchema(client, ch, file, true);
  expect(/Registered schema/.test(r.content?.[0]?.text), "worker-inbox: schema re-registered strict");

  t = await send(client, ch, missingTaskId);
  expect(/schema validation failed/.test(t), "worker-inbox strict: missing task_id rejected", t);

  // 16. strict: task with acceptance_criteria still accepted
  t = await send(client, ch, taskWithCriteria);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox strict: task with acceptance_criteria accepted", t);

  // 17. Valid task still accepted in strict
  t = await send(client, ch, validTask);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox strict: valid task accepted", t);

  // 18. strict: task with new fields still accepted
  t = await send(client, ch, taskWithNewFields);
  expect(/Sent #/.test(t) && !/WARN/.test(t), "worker-inbox strict: task with new fields accepted", t);

  await clearSchema(client, ch);
}

// ─── setup-schemas-sm.js idempotent re-run ──────────────────────────────

async function testSetupIdempotency() {
  console.log("\n── setup-schemas-sm.js idempotency ──");
  const { execSync } = await import("child_process");
  let output, exitCode;
  try {
    output = execSync(`"${process.execPath}" setup-schemas-sm.js`, { encoding: "utf-8", env: process.env });
    exitCode = 0;
  } catch (e) {
    output = e.stdout + e.stderr;
    exitCode = e.status ?? 1;
  }
  expect(exitCode === 0, "setup-schemas-sm.js: first run exits 0", output);
  expect(/sm-contracts/.test(output), "setup-schemas-sm.js: sm-contracts registered", output);
  expect(/sm-backend/.test(output), "setup-schemas-sm.js: sm-backend registered", output);
  expect(/sm-web/.test(output), "setup-schemas-sm.js: sm-web registered", output);

  // Second run — must also exit 0 (idempotent)
  try {
    output = execSync(`"${process.execPath}" setup-schemas-sm.js`, { encoding: "utf-8", env: process.env });
    exitCode = 0;
  } catch (e) {
    output = e.stdout + e.stderr;
    exitCode = e.status ?? 1;
  }
  expect(exitCode === 0, "setup-schemas-sm.js: second run (idempotent re-run) exits 0", output);
  expect(!/ERROR/.test(output), "setup-schemas-sm.js: no ERROR on second run", output);
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[test-schema-sm]  broker=${BROKER_URL}  run=${RUN_TAG}`);
  const { client, transport } = await connect("test-schema-sm");

  await testWorkerInbox(client);

  await transport.close();

  await testSetupIdempotency();

  console.log(`\n──────────────────────────────────────────────────────`);
  console.log(`[test-schema-sm]  passed=${passed}  failed=${failed}`);
  if (failed === 0) console.log("  ALL TESTS PASSED");
  else { console.error("  SOME TESTS FAILED"); process.exitCode = 1; }
}

main().catch(e => { console.error("[test-schema-sm] FATAL:", e); process.exit(1); });
