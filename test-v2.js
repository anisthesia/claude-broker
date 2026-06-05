/**
 * claude-broker v2 test suite
 * Tests all new features: filtering, delete, gated post, capabilities, dashboard, prune
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const BROKER_URL = process.env.BROKER_URL || "http://localhost:8080/mcp";
const SECRET     = process.env.SHARED_SECRET || "";

let passed = 0;
let failed = 0;

function ok(label) {
  console.log(`  ✓ ${label}`);
  passed++;
}

function fail(label, detail) {
  console.error(`  ✗ ${label}: ${detail}`);
  failed++;
}

function assert(condition, label, detail = "") {
  condition ? ok(label) : fail(label, detail || "assertion failed");
}

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

async function run() {
  const ch = `test-v2-${Date.now()}`;
  console.log(`\n[broker v2 test suite]  channel=${ch}  url=${BROKER_URL}\n`);

  const { client: a, transport: ta } = await connect("test-a");
  const { client: b, transport: tb } = await connect("test-b");

  // ── 1. Tool list ─────────────────────────────────────────────────────────
  console.log("1. Tool list");
  const tools = await a.listTools();
  const names = tools.tools.map(t => t.name);
  for (const expected of ["delete_message", "post_gated_message", "register_capability", "list_capabilities", "deregister_capability", "check_result"]) {
    assert(names.includes(expected), `tool '${expected}' registered`, `missing — got: ${names.join(", ")}`);
  }
  assert(names.includes("purge_channel"), "tool 'purge_channel' registered");

  // ── 2. filter_sender on read_messages ─────────────────────────────────────
  console.log("\n2. filter_sender on read_messages");
  await call(a, "send_message", { channel: ch, sender: "alice", content: JSON.stringify({ type: "task", msg: "from alice" }) });
  await call(a, "send_message", { channel: ch, sender: "bob",   content: JSON.stringify({ type: "note", msg: "from bob"   }) });
  await call(a, "send_message", { channel: ch, sender: "alice", content: JSON.stringify({ type: "result", msg: "alice result" }) });

  const aliceOnly = await call(a, "read_messages", { channel: ch, since_id: 0, filter_sender: "alice" });
  assert(aliceOnly.includes("alice") && !aliceOnly.includes("bob"), "filter_sender=alice returns only alice msgs");

  const bobOnly = await call(a, "read_messages", { channel: ch, since_id: 0, filter_sender: "bob" });
  assert(bobOnly.includes("bob") && !bobOnly.includes("alice"), "filter_sender=bob returns only bob msgs");

  // ── 3. filter_type on read_messages ──────────────────────────────────────
  console.log("\n3. filter_type on read_messages");
  const taskOnly = await call(a, "read_messages", { channel: ch, since_id: 0, filter_type: "task" });
  assert(taskOnly.includes('"task"') && !taskOnly.includes('"note"') && !taskOnly.includes('"result"'), "filter_type=task returns only task msgs");

  const resultOnly = await call(a, "read_messages", { channel: ch, since_id: 0, filter_type: "result" });
  assert(resultOnly.includes('"result"') && !resultOnly.includes('"task"'), "filter_type=result returns only result msgs");

  // ── 4. filter_type on wait_for_messages ──────────────────────────────────
  console.log("\n4. filter_type on wait_for_messages");
  const wch = `test-wait-${Date.now()}`;

  // Start a wait for type:"special" in the background
  const waitPromise = call(b, "wait_for_messages", { channel: wch, since_id: 0, timeout_ms: 5000, filter_type: "special" });

  // Post a non-matching message (should be skipped)
  await new Promise(r => setTimeout(r, 100));
  await call(a, "send_message", { channel: wch, sender: "a", content: JSON.stringify({ type: "irrelevant", v: 1 }) });

  // Post the matching message
  await new Promise(r => setTimeout(r, 100));
  await call(a, "send_message", { channel: wch, sender: "a", content: JSON.stringify({ type: "special", v: 99 }) });

  const waitResult = await waitPromise;
  assert(waitResult.includes('"special"') && !waitResult.includes('"irrelevant"'), "wait_for_messages filter_type skips non-matching, wakes on match");

  // ── 5. delete_message ────────────────────────────────────────────────────
  console.log("\n5. delete_message");
  const dch = `test-del-${Date.now()}`;
  await call(a, "send_message", { channel: dch, sender: "x", content: "keep me" });
  const sendRes = await call(a, "send_message", { channel: dch, sender: "x", content: "delete me" });
  // Extract id from "Sent #N to ..."
  const msgId = Number(sendRes.match(/#(\d+)/)?.[1]);
  assert(msgId > 0, `extracted message id ${msgId}`);

  const delRes = await call(a, "delete_message", { channel: dch, id: msgId });
  assert(delRes.includes("Deleted"), "delete_message returns confirmation");

  const afterDel = await call(a, "read_messages", { channel: dch, since_id: 0 });
  assert(afterDel.includes("keep me") && !afterDel.includes("delete me"), "deleted message absent, other message intact");

  // Wrong channel should fail
  const wrongChan = await a.callTool({ name: "delete_message", arguments: { channel: "wrong-channel", id: msgId } });
  assert(wrongChan.isError === true || wrongChan.content[0].text.includes("not found"), "delete_message on wrong channel returns not-found error");

  // ── 6. purge_channel older_than_ms ───────────────────────────────────────
  console.log("\n6. purge_channel with older_than_ms");
  const pch = `test-prune-${Date.now()}`;
  await call(a, "send_message", { channel: pch, sender: "x", content: "old msg" });
  await new Promise(r => setTimeout(r, 200));
  await call(a, "send_message", { channel: pch, sender: "x", content: "new msg" });

  // Prune messages older than 100ms (should remove first, keep second)
  const pruneRes = await call(a, "purge_channel", { channel: pch, older_than_ms: 100 });
  assert(pruneRes.includes("Pruned 1"), "purge_channel(older_than_ms) removes only old messages");

  const afterPrune = await call(a, "read_messages", { channel: pch, since_id: 0 });
  assert(afterPrune.includes("new msg") && !afterPrune.includes("old msg"), "recent message survives older_than_ms prune");

  // ── 7. register_capability / list_capabilities / deregister_capability ───
  console.log("\n7. Capability registry");
  await call(a, "register_capability", { worker: "test-worker-alpha", owns: ["module-a", "module-b"], channels: ["dv-alpha", "dv-status"] });
  await call(a, "register_capability", { worker: "test-worker-beta",  owns: ["module-c"],            channels: ["dv-beta",  "dv-status"] });

  const caps = await call(a, "list_capabilities", {});
  assert(caps.includes("test-worker-alpha") && caps.includes("module-a"), "registered worker alpha visible in list");
  assert(caps.includes("test-worker-beta")  && caps.includes("module-c"), "registered worker beta visible in list");

  // Idempotent re-registration
  await call(a, "register_capability", { worker: "test-worker-alpha", owns: ["module-a", "module-b", "module-x"], channels: ["dv-alpha"] });
  const capsAfterUpdate = await call(a, "list_capabilities", {});
  assert(capsAfterUpdate.includes("module-x"), "re-registration updates existing entry");

  // Deregister
  await call(a, "deregister_capability", { worker: "test-worker-alpha" });
  await call(a, "deregister_capability", { worker: "test-worker-beta"  });
  const capsAfterDel = await call(a, "list_capabilities", {});
  assert(!capsAfterDel.includes("test-worker-alpha"), "deregistered worker alpha removed");

  // ── 8. post_gated_message ────────────────────────────────────────────────
  console.log("\n8. post_gated_message");
  const gch  = `test-gate-${Date.now()}`;
  const wsch = `test-gate-watch-${Date.now()}`;
  const taskId = `task-gate-${Date.now()}`;

  // Post gated message — should block until result lands on wsch
  const gatePromise = call(a, "post_gated_message", {
    channel:       gch,
    sender:        "orchestrator",
    content:       JSON.stringify({ type: "task", msg: "gated task released" }),
    depends_on:    [taskId],
    watch_channel: wsch,
    timeout_ms:    6000,
  });

  // Verify it hasn't posted yet (give it 200ms to register)
  await new Promise(r => setTimeout(r, 200));
  const beforeResult = await call(a, "read_messages", { channel: gch, since_id: 0 });
  assert(beforeResult.includes("No new messages"), "gated message not posted before dep satisfied");

  // Post the result that unblocks it
  await call(b, "send_message", { channel: wsch, sender: "worker", content: JSON.stringify({ type: "result", task_id: taskId, summary: "done" }) });

  const gateRes = await gatePromise;
  assert(gateRes.includes("Deps satisfied"), "post_gated_message unblocked after result posted");

  const afterGate = await call(a, "read_messages", { channel: gch, since_id: 0 });
  assert(afterGate.includes("gated task released"), "gated message now visible in channel");

  // Timeout path
  const toch = `test-gate-timeout-${Date.now()}`;
  const timeoutRes = await call(a, "post_gated_message", {
    channel:       toch,
    sender:        "orchestrator",
    content:       JSON.stringify({ type: "task", msg: "should not post" }),
    depends_on:    ["nonexistent-task-id"],
    watch_channel: `nonexistent-watch-${Date.now()}`,
    timeout_ms:    500,
  });
  assert(timeoutRes.includes("Timed out"), "post_gated_message times out when dep never arrives");

  // ── 9. list_channels includes last_activity ───────────────────────────────
  console.log("\n9. list_channels last_activity");
  const chanList = await call(a, "list_channels", {});
  assert(chanList.includes("last_activity="), "list_channels output includes last_activity timestamp");

  // ── 10. Dashboard HTTP ────────────────────────────────────────────────────
  console.log("\n10. Dashboard");
  const dashRes = await fetch("http://localhost:8080/dashboard");
  assert(dashRes.ok, `dashboard HTTP ${dashRes.status} OK`);
  const dashHtml = await dashRes.text();
  assert(dashHtml.includes("claude-broker"), "dashboard HTML contains title");
  assert(dashHtml.includes("v2.0.0"), "dashboard HTML shows v2.0.0");
  assert(dashHtml.includes("Channels"), "dashboard HTML has Channels section");
  assert(dashHtml.includes("Worker Capabilities"), "dashboard HTML has Capabilities section");

  // ── 11. Health endpoint includes uptime_s ────────────────────────────────
  console.log("\n11. Health endpoint");
  const healthRes = await fetch("http://localhost:8080/health");
  const health = await healthRes.json();
  assert(typeof health.uptime_s === "number" && health.uptime_s >= 0, `health includes uptime_s=${health.uptime_s}`);

  // ── 12. check_result ─────────────────────────────────────────────────────
  console.log("\n12. check_result");
  const crch  = `test-cr-${Date.now()}`;
  const crch2 = `test-cr2-${Date.now()}`;
  const crId  = `task-cr-${Date.now()}`;

  // No result posted yet → found: false
  const cr1 = JSON.parse(await call(a, "check_result", { channel: crch, task_id: crId }));
  assert(cr1.found === false, "check_result: found=false on empty channel");
  assert(cr1.task_id === crId, `check_result: task_id echoed back`);
  assert(cr1.channel === crch, `check_result: channel echoed back`);

  // type:status posted (not a result) → still found: false
  await call(a, "send_message", { channel: crch, sender: "w", content: JSON.stringify({ type: "status", task_id: crId, body: "in progress" }) });
  const cr2 = JSON.parse(await call(a, "check_result", { channel: crch, task_id: crId }));
  assert(cr2.found === false, "check_result: found=false for type:status (not a result)");

  // type:result posted → found: true
  await call(a, "send_message", { channel: crch, sender: "w", content: JSON.stringify({ type: "result", task_id: crId, body: "done" }) });
  const cr3 = JSON.parse(await call(a, "check_result", { channel: crch, task_id: crId }));
  assert(cr3.found === true, "check_result: found=true after result posted");

  // Different task_id on same channel → no cross-contamination
  const otherId = `other-${Date.now()}`;
  const cr4 = JSON.parse(await call(a, "check_result", { channel: crch, task_id: otherId }));
  assert(cr4.found === false, "check_result: different task_id on same channel → found=false");

  // Same task_id on a different channel is tracked independently
  await call(a, "send_message", { channel: crch2, sender: "w", content: JSON.stringify({ type: "result", task_id: crId, body: "done ch2" }) });
  const cr5 = JSON.parse(await call(a, "check_result", { channel: crch2, task_id: crId }));
  assert(cr5.found === true, "check_result: result on separate channel visible independently");

  // Idempotency: multiple results for same task_id → still found: true
  await call(a, "send_message", { channel: crch, sender: "w", content: JSON.stringify({ type: "result", task_id: crId, body: "duplicate" }) });
  const cr6 = JSON.parse(await call(a, "check_result", { channel: crch, task_id: crId }));
  assert(cr6.found === true, "check_result: multiple results for same task_id → found=true (stable)");

  // check_result listed in tool list
  const toolNames = tools.tools.map(t => t.name);
  assert(toolNames.includes("check_result"), "check_result registered in tool list");

  // ── Cleanup ───────────────────────────────────────────────────────────────
  for (const c of [ch, wch, dch, pch, gch, wsch, toch, crch, crch2]) {
    await call(a, "purge_channel", { channel: c }).catch(() => {});
  }
  await ta.close();
  await tb.close();

  // ── Summary ───────────────────────────────────────────────────────────────
  console.log(`\n${"─".repeat(50)}`);
  console.log(`  passed: ${passed}   failed: ${failed}`);
  if (failed > 0) {
    console.error("  SOME TESTS FAILED");
    process.exit(1);
  } else {
    console.log("  ALL TESTS PASSED");
  }
}

run().catch(e => {
  console.error("\n[FATAL]", e.message || e);
  process.exit(1);
});
