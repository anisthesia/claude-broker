import "dotenv/config";
import express from "express";
import Database from "better-sqlite3";
import { EventEmitter } from "events";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { spawn } from "child_process";
import { readFileSync, mkdirSync, createWriteStream, openSync, closeSync } from "fs";

const PORT               = Number(process.env.PORT)                   || 8080;
const SHARED_SECRET      = process.env.SHARED_SECRET                  || "";
const DB_PATH            = process.env.DB_PATH                        || "./broker.db";
const PRUNE_INTERVAL_MS  = Number(process.env.PRUNE_INTERVAL_MS)      || 5 * 60 * 1000;   // 5 min
const PRUNE_MAX_AGE_MS   = Number(process.env.PRUNE_MAX_AGE_MS)       || 48 * 60 * 60 * 1000; // 48 h
const PRUNE_EXEMPT       = (process.env.PRUNE_EXEMPT || "dv-backlog").split(",").map(s => s.trim()).filter(Boolean);
const WATCHDOG_BIN       = process.env.WATCHDOG_BIN                   || "";
const WORKERS_CONFIG     = process.env.WORKERS_CONFIG                 || "";
const WORKERS_LOG_DIR    = process.env.WORKERS_LOG_DIR                || "./worker-logs";

// Worker definitions loaded from WORKERS_CONFIG JSON file.
// Format: [{ "name": "backend", "ns": "dv", "args": ["backend"] }, ...]
// ns is optional — if set, worker appears only on that namespace tab.
// args are passed directly to WATCHDOG_BIN.
let workerDefs = [];
if (WORKERS_CONFIG) {
  try {
    workerDefs = JSON.parse(readFileSync(WORKERS_CONFIG, "utf8"));
    console.log(`[claude-broker] loaded ${workerDefs.length} worker defs from ${WORKERS_CONFIG}`);
  } catch (e) {
    console.warn(`[claude-broker] WORKERS_CONFIG load failed: ${e.message}`);
  }
}

// name → { pid, proc, startedAt }
// Spawned watchdog processes are detached (own process group) so killing -pid kills the full tree.
const watchdogProcs = new Map();

const startedAt = Date.now();

// ── DB setup ──────────────────────────────────────────────────────────────────

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel     TEXT    NOT NULL,
    sender      TEXT    NOT NULL,
    content     TEXT    NOT NULL,
    created_at  INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_messages_channel_id ON messages(channel, id);

  CREATE TABLE IF NOT EXISTS channel_schemas (
    channel     TEXT    PRIMARY KEY,
    schema      TEXT    NOT NULL,
    strict      INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS capabilities (
    worker      TEXT    PRIMARY KEY,
    owns        TEXT    NOT NULL,
    channels    TEXT    NOT NULL,
    updated_at  INTEGER NOT NULL
  );
`);

// ── Prepared statements ───────────────────────────────────────────────────────

const stmtInsert       = db.prepare("INSERT INTO messages (channel, sender, content, created_at) VALUES (?, ?, ?, ?)");
const stmtSelect       = db.prepare("SELECT id, sender, content, created_at FROM messages WHERE channel = ? AND id > ? ORDER BY id ASC LIMIT ?");
const stmtSelectFilter = db.prepare(`
  SELECT id, sender, content, created_at FROM messages
  WHERE channel = ? AND id > ?
  AND (? IS NULL OR sender = ?)
  AND (? IS NULL OR json_extract(content, '$.type') = ?)
  ORDER BY id ASC LIMIT ?
`);
const stmtSelectFilterMulti = new Map(); // keyed by "nSenders_hasType"

function getMultiSenderStmt(nSenders, hasType) {
  const key = `${nSenders}_${hasType ? 1 : 0}`;
  if (!stmtSelectFilterMulti.has(key)) {
    const ph = Array(nSenders).fill("?").join(",");
    let sql = `SELECT id, sender, content, created_at FROM messages WHERE channel = ? AND id > ? AND sender IN (${ph})`;
    if (hasType) sql += ` AND json_extract(content, '$.type') = ?`;
    sql += ` ORDER BY id ASC LIMIT ?`;
    stmtSelectFilterMulti.set(key, db.prepare(sql));
  }
  return stmtSelectFilterMulti.get(key);
}
const stmtDeleteOne    = db.prepare("DELETE FROM messages WHERE id = ? AND channel = ?");
const stmtChans        = db.prepare("SELECT channel, COUNT(*) AS n, MAX(id) AS last_id, MAX(created_at) AS last_ts FROM messages GROUP BY channel ORDER BY channel");
const stmtPurge        = db.prepare("DELETE FROM messages WHERE channel = ?");
const stmtPruneOlder   = db.prepare("DELETE FROM messages WHERE channel = ? AND created_at < ?");
const stmtPruneAllOld  = db.prepare("DELETE FROM messages WHERE channel NOT IN (SELECT value FROM json_each(?)) AND created_at < ?");
const stmtCheckResult  = db.prepare("SELECT COUNT(*) AS n FROM messages WHERE channel = ? AND json_extract(content, '$.task_id') = ? AND json_extract(content, '$.type') = 'result'");

const stmtSchemaGet    = db.prepare("SELECT channel, schema, strict, updated_at FROM channel_schemas WHERE channel = ?");
const stmtSchemaUpsert = db.prepare(`
  INSERT INTO channel_schemas (channel, schema, strict, updated_at) VALUES (?, ?, ?, ?)
  ON CONFLICT(channel) DO UPDATE SET schema=excluded.schema, strict=excluded.strict, updated_at=excluded.updated_at
`);
const stmtSchemaDel    = db.prepare("DELETE FROM channel_schemas WHERE channel = ?");
const stmtSchemaList   = db.prepare("SELECT channel, strict, updated_at FROM channel_schemas ORDER BY channel");

const stmtLatestPerSender = db.prepare(`
  SELECT m.id, m.sender, m.content, m.created_at
  FROM messages m
  INNER JOIN (
    SELECT sender, MAX(id) AS max_id FROM messages WHERE channel = ? GROUP BY sender
  ) latest ON m.id = latest.max_id
  ORDER BY m.sender
`);

const stmtLatestMsgPerChannel = db.prepare(`
  SELECT m.channel, m.sender, m.content, m.created_at
  FROM messages m
  INNER JOIN (
    SELECT channel, MAX(id) AS max_id FROM messages GROUP BY channel
  ) latest ON m.channel = latest.channel AND m.id = latest.max_id
`);

// Sprint info: latest sprint-boundary note on a control channel
const stmtSprintInfo = db.prepare(`
  SELECT content, created_at FROM messages
  WHERE channel = ?
    AND json_extract(content, '$.type') = 'note'
    AND json_extract(content, '$.subject') LIKE 'sprint-%'
  ORDER BY id DESC LIMIT 1
`);

// Sprint progress: task vs result counts on a status channel
const stmtSprintProgress = db.prepare(`
  SELECT
    COUNT(CASE WHEN json_extract(content, '$.type') = 'task'   THEN 1 END) AS dispatched,
    COUNT(CASE WHEN json_extract(content, '$.type') = 'result' THEN 1 END) AS completed,
    COUNT(CASE WHEN json_extract(content, '$.type') = 'result'
               AND json_extract(content, '$.summary') LIKE '%FAIL%' THEN 1 END) AS failed
  FROM messages WHERE channel = ?
`);

const stmtCapUpsert = db.prepare(`
  INSERT INTO capabilities (worker, owns, channels, updated_at) VALUES (?, ?, ?, ?)
  ON CONFLICT(worker) DO UPDATE SET owns=excluded.owns, channels=excluded.channels, updated_at=excluded.updated_at
`);
const stmtCapList   = db.prepare("SELECT worker, owns, channels, updated_at FROM capabilities ORDER BY worker");
const stmtCapDel    = db.prepare("DELETE FROM capabilities WHERE worker = ?");

// ── AJV schema validation ─────────────────────────────────────────────────────

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validatorCache = new Map();

function getValidator(channel) {
  const row = stmtSchemaGet.get(channel);
  if (!row) { validatorCache.delete(channel); return null; }
  const cached = validatorCache.get(channel);
  if (cached && cached.updated_at === row.updated_at) return { validate: cached.validate, strict: row.strict };
  let validate;
  try { validate = ajv.compile(JSON.parse(row.schema)); }
  catch (e) { console.error(`[claude-broker] schema compile error '${channel}': ${e.message}`); return null; }
  validatorCache.set(channel, { validate, updated_at: row.updated_at });
  return { validate, strict: row.strict };
}

function validateContent(channel, content) {
  const v = getValidator(channel);
  if (!v) return { ok: true };
  let parsed;
  try { parsed = JSON.parse(content); }
  catch (e) { return { ok: false, strict: !!v.strict, errors: `content is not valid JSON: ${e.message}` }; }
  if (v.validate(parsed)) return { ok: true };
  const errors = (v.validate.errors || []).map(e => `${e.instancePath || "/"} ${e.message}`).join("; ");
  return { ok: false, strict: !!v.strict, errors };
}

// ── Message bus ───────────────────────────────────────────────────────────────

const messageBus = new EventEmitter();
messageBus.setMaxListeners(0);

function formatRows(rows, channel, sinceId) {
  if (rows.length === 0) return { content: [{ type: "text", text: `No new messages on '${channel}' (since_id=${sinceId}).` }] };
  const lines = rows.map(r => `[#${r.id}] ${new Date(r.created_at).toISOString()} <${r.sender}>: ${r.content}`);
  lines.push(`\n(next since_id: ${rows[rows.length - 1].id})`);
  return { content: [{ type: "text", text: lines.join("\n") }] };
}

function fetchFiltered(channel, sinceId, lim, filterSender, filterType) {
  if (filterSender && filterSender.includes(",")) {
    const senders = filterSender.split(",").map(s => s.trim()).filter(Boolean);
    const stmt = getMultiSenderStmt(senders.length, !!filterType);
    const params = [channel, sinceId, ...senders, ...(filterType ? [filterType] : []), lim];
    return stmt.all(...params);
  }
  if (filterSender || filterType) {
    return stmtSelectFilter.all(
      channel, sinceId,
      filterSender ?? null, filterSender ?? null,
      filterType   ?? null, filterType   ?? null,
      lim
    );
  }
  return stmtSelect.all(channel, sinceId, lim);
}

function fetchFeedRows(channels, signalOnly, limit) {
  if (channels.length === 0) return [];
  const typeClause = signalOnly
    ? `AND json_extract(content, '$.type') IN ('result', 'question', 'error')`
    : '';
  const ph = channels.map(() => '?').join(',');
  return db.prepare(
    `SELECT id, channel, sender, content, created_at FROM messages WHERE channel IN (${ph}) ${typeClause} ORDER BY id DESC LIMIT ?`
  ).all(...channels, limit);
}

// ── Background auto-pruning ───────────────────────────────────────────────────

function runAutoPrune() {
  const cutoff = Date.now() - PRUNE_MAX_AGE_MS;
  const exemptJson = JSON.stringify(PRUNE_EXEMPT);
  const r = stmtPruneAllOld.run(exemptJson, cutoff);
  if (r.changes > 0) {
    console.log(`[claude-broker] auto-pruned ${r.changes} msgs older than ${PRUNE_MAX_AGE_MS / 3600000}h (exempt: ${PRUNE_EXEMPT.join(", ") || "none"})`);
  }
}
setInterval(runAutoPrune, PRUNE_INTERVAL_MS).unref();

// ── MCP server builder ────────────────────────────────────────────────────────

function buildServer() {
  const server = new McpServer({ name: "claude-broker", version: "2.0.0" });

  // ── send_message ────────────────────────────────────────────────────────────
  server.registerTool("send_message", {
    title: "Send a message",
    description: "Post a message to a named channel. Other Claude Code sessions can read it via read_messages or wait_for_messages.",
    inputSchema: {
      channel: z.string().min(1).describe("Channel name."),
      sender:  z.string().min(1).describe("Sender identifier."),
      content: z.string().min(1).describe("Message body — plain text or JSON-as-string."),
    },
  }, async ({ channel, sender, content }) => {
    const check = validateContent(channel, content);
    if (!check.ok && check.strict) {
      return { content: [{ type: "text", text: `schema validation failed on '${channel}': ${check.errors}. Call get_channel_schema('${channel}') to see required fields.` }], isError: true };
    }
    const now = Date.now();
    const r = stmtInsert.run(channel, sender, content, now);
    messageBus.emit(`msg:${channel}`, { id: r.lastInsertRowid, sender, content });
    const warn = (!check.ok && !check.strict) ? `  [WARN schema mismatch: ${check.errors}]` : "";
    return { content: [{ type: "text", text: `Sent #${r.lastInsertRowid} to '${channel}' as '${sender}' at ${new Date(now).toISOString()}.${warn}` }] };
  });

  // ── read_messages ───────────────────────────────────────────────────────────
  server.registerTool("read_messages", {
    title: "Read messages",
    description: "Read messages from a channel newer than since_id. Supports optional sender and type filters.",
    inputSchema: {
      channel:       z.string().min(1),
      since_id:      z.number().int().nonnegative().optional().describe("Only return msgs with id > since_id. Omit for all."),
      limit:         z.number().int().positive().max(500).optional().describe("Max messages (default 100)."),
      filter_sender: z.string().optional().describe("Only return messages from this sender."),
      filter_type:   z.string().optional().describe("Only return messages where JSON content has type === this value."),
    },
  }, async ({ channel, since_id, limit, filter_sender, filter_type }) => {
    const rows = fetchFiltered(channel, since_id ?? 0, limit ?? 100, filter_sender, filter_type);
    return formatRows(rows, channel, since_id ?? 0);
  });

  // ── wait_for_messages ───────────────────────────────────────────────────────
  server.registerTool("wait_for_messages", {
    title: "Wait for new messages",
    description: "Long-polls until a matching message arrives or timeout_ms elapses. Supports filter_sender and filter_type — waits until a message matching the filters arrives, skipping non-matching ones. Default timeout 25s, max 300s.",
    inputSchema: {
      channel:       z.string().min(1),
      since_id:      z.number().int().nonnegative().optional(),
      timeout_ms:    z.number().int().positive().max(300000).optional().describe("Max wait in ms. Default 25000. Max 300000."),
      limit:         z.number().int().positive().max(500).optional(),
      filter_sender: z.string().optional().describe("Only wake/return for messages from this sender."),
      filter_type:   z.string().optional().describe("Only wake/return for messages where JSON content.type === this value."),
    },
  }, async ({ channel, since_id, timeout_ms, limit, filter_sender, filter_type }) => {
    const sinceId = since_id ?? 0;
    const lim     = limit ?? 100;
    const timeout = Math.min(timeout_ms ?? 25000, 300000);
    const event   = `msg:${channel}`;

    // Check for existing matching messages immediately
    const immediate = fetchFiltered(channel, sinceId, lim, filter_sender, filter_type);
    if (immediate.length > 0) return formatRows(immediate, channel, sinceId);

    return new Promise((resolve) => {
      let timer = null;

      function cleanup() {
        if (timer) { clearTimeout(timer); timer = null; }
        messageBus.off(event, onMessage);
      }

      function onMessage() {
        const rows = fetchFiltered(channel, sinceId, lim, filter_sender, filter_type);
        if (rows.length > 0) {
          cleanup();
          resolve(formatRows(rows, channel, sinceId));
        }
        // Non-matching message — keep listener registered, timer keeps running
      }

      timer = setTimeout(() => {
        cleanup();
        resolve({ content: [{ type: "text", text: `No new messages on '${channel}' within ${timeout}ms (since_id=${sinceId}).` }] });
      }, timeout);

      messageBus.on(event, onMessage);
    });
  });

  // ── delete_message ──────────────────────────────────────────────────────────
  server.registerTool("delete_message", {
    title: "Delete a single message",
    description: "Delete one message by id. Requires the channel for safety — prevents deleting a message from the wrong channel by accident.",
    inputSchema: {
      channel: z.string().min(1).describe("Channel the message belongs to."),
      id:      z.number().int().positive().describe("Message id (from the #N prefix in read_messages output)."),
    },
  }, async ({ channel, id }) => {
    const r = stmtDeleteOne.run(id, channel);
    if (r.changes === 0) return { content: [{ type: "text", text: `Message #${id} not found on '${channel}'.` }], isError: true };
    return { content: [{ type: "text", text: `Deleted message #${id} from '${channel}'.` }] };
  });

  // ── post_gated_message ──────────────────────────────────────────────────────
  server.registerTool("post_gated_message", {
    title: "Post a message gated on task results",
    description: "Post a message only after all listed task_ids have a type:result message on the watch_channel. Blocks until all dependencies are satisfied or timeout_ms elapses. Use this to enforce depends_on at the broker level rather than relying on workers to self-block.",
    inputSchema: {
      channel:       z.string().min(1).describe("Channel to post to when all deps are satisfied."),
      sender:        z.string().min(1),
      content:       z.string().min(1).describe("Message body to post when unblocked."),
      depends_on:    z.array(z.string().min(1)).min(1).describe("Array of task_ids to wait for. Each must have a type:result message on watch_channel."),
      watch_channel: z.string().optional().describe("Channel to watch for result messages. Default: dv-status."),
      timeout_ms:    z.number().int().positive().max(300000).optional().describe("Max wait in ms. Default 60000. Max 300000."),
    },
  }, async ({ channel, sender, content, depends_on, watch_channel, timeout_ms }) => {
    const watchChan = watch_channel ?? "dv-status";
    const timeout   = Math.min(timeout_ms ?? 60000, 300000);
    const event     = `msg:${watchChan}`;

    function allSatisfied() {
      return depends_on.every(taskId => stmtCheckResult.get(watchChan, taskId).n > 0);
    }

    function pendingList() {
      return depends_on.filter(taskId => stmtCheckResult.get(watchChan, taskId).n === 0);
    }

    if (allSatisfied()) {
      const check = validateContent(channel, content);
      if (!check.ok && check.strict) {
        return { content: [{ type: "text", text: `schema validation failed on '${channel}': ${check.errors}` }], isError: true };
      }
      const now = Date.now();
      const r = stmtInsert.run(channel, sender, content, now);
      messageBus.emit(`msg:${channel}`, { id: r.lastInsertRowid, sender, content });
      return { content: [{ type: "text", text: `All deps satisfied immediately. Sent #${r.lastInsertRowid} to '${channel}'.` }] };
    }

    return new Promise((resolve) => {
      let timer = null;

      function cleanup() {
        if (timer) { clearTimeout(timer); timer = null; }
        messageBus.off(event, onResult);
      }

      function onResult() {
        if (!allSatisfied()) return; // Still waiting on some deps
        cleanup();
        const check = validateContent(channel, content);
        if (!check.ok && check.strict) {
          resolve({ content: [{ type: "text", text: `Deps satisfied but schema validation failed: ${check.errors}` }], isError: true });
          return;
        }
        const now = Date.now();
        const r = stmtInsert.run(channel, sender, content, now);
        messageBus.emit(`msg:${channel}`, { id: r.lastInsertRowid, sender, content });
        resolve({ content: [{ type: "text", text: `Deps satisfied. Sent #${r.lastInsertRowid} to '${channel}'.` }] });
      }

      timer = setTimeout(() => {
        cleanup();
        resolve({ content: [{ type: "text", text: `Timed out after ${timeout}ms waiting for deps: ${pendingList().join(", ")}` }], isError: true });
      }, timeout);

      messageBus.on(event, onResult);
    });
  });

  // ── check_result ────────────────────────────────────────────────────────────
  server.registerTool("check_result", {
    title: "Check if a task result exists",
    description: "Returns whether a type:result message with the given task_id exists on a channel. Use this for idempotency checks — a single indexed SQL query vs. transferring the full channel history with read_messages(since_id=0).",
    inputSchema: {
      channel: z.string().min(1).describe("Channel to check (typically dv-status)."),
      task_id: z.string().min(1).describe("The task_id to look for."),
    },
  }, async ({ channel, task_id }) => {
    const row = stmtCheckResult.get(channel, task_id);
    return { content: [{ type: "text", text: JSON.stringify({ found: row.n > 0, task_id, channel }) }] };
  });

  // ── has_messages ────────────────────────────────────────────────────────────
  server.registerTool("has_messages", {
    title: "Check if a channel has messages (no content transfer)",
    description: "Returns {pending, max_id, channel} — lightweight pre-check before read_messages. Use to gate dv-control reads: skip entirely if pending=false. Zero content transferred.",
    inputSchema: {
      channel:  z.string().min(1).describe("Channel to check."),
      since_id: z.number().int().min(0).default(0).describe("Only count messages with id > since_id."),
    },
  }, async ({ channel, since_id }) => {
    const row = db.prepare(
      "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS max_id FROM messages WHERE channel = ? AND id > ?"
    ).get(channel, since_id ?? 0);
    return { content: [{ type: "text", text: JSON.stringify({ pending: row.n > 0, max_id: row.max_id, channel }) }] };
  });

  // ── read_last ────────────────────────────────────────────────────────────────
  server.registerTool("read_last", {
    title: "Read the last N messages from a channel",
    description: "Returns the N most recent messages in chronological order (oldest-first). Use for compaction recovery instead of read_messages(since_id=0) — transfers only the tail of history, not the full archive. Default n=20, max 100.",
    inputSchema: {
      channel: z.string().min(1).describe("Channel to read."),
      n:       z.number().int().min(1).max(100).default(20).describe("Number of most-recent messages to return."),
    },
  }, async ({ channel, n }) => {
    const rows = db.prepare(
      "SELECT id, sender, content, created_at FROM messages WHERE channel = ? ORDER BY id DESC LIMIT ?"
    ).all(channel, n ?? 20);
    rows.reverse();
    if (!rows.length) return { content: [{ type: "text", text: "No messages." }] };
    const text = rows.map(r => `[${r.id}] ${r.sender}: ${r.content}`).join("\n");
    return { content: [{ type: "text", text: text }] };
  });

  // ── list_channels ───────────────────────────────────────────────────────────
  server.registerTool("list_channels", {
    title: "List channels",
    description: "List all channels with message counts, latest id, and last activity.",
    inputSchema: {},
  }, async () => {
    const rows = stmtChans.all();
    if (rows.length === 0) return { content: [{ type: "text", text: "(no channels yet)" }] };
    const text = rows.map(r => `${r.channel}\t${r.n} msgs\tlast_id=${r.last_id}\tlast_activity=${new Date(r.last_ts).toISOString()}`).join("\n");
    return { content: [{ type: "text", text }] };
  });

  // ── purge_channel ───────────────────────────────────────────────────────────
  server.registerTool("purge_channel", {
    title: "Purge channel",
    description: "Delete messages from a channel. Without older_than_ms, deletes all messages. With older_than_ms, deletes only messages older than that many milliseconds.",
    inputSchema: {
      channel:       z.string().min(1),
      older_than_ms: z.number().int().positive().optional().describe("If set, only delete messages older than this many ms. Omit to delete all."),
    },
  }, async ({ channel, older_than_ms }) => {
    let r;
    if (older_than_ms != null) {
      r = stmtPruneOlder.run(channel, Date.now() - older_than_ms);
      return { content: [{ type: "text", text: `Pruned ${r.changes} messages older than ${older_than_ms}ms from '${channel}'.` }] };
    }
    r = stmtPurge.run(channel);
    return { content: [{ type: "text", text: `Purged ${r.changes} messages from '${channel}'.` }] };
  });

  // ── register_capability ─────────────────────────────────────────────────────
  server.registerTool("register_capability", {
    title: "Register worker capability",
    description: "Declare what a worker owns and which channels it uses. Workers should call this at startup so the orchestrator can route by capability instead of hardcoded role names.",
    inputSchema: {
      worker:   z.string().min(1).describe("Worker identifier, e.g. 'backend', 'frontend'."),
      owns:     z.array(z.string()).describe("List of domains/responsibilities this worker owns."),
      channels: z.array(z.string()).describe("List of channels this worker reads/writes."),
    },
  }, async ({ worker, owns, channels }) => {
    const now = Date.now();
    stmtCapUpsert.run(worker, JSON.stringify(owns), JSON.stringify(channels), now);
    return { content: [{ type: "text", text: `Registered capabilities for '${worker}' at ${new Date(now).toISOString()}.` }] };
  });

  // ── list_capabilities ───────────────────────────────────────────────────────
  server.registerTool("list_capabilities", {
    title: "List worker capabilities",
    description: "Show the capability registry — what each worker owns and which channels it uses.",
    inputSchema: {},
  }, async () => {
    const rows = stmtCapList.all();
    if (rows.length === 0) return { content: [{ type: "text", text: "(no capabilities registered yet)" }] };
    const lines = rows.map(r =>
      `${r.worker}\towns=${r.owns}\tchannels=${r.channels}\tupdated=${new Date(r.updated_at).toISOString()}`
    );
    return { content: [{ type: "text", text: lines.join("\n") }] };
  });

  // ── deregister_capability ───────────────────────────────────────────────────
  server.registerTool("deregister_capability", {
    title: "Deregister worker capability",
    description: "Remove a worker from the capability registry.",
    inputSchema: {
      worker: z.string().min(1),
    },
  }, async ({ worker }) => {
    const r = stmtCapDel.run(worker);
    return { content: [{ type: "text", text: r.changes ? `Deregistered '${worker}'.` : `No capability entry found for '${worker}'.` }] };
  });

  // ── get_latest_per_sender ───────────────────────────────────────────────────
  server.registerTool("get_latest_per_sender", {
    title: "Get latest message per distinct sender",
    description: "Return the most recent message per sender on a channel. Designed for heartbeat/telemetry channels — one snapshot per worker without paginating history.",
    inputSchema: {
      channel: z.string().min(1),
    },
  }, async ({ channel }) => {
    const rows = stmtLatestPerSender.all(channel);
    if (rows.length === 0) return { content: [{ type: "text", text: `No messages on '${channel}'.` }] };
    const lines = rows.map(r => `[#${r.id}] ${new Date(r.created_at).toISOString()} <${r.sender}>: ${r.content}`);
    return { content: [{ type: "text", text: lines.join("\n") }] };
  });

  // ── register_channel_schema ─────────────────────────────────────────────────
  server.registerTool("register_channel_schema", {
    title: "Register or replace a channel schema",
    description: "Bind a JSON Schema (draft-07) to a channel. Messages are validated on send_message.",
    inputSchema: {
      channel: z.string().min(1),
      schema:  z.string().min(2).describe("JSON Schema as a JSON-encoded string."),
      strict:  z.boolean().optional().describe("If true, invalid messages are rejected. Default false (warn-only)."),
    },
  }, async ({ channel, schema, strict }) => {
    let parsed;
    try { parsed = JSON.parse(schema); }
    catch (e) { return { content: [{ type: "text", text: `schema is not valid JSON: ${e.message}` }], isError: true }; }
    try { ajv.compile(parsed); }
    catch (e) { return { content: [{ type: "text", text: `schema does not compile as JSON Schema: ${e.message}` }], isError: true }; }
    const now = Date.now();
    stmtSchemaUpsert.run(channel, schema, strict ? 1 : 0, now);
    validatorCache.delete(channel);
    return { content: [{ type: "text", text: `Registered schema for '${channel}' (strict=${strict ? "on" : "off"}) at ${new Date(now).toISOString()}.` }] };
  });

  // ── get_channel_schema ──────────────────────────────────────────────────────
  server.registerTool("get_channel_schema", {
    title: "Get a channel's schema",
    description: "Return the JSON Schema bound to a channel, or indicate it is free-form.",
    inputSchema: {
      channel: z.string().min(1),
    },
  }, async ({ channel }) => {
    const row = stmtSchemaGet.get(channel);
    if (!row) return { content: [{ type: "text", text: `No schema registered for '${channel}' (free-form).` }] };
    return { content: [{ type: "text", text: `Channel: ${row.channel}\nStrict: ${row.strict ? "on" : "off (warn-only)"}\nUpdated: ${new Date(row.updated_at).toISOString()}\n\n${row.schema}` }] };
  });

  // ── clear_channel_schema ────────────────────────────────────────────────────
  server.registerTool("clear_channel_schema", {
    title: "Clear a channel's schema",
    description: "Remove the schema binding from a channel. The channel reverts to free-form.",
    inputSchema: {
      channel: z.string().min(1),
    },
  }, async ({ channel }) => {
    stmtSchemaDel.run(channel);
    validatorCache.delete(channel);
    return { content: [{ type: "text", text: `Cleared schema for '${channel}'.` }] };
  });

  // ── list_channel_schemas ────────────────────────────────────────────────────
  server.registerTool("list_channel_schemas", {
    title: "List channel schemas",
    description: "Show all channels with a schema bound, including strict-mode status.",
    inputSchema: {},
  }, async () => {
    const rows = stmtSchemaList.all();
    if (rows.length === 0) return { content: [{ type: "text", text: "(no channel schemas registered)" }] };
    const text = rows.map(r => `${r.channel}\tstrict=${r.strict ? "on" : "off"}\tupdated=${new Date(r.updated_at).toISOString()}`).join("\n");
    return { content: [{ type: "text", text }] };
  });

  // ── list_workers ────────────────────────────────────────────────────────────
  server.registerTool("list_workers", {
    title: "List workers",
    description: "List all workers defined in the worker config with their running state, PID, and uptime. Use this to check which watchdogs are active before starting or stopping one.",
    inputSchema: {},
  }, async () => {
    if (!workerDefs.length) return { content: [{ type: "text", text: "(no workers configured — set WORKERS_CONFIG)" }] };
    const lines = workerDefs.map(w => {
      const entry = watchdogProcs.get(w.name);
      const state = entry
        ? `running  pid=${entry.pid}  uptime=${Math.floor((Date.now() - entry.startedAt) / 1000)}s`
        : "stopped";
      return `${w.name}\t${state}`;
    });
    return { content: [{ type: "text", text: lines.join("\n") }] };
  });

  // ── start_worker ─────────────────────────────────────────────────────────────
  server.registerTool("start_worker", {
    title: "Start a worker watchdog",
    description: "Start the watchdog process for a named worker. The worker must be defined in the worker config (WORKERS_CONFIG). Returns the PID on success. No-ops (returns current PID) if already running.",
    inputSchema: {
      name: z.string().min(1).describe("Worker name as defined in WORKERS_CONFIG, e.g. 'backend', 'platform-orch'."),
    },
  }, async ({ name }) => {
    if (!WATCHDOG_BIN)
      return { content: [{ type: "text", text: "WATCHDOG_BIN not configured on broker — cannot start workers." }], isError: true };
    const def = workerDefs.find(w => w.name === name);
    if (!def)
      return { content: [{ type: "text", text: `Worker "${name}" not found in config. Use list_workers to see available workers.` }], isError: true };
    if (watchdogProcs.has(name)) {
      const pid = watchdogProcs.get(name).pid;
      return { content: [{ type: "text", text: `Worker "${name}" already running (pid ${pid}).` }] };
    }
    let outFd, errFd;
    try {
      mkdirSync(WORKERS_LOG_DIR, { recursive: true });
      outFd = openSync(`${WORKERS_LOG_DIR}/${name}.out.log`, "a");
      errFd = openSync(`${WORKERS_LOG_DIR}/${name}.err.log`, "a");
    } catch (e) {
      return { content: [{ type: "text", text: `Failed to open log files: ${e.message}` }], isError: true };
    }
    let proc;
    try {
      proc = spawn(WATCHDOG_BIN, def.args || [], { stdio: ["ignore", outFd, errFd], detached: true });
      proc.unref();
      closeSync(outFd);
      closeSync(errFd);
    } catch (e) {
      return { content: [{ type: "text", text: `spawn failed: ${e.message}` }], isError: true };
    }
    proc.on("exit", (code) => {
      console.log(`[claude-broker] watchdog "${name}" exited (code ${code ?? "?"})`);
      watchdogProcs.delete(name);
    });
    watchdogProcs.set(name, { pid: proc.pid, proc, startedAt: Date.now() });
    console.log(`[claude-broker] started watchdog "${name}" via MCP (pid ${proc.pid})`);
    return { content: [{ type: "text", text: `Started "${name}" (pid ${proc.pid}). Logs: ${WORKERS_LOG_DIR}/${name}.{out,err}.log` }] };
  });

  // ── stop_worker ──────────────────────────────────────────────────────────────
  server.registerTool("stop_worker", {
    title: "Stop a worker watchdog",
    description: "Stop the watchdog process for a named worker (SIGTERM to entire process group — kills watchdog + any in-flight Claude session). The worker will not restart until started again.",
    inputSchema: {
      name: z.string().min(1).describe("Worker name as defined in WORKERS_CONFIG, e.g. 'backend', 'platform-orch'."),
    },
  }, async ({ name }) => {
    const entry = watchdogProcs.get(name);
    if (!entry)
      return { content: [{ type: "text", text: `Worker "${name}" is not running.` }], isError: true };
    try {
      process.kill(-entry.pid, "SIGTERM");
      watchdogProcs.delete(name);
      console.log(`[claude-broker] stopped watchdog "${name}" via MCP (pid ${entry.pid})`);
      return { content: [{ type: "text", text: `Stopped "${name}" (pid ${entry.pid}).` }] };
    } catch (e) {
      watchdogProcs.delete(name);
      return { content: [{ type: "text", text: `kill failed: ${e.message}` }], isError: true };
    }
  });

  return server;
}

// ── Express app ───────────────────────────────────────────────────────────────

const app = express();

// Reusable auth middleware — pass as route-level middleware on protected endpoints.
function auth(req, res, next) {
  if (!SHARED_SECRET) return next();
  const header = req.headers.authorization || "";
  if (header !== `Bearer ${SHARED_SECRET}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req, res) => res.json({ ok: true, ts: Date.now(), uptime_s: Math.floor((Date.now() - startedAt) / 1000) }));

// Lightweight inbox pre-check — no Claude session needed.
// Returns {pending, count, max_id} so watchdog can avoid starting Claude when idle.
app.get("/inbox", (req, res) => {
  const { channel, since_id = "0" } = req.query;
  if (!channel) return res.status(400).json({ error: "channel required" });
  const sinceId = parseInt(since_id, 10);
  if (isNaN(sinceId)) return res.status(400).json({ error: "since_id must be numeric" });
  const row = db.prepare(
    "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS max_id FROM messages WHERE channel = ? AND id > ?"
  ).get(channel, sinceId);
  res.json({ channel, since_id: sinceId, pending: row.n > 0, count: row.n, max_id: row.max_id });
});

// Batch inbox pre-check — single round-trip for N channels.
// POST body: { "channel-name": since_id, ... }
// Response:  { "channel-name": { pending, count, max_id }, ... }
app.post("/inbox/batch", (req, res) => {
  const body = req.body;
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return res.status(400).json({ error: "body must be an object mapping channel to since_id" });
  }
  const stmt = db.prepare(
    "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS max_id FROM messages WHERE channel = ? AND id > ?"
  );
  const result = {};
  for (const [channel, rawSinceId] of Object.entries(body)) {
    const sinceId = parseInt(rawSinceId, 10);
    if (isNaN(sinceId)) { result[channel] = { error: "since_id must be numeric" }; continue; }
    const row = stmt.get(channel, sinceId);
    result[channel] = { pending: row.n > 0, count: row.n, max_id: row.max_id };
  }
  res.json(result);
});

// ── Worker control endpoints ───────────────────────────────────────────────────

app.get("/workers", (_req, res) => {
  res.json(workerDefs.map(w => ({
    name:      w.name,
    ns:        w.ns   || null,
    args:      w.args || [],
    running:   watchdogProcs.has(w.name),
    pid:       watchdogProcs.get(w.name)?.pid    ?? null,
    startedAt: watchdogProcs.get(w.name)?.startedAt ?? null,
  })));
});

app.post("/workers/:name/start", (req, res) => {
  const { name } = req.params;
  const def = workerDefs.find(w => w.name === name);
  if (!def)                    return res.status(404).json({ error: `Worker "${name}" not in config` });
  if (watchdogProcs.has(name)) return res.status(409).json({ error: `Worker "${name}" already running (pid ${watchdogProcs.get(name).pid})` });
  if (!WATCHDOG_BIN)           return res.status(503).json({ error: "WATCHDOG_BIN not configured" });

  let outFd, errFd;
  try {
    mkdirSync(WORKERS_LOG_DIR, { recursive: true });
    outFd = openSync(`${WORKERS_LOG_DIR}/${name}.out.log`, "a");
    errFd = openSync(`${WORKERS_LOG_DIR}/${name}.err.log`, "a");
  } catch (e) {
    return res.status(500).json({ error: `Failed to open log files: ${e.message}` });
  }

  let proc;
  try {
    proc = spawn(WATCHDOG_BIN, def.args || [], {
      stdio:    ["ignore", outFd, errFd],
      detached: true,
    });
    proc.unref();
    closeSync(outFd);
    closeSync(errFd);
  } catch (e) {
    return res.status(500).json({ error: `spawn failed: ${e.message}` });
  }

  proc.on("exit", (code) => {
    console.log(`[claude-broker] watchdog "${name}" exited (code ${code ?? "?"})`);
    watchdogProcs.delete(name);
  });

  watchdogProcs.set(name, { pid: proc.pid, proc, startedAt: Date.now() });
  console.log(`[claude-broker] started watchdog "${name}" (pid ${proc.pid}) → logs: ${WORKERS_LOG_DIR}/${name}.{out,err}.log`);
  res.json({ ok: true, name, pid: proc.pid });
});

app.post("/workers/:name/stop", (req, res) => {
  const { name } = req.params;
  const entry = watchdogProcs.get(name);
  if (!entry) return res.status(404).json({ error: `Worker "${name}" not running` });

  try {
    process.kill(-entry.pid, "SIGTERM"); // kill entire process group (watchdog + in-flight claude)
    watchdogProcs.delete(name);
    console.log(`[claude-broker] stopped watchdog "${name}" (pid ${entry.pid})`);
    res.json({ ok: true, name, pid: entry.pid });
  } catch (e) {
    watchdogProcs.delete(name);
    res.status(500).json({ error: e.message });
  }
});

// Dashboard helpers
function nsOf(channel) {
  const idx = channel.indexOf("-");
  return idx > 0 ? channel.slice(0, idx) : channel;
}
function agoStr(ts) {
  const s = Math.floor((Date.now() - ts) / 1000);
  return s < 60 ? `${s}s ago` : s < 3600 ? `${Math.floor(s/60)}m ago` : `${Math.floor(s/3600)}h ago`;
}
function escHtml(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

// Dashboard
app.get("/dashboard", (req, res) => {
  const now         = Date.now();
  const allChannels = stmtChans.all();
  const allCaps     = stmtCapList.all();
  const allSchemas  = stmtSchemaList.all();
  const latestMsgs  = stmtLatestMsgPerChannel.all();  // last msg per channel

  // Build preview map: channel -> {sender, content, created_at}
  const previewMap = new Map(latestMsgs.map(r => [r.channel, r]));

  // Auto-detect namespaces
  const nsSet = new Set(allChannels.map(r => nsOf(r.channel)).filter(Boolean));
  allCaps.forEach(r => { try { JSON.parse(r.channels).forEach(c => nsSet.add(nsOf(c))); } catch {} });
  const namespaces = [...nsSet].sort();

  const selectedNs = req.query.ns || "all";
  const isAll      = selectedNs === "all";
  const live       = req.query.live === "1";

  function matchesNs(ch) { return isAll || nsOf(ch) === selectedNs; }

  const channels  = allChannels.filter(r => matchesNs(r.channel));
  const caps      = allCaps.filter(r => {
    if (isAll) return true;
    try { return JSON.parse(r.channels).some(c => nsOf(c) === selectedNs); } catch { return false; }
  });
  const schemas   = allSchemas.filter(r => matchesNs(r.channel));
  const schemaSet = new Set(schemas.map(s => s.channel));

  // ── Sprint info ───────────────────────────────────────────────────────────────
  // Collect sprint info per namespace (or just for the selected one)
  function getSprintInfo(ns) {
    const controlChan = `${ns}-control`;
    const statusChan  = `${ns}-status`;
    const row = stmtSprintInfo.get(controlChan);
    if (!row) return null;
    let subject = "", body = "";
    try { const p = JSON.parse(row.content); subject = p.subject || ""; body = typeof p.body === "string" ? p.body : ""; } catch {}
    // Extract slug: "sprint-2026-06-04-foo opened…" → "sprint-2026-06-04-foo"
    const slug = subject.split(/\s+/)[0] || subject;
    const progress = stmtSprintProgress.get(statusChan) || { dispatched: 0, completed: 0, failed: 0 };
    return { ns, slug, startedAt: row.created_at, body, ...progress };
  }

  const sprintInfos = isAll
    ? namespaces.map(getSprintInfo).filter(Boolean)
    : [getSprintInfo(selectedNs)].filter(Boolean);

  // ── Worker liveness from telemetry ──────────────────────────────────────────
  // For selected ns, read <ns>-telemetry (or all *-telemetry channels if "all")
  const telemetryChan = isAll ? null : `${selectedNs}-telemetry`;
  const telemetryRows = telemetryChan
    ? stmtLatestPerSender.all(telemetryChan).map(r => ({...r, _ns: selectedNs}))
    : (() => {
        const tChans = allChannels.filter(r => r.channel.endsWith("-telemetry")).map(r => r.channel);
        return tChans.flatMap(c => {
          const ns = nsOf(c);
          return stmtLatestPerSender.all(c).map(r => ({...r, _ns: ns}));
        });
      })();

  // channel → message count, used for inbox-pending lookups
  const channelCountMap = new Map(allChannels.map(r => [r.channel, r.n]));
  const SPECIAL_SUFFIXES = new Set(["status", "control", "telemetry", "backlog"]);

  const workers = telemetryRows.map(row => {
    let hb = {};
    try { hb = JSON.parse(row.content); } catch {}
    const ageSec     = Math.floor((now - row.created_at) / 1000);
    const state      = hb.activity?.state || hb.state || "unknown";
    const task       = hb.activity?.current_task || null;
    const cost       = hb.cost_since_start?.estimated_usd ?? null;
    const rotating   = hb.context?.rotation_recommended === true;
    const inboxChan  = `${row._ns}-${row.sender}`;
    const inboxCount = channelCountMap.get(inboxChan) || 0;

    // on-demand model: offline between tasks is normal, not an alarm
    let status;
    if (state === "working" && ageSec <= 600) {
      status = "running";
    } else if (state === "working" && ageSec > 600) {
      status = "crashed";
    } else if (ageSec <= 120) {
      status = inboxCount > 0 ? "queued" : "idle";
    } else {
      status = inboxCount > 0 ? "queued" : "offline";
    }

    return { sender: row.sender, _ns: row._ns, state, task, cost, ageSec, status, rotating, inboxCount };
  });

  // Also surface workers with pending inbox but no telemetry (never ran or pre-first-heartbeat crash)
  if (!isAll) {
    const coveredChans = new Set(workers.map(w => `${w._ns}-${w.sender}`));
    allChannels
      .filter(r => {
        if (nsOf(r.channel) !== selectedNs || r.n === 0) return false;
        const suffix = r.channel.slice(selectedNs.length + 1);
        if (SPECIAL_SUFFIXES.has(suffix) || suffix.endsWith("-orch")) return false;
        return !coveredChans.has(r.channel);
      })
      .forEach(r => workers.push({
        sender: r.channel.slice(selectedNs.length + 1),
        _ns: selectedNs,
        state: "unknown",
        task: null,
        cost: null,
        ageSec: null,
        status: "queued",
        rotating: false,
        inboxCount: r.n,
      }));
  }

  workers.sort((a,b) => a.sender.localeCompare(b.sender));

  // ── Merge configured workers not yet in telemetry or inbox lists ──────────────
  if (!isAll && WATCHDOG_BIN) {
    const coveredNames = new Set(workers.map(w => w.sender));
    workerDefs
      .filter(d => (!d.ns || d.ns === selectedNs) && !coveredNames.has(d.name))
      .forEach(d => workers.push({
        sender:    d.name,
        _ns:       selectedNs,
        state:     "never-started",
        task:      null, cost: null, ageSec: null,
        status:    "offline",
        rotating:  false,
        inboxCount: channelCountMap.get(`${selectedNs}-${d.name}`) || 0,
      }));
    workers.sort((a,b) => a.sender.localeCompare(b.sender));
  }

  // ── Activity feed ─────────────────────────────────────────────────────────────
  const feedSignalOnly = req.query.feed !== "all";
  const statusChannels = isAll
    ? allChannels.filter(r => r.channel.endsWith("-status")).map(r => r.channel)
    : allChannels.some(r => r.channel === `${selectedNs}-status`) ? [`${selectedNs}-status`] : [];
  const feedRows = fetchFeedRows(statusChannels, feedSignalOnly, 50);
  const questionRows = feedRows.filter(r => {
    try { return JSON.parse(r.content).type === "question"; } catch { return false; }
  });

  const feedHref = (showAll) => {
    const p = [];
    if (selectedNs !== "all") p.push(`ns=${selectedNs}`);
    if (live) p.push("live=1");
    if (showAll) p.push("feed=all");
    return `/dashboard${p.length ? "?" + p.join("&") : ""}`;
  };

  const renderFeedRow = (r) => {
    let parsed = {};
    try { parsed = JSON.parse(r.content); } catch {}
    const type    = parsed.type || "?";
    const rawText = parsed.summary || parsed.subject || parsed.body || parsed.text || r.content;
    const summary = String(rawText).slice(0, 120);
    const taskTag = parsed.task_id
      ? `<span class="feed-taskid">${escHtml(String(parsed.task_id).slice(-12))}</span>` : "";
    const chanTag = isAll ? `<span class="feed-chan">${escHtml(r.channel)}</span>` : "";
    let badgeClass;
    if      (type === "result")   badgeClass = "feed-result";
    else if (type === "question") badgeClass = "feed-question";
    else if (type === "error")    badgeClass = "feed-error";
    else if (type === "task")     badgeClass = "feed-task";
    else                          badgeClass = "feed-other";
    return `<tr class="${type === "question" ? "feed-row-q" : ""}">
      <td class="feed-time">${agoStr(r.created_at)}</td>
      <td><span class="feed-badge ${badgeClass}">${escHtml(type)}</span></td>
      <td class="feed-sender">${escHtml(r.sender)}${chanTag}</td>
      <td class="feed-content">${escHtml(summary)}${summary.length >= 120 ? "…" : ""}${taskTag}</td>
    </tr>`;
  };

  const questionsAlertHtml = questionRows.length > 0 ? (() => {
    const items = questionRows.slice(0, 5).map(r => {
      let p = {};
      try { p = JSON.parse(r.content); } catch {}
      const from = escHtml(p.from || r.sender);
      const body = escHtml(String(p.body || p.subject || r.content).slice(0, 200));
      const tid  = p.task_id ? ` <span class="feed-taskid">[${escHtml(p.task_id)}]</span>` : "";
      return `<div class="questions-alert-item"><strong>${from}</strong>: ${body}${tid}</div>`;
    }).join("");
    return `<div class="questions-alert">
  <div class="questions-alert-title">⚠ ${questionRows.length} pending question${questionRows.length > 1 ? "s" : ""} — need your attention</div>
  ${items}
</div>`;
  })() : "";

  const feedTableHtml = feedRows.length === 0
    ? `<div class="empty">No ${feedSignalOnly ? "signal " : ""}messages yet${statusChannels.length > 0 ? ` on ${escHtml(statusChannels.join(", "))}` : " (no *-status channels found)"}.</div>`
    : `<table><thead><tr><th>Time</th><th>Type</th><th>Worker${isAll ? " / Channel" : ""}</th><th>Summary</th></tr></thead><tbody>${feedRows.map(renderFeedRow).join("\n")}</tbody></table>`;

  // Annotate each worker with managed/running flags for Start/Stop buttons
  workers.forEach(w => {
    w.isManaged = WATCHDOG_BIN.length > 0 &&
                  workerDefs.some(d => d.name === w.sender && (!d.ns || d.ns === w._ns));
    w.isRunning  = watchdogProcs.has(w.sender);
  });

  const hasActions = WATCHDOG_BIN.length > 0;

  // ── HTML helpers ─────────────────────────────────────────────────────────────
  const uptime = (() => {
    const s = Math.floor((now - startedAt) / 1000);
    return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m ${s%60}s`;
  })();

  const tabHref = (ns, lv) => {
    const params = [];
    if (ns !== "all") params.push(`ns=${ns}`);
    if (lv) params.push("live=1");
    return `/dashboard${params.length ? "?" + params.join("&") : ""}`;
  };

  const tabs = ["all", ...namespaces].map(ns => {
    const active = ns === selectedNs;
    const count  = ns === "all" ? allChannels.length : allChannels.filter(r => nsOf(r.channel) === ns).length;
    return `<a href="${tabHref(ns, live)}" class="tab${active ? " tab-active" : ""}">${ns === "all" ? "All" : ns} <span class="tab-count">${count}</span></a>`;
  }).join("\n");

  // ── Channel rows (with preview) ───────────────────────────────────────────────
  const chanRows = channels.map(r => {
    const exempt  = PRUNE_EXEMPT.includes(r.channel) ? '<span class="badge badge-yellow">exempt</span>' : '';
    const schema  = schemaSet.has(r.channel) ? '<span class="badge badge-blue">schema</span>' : '';
    const prev    = previewMap.get(r.channel);
    let preview   = "";
    if (prev) {
      let text = prev.content;
      try { const p = JSON.parse(text); text = p.subject || p.type || text; } catch {}
      preview = `<span class="preview">${escHtml(String(text).slice(0,80))}${text.length > 80 ? "…" : ""}</span>`;
    }
    return `<tr>
      <td>${escHtml(r.channel)}${exempt}${schema}</td>
      <td>${r.n}</td>
      <td>#${r.last_id}</td>
      <td>${agoStr(r.last_ts)}</td>
      <td class="preview-cell">${preview}</td>
    </tr>`;
  }).join("\n");

  // ── Worker liveness rows ──────────────────────────────────────────────────────
  const workerRows = workers.map(w => {
    let dotClass;
    if      (w.status === "running" || w.status === "idle") dotClass = "dot-green";
    else if (w.status === "queued")                         dotClass = "dot-yellow";
    else if (w.status === "offline")                        dotClass = "dot-grey";
    else                                                    dotClass = "dot-red";

    const stateLabel = w.ageSec === null  ? '<span class="dim">—</span>'
                     : w.status === "offline" ? "offline"
                     : w.state === "idle-polling" ? "idle"
                     : w.state;
    const taskCell   = w.task ? `<span class="task-label">${escHtml(String(w.task).slice(0,40))}</span>` : '<span class="dim">—</span>';
    const costCell   = w.cost != null ? `$${Number(w.cost).toFixed(3)}` : '<span class="dim">—</span>';
    const rotBadge   = w.rotating ? '<span class="badge badge-orange">rotate</span>' : '';
    const inboxCell  = w.inboxCount > 0 ? `<span class="inbox-count">${w.inboxCount}</span>` : '<span class="dim">—</span>';
    const lastSeen   = w.ageSec !== null ? agoStr(now - w.ageSec * 1000) : '<span class="dim">—</span>';
    const actionCell = w.isManaged
      ? w.isRunning
        ? `<button class="btn-stop"  onclick="workerAction('${escHtml(w.sender)}','stop')">Stop</button>`
        : `<button class="btn-start" onclick="workerAction('${escHtml(w.sender)}','start')">Start</button>`
      : (hasActions ? '<span class="dim">—</span>' : "");
    return `<tr>
      <td><span class="dot ${dotClass}"></span> ${escHtml(w.sender)}${rotBadge}</td>
      <td>${stateLabel}</td>
      <td>${taskCell}</td>
      <td>${costCell}</td>
      <td>${inboxCell}</td>
      <td>${lastSeen}</td>
      ${hasActions ? `<td>${actionCell}</td>` : ""}
    </tr>`;
  }).join("\n");

  // ── Cap rows ──────────────────────────────────────────────────────────────────
  const capRows = caps.map(r => {
    const owns  = JSON.parse(r.owns).join(", ");
    const chans = JSON.parse(r.channels).join(", ");
    return `<tr><td>${escHtml(r.worker)}</td><td>${escHtml(owns)}</td><td>${escHtml(chans)}</td><td>${agoStr(r.updated_at)}</td></tr>`;
  }).join("\n");

  const schemaRows = schemas.map(r =>
    `<tr><td>${escHtml(r.channel)}</td><td>${r.strict ? "yes" : "no (warn)"}</td><td>${new Date(r.updated_at).toISOString()}</td></tr>`
  ).join("\n");

  const totalMsgs = channels.reduce((a, r) => a + r.n, 0);
  const nsLabel   = !isAll ? `<span class="ns-label">${selectedNs}</span>` : "";

  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>claude-broker${selectedNs !== "all" ? ` · ${selectedNs}` : ""}</title>
${live ? `<meta http-equiv="refresh" content="30">` : ""}
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:ui-monospace,monospace;background:#0d1117;color:#c9d1d9;padding:24px;font-size:13px}
  h1{color:#58a6ff;font-size:18px;margin-bottom:4px}
  .meta{color:#8b949e;margin-bottom:16px;font-size:12px}
  h2{color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid #21262d;padding-bottom:6px;margin:20px 0 10px}
  table{width:100%;border-collapse:collapse;margin-bottom:4px}
  th{background:#161b22;color:#8b949e;text-align:left;padding:6px 12px;font-weight:normal;font-size:11px;text-transform:uppercase;letter-spacing:.06em}
  td{padding:6px 12px;border-bottom:1px solid #161b22;vertical-align:middle;max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  tr:last-child td{border-bottom:none}
  tr:hover td{background:#161b22}
  .badge{display:inline-block;padding:1px 6px;border-radius:3px;font-size:10px;margin-left:6px;vertical-align:middle}
  .badge-yellow{background:#6e4c00;color:#e3b341}
  .badge-blue{background:#0d419d;color:#79c0ff}
  .badge-orange{background:#5a3500;color:#f0883e}
  .empty{color:#484f58;font-style:italic;padding:10px 0}
  .config{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:12px 16px;margin-bottom:16px;display:flex;gap:24px;flex-wrap:wrap}
  .config-item{display:flex;flex-direction:column;gap:2px}
  .config-label{color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.06em}
  .config-value{color:#c9d1d9}
  .auth-on{color:#3fb950}.auth-off{color:#f85149}
  .sprint-bar{display:flex;gap:0;flex-wrap:wrap;margin-bottom:16px}
  .sprint-card{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 16px;margin-right:10px;margin-bottom:8px;min-width:260px;flex:1}
  .sprint-name{color:#e3b341;font-size:13px;font-weight:bold;margin-bottom:6px}
  .sprint-meta{color:#8b949e;font-size:11px;margin-bottom:8px}
  .sprint-progress{display:flex;gap:16px}
  .sprint-stat{display:flex;flex-direction:column;gap:1px}
  .sprint-stat-val{font-size:18px;font-weight:bold;color:#c9d1d9}
  .sprint-stat-val.done{color:#3fb950}
  .sprint-stat-val.fail{color:#f85149}
  .sprint-stat-val.open{color:#79c0ff}
  .sprint-stat-lbl{font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#8b949e}
  .progress-bar{height:4px;background:#21262d;border-radius:2px;margin-top:8px;overflow:hidden}
  .progress-fill{height:100%;background:#3fb950;border-radius:2px;transition:width .3s}
  .tabs{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:20px;border-bottom:1px solid #21262d;padding-bottom:0;align-items:flex-end}
  .tab{display:inline-block;padding:6px 14px;border-radius:6px 6px 0 0;font-size:12px;color:#8b949e;text-decoration:none;border:1px solid transparent;border-bottom:none;margin-bottom:-1px}
  .tab:hover{color:#c9d1d9;background:#161b22}
  .tab-active{color:#c9d1d9;background:#161b22;border-color:#21262d;border-bottom-color:#161b22}
  .tab-count{display:inline-block;background:#21262d;color:#8b949e;border-radius:10px;padding:0 6px;font-size:10px;margin-left:4px}
  .tab-active .tab-count{background:#30363d;color:#c9d1d9}
  .ns-label{display:inline-block;background:#1f3a5f;color:#79c0ff;border-radius:4px;padding:2px 8px;font-size:11px;margin-left:8px;vertical-align:middle}
  .preview{color:#484f58;font-size:11px}
  .preview-cell{max-width:280px}
  .dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:6px;vertical-align:middle;flex-shrink:0}
  .dot-green{background:#3fb950}
  .dot-yellow{background:#e3b341}
  .dot-red{background:#f85149;box-shadow:0 0 4px #f85149}
  .dot-grey{background:#484f58}
  .task-label{color:#79c0ff;font-size:11px}
  .inbox-count{color:#e3b341;font-weight:bold}
  .dim{color:#484f58}
  .live-btn{margin-left:auto;padding:4px 12px;border-radius:6px 6px 0 0;font-size:11px;font-family:inherit;cursor:pointer;border:1px solid #21262d;border-bottom:none;margin-bottom:-1px;text-decoration:none}
  .live-on{background:#0d2b0d;color:#3fb950;border-color:#1f5c1f}
  .live-off{background:#161b22;color:#8b949e}
  .btn-start,.btn-stop{padding:2px 10px;border-radius:4px;font-size:11px;font-family:inherit;cursor:pointer;border:1px solid}
  .btn-start{background:#0d2b0d;color:#3fb950;border-color:#1f5c1f}
  .btn-start:hover{background:#163d16}
  .btn-stop{background:#2b0d0d;color:#f85149;border-color:#5c1f1f}
  .btn-stop:hover{background:#3d1616}
  .btn-start:disabled,.btn-stop:disabled{opacity:.4;cursor:default}
  .questions-alert{background:#2b1d00;border:1px solid #6e4c00;border-radius:6px;padding:10px 16px;margin-bottom:16px}
  .questions-alert-title{color:#e3b341;font-weight:bold;margin-bottom:6px;font-size:12px}
  .questions-alert-item{padding:3px 0;border-bottom:1px solid #3b2800;font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .questions-alert-item:last-child{border-bottom:none}
  .section-hdr{display:flex;align-items:center;color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid #21262d;padding-bottom:6px;margin:20px 0 10px}
  .section-hdr-controls{margin-left:auto;text-transform:none;letter-spacing:normal;display:flex;gap:4px}
  .feed-toggle{padding:1px 8px;border-radius:3px;font-size:10px;text-decoration:none;color:#8b949e;border:1px solid #21262d}
  .feed-toggle-active{color:#c9d1d9;background:#21262d;border-color:#30363d}
  .feed-badge{display:inline-block;padding:1px 7px;border-radius:10px;font-size:10px;font-weight:bold;text-transform:uppercase;letter-spacing:.04em}
  .feed-result{background:#0d2b0d;color:#3fb950}
  .feed-question{background:#3b2800;color:#e3b341}
  .feed-error{background:#2b0d0d;color:#f85149}
  .feed-task{background:#0d1b3e;color:#79c0ff}
  .feed-other{background:#21262d;color:#8b949e}
  .feed-time{color:#8b949e;font-size:11px;white-space:nowrap;width:80px}
  .feed-sender{color:#79c0ff;white-space:nowrap;max-width:140px;overflow:hidden;text-overflow:ellipsis}
  .feed-content{max-width:480px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .feed-chan{font-size:10px;color:#484f58;margin-left:6px}
  .feed-taskid{font-size:10px;color:#484f58;margin-left:8px}
  .feed-row-q td{background:#1a1400}
  .feed-row-q:hover td{background:#241c00 !important}
</style>
</head>
<body>
<h1>claude-broker <span style="font-size:13px;color:#8b949e;font-weight:normal">v2.0.0</span></h1>
<div class="meta">uptime: ${uptime} &nbsp;·&nbsp; db: ${DB_PATH} &nbsp;·&nbsp; auth: <span class="${SHARED_SECRET ? "auth-on" : "auth-off"}">${SHARED_SECRET ? "on" : "OFF"}</span>${hasActions ? ` &nbsp;·&nbsp; workers: ${workerDefs.length} configured` : ""}${live ? ' &nbsp;·&nbsp; <span style="color:#3fb950">● live (30s)</span>' : ""}</div>

<div class="config">
  <div class="config-item"><span class="config-label">Auto-prune</span><span class="config-value">every ${PRUNE_INTERVAL_MS/60000}m · max age ${PRUNE_MAX_AGE_MS/3600000}h</span></div>
  <div class="config-item"><span class="config-label">Exempt</span><span class="config-value">${PRUNE_EXEMPT.join(", ") || "(none)"}</span></div>
  <div class="config-item"><span class="config-label">Channels${isAll ? "" : ` (${selectedNs})`}</span><span class="config-value">${channels.length} / ${allChannels.length}</span></div>
  <div class="config-item"><span class="config-label">Messages${isAll ? "" : ` (${selectedNs})`}</span><span class="config-value">${totalMsgs}</span></div>
  <div class="config-item"><span class="config-label">Projects</span><span class="config-value">${namespaces.join(", ") || "(none yet)"}</span></div>
</div>

${sprintInfos.length > 0 ? `
<div class="sprint-bar">
${sprintInfos.map(s => {
  const open   = Math.max(0, s.dispatched - s.completed);
  const pct    = s.dispatched > 0 ? Math.round((s.completed / s.dispatched) * 100) : 0;
  const nsTag  = isAll ? `<span style="font-size:10px;color:#8b949e;font-weight:normal;margin-left:6px">${s.ns}</span>` : "";
  return `<div class="sprint-card">
  <div class="sprint-name">${escHtml(s.slug)}${nsTag}</div>
  <div class="sprint-meta">started ${agoStr(s.startedAt)}</div>
  <div class="sprint-progress">
    <div class="sprint-stat"><span class="sprint-stat-val">${s.dispatched}</span><span class="sprint-stat-lbl">dispatched</span></div>
    <div class="sprint-stat"><span class="sprint-stat-val done">${s.completed}</span><span class="sprint-stat-lbl">completed</span></div>
    <div class="sprint-stat"><span class="sprint-stat-val open">${open}</span><span class="sprint-stat-lbl">open</span></div>
    ${s.failed > 0 ? `<div class="sprint-stat"><span class="sprint-stat-val fail">${s.failed}</span><span class="sprint-stat-lbl">failed</span></div>` : ""}
  </div>
  <div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div>
</div>`;
}).join("\n")}
</div>` : ""}

<div class="tabs">
${tabs}
<a href="${tabHref(selectedNs, !live)}" class="live-btn ${live ? "live-on" : "live-off"}">${live ? "● Live" : "Live"}</a>
</div>

${questionsAlertHtml}
<h2>Worker Liveness ${nsLabel}</h2>
${workers.length === 0
  ? `<div class="empty">No telemetry yet${isAll ? "" : ` for "${selectedNs}"`}. Workers emit heartbeats to <code>${isAll ? "*-telemetry" : telemetryChan}</code>.</div>`
  : `<table><thead><tr><th>Worker</th><th>State</th><th>Current Task</th><th>Cost</th><th>Inbox</th><th>Last Seen</th>${hasActions ? "<th>Actions</th>" : ""}</tr></thead><tbody>${workerRows}</tbody></table>`}

<div class="section-hdr">
  Activity Feed ${nsLabel}
  <span class="section-hdr-controls">
    <a href="${feedHref(false)}" class="feed-toggle${feedSignalOnly ? " feed-toggle-active" : ""}">Signal</a>
    <a href="${feedHref(true)}" class="feed-toggle${!feedSignalOnly ? " feed-toggle-active" : ""}">All</a>
  </span>
</div>
${feedTableHtml}

<h2>Channels ${nsLabel}</h2>
${channels.length === 0
  ? `<div class="empty">No channels${isAll ? " yet." : ` for "${selectedNs}".`}</div>`
  : `<table><thead><tr><th>Channel</th><th>Messages</th><th>Last ID</th><th>Last Activity</th><th>Last Message</th></tr></thead><tbody>${chanRows}</tbody></table>`}

<h2>Worker Capabilities ${nsLabel}</h2>
${caps.length === 0
  ? `<div class="empty">No capabilities registered${isAll ? ". Workers should call register_capability at startup." : ` for "${selectedNs}".`}</div>`
  : `<table><thead><tr><th>Worker</th><th>Owns</th><th>Channels</th><th>Registered</th></tr></thead><tbody>${capRows}</tbody></table>`}

<h2>Channel Schemas ${nsLabel}</h2>
${schemas.length === 0
  ? `<div class="empty">No schemas${isAll ? " registered." : ` for "${selectedNs}".`}</div>`
  : `<table><thead><tr><th>Channel</th><th>Strict</th><th>Updated</th></tr></thead><tbody>${schemaRows}</tbody></table>`}

<p style="margin-top:24px;color:#484f58;font-size:11px">Refreshed at ${new Date().toISOString()} &nbsp;·&nbsp; <a href="${tabHref(selectedNs, live)}" style="color:#58a6ff">reload</a></p>
<script>
function workerAction(name, action) {
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = action === 'start' ? 'Starting…' : 'Stopping…';
  fetch('/workers/' + encodeURIComponent(name) + '/' + action, { method: 'POST' })
    .then(r => r.json())
    .then(d => { if (d.ok) location.reload(); else { alert(d.error || 'Error'); btn.disabled = false; btn.textContent = action === 'start' ? 'Start' : 'Stop'; } })
    .catch(() => { alert('Request failed'); btn.disabled = false; });
}
</script>
</body></html>`);
});

// Auth middleware
if (SHARED_SECRET) {
  app.use("/mcp", (req, res, next) => {
    const auth = req.headers.authorization || "";
    if (auth !== `Bearer ${SHARED_SECRET}`) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    next();
  });
} else {
  console.warn("[claude-broker] WARNING: SHARED_SECRET is not set — broker is UNAUTHENTICATED. Set SHARED_SECRET in .env before exposing to any network.");
}

app.post("/mcp", async (req, res) => {
  try {
    const server    = buildServer();
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on("close", () => { transport.close(); server.close(); });
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error("[claude-broker] handler error:", err);
    if (!res.headersSent) res.status(500).json({ jsonrpc: "2.0", error: { code: -32603, message: "Internal error" }, id: null });
  }
});

app.listen(PORT, () => {
  console.log(`[claude-broker] v2.0.0 listening on :${PORT}  auth:${SHARED_SECRET ? "on" : "OFF"}  prune:${PRUNE_MAX_AGE_MS / 3600000}h  exempt:[${PRUNE_EXEMPT.join(",")}]`);
  console.log(`[claude-broker] dashboard: http://localhost:${PORT}/dashboard`);
});
