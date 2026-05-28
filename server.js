import "dotenv/config";
import express from "express";
import Database from "better-sqlite3";
import { EventEmitter } from "events";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import Ajv from "ajv";
import addFormats from "ajv-formats";

const PORT = Number(process.env.PORT) || 8080;
const SHARED_SECRET = process.env.SHARED_SECRET || "";
const DB_PATH = process.env.DB_PATH || "./broker.db";

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
`);

const stmtInsert  = db.prepare("INSERT INTO messages (channel, sender, content, created_at) VALUES (?, ?, ?, ?)");
const stmtSelect  = db.prepare("SELECT id, sender, content, created_at FROM messages WHERE channel = ? AND id > ? ORDER BY id ASC LIMIT ?");
const stmtChans   = db.prepare("SELECT channel, COUNT(*) AS n, MAX(id) AS last_id FROM messages GROUP BY channel ORDER BY channel");
const stmtPurge   = db.prepare("DELETE FROM messages WHERE channel = ?");

const stmtSchemaGet    = db.prepare("SELECT channel, schema, strict, updated_at FROM channel_schemas WHERE channel = ?");
const stmtSchemaUpsert = db.prepare(`INSERT INTO channel_schemas (channel, schema, strict, updated_at) VALUES (?, ?, ?, ?)
  ON CONFLICT(channel) DO UPDATE SET schema=excluded.schema, strict=excluded.strict, updated_at=excluded.updated_at`);
const stmtSchemaDel    = db.prepare("DELETE FROM channel_schemas WHERE channel = ?");
const stmtSchemaList   = db.prepare("SELECT channel, strict, updated_at FROM channel_schemas ORDER BY channel");
const stmtLatestPerSender = db.prepare(`
  SELECT m.id, m.sender, m.content, m.created_at
  FROM messages m
  INNER JOIN (
    SELECT sender, MAX(id) AS max_id
    FROM messages
    WHERE channel = ?
    GROUP BY sender
  ) latest ON m.id = latest.max_id
  ORDER BY m.sender
`);

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validatorCache = new Map(); // channel -> { validate, updated_at }

function getValidator(channel) {
  const row = stmtSchemaGet.get(channel);
  if (!row) {
    if (validatorCache.has(channel)) validatorCache.delete(channel);
    return null;
  }
  const cached = validatorCache.get(channel);
  if (cached && cached.updated_at === row.updated_at) {
    return { validate: cached.validate, strict: row.strict };
  }
  let validate;
  try {
    validate = ajv.compile(JSON.parse(row.schema));
  } catch (e) {
    console.error(`[claude-broker] schema for '${channel}' failed to compile: ${e.message}`);
    return null;
  }
  validatorCache.set(channel, { validate, updated_at: row.updated_at });
  return { validate, strict: row.strict };
}

function validateContent(channel, content) {
  const v = getValidator(channel);
  if (!v) return { ok: true };
  let parsed;
  try { parsed = JSON.parse(content); }
  catch (e) {
    return { ok: false, strict: !!v.strict, errors: `content is not valid JSON: ${e.message}` };
  }
  const valid = v.validate(parsed);
  if (valid) return { ok: true };
  const errors = (v.validate.errors || [])
    .map(e => `${e.instancePath || "/"} ${e.message}`).join("; ");
  return { ok: false, strict: !!v.strict, errors };
}

const messageBus = new EventEmitter();
messageBus.setMaxListeners(0);

function formatRows(rows, channel, sinceId) {
  if (rows.length === 0) {
    return { content: [{ type: "text", text: `No new messages on '${channel}' (since_id=${sinceId}).` }] };
  }
  const lines = rows.map(r =>
    `[#${r.id}] ${new Date(r.created_at).toISOString()} <${r.sender}>: ${r.content}`
  );
  const last = rows[rows.length - 1].id;
  lines.push(`\n(next since_id: ${last})`);
  return { content: [{ type: "text", text: lines.join("\n") }] };
}

function buildServer() {
  const server = new McpServer({ name: "claude-broker", version: "1.0.0" });

  server.registerTool(
    "send_message",
    {
      title: "Send a message",
      description: "Post a message to a named channel. Other Claude Code sessions subscribed to the same channel can read it via read_messages.",
      inputSchema: {
        channel: z.string().min(1).describe("Channel name, e.g. 'mac-to-windows' or 'project-x'."),
        sender:  z.string().min(1).describe("Identifier for the sender, e.g. 'mac' or 'windows'."),
        content: z.string().min(1).describe("Message body. Plain text or JSON-as-string."),
      },
    },
    async ({ channel, sender, content }) => {
      const check = validateContent(channel, content);
      if (!check.ok && check.strict) {
        return {
          content: [{
            type: "text",
            text: `schema validation failed on '${channel}': ${check.errors}. Call get_channel_schema('${channel}') to see required fields.`,
          }],
          isError: true,
        };
      }
      const now = Date.now();
      const r = stmtInsert.run(channel, sender, content, now);
      messageBus.emit(`msg:${channel}`, { id: r.lastInsertRowid });
      let warnSuffix = "";
      if (!check.ok && !check.strict) {
        console.warn(`[claude-broker] schema warn on '${channel}': ${check.errors}`);
        warnSuffix = `  [WARN: schema mismatch (warn-only): ${check.errors}]`;
      }
      return {
        content: [{
          type: "text",
          text: `Sent message #${r.lastInsertRowid} to channel '${channel}' as '${sender}' at ${new Date(now).toISOString()}.${warnSuffix}`,
        }],
      };
    }
  );

  server.registerTool(
    "read_messages",
    {
      title: "Read messages",
      description: "Read messages from a channel newer than since_id. Returns up to `limit` messages. Use the highest returned id as the next since_id to avoid re-reading.",
      inputSchema: {
        channel:  z.string().min(1).describe("Channel name to read."),
        since_id: z.number().int().nonnegative().optional().describe("Only return messages with id > since_id. Omit or 0 for all."),
        limit:    z.number().int().positive().max(500).optional().describe("Max messages to return (default 100)."),
      },
    },
    async ({ channel, since_id, limit }) => {
      const sinceId = since_id ?? 0;
      const rows = stmtSelect.all(channel, sinceId, limit ?? 100);
      return formatRows(rows, channel, sinceId);
    }
  );

  server.registerTool(
    "wait_for_messages",
    {
      title: "Wait for new messages",
      description: "Block-waits server-side until a new message arrives on the channel with id > since_id, or until timeout_ms elapses. Returns the same shape as read_messages. Use this instead of read_messages when you want to wait on coordination within a single turn — it returns the moment a worker posts, with no busy-poll cost. Default timeout 25s, max 60s.",
      inputSchema: {
        channel:    z.string().min(1).describe("Channel name to wait on."),
        since_id:   z.number().int().nonnegative().optional().describe("Only return messages with id > since_id. Omit or 0 for all."),
        timeout_ms: z.number().int().positive().max(60000).optional().describe("Max time to wait in milliseconds. Default 25000. Capped at 60000."),
        limit:      z.number().int().positive().max(500).optional().describe("Max messages to return (default 100)."),
      },
    },
    async ({ channel, since_id, timeout_ms, limit }) => {
      const sinceId = since_id ?? 0;
      const lim = limit ?? 100;
      const timeout = Math.min(timeout_ms ?? 25000, 60000);
      const event = `msg:${channel}`;

      let resolveWait;
      const wait = new Promise((resolve) => { resolveWait = resolve; });
      const onMessage = () => resolveWait("message");
      messageBus.on(event, onMessage);

      let timer;
      const timeoutPromise = new Promise((resolve) => {
        timer = setTimeout(() => resolve("timeout"), timeout);
      });

      try {
        const immediate = stmtSelect.all(channel, sinceId, lim);
        if (immediate.length > 0) {
          return formatRows(immediate, channel, sinceId);
        }
        const outcome = await Promise.race([wait, timeoutPromise]);
        if (outcome === "timeout") {
          return { content: [{ type: "text", text: `No new messages on '${channel}' within ${timeout}ms (since_id=${sinceId}).` }] };
        }
        const rows = stmtSelect.all(channel, sinceId, lim);
        return formatRows(rows, channel, sinceId);
      } finally {
        clearTimeout(timer);
        messageBus.off(event, onMessage);
      }
    }
  );

  server.registerTool(
    "list_channels",
    {
      title: "List channels",
      description: "List all channels with message counts and the latest message id.",
      inputSchema: {},
    },
    async () => {
      const rows = stmtChans.all();
      if (rows.length === 0) {
        return { content: [{ type: "text", text: "(no channels yet)" }] };
      }
      const text = rows.map(r => `${r.channel}\t${r.n} msgs\tlast_id=${r.last_id}`).join("\n");
      return { content: [{ type: "text", text }] };
    }
  );

  server.registerTool(
    "purge_channel",
    {
      title: "Purge channel",
      description: "Delete all messages in a channel. Useful for cleaning up after a session.",
      inputSchema: {
        channel: z.string().min(1).describe("Channel to purge."),
      },
    },
    async ({ channel }) => {
      const r = stmtPurge.run(channel);
      return { content: [{ type: "text", text: `Purged ${r.changes} messages from '${channel}'.` }] };
    }
  );

  server.registerTool(
    "register_channel_schema",
    {
      title: "Register or replace a channel schema",
      description: "Bind a JSON Schema (draft-07) to a channel. Messages on that channel will be validated against this schema on send_message. Idempotent — replaces any existing schema for the channel.",
      inputSchema: {
        channel: z.string().min(1).describe("Channel name."),
        schema:  z.string().min(2).describe("JSON Schema as a JSON-encoded string."),
        strict:  z.boolean().optional().describe("If true, invalid messages are rejected. If false (default), invalid messages are still stored but logged + warned in the send_message response."),
      },
    },
    async ({ channel, schema, strict }) => {
      let parsed;
      try { parsed = JSON.parse(schema); }
      catch (e) {
        return { content: [{ type: "text", text: `schema is not valid JSON: ${e.message}` }], isError: true };
      }
      try { ajv.compile(parsed); }
      catch (e) {
        return { content: [{ type: "text", text: `schema does not compile as JSON Schema: ${e.message}` }], isError: true };
      }
      const now = Date.now();
      stmtSchemaUpsert.run(channel, schema, strict ? 1 : 0, now);
      validatorCache.delete(channel);
      return { content: [{ type: "text", text: `Registered schema for '${channel}' (strict=${strict ? "on" : "off"}) at ${new Date(now).toISOString()}.` }] };
    }
  );

  server.registerTool(
    "get_channel_schema",
    {
      title: "Get a channel's schema",
      description: "Return the JSON Schema currently bound to a channel, or a null result if the channel is free-form.",
      inputSchema: {
        channel: z.string().min(1).describe("Channel name."),
      },
    },
    async ({ channel }) => {
      const row = stmtSchemaGet.get(channel);
      if (!row) {
        return { content: [{ type: "text", text: `No schema registered for '${channel}' (free-form).` }] };
      }
      const text = `Channel: ${row.channel}\nStrict: ${row.strict ? "on" : "off (warn-only)"}\nUpdated: ${new Date(row.updated_at).toISOString()}\n\n${row.schema}`;
      return { content: [{ type: "text", text }] };
    }
  );

  server.registerTool(
    "clear_channel_schema",
    {
      title: "Clear a channel's schema",
      description: "Remove the schema binding from a channel. The channel reverts to free-form.",
      inputSchema: {
        channel: z.string().min(1).describe("Channel name."),
      },
    },
    async ({ channel }) => {
      const r = stmtSchemaDel.run(channel);
      validatorCache.delete(channel);
      return { content: [{ type: "text", text: r.changes ? `Cleared schema for '${channel}'.` : `No schema was set for '${channel}'.` }] };
    }
  );

  server.registerTool(
    "get_latest_per_sender",
    {
      title: "Get latest message per distinct sender",
      description: "Return the most recent message on a channel for each distinct sender, in one query. Designed for heartbeat/telemetry channels where the orchestrator wants a snapshot of current state without paginating through history. Returns at most one row per sender.",
      inputSchema: {
        channel: z.string().min(1).describe("Channel to query."),
      },
    },
    async ({ channel }) => {
      const rows = stmtLatestPerSender.all(channel);
      if (rows.length === 0) {
        return { content: [{ type: "text", text: `No messages on '${channel}'.` }] };
      }
      const lines = rows.map(r =>
        `[#${r.id}] ${new Date(r.created_at).toISOString()} <${r.sender}>: ${r.content}`
      );
      return { content: [{ type: "text", text: lines.join("\n") }] };
    }
  );

  server.registerTool(
    "list_channel_schemas",
    {
      title: "List channel schemas",
      description: "Show all channels that have a schema bound, including strict-mode status.",
      inputSchema: {},
    },
    async () => {
      const rows = stmtSchemaList.all();
      if (rows.length === 0) {
        return { content: [{ type: "text", text: "(no channel schemas registered)" }] };
      }
      const text = rows.map(r => `${r.channel}\tstrict=${r.strict ? "on" : "off"}\tupdated=${new Date(r.updated_at).toISOString()}`).join("\n");
      return { content: [{ type: "text", text }] };
    }
  );

  return server;
}

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req, res) => res.json({ ok: true, ts: Date.now() }));

if (SHARED_SECRET) {
  app.use("/mcp", (req, res, next) => {
    const auth = req.headers.authorization || "";
    if (auth !== `Bearer ${SHARED_SECRET}`) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    next();
  });
} else {
  console.warn("[claude-broker] SHARED_SECRET is empty — broker is UNAUTHENTICATED. Do not expose to the internet.");
}

app.post("/mcp", async (req, res) => {
  try {
    const server = buildServer();
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on("close", () => { transport.close(); server.close(); });
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error("[claude-broker] handler error:", err);
    if (!res.headersSent) {
      res.status(500).json({ jsonrpc: "2.0", error: { code: -32603, message: "Internal error" }, id: null });
    }
  }
});

app.listen(PORT, () => {
  console.log(`[claude-broker] listening on :${PORT}  (auth: ${SHARED_SECRET ? "on" : "OFF"})`);
});
