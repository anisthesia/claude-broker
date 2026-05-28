# claude-broker

A tiny MCP HTTP server that lets multiple Claude Code sessions (e.g. one on Mac, one on Windows) talk to each other through named channels.

It exposes these tools to every connected Claude Code session:

- `send_message(channel, sender, content)` — post a message
- `read_messages(channel, since_id?, limit?)` — pull new messages
- `wait_for_messages(channel, since_id?, timeout_ms?)` — server-side long-poll
- `list_channels()` — show all channels + counts
- `purge_channel(channel)` — wipe a channel
- `register_channel_schema(channel, schema, strict?)` — bind a JSON Schema to a channel
- `get_channel_schema(channel)` — inspect the schema bound to a channel
- `clear_channel_schema(channel)` — remove a schema (channel reverts to free-form)
- `list_channel_schemas()` — show all channels with schemas

Messages are persisted in a local SQLite file (`broker.db`).

---

## 1. Run the broker (do this on ONE machine, or any always-on host)

```bash
cd /Users/anis/myprojects/claude-broker
npm install
cp .env.example .env
# edit .env and set a strong SHARED_SECRET
npm start
```

You should see:

```
[claude-broker] listening on :8080  (auth: on)
```

Verify:

```bash
curl http://localhost:8080/health
# {"ok":true,"ts":...}
```

## 2. Expose it to the other machine

Pick whichever fits:

- **Same LAN**: use the host's LAN IP, e.g. `http://192.168.1.50:8080/mcp`.
- **Different networks** (Windows at home, Mac at office): use a tunnel:

  ```bash
  # On the host running the broker
  ngrok http 8080
  # Use the https URL ngrok prints, with /mcp appended.
  ```

- **Production**: deploy to any small VPS / Fly.io / Railway. Put it behind TLS.

## 3. Register it in Claude Code on BOTH machines

Replace `<URL>` with the broker URL (ending in `/mcp`) and `<SECRET>` with the value from `.env`.

### Mac / Linux

```bash
claude mcp add --transport http broker <URL> \
  --header "Authorization: Bearer <SECRET>"
```

### Windows (PowerShell)

```powershell
claude mcp add --transport http broker <URL> `
  --header "Authorization: Bearer <SECRET>"
```

Confirm:

```bash
claude mcp list
```

You should see `broker` listed. Inside Claude Code, the four tools (`send_message`, `read_messages`, `list_channels`, `purge_channel`) will be available.

## 4. Use it

In the Mac session:

> "Use the broker to send 'hello from mac' on channel `demo`."

In the Windows session:

> "Read new messages on channel `demo`."

Pattern for ongoing back-and-forth: each session keeps track of the last `since_id` it has read, and polls (or you ask it to poll) to pick up new messages.

---

## Channel schemas (optional validator)

A channel can have a JSON Schema (draft-07) bound to it. When set, every
`send_message` to that channel will have its `content` validated:

- `strict: false` (default) — invalid messages are **stored** but the broker
  logs a warning and includes `[WARN: ...]` in the `send_message` response.
  Use this for rollout — workers won't break while CLAUDE.md changes are
  still propagating.
- `strict: true` — invalid messages are **rejected** with `isError: true`
  and a descriptive message pointing at the missing/wrong field. Flip to
  strict once warn-only logs have been clean for a sprint or two.

Schemas are stored in SQLite (`channel_schemas` table) alongside messages,
so they hot-load — no broker restart needed when registering or replacing.

### Quick setup

The repo ships with schemas for the dogsvilla pilot under `schemas/` and a
registration script:

```bash
# Warn-only mode (recommended for first rollout)
node setup-schemas.js

# Strict mode (after burn-in)
STRICT=1 node setup-schemas.js
```

### Smoke test

```bash
node test-schema-validation.js
```

Exercises a temporary channel through valid/invalid envelopes in both
warn-only and strict modes; cleans up after itself.

See `docs/protocol-v2.md` for the full protocol-intelligence spec
(heartbeat channel + validator design rationale).

## Notes

- **Authentication**: the broker checks `Authorization: Bearer <SHARED_SECRET>`. Don't expose it to the internet without setting one.
- **Statelessness**: each MCP request spins up a fresh server instance — safe for multiple concurrent clients.
- **Storage**: SQLite WAL mode, single file. Back up `broker.db` if messages matter.
- **Scope**: this is intentionally minimal. If you need pub/sub push, file transfer, or auth per-sender, extend `server.js`.
