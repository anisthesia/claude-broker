# /setup-broker — Scaffold a new broker-worker project

Scaffold the full `claude-broker` multi-session coordination setup for a new project.
Generates orchestrator + worker CLAUDE.md files, channel schemas, a schema registration
script, and broker config entries — works for local or remote brokers.

**Complete every step in order. Do not skip steps.**

---

## Step 1 — Collect configuration

Before asking questions, inspect a default path to infer project context:

```bash
ls ~/myprojects/ 2>/dev/null || ls ~/projects/ 2>/dev/null || echo "(no default projects dir found)"
```

**Question set 1** — use `AskUserQuestion` with up to 4 questions in one call:

1. "What is the broker URL?" — options: `http://localhost:8080` (Recommended), Other
2. "What namespace prefix for this project? (2-4 lowercase letters, e.g. nx, rp, ap)" — options: `nx`, `rp`, `ap`, Other
3. "What is the absolute path to the TARGET project being scaffolded?" — Other (free text)
4. "What is the absolute path to the claude-broker repo?" — options: `/Users/anis/myprojects/claude-broker` (Recommended), Other

After getting the target path, analyze its structure:

```bash
ls <TARGET_ROOT>/
ls <TARGET_ROOT>/src 2>/dev/null || true
ls <TARGET_ROOT>/app 2>/dev/null || true
find <TARGET_ROOT> -maxdepth 3 \( -name "*.ts" -o -name "*.js" -o -name "*.py" -o -name "*.go" -o -name "*.rs" \) 2>/dev/null | head -60
```

Based on the project structure, suggest worker names. Examples:
- Full-stack web app: `api, web, db`
- Microservices: `gateway, auth, billing, notifications`
- Data pipeline: `ingestion, transform, load, qa`
- Infrastructure tool: `core, protocol-qa`

**Question set 2** — use `AskUserQuestion`:

1. "Worker names for this project? (comma-separated, e.g. api,web,db)" — show your suggestion as the first option, Other for custom list
2. "Broker secret?" — Other (free text; paste the SHARED_SECRET value from the broker's .env file)

After step 1 you have: `PREFIX`, `BROKER_URL`, `BROKER_SECRET`, `TARGET_ROOT`, `BROKER_REPO`, `WORKERS: string[]`.

Derive `PROJECT_NAME` = basename of `TARGET_ROOT` (e.g. `/Users/anis/myprojects/myapp` → `myapp`).

---

## Step 2 — Analyze project structure and infer file ownership

Run:

```bash
find <TARGET_ROOT> -maxdepth 3 \( -name "*.json" -o -name "*.ts" -o -name "*.js" -o -name "*.py" -o -name "*.go" \) 2>/dev/null | head -80
ls -la <TARGET_ROOT>/
```

For each worker, determine:
- `owns`: files and directories that worker is responsible for
- `never_touch`: sibling workers' files

Example for a TypeScript app with workers `[api, web, db]`:
- `api` → owns `src/api/`, `src/routes/`, `src/middleware/` — never touch `src/frontend/`, `migrations/`
- `web` → owns `src/frontend/`, `public/` — never touch `src/api/`, `migrations/`
- `db` → owns `migrations/`, `src/models/`, `prisma/` — never touch `src/api/`, `src/frontend/`

---

## Step 2.5 — Git preflight

### Target project repo check

```bash
ls <TARGET_ROOT>/.git 2>/dev/null && echo "git repo exists" || echo "no git repo"
```

If **no `.git` found**: ask via `AskUserQuestion`:
- "No git repo found in <TARGET_ROOT>. Initialize one now?"
  - Yes (Recommended) — run `git init <TARGET_ROOT>` then continue
  - No — print a reminder at the end: "⚠ Remember to commit the generated files before starting workers"

### .gitignore check

```bash
cat <TARGET_ROOT>/.gitignore 2>/dev/null || echo "(no .gitignore)"
```

- If `.gitignore` **does not exist**: create it with this initial content:
  ```
  .claude/settings.local.json
  ```
- If `.gitignore` **exists**: check whether `.claude/settings.local.json` is already listed.
  If not, append it:
  ```bash
  echo '.claude/settings.local.json' >> <TARGET_ROOT>/.gitignore
  ```

---

## Step 3 — Confirm plan

Before writing any files, use `AskUserQuestion` to show the full plan:

Present:
```
PREFIX:    <PREFIX>
BROKER:    <BROKER_URL>
TARGET:    <TARGET_ROOT>

CHANNELS (5 standard + N worker inboxes):
  <PREFIX>-orchestrator   orchestrator inbox
  <PREFIX>-control        orchestrator broadcasts
  <PREFIX>-status         worker results firehose
  <PREFIX>-telemetry      heartbeats
  <PREFIX>-backlog        persistent deferred tasks (NEVER purge)
  <PREFIX>-<worker>       (one per worker)

WORKERS:
  <worker1> → inbox: <PREFIX>-<worker1>
               owns: [...]
               never-touch: [...]
  ...

FILES TO CREATE — target project:
  <TARGET_ROOT>/.claude/settings.json (or settings.local.json)
  <TARGET_ROOT>/orchestrators/<PROJECT_NAME>/CLAUDE.md
  <TARGET_ROOT>/workers/<worker>/CLAUDE.md  (× N workers)

FILES TO CREATE — broker repo:
  <BROKER_REPO>/schemas/<PREFIX>-worker-inbox.json
  <BROKER_REPO>/schemas/<PREFIX>-orchestrator-inbox.json
  <BROKER_REPO>/schemas/<PREFIX>-control.json
  <BROKER_REPO>/schemas/<PREFIX>-status.json
  <BROKER_REPO>/schemas/<PREFIX>-telemetry.json
  <BROKER_REPO>/schemas/<PREFIX>-backlog.json
  <BROKER_REPO>/setup-schemas-<PREFIX>.js

FILES TO MODIFY — broker repo:
  <BROKER_REPO>/workers-broker.json  (append entries)
  <BROKER_REPO>/.env                 (append <PREFIX>-backlog to PRUNE_EXEMPT)
```

Ask: "Proceed with scaffold?" — Yes / No / Change something.
If No: stop. If Change: re-collect the specific field and re-confirm.

---

## Step 4 — Generate TARGET PROJECT files

### 4a. `.claude/settings.json`

Determine target file:
- BROKER_URL is localhost/127.0.0.1: `<TARGET_ROOT>/.claude/settings.json`
- BROKER_URL is remote: `<TARGET_ROOT>/.claude/settings.local.json` — warn the user this file must be gitignored (it contains the secret). Check `.gitignore` and add `.claude/settings.local.json` if missing.

**Merge logic** — read the existing file before writing:
```bash
cat <TARGET_FILE> 2>/dev/null
```
- If the file exists: parse the JSON, set/replace `mcpServers.broker` while preserving all other keys, then write back.
- If the file does not exist: write the full file below.

Content (full file when creating from scratch, or the `mcpServers.broker` block when merging):

```json
{
  "mcpServers": {
    "broker": {
      "type": "http",
      "url": "<BROKER_URL>/mcp",
      "headers": {
        "Authorization": "Bearer <BROKER_SECRET>"
      }
    }
  }
}
```

Ensure `<TARGET_ROOT>/.claude/` directory exists first.

### 4b. `orchestrators/<PROJECT_NAME>/CLAUDE.md`

Ensure `<TARGET_ROOT>/orchestrators/<PROJECT_NAME>/` exists.

Write the FULL orchestrator protocol. Replace every `{{...}}` placeholder with actual values before writing:
- `{{PROJECT_NAME}}` → PROJECT_NAME (derived from TARGET_ROOT basename)
- `{{PREFIX}}` → the chosen prefix
- `{{BROKER_URL}}` → actual broker URL
- `{{TARGET_ROOT}}` → actual target project path
- `{{N_WORKERS}}` → count of workers
- `{{WORKER_LIST}}` → comma-separated worker names
- `{{WORKER_REGISTRY_TABLE}}` → Markdown table rows, one per worker
- `{{WORKER_INBOX_BULLETS}}` → bullet list of worker inbox channels
- `{{WORKER_NAMES_JSON}}` → JSON array of worker name strings, e.g. `["api","web","db"]`
- `{{WATCHDOG_PATH}}` → `/path/to/watchdog.sh` (ask user or derive from BROKER_REPO: `<BROKER_REPO>/../dogsvilla/scripts/watchdog.sh`)

~~~
# {{PROJECT_NAME}} Orchestrator

## Identity

You are the **ORCHESTRATOR** for `{{PROJECT_NAME}}`. You manage a {{N_WORKERS}}-worker
team ({{WORKER_LIST}}) that builds and maintains this project. You plan sprints,
sequence work, watch for conflicts, and gate releases.

You do **NOT** write code or edit source files directly. You dispatch tasks.

Your working directory is `{{TARGET_ROOT}}`. The broker MCP server is wired into
your session (tools: `mcp__broker__*`), connected to `{{BROKER_URL}}`.

## Role

- Decompose work into tasks scoped to one worker
- Write task envelopes and dispatch them to worker inboxes
- Sequence work to avoid file conflicts between workers
- Monitor `{{PREFIX}}-status` for results, questions, and blockers
- Maintain a task ledger: `task_id → {worker, status, blockers}`
- Gate sprint-close merges: confirm commits, then merge
- Gate every `purge_channel` call with `AskUserQuestion`

## Worker registry

| Worker | Inbox channel | Owns |
|---|---|---|
{{WORKER_REGISTRY_TABLE}}

## Channels

- `{{PREFIX}}-orchestrator` — your inbox (read each turn)
- `{{PREFIX}}-control` — broadcasts to workers (send broadcasts here)
{{WORKER_INBOX_BULLETS}}
- `{{PREFIX}}-status` — firehose: workers post status + results here
- `{{PREFIX}}-telemetry` — heartbeats (liveness monitoring)
- `{{PREFIX}}-backlog` — persistent deferred tasks — **NEVER purge**

## Turn-start ritual

At the start of every user turn, before doing anything else:

1. `read_messages(channel="{{PREFIX}}-orchestrator", since_id=<last>)` — your inbox
2. `has_messages(channel="{{PREFIX}}-control", since_id=<last_control_id>)` →
   if pending, `read_messages(channel="{{PREFIX}}-control", ...)` to pick up broadcasts
3. `read_messages(channel="{{PREFIX}}-status", since_id=<last_status_id>)` — new results
4. Update task ledger from any `type: result` or `type: status` messages
5. If any worker posted `type: question` addressed to you, answer it first —
   that worker is blocked

Use `since_id=0` on the first turn of a fresh session. Remember the highest id seen
per channel and persist across turns.

## Dispatching tasks

```json
{
  "type": "task",
  "task_id": "{{PREFIX}}-<YYYY-MM-DD>-<slug>",
  "from": "orchestrator",
  "to": "<worker-name>",
  "subject": "short label",
  "depends_on": ["<other-task_id>:<worker>"],
  "required_checks": ["test", "committed"],
  "body": "full instructions\n\nAcceptance criteria:\n- [ ] <deliverable 1>\n- [ ] Tests pass\n- [ ] Committed",
  "acceptance_criteria": [
    "Each item the worker must explicitly confirm in their result body"
  ],
  "refs": []
}
```

Rules:
- `task_id` format: `{{PREFIX}}-2026-06-10-add-auth` (date + slug)
- **Always include `acceptance_criteria`** — workers must confirm every item before posting `type: result`
- **One task = one deliverable.** Never combine a server change with schema registration — use `depends_on`
- For code tasks: `required_checks: ["test", "committed"]`
- For schema-only: `required_checks: ["schema-registered", "smoke-test", "committed"]`
- For read/advisory: omit `"committed"`
- **Verify before closing**: when a result arrives, check that the body confirms each `acceptance_criteria` item. If any is missing, dispatch a continuation task.

## Sprint lifecycle

### Pre-sprint

1. Read `{{PREFIX}}-backlog` with `since_id=0` — build open-item list (every `deferred`
   without a matching `deferred-resolved`)
2. Dispatch baseline tasks to each worker: run tests, report pass/fail count
3. Capture baseline before any code changes, then proceed to sprint dispatch

### During sprint

- Maintain task ledger in-context: `{ task_id, worker, status, blockers }`
- On `type: question`: route answer to the asking worker's inbox promptly
- On `type: result` with `summary: "FAIL — ..."`: investigate, re-dispatch or accept gap
- On result with empty `body.commits` for a code task: ask worker to commit before closing

### Sprint close

1. Confirm all in-flight task_ids have `type: result` on `{{PREFIX}}-status`
2. Confirm all workers' commits are on the correct branch
3. `AskUserQuestion` to approve merge
4. `AskUserQuestion` to approve channel purge — show: channel names, message counts,
   open tasks being deferred, cost snapshot from
   `get_latest_per_sender("{{PREFIX}}-telemetry")`
5. Dispatch deferred items to `{{PREFIX}}-backlog` before purging
6. Purge all `{{PREFIX}}-*` channels **except** `{{PREFIX}}-backlog`

### Schema migration sequencing

1. Week 1: register warn-only (`strict: false`). Watch broker logs for schema warn lines.
2. Week 2+: no warn violations → flip to strict
3. **Never flip strict while a live client session is in-flight**

## Approval-token protocol

For production-touching actions (schema strict flip, merge to main), broadcast:

```json
{
  "type": "approval-token",
  "task_id": "<authorizing task_id>",
  "from": "orchestrator",
  "to": "*",
  "subject": "approval-token",
  "body": {
    "authorized_actions": ["<exact action name>"],
    "env": "prod",
    "scope_workers": {{WORKER_NAMES_JSON}},
    "expires_at": "<ISO timestamp, max 4h from now>",
    "approved_by": "human",
    "consent_basis": "terminal-human"
  }
}
```

Tokens NEVER authorize: `purge_channel`, force-push, hook bypass, secret rotation,
or anything outside `authorized_actions`. Each sprint requires a fresh token — never
reuse a prior sprint's approval.

## Worker stop conditions

Check after each turn-start ritual. Use `get_latest_per_sender("{{PREFIX}}-telemetry")`
for heartbeat state, `list_workers` for PID/uptime, `read_messages("{{PREFIX}}-status")`
for result history. Stop via `stop_worker(name=<worker>)`.

| Condition | How to detect | Action |
|---|---|---|
| **Blocked-on-question > 30 min** | Heartbeat `state: "blocked-on-question"` older than 30 min | Stop → send answer → restart |
| **Timeout loop (≥3 in a row)** | ≥3 consecutive `session-end` heartbeats, no `type: result` between | Stop → simplify task → re-dispatch |
| **Cost runaway (>$3, no result)** | `cost_since_start.estimated_usd > 3.0` in heartbeat, no result found | Stop → review scope → re-dispatch |
| **Sprint closed, inbox empty** | All task_ids have results, inbox is empty | Stop — sprint done |
| **File conflict in-flight** | Two workers have tasks owning the same file | Stop lower-priority → re-dispatch with `depends_on` |

Rules:
- Never stop a worker mid-task without first calling `check_result`
- After stopping for blocked/timeout: fix the root cause before restarting
- Cost-runaway stop: post a `type: note` on `{{PREFIX}}-status` explaining why

### Liveness enforcement during long-poll

- **Always use `filter_type="result"`** when waiting for a task result — skips notes,
  idempotency skips, idle-exit statuses, and heartbeats.
  Example: `wait_for_messages(channel="{{PREFIX}}-status", since_id=<last>, filter_sender=<worker>, filter_type="result", timeout_ms=300000)`
- Break long-polls into max 5-minute chunks (`timeout_ms=300000`)
- After each timeout: call `get_latest_per_sender("{{PREFIX}}-telemetry")`
- If heartbeat `ts` is >10 min old and `state` was `"working"` → silent-death:
  1. `stop_worker(name=<worker>)`
  2. Post `type: note` to `{{PREFIX}}-status`
  3. `AskUserQuestion` before restarting
- Never accumulate more than 3 consecutive timeouts without a telemetry check

## Orchestrator discipline

- **No code.** If you are about to edit a file, stop and dispatch to the worker instead.
- **Ledger first.** Check the ledger before dispatching — do not re-dispatch in-flight tasks.
- **Sequence before parallelism.** Parallel dispatch only when tasks genuinely don't conflict.
- **Use `check_result` for idempotency.** Before re-dispatching, check if the task already ran.
- **Small envelopes.** Body ≤300 tokens; put refs in `refs: []`, not inline.

## Starting workers

```bash
# Orchestrator
BROKER_SECRET=<secret> \
  {{WATCHDOG_PATH}} orchestrators/{{PROJECT_NAME}} \
    --repo-root {{TARGET_ROOT}} \
    --inbox-channel {{PREFIX}}-orchestrator

# Each worker
BROKER_SECRET=<secret> \
  {{WATCHDOG_PATH}} workers/<worker-name> \
    --repo-root {{TARGET_ROOT}} \
    --inbox-channel {{PREFIX}}-<worker-name>
```

Or start via broker MCP if workers-broker.json is configured:
`start_worker(name="{{PREFIX}}-<worker-name>")`

## Broker registration (cold-start only)

On first turn of a fresh session:
```
register_capability(
  worker="{{PREFIX}}-orchestrator",
  owns=["sprint-planning", "task-dispatch", "ledger"],
  channels=["{{PREFIX}}-orchestrator", "{{PREFIX}}-control", "{{PREFIX}}-status", "{{PREFIX}}-telemetry"]
)
```
~~~

**Filling the placeholders:**
- `{{WORKER_REGISTRY_TABLE}}` → one row per worker:
  `| \`<worker>\` | \`{{PREFIX}}-<worker>\` | <one-line owns summary> |`
- `{{WORKER_INBOX_BULLETS}}` → one bullet per worker:
  `- \`{{PREFIX}}-<worker>\` — <worker> inbox`
- `{{WORKER_NAMES_JSON}}` → e.g. `["api","web","db"]`
- `{{WATCHDOG_PATH}}` → ask the user or default to `<BROKER_REPO>/../dogsvilla/scripts/watchdog.sh`

### 4c. `workers/<name>/CLAUDE.md` (one per worker)

For each worker, create `<TARGET_ROOT>/workers/<WORKER>/CLAUDE.md`.

Replace placeholders before writing:
- `{{WORKER}}` → worker name (e.g. `api`)
- `{{WORKER_TITLE}}` → capitalized (e.g. `Api`)
- `{{WORKER_UPPER}}` → ALL-CAPS (e.g. `API`)
- `{{PROJECT_NAME}}`, `{{PREFIX}}`, `{{BROKER_URL}}`, `{{TARGET_ROOT}}` → actual values
- `{{OWNED_FILES_SUMMARY}}` → one sentence (e.g. "all API routes, middleware, and server config")
- `{{NEVER_TOUCH_SUMMARY}}` → one sentence (e.g. "frontend files and database migrations")
- `{{OWNERSHIP_TABLE_ROWS}}` → Markdown table rows: `| \`path/\` | purpose |`
- `{{NEVER_TOUCH_LIST}}` → comma-separated: `src/frontend/, migrations/`
- `{{OWNS_LIST}}` → JSON array of domain strings: `["routes","middleware","server-config"]`

~~~
# {{WORKER_TITLE}} Worker — {{PROJECT_NAME}}

## Identity

You are the **{{WORKER_UPPER}} WORKER** for `{{PROJECT_NAME}}`. You own
{{OWNED_FILES_SUMMARY}}.

You are **NOT** the orchestrator. You do not dispatch tasks; you receive them.
You do **NOT** touch {{NEVER_TOUCH_SUMMARY}}.

## Role

You are a worker in a multi-session setup for `{{PROJECT_NAME}}`.
An orchestrator dispatches work to you via the broker MCP server at `{{BROKER_URL}}`.

## Scope — what you own

| File / dir | What |
|---|---|
{{OWNERSHIP_TABLE_ROWS}}

**Never touch**: {{NEVER_TOUCH_LIST}}

## Channels

- `{{PREFIX}}-{{WORKER}}` — your inbox (read this first each turn)
- `{{PREFIX}}-control` — broadcasts from the orchestrator (check each turn)
- `{{PREFIX}}-status` — post all status updates + results here
- `{{PREFIX}}-telemetry` — post heartbeats here (every 5 min during long tasks)

## Turn-start ritual

At the start of every user turn, before doing anything else:

1. `read_messages(channel="{{PREFIX}}-{{WORKER}}", since_id=<last>)` — your inbox.
   Default `since_id=0` on first turn of a new session.
2. `has_messages(channel="{{PREFIX}}-control", since_id=<last_control_id>)`:
   - `pending: false` → skip
   - `pending: true` → `read_messages(channel="{{PREFIX}}-control", ...)`, process broadcasts
3. **Rotate check.** If any message has `type: "rotate"`, handle it before other messages.
4. For each `type: task` addressed to `to: "{{WORKER}}"` or `to: "*"`:
   - **Idempotency check FIRST**: `check_result(channel="{{PREFIX}}-status", task_id=<id>)`.
     If `found: true`, post `type: note` ("task <id> already done — skipping") and move on.
   - If `depends_on` is set, verify the dependency result is on `{{PREFIX}}-status`.
     If not: `wait_for_messages(channel="{{PREFIX}}-status", since_id=<last>, timeout_ms=270000)`.
5. If `type: question` addressed to you: answer it first — another worker is blocked.

## Heartbeat posting (every 5 minutes during long-running tasks)

Call `upsert_heartbeat` with:

```json
{
  "worker": "{{WORKER}}",
  "state": "working",
  "task_id": "<current task_id>",
  "context_tokens": <approximate count>,
  "cost_since_start": { "estimated_usd": <approximate cost> }
}
```

Post at the start of each task and every ~5 minutes thereafter.

Heartbeat states: `"working"`, `"idle-exit"`, `"blocked-on-question"`, `"rotating"`, `"session-end"`.
Post `"session-end"` just before exiting. Post `"idle-exit"` at the start of your exit note.

## Idle state — on-demand (drain and exit)

You run on demand. After posting `type: result`:

1. Check context size (see below)
2. `read_messages(channel="{{PREFIX}}-{{WORKER}}", since_id=<last>)` — drain remaining tasks
3. Process each task, repeat until inbox is empty
4. Post exit note to `{{PREFIX}}-status`, then exit

**Do NOT call `wait_for_messages` for idle polling** — only use it for `depends_on` blocking.

### Context check before idle-loop pickup

Before picking up the next task from the inbox:
- If context is above ~50% of your tier threshold: exit cleanly instead
- Post to `{{PREFIX}}-status`:
  ```json
  { "type": "status", "task_id": "context-check-<YYYY-MM-DD>",
    "from": "{{WORKER}}", "to": "orchestrator",
    "subject": "context rotation before idle pickup",
    "body": { "reason": "context-rotation-before-idle-pickup", "rotation_recommended": true } }
  ```
- Then post exit note and stop. The watchdog restarts a fresh session.

**Exit note:**
```json
{
  "type": "status",
  "task_id": "idle-loop-exit-<YYYY-MM-DD>",
  "from": "{{WORKER}}",
  "to": "orchestrator",
  "subject": "idle-loop exit",
  "body": { "reason": "inbox-drained", "last_task_id": "<last or null>" }
}
```

## Commit protocol

1. Run tests before committing (if this is a code task)
2. Stage **only your files**: `git add <your files>` — NEVER `git add .` or `git add -A`
3. Commit:
   ```bash
   git commit -m "$(cat <<'EOF'
   [<task_id>] <subject verbatim from envelope>

   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   EOF
   )"
   ```
4. Verify: `git show HEAD --name-only` — confirm only your files appear
5. Include in result: `body.commits: [{sha, branch, message}]`
6. If nothing changed: `commits: [], no_commit_reason: "<reason>"`

## Result envelope

Every `type: result` must include a top-level `summary`:
- `"PASS — <what was done, ≤30 words>"`
- `"FAIL — <what failed and why>"`
- `"SKIP — <reason>"`

For production-touching tasks, `body` must include `consent_basis`.

```json
{
  "type": "result",
  "task_id": "<same as incoming task_id>",
  "from": "{{WORKER}}",
  "to": "orchestrator",
  "subject": "<same as incoming subject>",
  "summary": "PASS — <description>",
  "body": {
    "required_checks": {
      "test": "PASS (N/N)",
      "committed": "PASS"
    },
    "commits": [{ "sha": "abc1234", "branch": "main", "message": "[{{PREFIX}}-...] ..." }],
    "consent_basis": "orchestrator-dispatch-only"
  }
}
```

If any `acceptance_criteria` item is not yet complete: post `type: question` instead of `type: result`.

## Cost discipline

**Never use the `Agent` tool.** Use direct tools: `Read`, `Edit`, `Write`, `Bash`.

**Rotate at 150k context.** Finish the current sub-task cleanly, then post to `{{PREFIX}}-status`:
```json
{
  "type": "status", "task_id": "<current task_id>",
  "from": "{{WORKER}}", "to": "orchestrator",
  "subject": "rotating — context at <N>k",
  "body": {
    "handoff_notes": {
      "task_id": "<id>", "done": "<what's done>", "pending": "<what remains>",
      "last_files_touched": []
    }
  }
}
```
Then exit. The watchdog restarts a fresh session.

## Rotation protocol

If a message has `type: "rotate"`:
1. Finish any in-progress sub-task
2. Post to `{{PREFIX}}-status`:
   ```json
   {
     "type": "status", "task_id": "<rotate task_id>",
     "from": "{{WORKER}}", "to": "orchestrator",
     "subject": "idle-loop exit — rotate requested",
     "body": {
       "reason": "orchestrator-rotate",
       "last_task_id": "<last or null>",
       "open_since_ids": { "inbox": N, "control": N, "status": N }
     }
   }
   ```
3. Exit — do NOT call `wait_for_messages` again.

## Broker registration (cold-start only)

On first turn of a new session, once:
```
register_capability(
  worker="{{WORKER}}",
  owns={{OWNS_LIST}},
  channels=["{{PREFIX}}-{{WORKER}}", "{{PREFIX}}-control", "{{PREFIX}}-status", "{{PREFIX}}-telemetry"]
)
```
~~~

### 4d. Commit generated files in target project

If the target project has a git repo (Step 2.5 confirmed or initialised one):

```bash
cd <TARGET_ROOT>
git add orchestrators/ workers/ .claude/ .gitignore
git commit -m "chore: scaffold broker-worker arrangement via /setup-broker"
```

If the user declined git init in Step 2.5: skip the commit and print:
```
⚠  No git commit made. Run the following when ready:
   cd <TARGET_ROOT>
   git init && git add orchestrators/ workers/ .claude/ .gitignore
   git commit -m "chore: scaffold broker-worker arrangement via /setup-broker"
```

---

## Step 5 — Generate CLAUDE-BROKER REPO files

### 5a. Schema files (6 files)

Read these reference schemas from the broker repo:

```bash
cat <BROKER_REPO>/schemas/cb-worker-inbox.json
cat <BROKER_REPO>/schemas/cb-orchestrator-inbox.json
cat <BROKER_REPO>/schemas/cb-status.json
cat <BROKER_REPO>/schemas/cb-control.json
cat <BROKER_REPO>/schemas/cb-telemetry.json
cat <BROKER_REPO>/schemas/cb-backlog.json
```

Create adapted versions for the new prefix. For each file, copy the `cb-` reference and:

**`schemas/<PREFIX>-worker-inbox.json`**: adapt `cb-worker-inbox.json`
- Update `title`, `description` to name the new project
- Change `task_id.pattern`: `^cb-` → `^<PREFIX>-`
- Update `from.description`, `to.description` with new worker names

**`schemas/<PREFIX>-orchestrator-inbox.json`**: adapt `cb-orchestrator-inbox.json`
- Update `title`, `description`, `from.description`, `to.description`
- Keep same allowed types: `question`, `note`, `status`

**`schemas/<PREFIX>-status.json`**: adapt `cb-status.json`
- Update `title`, `description`, `from.description` with new worker names

**`schemas/<PREFIX>-control.json`**: adapt `cb-control.json`
- Update `title`, `description`
- Keep same allowed types: `sprint-start`, `note`, `approval-token`, `approval-revoke`, `rotate`, `contract-change`

**`schemas/<PREFIX>-telemetry.json`**: adapt `cb-telemetry.json`
- Update `title`, `description`, `from.description` with new worker names

**`schemas/<PREFIX>-backlog.json`**: adapt `cb-backlog.json`
- Update `title`, `description` to reference new project and prefix

### 5b. Schema registration script

Create `<BROKER_REPO>/setup-schemas-<PREFIX>.js`:

```javascript
// Register <PROJECT_NAME> channel schemas with a running broker.
// Usage:
//   node setup-schemas-<PREFIX>.js                   # warn-only (safe default)
//   STRICT=1 node setup-schemas-<PREFIX>.js          # strict (reject invalid)
//   BROKER_URL=... BROKER_SECRET=... node setup-schemas-<PREFIX>.js
//
// Idempotent: re-running replaces schemas without side effects.

import { readFileSync } from "fs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import "dotenv/config";

const BROKER_URL = process.env.BROKER_URL || "http://localhost:8080/mcp";
const SECRET     = process.env.BROKER_SECRET || process.env.SHARED_SECRET || "";
const STRICT     = process.env.STRICT === "1";

const REGISTRATIONS = [
  { channel: "<PREFIX>-orchestrator", file: "schemas/<PREFIX>-orchestrator-inbox.json", strict: STRICT },
  { channel: "<PREFIX>-control",      file: "schemas/<PREFIX>-control.json",            strict: STRICT },
  { channel: "<PREFIX>-status",       file: "schemas/<PREFIX>-status.json",             strict: STRICT },
  { channel: "<PREFIX>-telemetry",    file: "schemas/<PREFIX>-telemetry.json",          strict: STRICT },
  { channel: "<PREFIX>-backlog",      file: "schemas/<PREFIX>-backlog.json",            strict: STRICT },
  // Worker inboxes
  { channel: "<PREFIX>-<worker1>",    file: "schemas/<PREFIX>-worker-inbox.json",       strict: STRICT },
  { channel: "<PREFIX>-<worker2>",    file: "schemas/<PREFIX>-worker-inbox.json",       strict: STRICT },
  // ... one entry per worker
];

async function main() {
  const transport = new StreamableHTTPClientTransport(new URL(BROKER_URL), {
    requestInit: SECRET ? { headers: { Authorization: `Bearer ${SECRET}` } } : {},
  });
  const client = new Client({ name: "setup-schemas-<PREFIX>", version: "1.0.0" });
  await client.connect(transport);

  console.log(`[setup-<PREFIX>] broker: ${BROKER_URL}`);
  console.log(`[setup-<PREFIX>] mode:   ${STRICT ? "STRICT (reject invalid)" : "warn-only (log but allow)"}`);
  console.log();

  for (const { channel, file, strict } of REGISTRATIONS) {
    const schema = readFileSync(file, "utf-8");
    const strictMode = strict !== undefined ? strict : STRICT;
    const res = await client.callTool({
      name: "register_channel_schema",
      arguments: { channel, schema, strict: strictMode, version: "1.0" },
    });
    const text = res.content?.[0]?.text ?? "(no response)";
    console.log(`  ${channel.padEnd(28)} ← ${file}`);
    console.log(`    ${text}`);
  }

  console.log();
  const list = await client.callTool({ name: "list_channel_schemas", arguments: {} });
  console.log(`[setup-<PREFIX>] registered schemas:`);
  console.log(list.content[0].text);

  await transport.close();
  console.log(`\n[setup-<PREFIX>] done`);
}

main().catch(e => { console.error("[setup-<PREFIX>] FAIL:", e); process.exit(1); });
```

Replace all `<PREFIX>` and `<worker>` placeholders with actual values. Expand the REGISTRATIONS array with one entry per worker inbox.

### 5c. Append to workers config

First detect the correct workers config file from the broker's `.env`:
- Read `<BROKER_REPO>/.env`
- Find the line `WORKERS_CONFIG=<path>`
- If present: use that path as the workers config file
- If absent: fall back to `<BROKER_REPO>/workers-broker.json` and warn: `[setup-broker] WORKERS_CONFIG not set in .env — falling back to workers-broker.json`

Then apply a name-collision guard before appending to that file. It is a JSON array — for each entry you plan to add, check if an entry with that `name` already exists:
- If `array.some(e => e.name === newEntry.name)` → skip that entry (already present)
- Otherwise → append it to the array
- If all entries were skipped: print `[setup-broker] workers-broker.json — all entries already present, skipped` and do not rewrite the file
- If any entries were appended: write the updated array back to the file and proceed to commit as normal

Append one entry per worker and one for the orchestrator. Use this shape:

```json
{
  "name": "<PREFIX>-<worker>",
  "ns": "<PREFIX>",
  "args": ["workers/<worker>", "--repo-root", "<TARGET_ROOT>", "--inbox-channel", "<PREFIX>-<worker>"]
}
```

Orchestrator entry:
```json
{
  "name": "<PREFIX>-orch",
  "ns": "<PREFIX>",
  "args": ["orchestrators/<PROJECT_NAME>", "--repo-root", "<TARGET_ROOT>", "--inbox-channel", "<PREFIX>-orchestrator"]
}
```

> **Note — env var distribution**: `workers-broker.json` entries do not support an `env` block; the broker's `start_worker` ignores it. `BROKER_URL` and `BROKER_SECRET` must be set in the environment where the broker server runs (the broker's own `.env` file) **or** passed explicitly in the watchdog start commands printed in Step 7.

### 5d. Update `.env` PRUNE_EXEMPT

Read `<BROKER_REPO>/.env`. Find the line starting with `PRUNE_EXEMPT=`. Before appending, check if `<PREFIX>-backlog` is already in the value:
- If `<PREFIX>-backlog` is already in the value → skip, print `[setup-broker] PRUNE_EXEMPT already contains <PREFIX>-backlog, skipped`
- Otherwise → append `,<PREFIX>-backlog` to the value and write the file back

Example: `PRUNE_EXEMPT=dv-backlog,cb-backlog` → `PRUNE_EXEMPT=dv-backlog,cb-backlog,<PREFIX>-backlog`

If there is no `PRUNE_EXEMPT` line: add `PRUNE_EXEMPT=<PREFIX>-backlog` at the end.

### 5e. Commit broker repo changes

Use the workers config file path detected in Step 5c (`<WORKERS_CONFIG_FILE>` = the `WORKERS_CONFIG` path from `.env`, or `workers-broker.json` as fallback):

```bash
cd <BROKER_REPO>
git add schemas/<PREFIX>-*.json setup-schemas-<PREFIX>.js <WORKERS_CONFIG_FILE> .env
git commit -m "chore: add <PREFIX> project schemas and worker config"
```

If the commit is empty (no changes detected — e.g. re-running setup for an existing prefix): skip silently.

---

## Step 6 — Run schema registration

```bash
cd <BROKER_REPO>
BROKER_URL=<BROKER_URL> BROKER_SECRET=<BROKER_SECRET> node setup-schemas-<PREFIX>.js
```

If connection fails: warn the user that schemas were not registered yet. They can run this manually once the broker is reachable. Continue to step 7.

If it succeeds: confirm each expected channel appears in the output.

---

## Step 7 — Print watchdog start commands

Print the following for the user to copy-paste into terminals.

**If BROKER_URL is localhost or 127.0.0.1** — omit BROKER_URL from each command:

```
=== WORKER START COMMANDS ===

# Orchestrator (open a dedicated terminal tab)
BROKER_SECRET=<BROKER_SECRET> \
  <WATCHDOG_PATH> orchestrators/<PROJECT_NAME> \
    --repo-root <TARGET_ROOT> \
    --inbox-channel <PREFIX>-orchestrator

# Worker: <worker1>
BROKER_SECRET=<BROKER_SECRET> \
  <WATCHDOG_PATH> workers/<worker1> \
    --repo-root <TARGET_ROOT> \
    --inbox-channel <PREFIX>-<worker1>

# (one block per additional worker)
```

**If BROKER_URL is remote (not localhost/127.0.0.1)** — prepend `BROKER_URL=<BROKER_URL>` to every command:

```
=== WORKER START COMMANDS ===

# Orchestrator (open a dedicated terminal tab)
BROKER_URL=<BROKER_URL> BROKER_SECRET=<BROKER_SECRET> \
  <WATCHDOG_PATH> orchestrators/<PROJECT_NAME> \
    --repo-root <TARGET_ROOT> \
    --inbox-channel <PREFIX>-orchestrator

# Worker: <worker1>
BROKER_URL=<BROKER_URL> BROKER_SECRET=<BROKER_SECRET> \
  <WATCHDOG_PATH> workers/<worker1> \
    --repo-root <TARGET_ROOT> \
    --inbox-channel <PREFIX>-<worker1>

# (one block per additional worker)
```

If BROKER_URL is NOT localhost/127.0.0.1, also print:

```
⚠  REMOTE BROKER — additional steps required
   ─────────────────────────────────────────
   1. TLS:  Ensure broker endpoint is HTTPS — use a reverse proxy
            (nginx, Caddy) in front of the broker's HTTP port.
            Never expose the broker's plain HTTP port to the internet.

   2. Auth: BROKER_SECRET must be distributed out-of-band to every
            machine running a worker session (password manager,
            encrypted secret store). NEVER commit it to git.

   3. Env:  Set BROKER_URL=<BROKER_URL> in each worker's environment
            (or in the watchdog start command) so it reaches the
            remote broker rather than localhost.

   4. Team: If multiple developers share the broker, rotate
            SHARED_SECRET and redistribute after any team membership
            change.
```

---

## Verification checklist

Confirm each item aloud to the user before finishing:

- [ ] `<TARGET_ROOT>/.claude/settings.json` (or `settings.local.json`) written
- [ ] `<TARGET_ROOT>/orchestrators/<PROJECT_NAME>/CLAUDE.md` written — full protocol, not a stub
- [ ] `<TARGET_ROOT>/workers/<name>/CLAUDE.md` written for each worker — full protocol
- [ ] Worker templates include: turn-start ritual, 5-min heartbeat cadence, idle-loop exit, result envelope with `summary` + `consent_basis`
- [ ] Orchestrator template includes: turn-start ritual, sprint lifecycle, stop conditions, approval-token protocol, channel layout table, worker registry
- [ ] `<BROKER_REPO>/schemas/<PREFIX>-*.json` created (6 files)
- [ ] `<BROKER_REPO>/setup-schemas-<PREFIX>.js` created — uses `BROKER_URL` env var (not hardcoded)
- [ ] Workers config file updated (path detected from `WORKERS_CONFIG` in broker `.env`, or `workers-broker.json` if unset) — new entries appended, name-collision guard skipped duplicates (no `env` block; env vars distributed via watchdog commands or broker `.env`)
- [ ] `<BROKER_REPO>/.env` `PRUNE_EXEMPT` line updated to include `<PREFIX>-backlog`
- [ ] Schema registration ran successfully (or user notified of failure + manual command)
- [ ] Watchdog start commands printed
- [ ] Remote broker HTTPS + secret distribution note printed (if applicable)
- [ ] Target project files committed to git (or reminder printed if no repo)
- [ ] Broker repo schema + config files committed
