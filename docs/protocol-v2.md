# Protocol intelligence v2 — heartbeat channel + envelope validator

Two complementary additions to the claude-broker protocol. Together they shift
the orchestrator from "reconstruct system state from message archaeology" to
"read it directly," and stop workers from silently violating the envelope
contract.

Drafted 2026-05-27 from the dogsvilla pilot cost analysis.

| Spec | Status | Notes |
|---|---|---|
| Spec 1 — Heartbeat + telemetry channel | **Implemented** | `upsert_heartbeat`, `get_latest_per_sender` tools live; dv-telemetry + cb-telemetry registered |
| Spec 2 — Broker-side envelope validator | **Implemented** | `register_channel_schema`, warn-only mode live; dv-* and cb-* schemas registered |
| Spec 3 — Sprint-close conflict pre-flight | **Implemented** | `sprint_file_conflicts` tool live; dv-status schema enforces `affected_files` |

Motivating data: the dogsvilla pilot (4 sessions, 34h, 4815 turns) cost
~$3,472 at API-equivalent pricing. ~$1,200 of that was the 1M-context-tier
surcharge from sessions running past 200k context. The pilot also surfaced
silent envelope drift (missing `consent_basis`, mistyped `task_id`,
wrong-cased `to:` values) that the broker had no way to catch.

---

## Spec 1 — Heartbeat envelope + `dv-telemetry` channel

### Channel

`dv-telemetry` — new channel, purpose-built for machine consumption.
Separate from `dv-status` because:

- `dv-status` is the durable audit log of human-meaningful events.
  Heartbeats would flood it.
- `dv-telemetry` gets purged at sprint start; stale heartbeats are noise.
- Different cadence, different retention, different consumer.

Workers write; only the orchestrator reads.

### Envelope shape

JSON-as-string in `content`, new `type: "heartbeat"`.

```json
{
  "type": "heartbeat",
  "from": "backend" | "frontend" | "customer-portal" | "orchestrator",
  "ts": "2026-05-27T18:00:00Z",
  "session_id": "<jsonl filename stem>",
  "model": "claude-opus-4-7" | "claude-sonnet-4-6",
  "context": {
    "size_tokens": 147823,
    "cache_read": 145200,
    "cache_create": 2623,
    "tier_threshold_pct": 73.9,
    "rotation_recommended": true
  },
  "activity": {
    "last_tool_call_ts": "2026-05-27T17:59:42Z",
    "current_task_id": "fix-2026-05-27-loyalty-03",
    "state": "working" | "idle-polling" | "blocked-on-question" | "rotating"
  },
  "cost_since_start": {
    "input_tokens": 18432,
    "output_tokens": 281443,
    "cache_read_tokens": 89401234,
    "cache_create_tokens": 1284322,
    "estimated_usd": 217.43
  }
}
```

### Cadence

| state | interval |
|---|---|
| `working` | every 90s |
| `idle-polling` | every 5 min |
| state transitions | immediate |
| session start | cold-start ping with zero costs |
| session end | final ping with `state: "rotating"` |

Cost envelope: ~150 output tokens × 4 workers × 40/hr peak ≈ 24k tok/hr ≈
$1.80/hr at standard tier. Acceptable for the observability gained.

### Field semantics worth pinning

- **`context.tier_threshold_pct`**: `size_tokens / 200_000 * 100`. Crossing
  75% sets `rotation_recommended: true` — the contract orchestrator depends on.
- **`activity.state`**: smallest enum that captures decision-affecting state.
  `working` vs `idle-polling` distinguishes "in-flight work" from "waiting on
  broker"; `blocked-on-question` is the deadlock signal.
- **`cost_since_start.estimated_usd`**: computed by the worker from its own
  usage telemetry plus a hardcoded pricing table. Not authoritative —
  orientation, not billing.

### Orchestrator consumption

At turn-start, read `dv-telemetry` with `since_id`. Build in-context map
`worker → latest_heartbeat`. Triggers:

1. **Rotation reminder.** Any worker with `rotation_recommended: true` AND
   `state: working` → dispatch `type: note` to its inbox:
   *"rotate when current sub-task closes — context at \<N\>k"*.
2. **Deadlock escalation.** Any worker `state: blocked-on-question` for
   > 5min with no question reply landing → escalate via `AskUserQuestion`.
3. **Budget alert.** Sum of `cost_since_start.estimated_usd` across workers
   crosses configured sprint budget → `type: note` to `dv-control`.
4. **Liveness.** No heartbeat from a known-running worker for > 2× expected
   interval → assume crashed; surface to user.

### Purge policy

Orchestrator calls `purge_channel("dv-telemetry")` at sprint start. Workers
don't read this channel — only orchestrator does — so purge is safe.

### Worker implementation surface

Each worker's CLAUDE.md gets a "Heartbeat protocol" section alongside
Turn-start ritual and Idle state:

> After every tool call that returned a `usage` payload, update in-memory
> cost accumulators. Before the absolute-last tool of each turn
> (`wait_for_messages` for idle, or text response), if it's been ≥90s
> since last heartbeat OR state has changed OR `tier_threshold_pct` just
> crossed 75%, send a heartbeat to `dv-telemetry`.

No broker changes required for the heartbeat itself — pure convention layer.

### Heartbeat compliance requirement

All active workers MUST post a heartbeat to their project's telemetry channel
at least once every **5 minutes** while active.

Telemetry channel by namespace:

| Namespace | Telemetry channel |
|---|---|
| `cb-*` workers | `cb-telemetry` |
| `dv-*` workers | `dv-telemetry` |
| `rp-*` workers | `rp-telemetry` |

A worker that has been running for more than one turn with zero telemetry
entries is **non-compliant**. The orchestrator cannot apply standard stop
conditions (context rotation, cost-runaway, blocked-on-question) without
heartbeat data — the worker becomes a monitoring blind spot.

**Stop conditions table: Heartbeat non-compliance**

| Condition | How to detect | Orchestrator action |
|---|---|---|
| Worker active >20 min, zero telemetry entries, no result on status channel | `read_messages(telemetry, since_id=0)` returns empty | Treat as **silent-death** — escalate to human via `AskUserQuestion` |
| Worker active >5 min with no new heartbeat after last known entry | Stale timestamp on most recent heartbeat | Post `type: note` to worker inbox: *"no heartbeat in >5m — are you still active?"* |

**Background (2026-06-20, rp-admin):** during ridepro Phase 4 sprint the
rp-admin worker ran for 30+ minutes with zero entries in rp-telemetry. Normal
stop conditions (cost-runaway, blocked-on-question) could not be applied;
human intervention was required. This incident motivated the formal 5-minute
compliance requirement and the >20-min silent-death escalation rule.

---

## Spec 2 — Broker-side envelope validator

### Goal

Reject malformed messages at send-time with a clear error pointing at the
missing field. Today the broker is opaque to content; drift accumulates
silently.

### Design principles

- **Opt-in per channel.** General-purpose channels (e.g. ad-hoc `demo`)
  stay free-form. Schemas registered explicitly.
- **Warn-only first, strict later.** Ship in warn-only mode for a week so
  in-flight sessions (using old conventions) don't break mid-sprint. Flip
  to strict after clean logs.
- **Schemas live in SQLite.** Not in a config file — same persistence
  story as messages, hot-reload without server restart.

### Schema storage

```sql
CREATE TABLE IF NOT EXISTS channel_schemas (
  channel    TEXT PRIMARY KEY,
  schema     TEXT NOT NULL,            -- JSON Schema draft-07
  strict     INTEGER NOT NULL DEFAULT 0, -- 0 = warn-only, 1 = reject
  updated_at INTEGER NOT NULL
);
```

### New MCP tools

```
register_channel_schema(channel, schema_json, strict?)   -- idempotent upsert
get_channel_schema(channel)                              -- returns current schema or null
clear_channel_schema(channel)                            -- revert to free-form
list_channel_schemas()                                   -- which channels are schema'd
```

### Validation hook (inside existing `send_message`)

Pseudocode insertion right after destructuring `{channel, sender, content}`:

```js
const schemaRow = stmtGetSchema.get(channel);
if (schemaRow) {
  let parsed;
  try { parsed = JSON.parse(content); }
  catch (e) {
    if (schemaRow.strict) return errResp(`channel '${channel}' requires JSON: ${e.message}`);
  }
  if (parsed) {
    const valid = ajv.validate(JSON.parse(schemaRow.schema), parsed);
    if (!valid) {
      const msg = ajv.errors.map(e => `${e.instancePath || '/'} ${e.message}`).join('; ');
      if (schemaRow.strict) {
        return errResp(
          `schema validation failed on '${channel}': ${msg}. ` +
          `Call get_channel_schema('${channel}') to see required fields.`
        );
      } else {
        console.warn(`[validator] ${channel} warn: ${msg}`);
      }
    }
  }
}
// existing stmtInsert.run(...) continues unchanged
```

Add `ajv` to package.json. Net ~80 LOC in server.js + a small handful of new
tool registrations.

### Initial schemas to register

**Worker inboxes** (`dv-backend`, `dv-frontend`, `dv-customer-portal`):

```json
{
  "type": "object",
  "required": ["type", "task_id", "from", "to", "subject"],
  "properties": {
    "type": {"enum": ["task", "question", "note", "approval-token", "approval-revoke"]},
    "task_id": {"type": "string", "pattern": "^[a-z][a-z0-9-]*-\\d{4}-\\d{2}-\\d{2}-[a-z0-9-]+$"},
    "from": {"const": "orchestrator"},
    "to": {"enum": ["backend", "frontend", "customer-portal", "*"]},
    "subject": {"type": "string", "minLength": 3},
    "depends_on": {"type": "array", "items": {"type": "string"}},
    "required_checks": {"type": "array", "items": {"type": "string"}},
    "wire_compat": {"enum": ["additive", "breaking"]},
    "body": {},
    "refs": {"type": "array"}
  },
  "additionalProperties": false
}
```

**`dv-status`** — worker → orchestrator. Conditional rule: `type: result` for
a prod-touching task MUST carry `body.consent_basis`:

```json
{
  "type": "object",
  "required": ["type", "task_id", "from", "to", "subject"],
  "properties": {
    "type": {"enum": ["status", "result", "question", "note"]},
    "from": {"enum": ["backend", "frontend", "customer-portal"]},
    "to": {"enum": ["orchestrator", "backend", "frontend", "customer-portal"]}
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "type": {"const": "result"},
          "body": {"properties": {"production_touching": {"const": true}}}
        }
      },
      "then": { "properties": { "body": { "required": ["consent_basis"] } } }
    }
  ]
}
```

**`dv-control`** — broadcasts. `contract-change` requires `before`/`after`/
`affected_workers`. `approval-token` requires `authorized_actions`/`env`/
`scope_workers`/`expires_at`.

**`dv-telemetry`** — the heartbeat schema from Spec 1.

### Error shape returned to worker

Standard MCP error response, but the human-readable text is what matters
because that's what the model sees:

```
schema validation failed on 'dv-status': /body 'consent_basis' is required (production_touching: true).
Call get_channel_schema('dv-status') to see required fields.
```

Self-correcting: model reads the error, sees what's missing, retries with
the fix.

### Migration / rollout

1. Land the validator code in warn-only mode for ALL existing schemas.
2. Watch `console.warn` logs for a week of active sprints.
3. Patch any false-positives or schema bugs.
4. Flip `dv-control` and worker inboxes to `strict: 1`. Leave `dv-status`
   warn-only longer (most diverse, highest-risk to make strict).
5. After two clean sprints in strict mode, declare done.

### Failure mode for orchestrator

Repeated validation failures from the same worker → that worker's CLAUDE.md
is out of sync with the schema. Orchestrator should `AskUserQuestion` whether
to restart it.

### Effort estimate

- server.js: ~80 LOC for 4 new tools + validation hook
- 1 dependency: `ajv` (~100KB)
- 4 initial schemas: ~150 lines JSON total
- README update: 1 new section

Half-day for the server work + an afternoon for the schema authoring + week
of warn-only burn-in before flipping strict.

---

---

## Spec 3 — Sprint-close conflict pre-flight

### Motivation

Real incident (2026-06-18, dogsvilla): `worker/bs` cherry-picked orphaned
commits from `worker/backend`'s scope to obtain implementation code before
writing tests. Those commits created `deposit-reconciliation.service.ts` in
`worker/bs`. Later, `worker/backend` added a per-payment try/catch to the same
file. Sprint-close merged `worker/backend` first (fine), then tried to merge
`worker/bs` — add/add conflict. Manual resolution required:

```bash
git checkout --ours backend/src/jobs/deposit-reconciliation.service.ts
git add ...
GIT_EDITOR=true git merge --continue
git push origin main          # ← required before re-run, or startup rebase fails
./scripts/sprint-close-merge.sh ...  # re-run skips resolved workers
```

The conflict was detectable in advance: both workers had posted `type:result`
messages with `affected_files` listing the same file. A pre-flight query
would have caught it before the merge script ran.

### New broker tool: `sprint_file_conflicts`

Added to `server.js`. Accepts `status_channel` + optional `since_id`.

Algorithm:
1. Query all `type:result` messages (SKIP results excluded).
2. Parse `affected_files` from each; group by file path → list of `{worker, task_id}`.
3. Files with 2+ distinct workers → `conflicts` list (merge conflict risk).
4. Workers that posted results without `affected_files` → `blind_spots` list.

Returns:
```json
{
  "conflicts": [
    {
      "file": "backend/src/jobs/deposit-reconciliation.service.ts",
      "workers": ["backend", "bs"],
      "touches": [
        { "worker": "backend", "task_id": "add-payment-deposit-reconciliation-job-2026-06-18" },
        { "worker": "bs",      "task_id": "add-tests-for-add-payment-deposit-reconciliation-job-2026-06-18" }
      ]
    }
  ],
  "clean_count": 14,
  "blind_spots": ["frontend"],
  "summary": "1 conflict(s) detected — resolve before running sprint-close"
}
```

### Schema enforcement: `affected_files` required when commits non-empty

`dv-status.json` updated with an `allOf` condition: when `type === "result"`
AND `body.commits` is a non-empty array, `affected_files` is required.
Broker runs in warn-only mode, so this produces a schema warning rather than
a hard rejection, but it makes the expectation explicit and surfaced.

### Worker protocol rule: cherry-pick declaration

**When any worker cherry-picks commits that touch files outside its scope
ownership table, it MUST:**

1. Post a `type: note` to its cluster status channel immediately after the
   cherry-pick, before continuing with the task:

   ```json
   {
     "type": "note",
     "task_id": "<current task_id>",
     "from": "<worker>",
     "to": "orchestrator",
     "subject": "cherry-pick adoption: <short description>",
     "body": {
       "adopted_files": ["<list of files cherry-picked outside scope>"],
       "source_branch": "worker/<other-worker>",
       "reason": "<why the cherry-pick was necessary>",
       "conflict_risk": "<low|medium|high> — <one sentence assessment>"
     }
   }
   ```

2. Include the cherry-picked files in its `affected_files` on the final
   `type: result`, even though those files are outside its normal scope
   ownership table.

**Why this matters:** cherry-picks from another worker's scope create
add/add conflicts at sprint-close if the source worker subsequently modifies
the same file. The declaration gives the orchestrator visibility to run
`sprint_file_conflicts` and pre-resolve before the merge script runs.

**Rule of thumb:** if you are cherry-picking to "bootstrap" implementation
code before writing tests, that is a scope ownership signal — either the
task should have been dispatched to the owning worker first (with
`depends_on`), or the test task should be `depends_on` the implementation
result and run only after it is committed on the owner's branch.

### Orchestrator sprint-close checklist addition

Before calling `scripts/sprint-close-merge.sh`, add this step:

```
3a. Call sprint_file_conflicts(status_channel="dv-status").
    - If conflicts = []: proceed.
    - If conflicts non-empty: for each conflict, inspect which version to
      keep, update the lower-priority worker's branch to match (or note the
      conflict for manual --ours resolution during the merge), then proceed.
    - If blind_spots non-empty: warn; those workers may have undetected
      conflicts. Check their affected files manually via git diff.
```

---

## Schema registry — current coverage

All schemas registered warn-only (strict=false). Flip to strict only after two
clean sprints with no `[claude-broker] schema warn` lines in broker logs.

Registration script: `node setup-schemas-broker.js` (re-run is idempotent).

### dogsvilla (`dv-` namespace)

| Channel | Schema file | Registered | Strict |
|---|---|---|---|
| `dv-backend`, `dv-frontend`, `dv-customer-portal` | `schemas/dv-worker-inbox.json` | 2026-05-27 | warn-only |
| `dv-orchestrator` | *(uses dv-worker-inbox schema)* | 2026-05-27 | warn-only |
| `dv-qa` | `schemas/dv-worker-inbox.json` | 2026-06-20 (sprint-009) | **strict** (flipped sprint-011) |
| `dv-control` | `schemas/dv-control.json` | 2026-05-27 | warn-only |
| `dv-status` | `schemas/dv-status.json` | 2026-05-27 | warn-only |
| `dv-telemetry` | `schemas/dv-telemetry.json` | 2026-05-27 | warn-only |

Registered via `node setup-schemas.js`.

### claude-broker self-maintenance (`cb-` namespace)

| Channel | Schema file | Registered | Strict |
|---|---|---|---|
| `cb-core` | `schemas/cb-worker-inbox.json` | 2026-06-19 | warn-only |
| `cb-protocol-qa` | `schemas/cb-worker-inbox.json` | 2026-06-19 | warn-only |
| `cb-orchestrator` | `schemas/cb-orchestrator-inbox.json` | 2026-06-19 | warn-only |
| `cb-control` | `schemas/cb-control.json` | 2026-06-19 | warn-only |
| `cb-status` | `schemas/cb-status.json` | 2026-06-19 | warn-only |
| `cb-telemetry` | `schemas/cb-telemetry.json` | 2026-06-19 | warn-only |

Registered via `node setup-schemas-broker.js`.

Key cb-* schema constraints:
- `cb-status` type:result requires `summary` and `body.consent_basis`. Use
  `"consent_basis": "orchestrator-dispatch-only"` for non-production tasks.
- `cb-status` type:result with `body.commits` non-empty requires `affected_files`.
- `cb-worker-inbox` type:approval-token requires full `body` (authorized_actions,
  env, scope_workers, expires_at).

---

## What this pair gives you, together

- **Orchestrator stops reconstructing system state from message archaeology**
  and starts reading it directly from `dv-telemetry`.
- **Workers can't silently violate the envelope contract** — broker says
  "no" with a fixable error.
- **The 150k rotation rule becomes data-driven, not vibes-driven** —
  heartbeats include the threshold percentage and the orchestrator
  dispatches reminders mechanically.
- **Cost telemetry becomes a first-class observable** instead of an
  after-the-fact 4800-row JSONL crunch.

## Schema Versioning

Schema versions are tracked with the `version` field on `register_channel_schema`.
All schemas registered by the setup scripts carry `version: "1.0"` as of sprint-011
(2026-06-20, task `cb-2026-06-20-pqa-023`).

### Baseline

All channels in `cb-*`, `dv-*`, `rp-*`, and `dx-*` namespaces are stamped `version: 1.0`.

Verify via `get_channel_schema`:
```
Channel: cb-status
Strict: on
Version: 1.0
Updated: 2026-06-20T05:59:55.807Z
```

### Versioning convention

| Change type | Version bump |
|---|---|
| Add optional field | none (backwards-compatible) |
| Add required field | minor bump (e.g. `1.0` → `1.1`) |
| Remove field or change type | major bump (e.g. `1.0` → `2.0`) |
| Tighten an `enum` or `pattern` | major bump |

Schema updates mid-sprint require a sprint boundary: workers that sent messages
under the old schema may fail validation against the new schema. The chosen
approach is (b) — schema updates land at sprint start, not mid-flight.

When bumping, re-run the relevant `setup-schemas-*.js` script with the new
version string and file the change under the sprint's protocol-qa result.

### Channel strict-flip log

| Channel | Registered warn-only | Flipped strict | Hold reason |
|---|---|---|---|
| `dv-qa` | sprint-009 (2026-06-20) | sprint-011 (2026-06-20, pqa-024b) | `task_id` pattern `^…-[0-9]{4}-[0-9]{2}-[0-9]{2}-[a-z0-9-]+$` required trailing suffix; date-terminal IDs (e.g. `qa-fix-verify-bundle-gaps-2026-06-20`) are valid — suffix made optional |

### Schema coverage milestone — sprint-016 (2026-06-20)

As of sprint-016, **all 36 channels** across all active namespaces are strict-validated. Zero warn-only channels remain.

| Namespace | Channel count | Status |
|---|---|---|
| `cb-*` | 7 | All strict |
| `dv-*` | 10 | All strict |
| `rp-*` | 11 | All strict |
| `dx-*` | 8 | All strict |

### Test coverage milestone — sprint-017 (2026-06-20)

`test-v2.js` now covers all **29 MCP tools** with **167 assertions** (added Section 19 in sprint-017, task `cb-2026-06-20-pqa-035`).

## Open questions

- **Heartbeat compaction.** At 90s cadence × 4 workers × multi-hour sprint,
  `dv-telemetry` will accumulate hundreds of messages. Purge-at-sprint-start
  handles freshness but the orchestrator's `read_messages` will return a
  large set. Consider a `get_latest_heartbeats()` MCP tool that returns
  one-per-sender efficiently.
- **Hook-based enforcement as alternative.** Both items here could
  alternatively be implemented as Claude Code hooks on the worker side,
  no broker changes. Broker-centric design chosen because (a) it's the
  single source of truth across heterogeneous sessions, (b) hooks would
  duplicate the schema knowledge in every worker.

## Sequencing recommendation

Ship the validator first (Spec 2). It's lower-risk, doesn't depend on
worker behavior changes, and immediately catches the existing envelope
drift in dogsvilla. The heartbeat protocol (Spec 1) builds on a validated
envelope baseline — easier to land when malformed heartbeats can't poison
the telemetry channel.
