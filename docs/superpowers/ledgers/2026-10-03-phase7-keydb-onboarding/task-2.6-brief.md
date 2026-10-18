# Task 2.6 brief: `KEYDB.MVCCRESTORE` applied, LWW-guarded (implementer: Sonnet 5.5)

You are the implementer for **Task 2.6** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
The orchestrator commits; **you do not commit or push**. Tasks 2.1–2.5 are committed. Read
`task-2.4-report.md` and the "For the next task" notes of `task-2.2-review.md` (2.6 paragraph:
`KEYDB.MVCCRESTORE` is currently dropped by the unknown-command check *before* stamping; when it
becomes known its `Stamp` must use its **own** `<mvcc>`) and `task-2.3-review.md` first.

## Read, in order

1. Plan: Global Constraints; **Task 2.6** in full (Step 0 re-probe, the exact enum and function,
   every test name, falsifications (a)–(f) and the vacuity note).
2. Spec **D-7a** (binding), D-7, D-5 (the leaf: a non-`kOk` translation is "consumed, `Advance`
   only", like a skipped command; a `kOk` one is a leaf like any other), D-13 (the counter renders
   like `keydb_cmds_dropped`, i.e. one `kClassicCounters` row + `ClassicLinkCounts` field).
3. `task-2.1-brief.md` "Environment". Same rules. **The container is shared with a running perf
   measurement (Task 1.6 Step 1) until the orchestrator tells you it finished: you may read code
   and write tests, but do not build or run anything in `drakeydb-p7` before that message.**
4. Code: `ApplyCommand`'s order (skip checks → `CommandScope` → unknown check → `Reserve` →
   `Reservation` → rewrites → `Stamp` → `Unguard` → `Dispatch` → `Commit`); the hook goes **before**
   the unknown-command check and **after** the KeyDB-only skip (`IsKeyDbOnlyCommand` stays
   unchanged: `KEYDB.MVCCRESTORE` is not in its list, decision 22). `generic_family.cc:69-97`
   (`GetRdbVersion`, anonymous: reimplement the 10-line length/version check, say why),
   `rdb_extensions.h:21` (`rdbIsObjectTypeDF`), `RESTORE`'s handler and its `ABSTTL`/`REPLACE`
   parsing, `multimaster_lww.cc:111-125` (the `RESTORE` guard rewrite finds `REPLACE` present).
   KeyDB (read-only reference clone `KeyDB/` in the main checkout, or `~/.paseo/…/KeyDB` if present):
   `cluster.cpp:5206-5240` (handler), `replication.cpp:5573-5594` (emitter), `expire.h:8`
   (`INVALID_EXPIRE`), `rdb.h:96` (`RDB_TYPE_CRON` = 64).

## Deliver

Exactly Task 2.6 Steps 0–5, with:

- Step 0's three probes recorded verbatim in the report (a real active KeyDB X with a raw replica,
  merge-syncing from active KeyDB Z: one `KEYDB.MVCCRESTORE` envelope per key, keep the bytes as a
  golden vector under `tests/dragonfly/data/` with a README row; `DUMP`/`RESTORE … REPLACE ABSTTL`
  of each type onto drakeydb; `RESTORE` with `ABSTTL` 0/1/-1/LLONG_MAX and `PTTL`).
- Peer mode: `repl_mvcc = own_mvcc != 0 ? own_mvcc : envelope.mvcc`, author's origin; the dedup
  reservation and watermark use the **envelope's** `(uuid, mvcc)` as for any leaf (D-7a: "the dedup
  watermark is unaffected"). Plain: `repl_mvcc = 0`, verbatim apply.
- `keydb_mvccrestore_failed` per link (+ totals, INFO, peer line, `/metrics`), the exact-equality
  pins extended; `kCronPayload` counts in `keydb_cmds_dropped` only. An error reply from the
  dispatched `RESTORE` counts as `keydb_mvccrestore_failed`, not `classic_apply_errors`.
- The two pytests as named (the mesh-merge one needs two active KeyDBs, D and D2; record the time
  from `REPLICAOF A` to full arrival; the fake-master one wraps in `flock`).

## Report

`task-2.6-report.md`, same shape as before, plus the Step 0 probe outputs and the golden vector's
provenance. Reply with changed files, test counts before/after, report path. Do not commit.
