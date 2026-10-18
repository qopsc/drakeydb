# Task 2.8 brief: envelope-time apply (implementer: Sonnet 5.5)

You are the implementer for **Task 2.8** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
The orchestrator commits; **you do not commit or push**. Tasks 2.1–2.7 are committed; read
`task-2.7-report.md` (the depth-1 sample site you share) and the "For the next task" notes of the
2.4–2.7 reviews first. `CommandScope` (per-dispatch RAII in `ApplyCommand`) is where `repl_time_ms`
is set and restored.

## Read, in order

1. Plan: Global Constraints; **Task 2.8 in full** — Step 0's clock-read audit table is a
   deliverable, every interface paragraph is binding (`EnvelopeTimeMs` pure, the 60 s window
   tested without unsigned wrap, the `m - 1` edge, evaluated **once** per envelope tree at depth 1
   for the **outermost** layer, after `kOk`/`running()`/self check and before `ParseSingleCommand`;
   `ConnectionContext::repl_time_ms` per command; `PrepareTransaction` copies it beside
   `SetReplOrigin`; `Transaction::SetReplTime`/`InitTxTime`; the squashed stub copies the parent's
   time; raw batches keep the local clock), every test name and the ten pytest flips.
2. Spec D-9 (the window, what 2.8 closes and leaves open), decisions 24, 27, 30 and **45**.
3. `task-2.1-brief.md` "Environment". Same rules; the container is free.
4. Code: `conn_context.h` (`repl_*` fields, Task 2.4's updated comments), `main_service.cc`
   `PrepareTransaction` (`:891-893` region: both branches), `transaction.{h,cc}` (`InitTxTime`,
   `time_now_ms_`, the constructor's clock read at `:144`, `MultiUpdateWithParent` `:556`, the
   retry at `:943`), `ReplicaActiveExpiryTest` in `classic_replay_test.cc` (`--hz 0`, hand-run
   heartbeats, `AdvanceTime`), the ten window pytests in `keydb_onboarding_test.py` (search
   `loses_a_ttl_refresh`, `recomputes_a_counter`, `ttl_less_orphan`).

## Deliver

Exactly Task 2.8 Steps 0–5, plus **decision 45 (owner, 2026-10-17): the seventh counter.**
The plan's "a fallback of a non-zero mvcc … is a `LOG_EVERY_T(WARNING, 60)` … and no counter" is
superseded: add the process-wide counter `multimaster_envelope_time_fallback` (relaxed atomic in
`classic_replay.cc` beside the other process-wide values), bumped at the single depth-1
evaluation when a **non-zero** mvcc falls outside the 60 s window (mvcc 0 is not a fallback), kept
beside the `LOG_EVERY_T`. Render it through `ClassicProcessWideValues` (INFO active block,
plain-replica block, `/metrics` as `multimaster_envelope_time_fallback_total`) under the same
`active_master_seen || value != 0` predicate, and extend the exact-equality INFO/metrics pins
(now 4 process-wide values). Falsify by not bumping (the far-stamp pytest's counter assertion
fails) and by bumping on mvcc 0.

Other notes:
- Task 2.7's sampler and this evaluation live at the same depth-1 site; the clock is read **once**
  there and both consumers get the same `now_ms` (the plan's "why once").
- `repl_time_ms` is a separate field, not `repl_mvcc >> 20` (plain links need it; peer stamps
  stay whole).
- Not in scope: the stream clock, the sweep and the read-path hide (2.9). Do not touch
  `engine_shard.cc`, `db_slice.cc` or `generic_family.cc`.
- `docs/UPSTREAM-SYNC.md` rows for `transaction.{h,cc}`, `conn_context.h`, `main_service.cc`.

## Tests

All names in the plan's Step 1 (the `ClassicApplyFamilyTest` envelope-time cases incl. the 1 ms
edge, nested outermost, relative TTL anchoring, the override not outliving its command on every
exit path, plain and peer, the fallback keeping the peer stamp; `ClassicReplayTest.EnvelopeTimeMs*`
table; the direct `Transaction` retry test with a `friend`; the pytest flips exactly as listed
and the new far-stamp pytest), plus the counter tests above. Falsify per Step 4 (a)–(j), each
restored and recorded verbatim; (k) is a review item, say so.

## Report

`task-2.8-report.md`, same shape, with the Step 0 audit table and the flip table (which of the ten
cases flipped, which stayed, and why). Reply with changed files, test counts before/after, report
path. Do not commit.
