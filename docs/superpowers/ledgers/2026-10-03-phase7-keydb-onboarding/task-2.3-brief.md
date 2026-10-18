# Task 2.3 brief: `AuthorDedup` (implementer: Sonnet 5.5)

You are the implementer for **Task 2.3** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
The orchestrator commits; **you do not commit or push**. Tasks 2.1 and 2.2 are committed on this
branch; read `task-2.1-report.md` and `task-2.2-report.md` (and the "For the next task" sections of
their reviews) before anything else: the applier's per-command RAII scope and `HandleRreplay`'s
layer loop are what you hook into.

## Read, in order

1. Plan `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md`: Global Constraints, **Task
   2.3** (every test name there is a deliverable), and D-3's `EnvelopeResult` contract in
   `classic_replay.h` (`kConsumed` / `kNotConsumed`).
2. Spec **D-5** in full (`docs/superpowers/specs/2026-10-03-phase7-keydb-onboarding-design.md`):
   the reservation protocol, leaf vs outer levels, cancellation and offsets, the flushing-full-sync
   `Clear()`, the bound. D-5 is binding; where the plan and D-5 differ, D-5 wins and you say so.
3. `task-2.1-brief.md` "Environment" (container `drakeydb-p7`, `/home/user/drakeydb`, `-j4`, flock,
   pinned venv, host pre-commit). Same rules. Two-fiber gtests run on `pp_->at(0)` / `pp_->at(1)`
   as `MvccOwnershipFiberTest`-style tests in `multi_master_test.cc` do; look there first.
4. `helio/util/fibers/synchronization.h` (`fb2::Mutex`, `CondVarAny::wait_for`); `replica.cc`
   `InitiatePSync` (`FlushAll` / `FlushSlots` call sites, the peer merge path that must **not**
   clear), `ConsumeRedisStream` (where `repl_offs_ += total_read` happens relative to
   `HandleRreplay`), `classic_replay.cc` (`HandleRreplay`, `ApplyCommand`, `Dispatch`'s result
   classification: OK vs rejected/OOM).

## Deliver

Exactly Task 2.3 Steps 1–5 as written, with:

- `AuthorDedup` in `classic_replay.{h,cc}`: process-wide (one instance, `AuthorDedup::Instance()` or
  passed from `Replica`; pick the smaller hook and say why), `Entry{applied, inflight, last_seen_ms}`
  in an `absl::flat_hash_map` under one `fb2::Mutex` + `fb2::CondVarAny`; **leaf lock**: never held
  across a dispatch, an `AddOrGet`, an `AwaitBrief` or a log call; re-look-up after every wait.
  `Reserve(uuid, mvcc, running)`, `Commit`, `Release`, `Advance`, `IsApplied`, `Clear()`,
  `kMaxAuthorDedupEntries = 4096` with D-5's eviction. An RAII reservation guard in the applier so
  every exit of the leaf commits or releases.
- Wiring per D-5: one reservation per envelope tree on the **innermost** dispatched level only;
  outer levels `IsApplied` early-out, then `Advance` on an inner `kConsumed` (deduped inner
  included); skipped inner commands are `Advance` only; malformed/self-authored never advance;
  `kCancelled` ⇒ `kNotConsumed` (dbs may already be selected: allowed, see D-5); `Commit`/`Advance`
  before `repl_offs_ +=` (check `ConsumeRedisStream`'s order and pin it with
  `ReplayAfterCommitBeforeCountIsDeduped`); ERROR/OOM ⇒ `Release` + `classic_apply_errors`, still
  consumed. `multimaster_rreplay_deduped` counter rendered by D-13 (Task 2.1's `ClassicProcessWideValues`
  path for process-wide values; extend the exact pins from 2 to 3 process-wide values).
- `Clear()` right after `FlushAll` and after `FlushSlots` in `InitiatePSync`; **not** on a peer merge
  sync, **not** on `+CONTINUE`.
- Interaction with Task 2.2's scope: the reservation wraps the dispatch inside the same per-command
  scope (stamps set, then reserve, dispatch, commit/release, restore). Keep the scope's structure so
  Task 2.4 (guard/rewrites) and 2.8 (`repl_time_ms`) slot in.
- Not in scope: rewrites, guard bit, EVAL (2.4); `KEYDB.MVCCRESTORE` (2.6).

## Tests

All names in the plan's Task 2.3 Step 1: the `AuthorDedupTest.*` two-fiber unit tests, the
`ClassicApplyFamilyTest` cases, `ClassicAuthorMapTest.DedupStillWorksForOverCapAuthor`, and the two
pytests (`test_keydb_mesh_forwarded_duplicates_deduped` with two forwarding KeyDBs and **pipelined**
`INCR` at full rate; `test_master_switch_full_sync_resets_author_dedup` with the proxy). Falsify per
Step 4 (a)–(f), each restored and recorded verbatim, including the statistical note for (a)'s pytest
over 5 runs.

## Report

`task-2.3-report.md`, same shape as `task-2.1-report.md`. Say explicitly: where `Commit` sits relative
to `repl_offs_ +=`; every lock acquisition site of `mu_` and what runs under it; the eviction cost;
what each new test passes under with the feature removed. Reply with changed files, test counts
before/after, report path. Do not commit.
