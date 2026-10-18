# Task 2.4 brief: guard ON for classic peer links; rewrites and EVAL on every envelope link (Sonnet 5.5)

You are the implementer for **Task 2.4** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
The orchestrator commits; **you do not commit or push**. Tasks 2.1–2.3 are committed; read
`task-2.2-report.md` and `task-2.3-report.md` plus the "For the next task" sections of
`task-2.2-review.md` and `task-2.3-review.md` first: `CommandScope` (the per-dispatch RAII scope in
`ApplyCommand`) and the dedup reservation are what you extend.

## Read, in order

1. Plan: Global Constraints; **Task 2.4** (every test name is a deliverable, Step 5's peer-mode
   release perf re-run included); Task 2.8's first paragraph (the shared RAII scope).
2. Spec **D-4** points 2–4 and **D-6** (the KeyDB wire-form table; `ClassicApplyRewrites`'s exact
   rules; why rewrites are not gated on the guard), D-9's residual paragraph the plan quotes.
3. `task-2.1-brief.md` "Environment". Same rules. The release build for Step 5: `./helio/blaze.sh
   -release -DWITH_AWS=OFF -DWITH_GCP=OFF` then **only** `CCACHE_DISABLE=1 ninja -C build-opt -j4
   dragonfly` inside `drakeydb-p7`; free disk first if needed (`df -h`; delete non-gate debug test
   binaries, they relink later). `task-1.5-report.md` has the perf harness and how `DRAKEYDB_PERF=1`
   is run; the test is `test_keydb_onboarding_keeps_up_under_load` (`keydb_onboarding_test.py`).
4. Code: `multimaster_lww.{h,cc}` (`ApplyLwwRewrites` contract comment, `LwwGuardActive`,
   `IncomingStamp`, the guard table), `journal/executor.cc:47-62` (the DFLY-link gate, **unchanged**),
   `transaction.cc` `RunSquashedMultiCb` tripwire comment and `ShouldDropForLww`, `transaction.h`
   `IsLwwGuarded`, `conn_context.h` `repl_lww_guard`, `replica.cc` where `repl_lww_guard` is set
   for DFLY links (search `repl_lww_guard`; the classic path must set it once per link with the
   same expression `IsPeerMode() && IsActiveReplica() && FLAGS_multi_master_stream_lww`),
   `string_family.cc` `SetCmd` NX/XX/GET/KEEPTTL parsing and `MSETNX`, `docs/multi-master.md:311-381`.

## Deliver

Exactly Task 2.4 Steps 1–6, with:

- `bool ClassicApplyRewrites(cmn::BackedArguments*)` in `classic_replay.{h,cc}`, table-driven and
  pure: `SET` loses `NX`/`XX`/`GET` anywhere after the value, case-insensitive; `PX|PXAT|EX|EXAT <n>`
  and `KEEPTTL` kept, **except** `KEEPTTL` dropped when `NX` was present; `MSETNX` → `MSET`;
  everything else untouched. Returns whether it changed anything.
- On **every** envelope dispatch, plain and peer: `ApplyLwwRewrites` then `ClassicApplyRewrites`,
  unconditionally, in `ApplyCommand` before `Dispatch` (raw stream commands untouched;
  `JournalExecutor`'s gate untouched). `ApplyLwwRewrites`'s contract comment gets the envelope
  carve-out.
- `repl_lww_guard` set once per classic link at link setup (`ConsumeRedisStream`, beside
  `repl_origin_idx`) to `IsPeerMode() && IsActiveReplica() && FLAGS_multi_master_stream_lww`.
- `EVAL`/`EVALSHA`: stamped (2.2's `Stamp`) with `repl_lww_guard = false` for that dispatch,
  restored after: extend `CommandScope` (set/restore the guard bit; the link's bit is what it
  restores to, captured at construction like `link_origin_idx_`). The `RunSquashedMultiCb` tripwire
  comment and `docs/multi-master.md:311-381` are rewritten as the plan says.
- Interaction with 2.3: order inside the leaf is reserve → rewrites → stamp → dispatch → commit/
  release (D-5 "kApply: rewrites (D-4.3), DispatchCommand"); keep the RAII shapes.
- Not in scope: `KEYDB.MVCCRESTORE` (2.6), `repl_time_ms` (2.8).

## Tests

All names in the plan's Step 1 (gtest `ClassicReplayTest.ClassicApplyRewrites*` table incl. `NX
KEEPTTL`, `XX KEEPTTL`, `NX GET`, lower case, `MSETNX`, non-SET untouched; the `ClassicApplyFamilyTest`
/ `ClassicPeerApplyFamilyTest` cases; the four pytests incl. `test_keydb_writes_not_forwarded_to_peers`
with two peer drakeydbs both attached to one KeyDB and `assert_no_command_storm`). Every LWW test
asserts **which value survived**. Falsify per Step 4, each restored and recorded verbatim.

## Step 5: peer-mode perf re-run (release build)

Parametrize Task 1.5's test over peer mode; three release runs, median, `DRAKEYDB_PERF=1`; record
the numbers beside Task 1.5's plain-mode ones. If the peer-mode release bar fails, **stop and
report** (it reopens Task 1.6); do not optimize in this task.

## Report

`task-2.4-report.md`, same shape as `task-2.1-report.md`, plus the perf table. Reply with changed
files, test counts before/after, perf numbers, report path. Do not commit.
