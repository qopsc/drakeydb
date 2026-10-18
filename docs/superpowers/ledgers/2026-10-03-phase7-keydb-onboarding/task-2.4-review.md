# Task 2.4 review: guard ON for classic peer links; rewrites and EVAL on every envelope link

Reviewer: Opus 5.5. BASE `52b35e47`, working tree uncommitted (the report's state). I read the plan's
Task 2.4 section, spec D-4, D-5 and D-6, `decisions.md` 42–46, `task-2.3-review.md` "For the next task",
the report, and every hunk of `git diff 52b35e47 -- src tests docs`. I also read the comments on every
`repl_lww_guard` / `ApplyLwwRewrites` site outside the diff. Source is unchanged: the probes and mutants
were applied to copies and restored, and all three files `cmp`-identical to the saved originals afterwards.

## Verdict: **Needs fixes**

The code does what the spec asks, and I found no behavioural defect. Every probe passed, and the
falsifications I re-ran fail as the report says. One Important item: the comments outside the diff still
state the invariants this task breaks: the guard bit is "never reset per command" and "false for a classic
link", `ApplyLwwRewrites` "lives exclusively inside `JournalExecutor::Execute`", and "a classic link never
carries an mvcc". Plan Step 3 asked for the "classic links are never guarded" premise to be retired. The
diff does that in `docs/multi-master.md:311-381` but leaves it standing elsewhere. The rest is Minor.

R15 (perf) is outside the verdict. Its attribution is **confirmed**: see "R15 perf runs".

## Findings

| ID | Sev | file:line | What | Why | Suggested fix |
|---|---|---|---|---|---|
| I1 | Important | `src/server/conn_context.h:371-377`; `src/server/test_utils.h:119-125`; `src/server/transaction.h:396-398`, `:739-741`; `docs/multi-master.md:140-142` | Stale invariants. **conn_context.h** (the field's own doc): `repl_lww_guard` is "set once via `JournalExecutor::SetApplyLwwGuard()`", "stays false for … every other link (plain replica, classic Redis/KeyDB stream)" and is "never reset per command". It is now set by `ConsumeRedisStream`, true on a classic peer link, and toggled per EVAL dispatch by `CommandScope::Unguard`. **test_utils.h**: `ApplyLwwRewrites` "lives exclusively inside `JournalExecutor::Execute`"; `ClassicApplier::ApplyCommand` now calls it too. **transaction.h:397**: "a zero mvcc -- classic Redis/KeyDB link"; **:741**: false for "a non-guarded apply (plain replica, classic link, flag off)". **multi-master.md:140-142**: "a classic Redis/KeyDB link never carries an `mvcc` at all, so guarding it would silently drop its entire stream". It contradicts the rewritten section 170 lines below it. | These are the canonical comments for the field and the predicate. A reader reasoning about the tripwire or the guard from them gets the old model. The brief counts an overstating comment as a finding, and Step 3 asked for the premise to be retired. | conn_context.h: name `ConsumeRedisStream` as the second setter, "true on a classic peer link (only enveloped commands carry a real mvcc)", and "never reset per command, except `ClassicApplier`'s per-dispatch `CommandScope` (EVAL unguarded, restored)". test_utils.h: "inside `JournalExecutor::Execute` (and unconditionally in `ClassicApplier::ApplyCommand` for envelope commands)". transaction.h: "a raw classic command" / "classic raw command" instead of "classic link". multi-master.md:141: "a raw classic command never carries an mvcc (an RREPLAY envelope's does, see below)". |
| M1 | Minor | `docs/multi-master.md:331-341` | "Two things are done on **every** envelope link, plain or peer": the second is "`EVAL`/`EVALSHA` are stamped with the envelope's mvcc and author like any command". On a plain link nothing is stamped (`authors_.stamping()` is false, `repl_mvcc` stays 0, as D-4.4 says), and `Unguard` is a no-op there. | It overstates: it reads as if plain links stamp EVALs. | Make the EVAL sentence peer-only: "On a peer node, `EVAL`/`EVALSHA` are stamped … with `repl_lww_guard` forced `false`". Keep "two things on every link" for the rewrites alone. |
| M2 | Minor | `src/server/classic_replay.cc:836-837`; `src/server/transaction.cc:1641-1642` | classic_replay.cc: "the guard compares one key per command and trips on a multi-key one (RunSquashedMultiCb)". transaction.cc: "would trip this on every script of an envelope". The tripwire is reached only through the `SQUASHED_STUB` that `CanRunSingleShardMulti` builds for a script whose keys are all on **one** shard. A multi-shard script does not reach it (my probe `EvalMultiShardKeys`, three keys, passes either way). A multi-key `MSET` is guarded per key elsewhere. | Imprecise about the mechanism the override exists for. | classic_replay.cc: "a script's inner commands share one stamp; a single-shard script runs them through a SQUASHED_STUB, where the guard cannot compare per command (the RunSquashedMultiCb tripwire)". transaction.cc: "on every single-shard script of an envelope". |
| M3 | Minor | `src/server/classic_replay.cc:838` | Only `EVAL`/`EVALSHA` are unguarded. `EVAL_RO`/`EVALSHA_RO` (registered, `main_service.cc:3232-3238`) would reach the stub guarded, and the tripwire would fire (DFATAL in debug). | Unreachable from KeyDB 6.3.4: no `_RO` variants, and a read-only script is never propagated. The tree's own idiom elsewhere is `StartsWith(name, "EVAL")` (`main_service.cc:601,668`). | `absl::StartsWithIgnoreCase(name, "EVAL")`, or a comment saying why `_RO` cannot arrive. |
| M4 | Minor | `tests/dragonfly/keydb_onboarding_test.py` (none) | D-6 "raw streams untouched" is structural (`ClassicApplyRewrites` is called only from `ApplyCommand`) and **not pinned**: no test sends a raw `SET k v NX` and checks that it is still conditional. | Brief probe (d). A refactor that moves the rewrites into a shared dispatch path would go unnoticed. | FakeClassicMaster, plain and peer: raw `SET k old`, raw `SET k new NX`, marker. Assert `k == old`. |
| M5 | Minor | `src/server/classic_replay.cc:276-299` | `RewriteSet` builds `kept` (a copy of every arg) for any `SET` with ≥ 4 args, even when nothing is dropped. `SET k v PXAT <t>`, the common KeyDB expiry form, pays a 5-string copy per command and then discards it. | Wasted allocations on the apply hot path. **Not** the R15 cause: the harness's SETs have 3 args (early return), and the report's diagnostic run 2 (no rewrites) still failed. | Set `changed` in the first pass (any NX/XX/GET, or KEEPTTL under NX). Build `kept` only if it is true. |
| M6 | Minor | `keydb_onboarding_test.py:2378`, `:4761`, `:4938` | "(Task 2.4)" / "(Task 2.4's path)" in test comments and docstrings: plan numbering. The file had one such reference at BASE. | Process text, a mild form. A reader without the ledger cannot resolve it. | Say what it means: "(a peer node's classic link is LWW-guarded)". |
| M7 | Minor | `src/server/classic_replay_test.cc:618-637` | `ClassicApplyRewritesLeavesOtherCommandsAlone` special-cases `{"MSETNX"}` inside its "untouched" loop. | The table claims "untouched" and then makes an exception. | Move the bare-`MSETNX` row into `ClassicApplyRewritesMsetnxBecomesMset`. |
| M8 | Minor | `src/server/classic_replay_test.cc:1683-1700` | Mutant m8 (the guard bit not restored) is caught only by the field read `EXPECT_TRUE(link.cntx.repl_lww_guard)`. No test shows a command after an EVAL on the **same** link being vetoed again. | The field read is enough today. A behavioural check would survive a refactor of where the bit lives. | Optional: after the EVAL in `EvalStamped…`, apply a stale `SET e x` on the same link and assert `e == from-script`. |

No Critical findings.

## Spec axis, item by item

- **Interfaces.** `bool ClassicApplyRewrites(cmn::BackedArguments*)` (`classic_replay.h:92-100`) matches.
  - `SET` loses `NX`/`XX`/`GET` anywhere after the value, in any case. The expiry options' values are skipped in both passes. `KEEPTTL` goes only under `NX`.
  - `MSETNX` becomes `MSET`.
- **Guard bit.** `replica.cc:1227-1228` uses `IsPeerMode() && IsActiveReplica() && FLAGS_multi_master_stream_lww`, the same expression as the DFLY flow at `:1921-1922`. Every consumer goes through `LwwGuardActive`, which I grepped: `generic_family.cc:621,767,1504`, `string_family.cc:419,1009`, `executor.cc:57`, `transaction.h:401`. So a raw command (mvcc 0) on a now-true link is unguarded, and so is an mvcc-0 envelope.
- **`--active_replica` off.** The guard bit is false. A non-KeyDB master sends no envelopes, and DFLY links are untouched, so behaviour is byte-identical for every non-KeyDB master. On a plain replica of an active KeyDB, the rewrites change behaviour: that is the spec's intent (D-4.3), with the D-9 residual.
- **Order (2.3 review's items).** Skip checks → `CommandScope` → unknown check → `Reserve` → `Reservation` → `ApplyLwwRewrites`, `ClassicApplyRewrites` → `Stamp` → `Unguard` → `Dispatch` → `Commit`. Both items landed:
  - every setter and rewrite comes after `Reserve`, so a `kDrop`/`kCancelled` leaf returns before them;
  - the `Reservation` is built from `(author, mvcc)` before `cmd` is edited, and the rewrites touch only `cmd`.
- **EVAL.** It is stamped with the envelope's mvcc and author only when `stamping()` is true (peer), so a plain link keeps mvcc 0, as D-4.4 says. The guard bit is forced false for that dispatch and restored by `~CommandScope`.
- **Not asked for, done.** The perf harness gained `master_link()` (reads the `master0` line in peer mode) and skips `dragonfly_replica_reconnect_count` for a peer node, relying on KeyDB's sync counters. Both are needed for Step 5; fine. The report already says that the contract comment is in `multimaster_lww.h`, not `.cc`.
- **Not done.** Step 6 (commit): the report stops at Step 5 as the brief said.

## Probes (brief a–e)

I appended throwaway tests to `classic_replay_test.cc`, built, ran them and restored the file (`cmp`
identical). Verbatim:

```
[       OK ] ClassicReplayTest.ReviewProbeRewriteTable (0 ms)
I1018 15:08:23.711534  103488 classic_replay_test.cc:3501] PROBE lww_dropped delta=1
[       OK ] ClassicPeerApplyFamilyTest.ReviewProbeVetoCountedOnceAndDedupedOnSecondLink (141 ms)
I1018 15:08:23.754743  103494 classic_replay_test.cc:3510] PROBE msetnx odd apply_errors=1
I1018 15:08:23.754837  103494 classic_replay_test.cc:3513] PROBE set-2-args apply_errors=2
I1018 15:08:23.754876  103494 classic_replay_test.cc:3516] PROBE msetnx-alone apply_errors=3
[       OK ] ClassicPeerApplyFamilyTest.ReviewProbeMsetnxOddArgsNoCrash (40 ms)
I1018 15:08:23.818318  103497 classic_replay_test.cc:3548] PROBE eval apply_errors=4
[       OK ] ClassicPeerApplyFamilyTest.ReviewProbeFailingEvalRestoresGuard (59 ms)
[       OK ] ClassicPeerApplyFamilyTest.ReviewProbeEvalMultiShardKeys (25 ms)
```

- **(a) The table.** All of these rewrite as expected:

  | Input | Result |
  |---|---|
  | `SET k v GET` | `SET k v` |
  | `SET k v NX PX 100` | `SET k v PX 100` (`100` is not read as an option) |
  | `SET k v KEEPTTL NX` | `SET k v` |
  | `SET k nx` | untouched, `changed == false` |
  | `SET k nx NX` | `SET k nx` |
  | `SET k v NX PX` (dangling) | `SET k v PX` |
  | `SET k v keepttl nx px nx` | `SET k v px nx` |
  | `SET k` | untouched |

  Odd-arity commands do not crash:
  - `MSETNX a 1 b` becomes `MSET a 1 b`. The dispatcher rejects its arity, so it counts one `classic_apply_errors`, is consumed, and the watermark is not advanced (`Release`).
  - `SET k` and a bare `MSETNX` behave the same way: consumed, counted, no crash.
  - KeyDB never sends any of these.
- **(b) `Unguard` scope.** After each of these, on the same link, the bit is back to `true` and `repl_mvcc` back to 0:
  - an EVAL that throws (`error('boom')`);
  - an EVAL whose `redis.call` fails;
  - an `EVALSHA` NOSCRIPT;
  - an arity-rejected bare `EVAL`.

  Every exit after `CommandScope` goes through its destructor; there is no early return between `Unguard` and the end of `ApplyCommand`.

  The guard bit is copied onto the Transaction **at dispatch time**: `PrepareTransaction` → `SetReplOrigin(..., dfly_cntx->repl_lww_guard)` (`main_service.cc:891-893`), after `Unguard`. The stub constructor copies it from that parent (`transaction.cc:183`), so the stub sees the override. Mutant m5 confirms that without it the stub sees `true`.
- **(c) A vetoed envelope.**
  - The veto replies OK, so `Dispatch` returns `ran == true` with no error, and the leaf `Commit`s. The author's watermark advances (`IsApplied(kAuthorA, kMvccA)` is true afterwards).
  - `multimaster_lww_dropped` moved by **exactly 1**.
  - The same envelope on a second link of the same node is **dropped by dedup, not re-vetoed**: the delta stays 1, there are no apply errors, and `k` stays `local`.
  - That is right per D-5: `DispatchResult::OK` is `Commit`, and OK includes a command whose own outcome was a no-op or an error reply. The veto is the command's outcome. A re-veto would give the same result anyway, since a key's stamp only grows.
- **(d) The raw path.** It is untouched by construction. Raw commands go through `DispatchSquashedBatch`/`DispatchCommand` in `ConsumeRedisStream` and never through `ApplyCommand`, and `ClassicApplyRewrites` has one caller. Not pinned by a test (M4).
- **(e) `docs/multi-master.md:311-365`.** Accurate against the code:
  - the guard needs peer mode, `--active_replica` and the flag (the expression is quoted); plain links are never guarded;
  - raw commands are never guarded (mvcc 0);
  - EVAL is stamped and unguarded;
  - the only route to the stub is a single-shard script.

  Exceptions: M1 (EVAL stamping reads as if it happened on plain links too) and I1 (line 140 still says a classic link has no mvcc).

## Falsification audit (reproduced)

Each mutant was applied to the real file, rebuilt with `ninja -C build-dbg -j4 <target>` in `drakeydb-p7`,
run, then the file was restored from a saved copy. Afterwards: `cmp` identical for all three files,
`classic_replay_test` 148/148, and the 7 new or adjusted pytest cases `7 passed, 180 deselected in 18.49s`.

**m5: `scope.Unguard()` removed** (`ClassicPeerApplyFamilyTest.Eval*`):
```
F1018 15:09:34.625033  103538 transaction.cc:1649] multi-master LWW guard reached RunSquashedMultiCb: repl_mvcc_ is batch-level here, not per-command, and cannot be safely compared -- applying unguarded
*** SIGABRT received at time=1792336175 on cpu 0 ***
```

**m8: `~CommandScope` does not restore `repl_lww_guard`** (full suite):
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1692: Failure
Value of: link.cntx.repl_lww_guard
  Actual: false
Expected: true
[  FAILED  ] ClassicPeerApplyFamilyTest.EvalStampedWithEnvelopeMvccUnguardedNoDfatal (20 ms)
[  PASSED  ] 147 tests.
```

**m3: both rewrites gated on `LwwGuardActive(cntx_->repl_lww_guard, mvcc)`** (full suite):
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1743: Failure
    Which is: 'old'
/home/user/drakeydb/src/server/classic_replay_test.cc:1744: Failure
    Which is: nil
/home/user/drakeydb/src/server/classic_replay_test.cc:1745: Failure
    Which is: 'old-a'
/home/user/drakeydb/src/server/classic_replay_test.cc:1746: Failure
    Which is: nil
[  FAILED  ] ClassicApplyFamilyTest.PlainApplierRewritesConditionalSet (21 ms)
[  PASSED  ] 147 tests.
```
Only the plain test fails. The peer tests pass under m3, as the report says, because a peer link is guarded.

**m1: `conn_context.repl_lww_guard = false;` in `ConsumeRedisStream`** (pytest, debug):
```
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_lww_concurrent_set_converges[df_factory0-guard_on]
E           AssertionError: ('keydb', True)
E              -'keydb'   +'local'
================= 1 failed, 3 passed, 183 deselected in 9.55s ==================
```
(`guard_off` and both `test_unwrap_flushes…` params pass under m1, as expected.)

**R16: P7-1 mutant (a), `flush_batch();` removed from the envelope branch** (`replica.cc:1362`):
```
FAILED 😰 … test_unwrap_flushes_raw_batch_before_envelope[df_factory0-plain_replica]
FAILED 😰 … test_unwrap_flushes_raw_batch_before_envelope[df_factory0-peer_mode]
E           AssertionError: a ended at 2: the stream was not applied in order
E              -'2'   +'4'
E           AssertionError: a ended at 2: the stream was not applied in order
E              -'2'   +'4'
```
**R16 is accepted.** The test still asserts that the raw batch is flushed before the envelope (end value
4, not 2), and it fails under its original mutant in both modes. The hour-ahead mvcc only stops the guard
from vetoing the envelope `SET a 2` against the raw `SET a 1`'s local mint. The new comment explaining
this is accurate.

**Vacuity.** The report's "What each test still passes with the feature removed" paragraph is accurate,
and I add nothing to it:
- the gtests set the guard bit in the fixture, so only pytest sees m1;
- `test_keydb_writes_not_forwarded_to_peers` pins Task 2.2's no-forward, not this task's guard, and fails only under `kSelfIdx`;
- the "a duplicate runs no rewrite" ordering is structural.

## R15 perf runs (existing `build-opt/dragonfly`, not rebuilt)

**Setup.**
- `ninja -C build-opt -n dragonfly` reports "no work to do": the binary (07:50:53) is newer than the restored `classic_replay.cc` (07:50:19).
- Before each run: wait until the 1-minute load is < 0.5, take `flock /tmp/drakey-pytest.lock`, then run with `DRAKEYDB_PERF=1` and the Task 1.5 harness pinning. Nothing else was running.
- Every top-5 `ps` at lock showed only idle processes, plus a stale, idle `classic_replay_` (pid 50327, 0% CPU) left in the container by an earlier session.

**Results.**

| # | Test | Load at lock | Result |
|---|---|---|---|
| 1 | peer mode, guard on (`…keeps_up_under_load[df_factory0-peer_mode]`) | 0.36 | **FAIL**, link down, 14.87 s |
| 2 | plain replica control | 0.45 | **pass**, 32.95 s |
| 3 | peer mode, `--df multi_master_stream_lww=false` | 0.45 | **FAIL**, link down, 24.31 s |
| 1b | peer mode, guard on (re-run for KeyDB's log) | 0.08 | **FAIL**, another sync during load, 33.13 s |

1. **Peer mode, guard on**, 14:58Z:
   ```
   E       AssertionError: the replica's link is down: {… 'active_replica': 1, … 'master0': {'host': 'localhost', 'port': 5555, 'link_status': 'down', … 'rreplay_unwrapped': 6694968, … 'classic_apply_errors': 0, … 'repl_offset': 957276378}, … 'multimaster_lww_dropped': 0, 'multimaster_rreplay_deduped': 0, … 'multimaster_peer_registry_size': 2}
   tests/dragonfly/keydb_onboarding_test.py:4634: AssertionError
   ============================== 1 failed in 14.87s ==============================
   ```
   The harness wipes the per-test log directory at the start of each test, so run 3 (same test id) overwrote this run's KeyDB log. I re-ran it once (1b) to capture KeyDB's log.

   **1b**, 15:03Z, KeyDB log (verbatim, truncated at 330 characters):
   ```
   M 18 Oct 2026 15:03:33.298 * Synchronization with replica 127.0.0.1:36063 succeeded
   M 18 Oct 2026 15:03:47.778 # Client id=4 addr=127.0.0.1:47048 … flags=S … omem=268435455 tot-mem=268485495 events=rw cmd=replconf … scheduled to be closed ASAP due to exceeding out
   M 18 Oct 2026 15:03:47.779 # Connection with replica 127.0.0.1:36063 lost.
   M 18 Oct 2026 15:03:49.198 * Synchronization with replica 127.0.0.1:36063 succeeded
   M 18 Oct 2026 15:04:01.660 # Client id=60 … omem=268435448 … scheduled to be closed ASAP due to exceeding ou
   M 18 Oct 2026 15:04:01.664 # Connection with replica 127.0.0.1:36063 lost.
   M 18 Oct 2026 15:04:03.081 * Synchronization with replica 127.0.0.1:36063 succeeded
   ```
   In 1b the sampler happened to see the link up each second, so the test failed later, on the after-load check:
   ```
   E           AssertionError: KeyDB served another sync during the load
   (3 syncs, 1 expected)
   ============================== 1 failed in 33.13s ==============================
   ```

2. **Plain replica control**, 14:59Z. The JSON line, with the series fields left out:
   ```
   {'test': 'drakeydb:perf', 'offered_ops_per_s': 721528, 'produce_bytes_per_s': 103168595, 'apply_bytes_per_s': 103173340, 'apply_ops_per_s': 721561, 'bytes_per_op': 143.0, 'ratio': 1.0, 'max_lag': 1025934, 'median_lag': 729100, 'cpu_pct': {'keydb_master': 99.8, 'replica': 81.5, 'loader': 9.5}, 'applied_envelopes_per_s': 721560, 'replication_delay_ms': {'n': 233, 'p50': 4.0, 'p99': 7.8, 'max': 54.8}, … 'bounds': {'min_ratio': 0.95, 'max_lag': 206337190, 'max_drain_s': 2.0}, 'loadavg_1m_before': 0.49560546875, 'binary': '/home/user/drakeydb/build-opt/drakeydb', 'drain_s': 0.006, 'writes_total': 21756969, 'envelopes_total': 21756972, 'peer_mode': False}
   ============================== 1 passed in 32.95s ==============================
   ```

3. **Peer mode, `--df multi_master_stream_lww=false`**, 15:00Z:
   ```
   E       AssertionError: the replica's link is down: {… 'master0': {… 'link_status': 'down', … 'rreplay_unwrapped': 8418977, … 'classic_apply_errors': 0, … 'repl_offset': 1203829072}, … 'multimaster_lww_dropped': 0, …}
   ============================== 1 failed in 24.31s ==============================
   M 18 Oct 2026 15:00:45.398 * Synchronization with replica 127.0.0.1:34459 succeeded
   M 18 Oct 2026 15:01:06.120 # Client id=4 addr=127.0.0.1:38202 … flags=S … omem=268435437 tot-mem=268485477 events=rw cmd=replconf … scheduled to be closed ASAP due to exceeding out
   M 18 Oct 2026 15:01:06.121 # Connection with replica 127.0.0.1:34459 lost.
   ```

**Attribution confirmed.** The plain node keeps up at about 720k ops/s, about 103 MB/s. A peer node fails the
same way with the guard on and with it off: KeyDB drops the replica when its output buffer reaches 256 MB.
With the guard off nothing is vetoed, and `multimaster_lww_dropped` is 0 in every run. So the guard (this
task's feature) is not the cause.

**Rough rates.** These come from `age` at the drop and include the sync, so they are only indicative:
- guard on: about 256 MB / 14 s ≈ 18 MB/s of lag growth, i.e. the node applies about 82 % of the load;
- guard off: about 256 MB / 21 s ≈ 12 MB/s, about 88 %.

That is one run each, too few to call a difference. The report's 74 % (from its 9 s drop) is in the same
range. I did not re-run the report's compiled-out variants. My runs do not contradict them.

## For the next task / the orchestrator

- **The main checkout's git object store is partly evicted by iCloud.** `git diff 52b35e47` succeeded at the
  start of this review. Later, `git diff` / `git show` failed with:
  ```
  error: file /Users/darkspadez/Documents/qops/git/drakeydb/.git/objects/pack/pack-d072786d495a7a9a188e14ea241eca8c68e7f893.pack is far too short to be a packfile
  fatal: unable to read 7e6d194db8bec88c666917e0073deacb4b71dc94
  ```
  `ls -lO` shows the pack as `hidden,compressed,dataless`: macOS "Optimize Storage" offloaded it from
  `~/Documents`. I did not touch it. Fix it before committing: download it (`brctl download <path>`, or open
  the folder in Finder), and keep the repo out of iCloud-synced `Documents`.
- **Container cleanup.** A stale `classic_replay_` (pid 50327) idles in `drakeydb-p7`. It does not hold the
  pytest lock, but it is worth killing before Task 1.6's perf runs.
- **Task 1.6.** The peer node applies about 75–88 % of a load the plain node keeps up with, with or without
  the guard. The report found the same with no rewrites and no stamping. So the cost is in what peer mode
  does per write regardless of Tasks 2.2–2.4 (journal, mvcc side table, …), not in the guard.
- **Still to do.** I1 (and the Minors at the orchestrator's discretion), then the Step 6 commit. Run the
  gate suites the report lists as not run (`multimaster_test.py`, `keydb_harness_test.py`,
  `redis_replication_test.py`, `ctest -L DFLY`) before the PR.
- **Formatting.** clang-format 14.0.6 (dry run, byte-compared) is clean on all six C++ files, and `black --check`
  is clean on the pytest file.

## Re-review (fix round 1)

Scope: the rulings in `task-2.4-fix1.md`, the report's "## Fix round 1", and a re-read of
`git diff 52b35e47 -- src tests docs`. Git works again in the rebuilt store. No perf runs, as ruled.

### Verdict: **Clean**

I1 and M1–M8 are addressed as ruled, and the diff contains nothing beyond the rulings. I reproduced both
falsifications (M4, M8) myself. The rewrite results are unchanged after M5.

### Rulings, checked

| Item | Status | Evidence |
|---|---|---|
| I1 | Done | `conn_context.h:372-380`: names both setters (`SetApplyLwwGuard` for DFLY, `ConsumeRedisStream` for classic), says the bit is true on a classic peer link where only enveloped commands carry an mvcc, and gives the `CommandScope` exception to "never reset per command". `test_utils.h:119-121`: "and, unconditionally, in `ClassicApplier::ApplyCommand` for envelope commands". `transaction.h:397` and `:741`: "a raw classic command". `multi-master.md:140-142`: "a raw classic command never carries an `mvcc` (an RREPLAY envelope's does, see below)". I found no other stale claim of this kind. |
| M1 | Done | `multi-master.md:331`: "The rewrites are done on **every** envelope link". The EVAL sentence starts "On a peer node" and ends "A plain link stamps nothing." |
| M2 | Done | `classic_replay.cc:839-842` and `transaction.cc:1642` (the latter now says "on every single-shard script of an envelope") both describe the single-shard script → `SQUASHED_STUB` → tripwire path. |
| M3 | Done | `absl::StartsWithIgnoreCase(name, "EVAL")` (`:843`), with the comment about KeyDB 6.3.4 and the `_RO` variants. |
| M4 | Done | `test_raw_stream_conditional_set_stays_conditional[plain_replica\|peer_mode]` (FakeClassicMaster: raw `SET k old`, raw `SET k new NX`, marker; asserts `k == old`). Falsification reproduced below. |
| M5 | Done | The first pass sets `changed` on NX/XX/GET, and `kept` is built only after `if (!changed) return false;`. Equivalent to before: `KEEPTTL` is dropped only under NX, which already sets `changed`. The two table tests have the same rows as before (35 table lines), and the four `ClassicApplyRewrites*` tests pass. My round-1 probe table (14 assertions, incl. dangling `PX`, `PXAT get`, `KEEPTTL PX xx`, `SET k nx`) also passes on the new code (throwaway, reverted). |
| M6 | Done | No "Task N" or "fix round" text left in the src/tests diff. |
| M7 | Done | Bare `MSETNX` is now in `ClassicApplyRewritesMsetnxBecomesMset`; the "untouched" loop has no special case. |
| M8 | Done | After the EVAL, a `kAuthorB` `SET e x` at `kMvccA - 1` on the same link must be vetoed (`e == from-script`, stamp still `kMvccA`). A different author is needed because `kAuthorA`'s watermark is past that mvcc; the comment says so. |

### Falsifications, reproduced

**M8**: removed `cntx_->repl_lww_guard = link_guard_;` from `~CommandScope`, **and** removed the field read
`EXPECT_TRUE(link.cntx.repl_lww_guard)` after the EVAL, so only the behavioural check is left. Ran the full
`classic_replay_test`:
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1697: Failure
Expected equality of these values:
    Which is: 'x'
/home/user/drakeydb/src/server/classic_replay_test.cc:1701: Failure
Expected equality of these values:
    Which is: 7953565813638624896
    Which is: 7953565813638624897
[  FAILED  ] ClassicPeerApplyFamilyTest.EvalStampedWithEnvelopeMvccUnguardedNoDfatal (20 ms)
[  PASSED  ] 147 tests.
```

**M4**: added `ClassicApplyRewrites(ctx);` after `FillBackedArgs(last_args, ctx);` on the raw batch path of
`ConsumeRedisStream` (`replica.cc:1400`):
```
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_raw_stream_conditional_set_stays_conditional[df_factory0-plain_replica]
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_raw_stream_conditional_set_stays_conditional[df_factory0-peer_mode]
E           AssertionError: assert equals failed
E              -'new'   +'old'
E           AssertionError: assert equals failed
E              -'new'   +'old'
```

After restoring:
- `classic_replay.cc`, `classic_replay_test.cc` and `replica.cc` are `cmp`-identical to the pre-mutation copies, and the tree was rebuilt.
- `classic_replay_test`: 148/148.
- Pytest `-k "raw_stream_conditional or lww_concurrent or set_nx_wins or conditional_set_applies or not_forwarded_to_peers or flushes_raw_batch"`: `9 passed, 180 deselected in 21.65s`.
- Formatting: clang-format 14.0.6 dry run (byte-compared) is clean on all nine touched C++ files, and `black --check` is clean.

### Left for the orchestrator

- The Step 6 commit.
- The gate suites: `multimaster_test.py`, `keydb_harness_test.py`, `redis_replication_test.py`, `ctest -L DFLY`.
- Task 1.6, for the R15 perf failure.
