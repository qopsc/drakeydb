# Task 2.4 report: guard ON for classic peer links; rewrites and EVAL on every envelope link

Branch `feat/phase7-2-author-stamps-dedup-guard`, on top of Task 2.3 (`004c8720`, ledger `52b35e47`).
Not committed. Implementer: Sonnet 5.5. Plan: Task 2.4; spec D-4.2-4.4, D-6, D-9 residual.

## Summary

- **Code and tests are done and falsified. The peer-mode release bar FAILS (Step 5), so per the
  brief this stops here: Task 1.6 is re-opened. Nothing was optimized.** The failure is not caused
  by this task's code: with the guard off, the rewrites compiled out and stamping compiled out
  (below) a peer node fails the same way. See "Step 5".
- `ClassicApplyRewrites` (`classic_replay.{h,cc}`): `SET` loses `NX`/`XX`/`GET` after the value
  (any case, an expiry option's value is skipped, not read as an option), `KEEPTTL` goes when `NX`
  was present, `MSETNX` becomes `MSET`; pure, returns whether it changed anything.
- `ApplyCommand` order is now: skip checks → `CommandScope` → unknown check → `Reserve` →
  `Reservation` → **`ApplyLwwRewrites`, `ClassicApplyRewrites`** → `Stamp` → **`Unguard` for
  `EVAL`/`EVALSHA`** → `Dispatch` → `Commit`/release. The rewrites run on every envelope link, plain
  or peer, not gated on the guard; a duplicate or cancelled leaf returns before them (R11). A rewrite
  never changes the reserved `(author, mvcc)`: the reservation is taken from the envelope before the
  command is edited.
- `CommandScope` captures the link's `repl_lww_guard` at construction (like `link_origin_idx_`) and
  puts it back on every exit; `Unguard()` clears it for one dispatch.
- `Replica::ConsumeRedisStream` sets `conn_context.repl_lww_guard = IsPeerMode() &&
  IsActiveReplica() && FLAGS_multi_master_stream_lww` once, beside `repl_origin_idx`. Raw commands
  (mvcc 0) stay unguarded by `LwwGuardActive`; `JournalExecutor`'s gate is untouched.
- Comments/docs rewritten: `RunSquashedMultiCb` tripwire comment (`transaction.cc`), the
  `ApplyLwwRewrites` contract comment with the envelope carve-out (it lives in `multimaster_lww.h`,
  not `.cc`), `docs/multi-master.md` (the classic-links section: guarded for envelopes with a real
  mvcc on a peer node, raw never, rewrites on every envelope link, EVAL stamped but unguarded).

## Files

| File | Change |
|---|---|
| `src/server/classic_replay.{h,cc}` | `ClassicApplyRewrites`; `CommandScope` guard bit + `Unguard`; rewrites and EVAL in `ApplyCommand` |
| `src/server/replica.cc` | +6 lines: the per-link guard bit |
| `src/server/transaction.cc`, `multimaster_lww.h`, `docs/multi-master.md` | comments / docs |
| `src/server/classic_replay_test.cc` | 11 new gtests; `PeerLink` sets the guard bit as production does; `FutureMvcc()` |
| `tests/dragonfly/keydb_onboarding_test.py` | 5 new pytest cases; the throughput harness takes `peer_mode`; one existing test adjusted (below) |

## Tests

| | before | after |
|---|---|---|
| `classic_replay_test` | 137 | **148** (+11) |
| `multi_master_test` | 230 (229 passed, 1 skipped: root can write the unwritable dir) | unchanged, same result |
| `rdb_test` | 138 | 138 pass |
| `server_family_test` | 44 | 44 pass |
| `keydb_onboarding_test.py -m 'not slow'` | 172 passed (task 2.3 report) | **181 passed**, 6 slow deselected (the 2 smoke cases ran apart: pass) |

New gtests: `ClassicReplayTest.ClassicApplyRewritesSet` (23 rows incl. `NX KEEPTTL`, `XX KEEPTTL`,
`NX GET`, lower/mixed case, expiry values spelled like options, key/value named `nx`/`get`),
`...SetWithNothingToDropIsUntouched`, `...MsetnxBecomesMset`, `...LeavesOtherCommandsAlone`;
`ClassicPeerApplyFamilyTest.{StaleKeyDbSetLosesToNewerLocalWrite, NxSetAppliesAsGuardedSet,
MsetnxAppliesAsGuardedMset, EvalStampedWithEnvelopeMvccUnguardedNoDfatal,
EvalAppliesEvenWhenLocalIsNewer, GuardOffAppliesInArrivalOrder}`;
`ClassicApplyFamilyTest.PlainApplierRewritesConditionalSet`. The plan names these under
`ClassicApplyFamilyTest`; the guarded ones need the peer fixture, so they are in
`ClassicPeerApplyFamilyTest` (it derives from it). Each LWW test asserts which value survived; the
stale/fresh pairs apply the older envelope first because the author's dedup watermark is monotone.
`EvalStamped...` also asserts the link's guard bit is back after the dispatch (it fails if
`CommandScope` does not restore it, m8).

New pytests (all pass on the debug build): `test_keydb_lww_concurrent_set_converges[guard_on|
guard_off]`, `test_keydb_set_nx_wins_over_stale_local`,
`test_plain_replica_conditional_set_applies_despite_expiry_skew`,
`test_keydb_writes_not_forwarded_to_peers` (two peer nodes, each other's peers and both on one
KeyDB, 200 INCRs, `assert_no_command_storm`). `test_keydb_onboarding_keeps_up_under_load` and the
comparator test are now parametrized `plain_replica` / `peer_mode`.

**One existing test changed:** `test_unwrap_flushes_raw_batch_before_envelope[peer_mode]` failed.
Its envelopes carried the harness's ancient mvccs (`1 << 40`), and on a guarded peer link the raw
`SET a 1` (stamped by the replica's clock) now beats the stale envelope `SET a 2`, so `a` ended at 3
not 4. That is the intended semantics, not a regression; the envelopes now carry an mvcc an hour
ahead (`keydb_mvcc(now_ms() + 3_600_000)`), the ordering check is unchanged, passes in both modes.

## Falsification (Step 4)

Method: mutate the file under `src/server`, `ninja -C build-dbg -j4 classic_replay_test [dragonfly]`
in `drakeydb-p7`, run the named tests, restore (byte-compared against a copy). All eight fail as the
plan says; the restored tree passes (148/148).

| # | Mutation | Failing evidence (verbatim excerpts) |
|---|---|---|
| m1 | `repl_lww_guard = false` in `ConsumeRedisStream` | `test_keydb_lww_concurrent_set_converges[guard_on]`: `AssertionError: ('keydb', True)` / `assert equals failed -'keydb' +'local'` (the stale write wins) |
| m2 | remove `ClassicApplyRewrites(&cmd)` | gtest: `PlainApplierRewritesConditionalSet`, `NxSetAppliesAsGuardedSet` (`'local'` where `fresh` expected), `MsetnxAppliesAsGuardedMset` (`nil` for `b`/`c`) fail; pytest: `test_keydb_set_nx_wins_over_stale_local` and `test_plain_replica_conditional_set_applies_despite_expiry_skew` fail (`-'old' +'new'`) |
| m3 | gate both rewrites on `LwwGuardActive(cntx_->repl_lww_guard, mvcc)` | `PlainApplierRewritesConditionalSet` fails, `test_plain_replica_conditional_set_applies_despite_expiry_skew` fails (`-'old' +'new'`); the peer tests keep passing (a peer link is guarded) |
| m4 | keep `KEEPTTL` under `NX` | `ClassicApplyRewritesSet`: `{ "SET", "k", "v", "KEEPTTL" }` vs `{ "SET", "k", "v" }`, 4 rows |
| m5 | EVAL dispatched with the guard on (`Unguard` removed) | `F1018 07:01:04 transaction.cc:1649] multi-master LWW guard reached RunSquashedMultiCb: repl_mvcc_ is batch-level here, not per-command, and cannot be safely compared -- applying unguarded` / `*** SIGABRT` in `EvalStampedWithEnvelopeMvccUnguardedNoDfatal` |
| m6 | EVAL dispatched with `repl_mvcc = 0` | `classic_replay_test.cc:1698` Expected equality: `1879369930280796160` (a local mint) vs `1782579200000000005` (`kMvccA`) |
| m7 | stamp classic authors with `kSelfIdx` | `test_keydb_writes_not_forwarded_to_peers`: `assert 400 == 200` (each node holds 2N) |
| m8 | `CommandScope` does not restore the guard bit | `EvalStamped...`: `Value of: link.cntx.repl_lww_guard / Expected: true` |

**What each test still passes with the feature removed:** under m2 (no `ClassicApplyRewrites`)
`StaleKeyDbSetLosesToNewerLocalWrite`, `EvalStamped...`, `EvalAppliesEvenWhenLocalIsNewer` and
`GuardOffAppliesInArrivalOrder` still pass (a plain SET needs no rewrite), and so do
`test_keydb_lww_concurrent_set_converges` and `test_keydb_writes_not_forwarded_to_peers`. Under m1
(no guard bit; the gtests set the bit in the fixture, so only pytest sees it)
`test_keydb_set_nx_wins_over_stale_local` and `test_plain_replica_conditional_set_applies_...` pass
(the NX there is newer than the local write, and a plain link is never guarded) and so does the
whole gtest suite. Under m3 the peer gtests and `test_keydb_set_nx_wins_over_stale_local` pass (the
link is guarded, so the gate is true). `test_keydb_writes_not_forwarded_to_peers` passes under m1,
m2, m3, m5, m6.
Not mutated: the "a duplicate runs no rewrite" ordering is structural (the dedup tests fail if
`Reserve` moves) and has no behavioural test of its own.

## Step 5: peer-mode release performance: **FAILS**

Release build `build-opt/dragonfly` (`-release -DWITH_AWS=OFF -DWITH_GCP=OFF`, `CCACHE_DISABLE=1`,
`ninja -j4 dragonfly`) of this tree; `DRAKEYDB_PERF=1`, the harness of Task 1.5 pinned as there
(KeyDB cpu 0, replica 1-2, loader 3), 30 s window, load average < 0.5 at start (0.49), `flock`. The
box is a 12-cpu container; Task 1.5's numbers were from a 4-cpu one, and KeyDB offers 3.6x the load
here, so the plain-mode control was re-run on this box.

| mode | run | offered ops/s | produce MB/s | ratio | max lag | drain | result |
|---|---|---|---|---|---|---|---|
| plain_replica (control, this box) | 1 | 775,640 | 110.9 | 1.0001 | 0.97 MB | 0.066 s | pass |
| peer_mode | 1 | n/a | n/a | n/a | n/a | n/a | **FAIL**: link down at t≈9 s |
| peer_mode | 2 | n/a | n/a | n/a | n/a | n/a | **FAIL** (11 s test time) |
| peer_mode | 3 | n/a | n/a | n/a | n/a | n/a | **FAIL** (11 s test time) |
| Task 1.5 plain (4-cpu box, for reference) | median of 3 | 213,025 | 30.5 | 1.0003 | 1.35 MB | 0.036 s | pass |

A failed peer run has no row of numbers: KeyDB drops a replica whose output buffer reaches its
256 MB limit and the sampler (`take_sample`) reports the link down. KeyDB's log shows exactly that,
twice per run (the replica reconnects, takes a full sync, falls behind again):
`# Client id=4 ... age=9 ... omem=268435401 ... # Connection with replica 127.0.0.1:43505 lost.`
The replica's INFO at the failure: `link_status: down`, `rreplay_unwrapped: 9507301`,
`classic_apply_errors: 0`, `multimaster_lww_dropped: 0`. 256 MB over 9 s against 110.9 MB/s
produced is a lag growing at about 28 MB/s, i.e. a peer node applying about 74% of the offered
load (derived from KeyDB's log, not sampled). The smoke (5k ops/s) passes in both modes on debug.

**It is not this task's code.** Three diagnostic release runs of the peer test, each failing the
same way within 22 s:

1. `--df multi_master_stream_lww=false` (guard off): fails.
2. also the two rewrite calls removed from `ApplyCommand`: fails.
3. also `Stamp` never called (`if (false && authors_.stamping())`, so the writes are local mints,
   as before P7-2): fails.

So a peer-mode drakeydb already cannot keep up with an active KeyDB at this rate, whatever Tasks
2.2-2.4 add (the per-write MVCC side-table stamp, the journal a peer node keeps, ...); I did not
profile it. The diagnostic edits are reverted (the tree and `build-opt` are the real code).
**Task 1.6 (squasher micro-batch fallback) is re-opened by this result, for the orchestrator to
decide; whether the right bar for a peer node is the plain one is also the owner's call.** The
comparator test (1.5x of a KeyDB replica) was not run for peer mode: the absolute bar already fails.

## Full file

`keydb_onboarding_test.py -m 'not slow'` (debug build), first run: 180 passed, 1 failed
(`test_unwrap_flushes_raw_batch_before_envelope[peer_mode]`, fixed above). After the fix, the whole
file again: `181 passed, 6 deselected in 396.08s`.

## Open questions

- The plan's step 4 says the not-forwarded test's `kSelfIdx` mutant sees 2N: it does (400 for 200).
- `ClassicApplyRewrites` treats any token after the value that is not an expiry option's value as an
  option; an unknown option (`SET k v WHATEVER`) is kept as written.
- Not run here: `multimaster_test.py`, `keydb_harness_test.py`, `redis_replication_test.py`,
  `ctest -L DFLY` (the gate).

## Fix round 1

Rulings I1, M1-M8 of `task-2.4-fix1.md` applied, nothing else. The perf test was not run.

- **I1**: `conn_context.h` (second setter `ConsumeRedisStream`; true on a classic peer link; "never
  reset per command, except `ClassicApplier`'s per-dispatch `CommandScope`"), `test_utils.h`,
  `transaction.h` (both comments: "a raw classic command"), `docs/multi-master.md:140` ("a raw
  classic command never carries an mvcc").
- **M1**: the doc's "on every link" now covers the rewrites alone; the EVAL sentence is peer-only
  and says a plain link stamps nothing. **M2**: both comments say single-shard script →
  `SQUASHED_STUB` → tripwire. **M3**: `absl::StartsWithIgnoreCase(name, "EVAL")`, with a comment that
  KeyDB 6.3.4 never sends `_RO`.
- **M5**: `RewriteSet`'s first pass sets `changed` (NX/XX/GET); it returns early when nothing is to
  go, and `kept` is built only then (`KEEPTTL` alone, `PX <t>` alone allocate nothing).
- **M6**: the three "Task 2.4" references in the pytest now say what they mean. **M7**: the bare
  `MSETNX` row moved to `ClassicApplyRewritesMsetnxBecomesMset`.
- **M4**: new `test_raw_stream_conditional_set_stays_conditional[plain_replica|peer_mode]`
  (FakeClassicMaster: raw `SET k old`, raw `SET k new NX`, marker; `k == old`).
- **M8**: after the EVAL in `EvalStamped...`, a write older than the script's stamp (kAuthorB at
  `kMvccA - 1`; another author, because kAuthorA's dedup watermark is past it) is applied on the
  same link and must be vetoed: `e == from-script`.

### Falsification

**M4**: `ClassicApplyRewrites(ctx);` added after `FillBackedArgs(last_args, ctx)` in the raw path of
`ConsumeRedisStream`, `ninja dragonfly`, restored afterwards (`cmp`):
```
E           AssertionError: assert equals failed
E              -'new'   +'old'
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_raw_stream_conditional_set_stays_conditional[df_factory0-plain_replica]
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_raw_stream_conditional_set_stays_conditional[df_factory0-peer_mode]
====================== 2 failed, 187 deselected in 1.79s ======================
```
**M8**: `~CommandScope` without `repl_lww_guard = link_guard_`, **and** the field-read
`EXPECT_TRUE(link.cntx.repl_lww_guard)` removed, so the behavioural check stands alone:
```
classic_replay_test.cc:1697: Failure
Expected equality of these values:
    Which is: 'x'
classic_replay_test.cc:1701: Failure   (the stamp: the vetoed write took the key's stamp over)
[  FAILED  ] ClassicPeerApplyFamilyTest.EvalStampedWithEnvelopeMvccUnguardedNoDfatal (352 ms)
```
Both files restored and `cmp`-identical to the pre-mutation copies; the tree was rebuilt.

### Final counts

`classic_replay_test`: 148/148 (unchanged: M7 moved a row, M8 added assertions). `multi_master_test`:
229 passed, 1 skipped. Pytest `-k "lww or nx or msetnx or eval or not_forwarded or conditional_set or
raw"`: `46 passed, 1 skipped, 142 deselected in 90.17s`, which includes the two new M4 cases. The
host pre-commit (clang-format, black) is clean on the touched files.
