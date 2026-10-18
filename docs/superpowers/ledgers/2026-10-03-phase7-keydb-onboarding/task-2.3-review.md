# Task 2.3 review: `AuthorDedup`

Reviewer: Opus 5.5. BASE `5f1e011c`, working tree as left by the implementer (not committed).
Read: plan Task 2.3, spec D-3.2/D-3.3/D-3.6/D-5, decisions 42–46, `task-2.3-brief.md`,
`task-2.3-report.md`, every hunk of `git diff 5f1e011c -- src tests`, and KeyDB's
`replicaReplayCommand` (`KeyDB/src/replication.cpp:5374-5512`).

## Verdict: **Needs fixes** (0 Critical, 2 Important, both test-only)

The code does what D-5 and rulings R10–R13 say. I found no defect in the reservation protocol, the
lock discipline, the `HandleRreplay` rewrite or the `Clear()` sites. The two Important findings are
behaviours the rulings and the brief call out that no test pins: a mutant for each passes every
test.

## Test runs (this review, container `drakeydb-p7`, debug build of the working tree)

| Suite | Result |
|---|---|
| `classic_replay_test` ×3 | 134 passed, 134 passed, 134 passed |
| `multi_master_test` | 229 passed, 1 skipped (`NodeIdentityFile.UnwritableDirIsEphemeral`) |
| `keydb_onboarding_test.py`, **whole file**, `KEYDB_REQUIRED=1`, no `-m` filter | **175 passed, 3 skipped** in 391.85 s |

The 3 skips are perf-gated, not slow-deselected: `test_keydb_onboarding_lag_within_1_5x_of_a_keydb_replica`
and the two `test_keydb_throughput_reference_setups` params (`skipif(not perf_mode())`, they need
`DRAKEYDB_PERF=1` and a release binary). `test_keydb_onboarding_keeps_up_under_load` (also
`slow`) ran as its functional smoke and passed. This closes the report's open question 3. It also
covers the P7-1 exact-counter pytests (`rreplay_unwrapped` etc.), so (c) below holds end to end.

Formatting: the pre-commit `clang-format` (`--dry-run`) on the 6 C++ files reports 0 warnings, and
`black --check` leaves the pytest file unchanged.

## Findings

| ID | Sev | File:line | What | Why | Suggested fix |
|---|---|---|---|---|---|
| I1 | Important | `classic_replay.cc:735` / `classic_replay_test.cc:1871` | R11 (reserve **before** stamp) is implemented but no test checks it. Mutant: move `if (authors_.stamping()) scope.Stamp(mvcc, authors_.IdxFor(author));` above `dedup_.Reserve(...)`. **All 134 gtests pass.** | R11 asked for the cancelled-reserve test to assert "no counter moved", including `multimaster_keydb_author_overflow` and the registry size. `CancelledReserveAfterSelect…` runs in `ClassicApplyFamilyTest` on a link with no registry (`stamping()` is false), so `IdxFor` is never reached and the counters cannot move. It asserts neither of them anyway. `DedupStillWorksForOverCapAuthor` cannot catch the mutant either: its duplicates are dropped at the layer `IsApplied` early-out and never reach the leaf. | Add a peer-link variant (a `ClassicAuthorMapTest` applier with a registry, `--classic_author_cap=0` and once with a cap > 0). It holds `AuthorDedup::Instance().Reserve(A, m)` from the test, then (1) applies `A@m` on a link whose `running()` flips, giving `kNotConsumed`, and (2) applies it on a live link and `Commit`s from the test while the link waits, giving a leaf `kDrop`. Each case asserts `ClassicAuthorOverflowTotal()` unchanged and `registry.Size()` / `FindIdx(A)` unchanged. Falsify with the mutant above. |
| I2 | Important | `replica.cc:826-837` | "No `Clear()` on a peer merge sync" (D-5, brief) has no test. Mutant: add `AuthorDedup::Instance().Clear();` right after `LOG(INFO) << "Peer full sync: merging without flush "` in `InitiatePSync`. **The whole pytest file passes (175 passed, 3 skipped)**, and the gtests cannot see `replica.cc`. | A regression here fails silently. A peer node that re-syncs one link (a merge, no flush) would forget every author's watermark while its other links are live, so forwarded copies of envelopes it has already applied would apply again (a double `INCR`). The brief asked, "a test?": there is none, and the report does not say so. | A deterministic `FakeClassicMaster` pytest in peer mode (`attach_scripted_master(..., peer_mode=True)`): stream `rreplay("INCR","c", mvcc=5)`, make the link re-sync (the fake master drops the connection and scripts a second full sync whose tail re-streams the same envelope), then assert `c == 1` and `multimaster_rreplay_deduped == 1`. The same harness in plain mode gives a cheap deterministic twin of the master-switch test (`c == 1` after the flush and replay). |
| M1 | Minor | `classic_replay_test.cc:2524` | The `ClassicAuthorMapTest` fixture now sets `--active_replica`, commented "boot-only: the applier stamps through it". With the line removed, **all 134 pass** (probe `h`). `ClassicAuthorMap::stamping()` depends only on the registry (`classic_replay.h:224-226`). | The comment states a dependency that does not exist. The flag also changes the environment of the 9 pre-existing `ClassicAuthorMapTest` tests. It is reset in `TearDown`, so nothing leaks. | Drop the line, or replace the comment with a real reason. (h) answered: not needed, and it is reset. |
| M2 | Minor | `classic_replay_test.cc:1806`; `classic_replay.cc:588-590` | The comment on `DoesNotAdvanceWhenMalformedOrSelf` says the test covers "one whose db could not be selected", but the test has no such case. The code path (`!db_selected` sets `advance = false`, so the **enclosing layers do not advance either**) is untested. It is also a choice D-5 does not list: D-5 says skipped commands are "consumed and `Advance` only", and a db-failure skip is not among its examples. | The comment overstates. The choice matches R10's "retry on another link" reasoning, but it is the implementer's (report note 4, second half) and no ruling covers it. | Either add the case (`--cluster_mode=yes`, `SELECT 3` fails) or fix the comment. Have the orchestrator confirm the choice beside R10, and record it in Task 2.5's spec amendment. |
| M3 | Minor | `classic_replay.h:385`, `:397-398` | The comment on `Leaf::kRejected` ("the dispatcher refused it before it ran") and `Dispatch`'s "`*ran` … false iff the dispatcher rejected it before it ran" overstate. `DispatchResult::OOM` also comes back when the command **ran** and replied OOM (`main_service.cc:1753-1758`), and `ERROR` comes back on an exception thrown mid-`Invoke` (`:1747-1751`). | The behaviour (`Release`) is what D-5 asks for ("or reported OOM by the command itself"). The comments are not accurate. | Change to "false iff the dispatch result was not OK: rejected before running, or the command reported OOM". |
| M4 | Minor | `classic_replay.cc:684` | The mvcc-0 guard in `Reservation` (`held_(mvcc != 0)`) has no test. Mutant `held_(true)`: **all 134 pass**. With the mutant, a leaf with mvcc 0 calls `Release(author)` and clears another link's in-flight reservation for that author. | Correct by inspection, and unreachable from v6.3.4 (it always sends a nonzero mvcc). The brief named this probe. | Optional: hold `Reserve(A, 10)` from the test, apply `RREPLAY A <INCR> 0` (no mvcc) on a link, then assert that `Reserve(A, 10, []{return false;})` is still `kCancelled` (the reservation survived). |
| M5 | Minor | `classic_replay_test.cc:2306-2309` | `AuthorDedupTest` was inserted between the `ClassicAuthorMapTest` lead comment ("ClassicAuthorMap on a registry in a running service: …") and its class. That comment now reads as part of `AuthorDedupTest`'s. | Misplaced comment. | Move it back above `class ClassicAuthorMapTest` (`:2518`). |
| M6 | Minor | `classic_replay.cc:674-676` | `}  // namespace` immediately followed by `namespace {` (`CommandScope`, then `Reservation`). | Style. | Put `Reservation` in `CommandScope`'s anonymous namespace. |
| M7 | Minor | report "(a) deterministic proof"; `classic_replay_test.cc` `ConcurrentSameEnvelopeAppliesOnce` | The test depends on timing: two `post`ed fibers on two proactors and a 10 ms window. It always passes on correct code. Under mutant (a) it fails only if the second fiber reserves within those 10 ms. I reproduced the failure 2/2. | The report calls it "deterministic". | Wording only. If it should be deterministic, have the holder wait on a latch the second fiber sets after it enters `Reserve`. |
| M8 | Minor | `classic_replay_test.cc:674` | `FreshMvcc()` starts at 1000, and the explicit mvccs in the new tests are 5–911. A future test that applies a default `Envelope(kAuthorX, …)` and then an explicit lower mvcc of the same author will have the explicit one silently dropped. No current test does this. The pytest `rreplay()` counter versus explicit `mvcc=1..11` has the same hazard, but only within one node. | A test-writing trap for Tasks 2.4–2.8. | Mention it in the helper's comment, or start `FreshMvcc` above any explicit value used (for example `1 << 40`). |
| M9 | Minor | `classic_replay.cc:576`, `:735`, Reservation | The hot path now takes the process-wide, cross-thread `mu_` at least 3 times per envelope (`IsApplied` per layer, `Reserve`, `Commit`) and calls `absl::Now()` twice under it. The release bar (`DRAKEYDB_PERF=1`) was not run. | Task 1.5's "must keep up" bar was set without this lock. | A phase-gate item: run the perf bar on a release build at the P7-2 gate. Nothing to do in this task. |

### Rulings, checked

- **R10**: implemented (`case Leaf::kRejected: advance = false`). The test pins the outer:
  `RejectedDispatchCountsBytesDoesNotAdvance` asserts `EXPECT_FALSE(Applied(kAuthorB, 801))`, and
  mutant `r10` fails exactly that line (below). Note for Task 2.5's spec amendment: this
  **diverges from KeyDB**. KeyDB's outer layer advances whenever its inner `RREPLAY` was called
  (`fExec` from `commandsExecuted`, `replication.cpp:5476-5493`), a rejected leaf included. The
  amendment should say so.
- **R11**: the order is implemented (`Reserve` at `:735`, `Stamp` after it). It is **not pinned**
  (I1).
- **R12**: done by this review (table above).
- **R13**: implemented (`Clear()` erases idle entries and zeroes `applied` of in-flight ones);
  `ClearResetsAppliedKeepsInflight` pins it.

### The brief's probes

- **(a) Reservation under concurrency.** I reproduced falsification (a) (below). A waiter whose
  `running()` flips mid-wait returns `kCancelled` (`CancelledWaiterReturnsNotConsumed`, which also
  fails under (a)). The mvcc-0 `Release`: correct, but no test catches the mutant (M4).
- **(b) Deadlock surface.** `mu_` is taken only in `AuthorDedup`'s own methods. Its callers are
  `HandleRreplay`, `ApplyCommand` and `~Reservation` on the stream fiber, where
  `ConsumeRedisStream` holds no lock, and `InitiatePSync` at the flush, where none is held either.
  `ForwardedAuthors`' and `PeerRegistry`'s mutexes are never held around those calls, since
  `IdxFor` runs after `Reserve` returns and before `Reservation` releases. Nothing under `mu_` takes
  another lock: `EntryLocked`, `NowMs`/`absl::Now()`, field writes, `notify_all`. `running()` is
  called with `mu_` unlocked. `cv_.wait_for` gets a `std::unique_lock<fb2::Mutex>`, which
  `CondVarAny` accepts. `EntryLocked` re-looks-up the entry on every pass of the loop. No
  hold-and-wait: a link calls `Reserve` only while it holds no reservation (one leaf per tree, a
  serial stream), so no cycle can form between links. A waiter notices its link stopping within
  100 ms.
- **(c) `HandleRreplay` rewrite.** The order is unchanged against Task 2.2: parse → `running()`
  (depth 1) → `SelectDb` → malformed → self → nesting → *new* `IsApplied` → parse the inner. The db
  is still selected before the self check (D-3.3). `rreplay_unwrapped` (`++layers` after
  `ParseSingleCommand`) is unchanged for non-deduped layers. A layer dropped at the early-out is not
  counted, while a leaf `kDrop` in `Reserve` is (its layer was taken apart): both are consistent
  with "unwrapped = inner parsed", and report note 5 documents it. The P7-1 exact-counter pytests
  pass.
- **(d) Early-out at the layer.** Given `A(B(m))` with A applied and B not, B's command is lost.
  KeyDB does the same: `if (mvcc != 0 && remoteState.mvcc >= mvcc) { … return; }`
  (`replication.cpp:5456-5461`) returns before `processInputBuffer` ever sees the inner.
  `NestedOuterAdvancesWhenInnerDeduped` asserts this direction: its third step,
  `B@500(C@7(INCR))` after B@500, asserts `!Applied(kAuthorC, 7)` and `c == 1`. Removing the
  early-out (probe `noearly`) fails that test.
- **(e) `Clear()` sites.** `InitiatePSync` only, after `FlushSlots` and after `FlushAll`, in the
  `else if`/`else` of the `IsPeerMode()` branch. The DFLY-sync pair (`InitiateDflySync`,
  `replica.cc:1139-1145`) is correctly left alone: no RREPLAY watermark exists on that protocol.
  The peer merge has no test (I2).
- **(f) Bound.** The map is not unbounded. It grows past 4096 only when every entry is in flight,
  and the in-flight entries number at most L, the count of links (one reservation per link at a
  time, see (b)). So `size <= 4096 + L`. It never shrinks back below its high-water mark (each later
  insert evicts one idle entry), so it stays at `<= 4096 + L`. One theoretical case: an entry that
  has just committed can be evicted before a waiter re-looks it up only if it is the map's only idle
  entry, which needs about 4095 links. Not a concern.
- **(g) Helper changes.** No P7-1 or Task 2.2 test pinned the old default mvcc (`7` / `1`): a grep
  of the base file finds no stamp or watermark expectation of 7. The diff adds assertions and
  removes none: `RunningFalseDuringFirstDispatch…` gained 4,
  `ClassicProcessWideValuesFollowTheRenderPredicate` grew from 2 to 3 values, and
  `assert_no_classic_fields` and `test_info_and_metrics_show_classic_counters` gained the deduped
  pins. The pytest streams with explicit mvccs (`:2423`, `:2484-2489`, `:2538`, `:3049-3054`) were
  already strictly increasing. The trap is M8.
- **(h) `--active_replica` in `ClassicAuthorMapTest`.** Not needed; it is reset (M1).

### Spec axis, other

- `--active_replica` off stays byte-identical: `assert_no_classic_fields` now also pins the absence
  of `multimaster_rreplay_deduped` in INFO and `/metrics`. The render predicate is
  `active_master_seen || deduped != 0`, D-13's rule. `Clear()` on a non-KeyDB flush only empties an
  empty map.
- The plan lists `multi_master.cc` (INFO), which is not modified. The value renders through
  `ClassicProcessWideValues` at both `server_family.cc` sites and in `metrics.cc`, which covers the
  active-node and plain-replica blocks. That is fine.
- Nothing was done beyond the brief except the two extra tests (`ErrorReplyOfARunCommandIsCommitted`,
  `EnvelopeWithoutMvccIsNeverDeduped`), which are in scope. There is no process text in `src/` or
  `tests/`.

## Falsifications reproduced

Method: `/tmp/review23-mutants.py <m>` edits the file, `ninja -C build-dbg -j4 classic_replay_test`
builds, the whole `classic_replay_test` runs, and the file is restored from `/tmp/review23-good/`
with `cmp` clean (`RESTORED-CLEAN` after each). The `replica.cc` mutants were built as `dragonfly`,
snapshotted to `/tmp/df-mut-*`, restored, and rebuilt. Final state: all four files `cmp`-identical
to the implementer's, and `build-dbg` rebuilt from them.

| Mutant | Result |
|---|---|
| (a) `Reserve` check-only: never sets `inflight`, never waits | 5 fail: `CancelledReserveAfterSelect…`, `ConcurrentSameEnvelopeAppliesOnce`, `CancelledWaiterReturnsNotConsumed`, `EvictionSkipsInflight`, `ClearResetsAppliedKeepsInflight` (same set as the report) |
| (c) `Commit` also when rejected | 1 fails: `RejectedDispatchCountsBytesDoesNotAdvance` (as the report says) |
| R10: a rejected leaf still advances the outers | 1 fails: `RejectedDispatchCountsBytesDoesNotAdvance` at `:1917` |
| (e_replica) both `Clear()` calls removed | pytest `test_master_switch_full_sync_resets_author_dedup` fails 3/3 (2 + 1 verbatim run) |
| `noearly`: the layer `IsApplied` early-out removed | 1 fails: `NestedOuterAdvancesWhenInnerDeduped` |
| **R11: stamp before reserve** | **134 pass** (I1) |
| **`Clear()` also on the peer merge** | **whole pytest file: 175 passed, 3 skipped** (I2) |
| `held_(true)` (mvcc-0 `Release`) | 134 pass (M4) |
| fixture without `--active_replica` | 134 pass (M1) |

Verbatim:

(a):
```
[ RUN      ] AuthorDedupTest.ConcurrentSameEnvelopeAppliesOnce
/home/user/drakeydb/src/server/classic_replay_test.cc:2359: Failure
Expected equality of these values:
  applied.load()
    Which is: 2
  1

/home/user/drakeydb/src/server/classic_replay_test.cc:2360: Failure
Expected equality of these values:
  dropped.load()
    Which is: 0
  1
...
[  PASSED  ] 129 tests.
[  FAILED  ] 5 tests, listed below:
[  FAILED  ] ClassicApplyFamilyTest.CancelledReserveAfterSelectIsNotConsumedAndReplaysInTheSameDb
[  FAILED  ] AuthorDedupTest.ConcurrentSameEnvelopeAppliesOnce
[  FAILED  ] AuthorDedupTest.CancelledWaiterReturnsNotConsumed
[  FAILED  ] AuthorDedupTest.EvictionSkipsInflight
[  FAILED  ] AuthorDedupTest.ClearResetsAppliedKeepsInflight
```

(c) (the run's grep kept only these lines):
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1916: Failure
  Actual: true
Expected: false
/home/user/drakeydb/src/server/classic_replay_test.cc:1925: Failure
Expected equality of these values:
  deduped.moved()
    Which is: 1
    Which is: 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1929: Failure
Expected equality of these values:
  deduped.moved()
    Which is: 2
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1931: Failure
[  FAILED  ] ClassicApplyFamilyTest.RejectedDispatchCountsBytesDoesNotAdvance (27 ms)
[  PASSED  ] 133 tests.
```

R10:
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1917: Failure
  Actual: true
Expected: false
[  FAILED  ] ClassicApplyFamilyTest.RejectedDispatchCountsBytesDoesNotAdvance (21 ms)
[  PASSED  ] 133 tests.
```

(e_replica) (D holds only the one INCR that leaked through the proxy, not 100):
```
E       AssertionError: assert equals failed
E         '1'    '1 +00'
============================== 1 failed in 36.29s ==============================
```

R11, held, h: `[  PASSED  ] 134 tests.` Peer-merge `Clear()`:
`================== 175 passed, 3 skipped in 384.94s (0:06:24) ==================`.

### Tests that would pass with the feature removed

The report's list ("What each new test passes under…") is accurate. I add three points:
`CancelledReserveAfterSelect…` is vacuous for the overflow and registry half of "no counter
moved" (I1). No test pins the peer-merge half of the `Clear()` rule (I2). `ReleaseLetsSecondLinkApply`
does not pin `notify_all`: a waiter that missed the notify would still take over after its 100 ms
timeout. The report states the general version of this ("`Reserve`'s hand-off … not pinned").

## For the next task (2.4)

- `ApplyCommand`'s order is now: skip checks → `CommandScope` → unknown-command check → `Reserve` →
  `Reservation` guard → `Stamp` → `Dispatch` → `Commit`/release. Task 2.4's guard bit, rewrites and
  EVAL setters go beside `Stamp`, after `Reserve`: a dropped or cancelled duplicate must not run a
  rewrite or touch the guard (R11's reasoning extends to them). The `!ran` → `kRejected` path is the
  only `Release`.
- A rewrite that changes the dispatched command must not change which `(author, mvcc)` is reserved:
  the innermost envelope's.
- Test helpers: `Envelope()` without an mvcc mints a fresh one starting at 1000, and fixtures call
  `AuthorDedup::Instance().ClearAllForTest()`. A new fixture that applies envelopes must do the same
  (the dedup is process-wide). See M8 on mixing in explicit mvccs.
- R10 and the db-failure no-advance (M2) go into Task 2.5's D-5 amendment, with the KeyDB
  divergence noted.

## Re-review (fix round 1)

Scope: `task-2.3-fix1.md` (rulings I1, I2, M1–M9, R14), the report's "## Fix round 1", and the
fix-round delta (source diffed against my pre-fix copies, plus `git diff 5f1e011c -- src tests`
re-read for the new hunks).

### Verdict: **Clean**

One Minor nit remains: N1, a one-word comment edit the orchestrator can make when committing. It
needs no further round.

### Rulings, checked

| Item | Status | Evidence |
|---|---|---|
| I1 | Done | `ClassicAuthorMapTest.ReservedAuthorCostsNoSlotAndNoOverflow`: cap 0 and 4096, a cancelled link and a live link that drops at the leaf; it asserts the overflow total, `registry.Size()` and `FindIdx(A)`. The cancelled case is deterministic: `running()` true once, then false after the 100 ms wait. Mutant r11 fails it (below). |
| I2 | Done | `test_resync_keeps_author_dedup_on_a_peer_merge_and_resets_it_on_a_flush[peer_mode/plain_replica]` (FakeClassicMaster). Peer: `("1", 1)`. Plain: `("1", 0)`; the docstring explains why plain is 0. Mutant peer_clear fails the peer param (below). |
| M1 | Done | The flag line is gone from `SetUp` and `TearDown`. |
| M2 / R14 | Done | `ClassicApplyFamilyTest.FailedSelectAdvancesNoLayer` (`cluster_mode=yes` behind a `FlagSaver`, the slot config via `DFLYCLUSTER CONFIG`): its own db fails, and an inner db fails under a good outer. No layer advances, and there is a db-0 control. I did not re-run the implementer's M2 mutant. |
| M3 | Done | `classic_replay.h:385-386`, `:398-399`. |
| M4 | Done | `MvccZeroLeafLeavesAnotherLinksReservationAlone`. The report's point is right: only the rejected 0-mvcc leaf reaches `~Reservation`'s `Release` (`Commit()` clears `held_` on the run path). |
| M5, M6 | Done | The comment is back above `class ClassicAuthorMapTest`. One anonymous namespace holds `CommandScope` and `Reservation`. |
| M7 | Done | The second link is launched only after the first holds the reservation, and the first yields until the second sets its flag, so it can no longer commit before the second exists. The test passes 400/400 (2×200 repeats). **Under mutant (a) it fails 200/200 repeats.** Not a formal proof: the flag is set just before `Reserve`, and the report says so. |
| M8 | Done | `FreshMvcc()` starts at `1 << 40` and pytest `RREPLAY_MVCC` at `1 << 40`, and both comments name the trap. `keydb_mvcc()` keeps its counter in the low bits, which the millisecond high bits order, as the report says. Acceptable. |
| M9 | — | Nothing in code, as ruled. |

### Runs (this re-review)

- `ninja -C build-dbg -j4 classic_replay_test multi_master_test dragonfly`: no work to do (already
  built from this tree).
- `classic_replay_test` ×3: **137 / 137 / 137 passed**. `multi_master_test`: **229 passed**.
- New pytest ×5, both params: `2 passed, 178 deselected` five times (3.57–3.93 s each).
- Flakiness: `ConcurrentSameEnvelopeAppliesOnce` `--gtest_repeat=200`, twice: 200 OK and 0 FAILED.
  `ReservedAuthorCostsNoSlotAndNoOverflow` + `FailedSelectAdvancesNoLayer` +
  `MvccZeroLeafLeavesAnotherLinksReservationAlone` `--gtest_repeat=20`: 60/60 OK.
- Formatting: the pre-commit `clang-format --dry-run` gives 0 warnings on the 3 C++ files, and
  `black --check` leaves the file unchanged.
- I did not re-run the whole pytest file. The fix round changes only tests, test helpers and two
  comments, plus the `RREPLAY_MVCC` start value.

### Mutants reproduced (restored with `cmp` clean, then rebuilt; final tree identical to the implementer's)

r11 (`Stamp(IdxFor)` above `Reserve`): `[  PASSED  ] 136 tests.` and
`[  FAILED  ] ClassicAuthorMapTest.ReservedAuthorCostsNoSlotAndNoOverflow`:
```
classic_replay_test.cc:2849: Failure
  ClassicAuthorOverflowTotal()
    Which is: 8
  overflow_before
    Which is: 7
cancelled cap 0
classic_replay_test.cc:2849: Failure  ... Which is: 9 / Which is: 7  dropped cap 0
classic_replay_test.cc:2850: Failure
  node.registry.Size()
    Which is: 3
  size_before
    Which is: 2
cancelled cap 4096
classic_replay_test.cc:2851: Failure
Value of: node.registry.FindIdx(kAuthorA).has_value()
  Actual: true
Expected: false
cancelled cap 4096
(and the same two for "dropped cap 4096")
```
In my run, **only** the new test failed. The report says the mutant also fails
`PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin` and
`DedupStillWorksForOverCapAuthor`; I did not reproduce that. It does not affect the verdict, since
the new test is what pins R11 and it fails in all four case/cap combinations. The implementer's r11
edit may have differed from mine.

peer_clear (`Clear()` after the peer-merge log line in `InitiatePSync`), 2 of 2 runs:
```
E           AssertionError: (c, deduped) is ('2', 0), not ('1', 1)
FAILED 😰  dragonfly/keydb_onboarding_test.py::test_resync_keeps_author_dedup_on_a_peer_merge_and_resets_it_on_a_flush[df_factory0-peer_mode]
================= 1 failed, 1 passed, 178 deselected in 3.69s ==================
```

(a) against the M7 latch: `--gtest_repeat=200` gives 200 of 200 iterations FAILED.

### New findings

| ID | Sev | File:line | What | Fix |
|---|---|---|---|---|
| N1 | Minor | `classic_replay_test.cc:2832` | `// R11: a reservation held by another link …` puts a ruling ID (process text) in `tests/`. | Drop the `R11: ` prefix; the sentence stands on its own. |
| N2 | Minor (report) | report "Fix round 1", I1 | It says two older tests also fail under r11; not reproduced here. | Note only. |
