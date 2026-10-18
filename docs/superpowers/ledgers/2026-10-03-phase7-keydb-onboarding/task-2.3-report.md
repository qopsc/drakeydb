# Task 2.3 report: `AuthorDedup`

Branch `feat/phase7-2-author-stamps-dedup-guard`, on top of Task 2.2 (`5f1e011c`). Not committed.
Implementer: Sonnet 5.5. Plan: Task 2.3; spec D-5 (binding), D-3.2, D-13.

## Summary

- `AuthorDedup` (`classic_replay.{h,cc}`): process-wide (`AuthorDedup::Instance()`), `Entry{applied,
  inflight, last_seen_ms}` in an `absl::flat_hash_map<string, Entry>` under one `fb2::Mutex mu_` +
  `fb2::CondVarAny cv_`. `Reserve / Commit / Release / Advance / IsApplied / Clear`,
  `kMaxAuthorDedupEntries = 4096`.
- `ClassicApplier::HandleRreplay` now: drops a layer whose `(uuid, mvcc)` is applied before its inner
  is parsed; reserves once, on the leaf, inside the per-command `CommandScope`; commits when the
  command ran, releases when the dispatcher rejected it; advances the enclosing layers (and a skipped
  leaf) before it returns.
- `Replica::InitiatePSync` calls `AuthorDedup::Instance().Clear()` after `FlushSlots` and after
  `FlushAll`; not on the peer merge, not on `+CONTINUE`.
- `multimaster_rreplay_deduped` is a process-wide counter, rendered by `ClassicProcessWideValues`
  (now 3 values) in INFO (active-node block and plain-replica block) and `/metrics`
  (`dragonfly_multimaster_rreplay_deduped_total`).

## The two flush sites in `replica.cc`

| Site | Function | Path | Clear? |
|---|---|---|---|
| `:829` `FlushSlots` / `:835` `FlushAll` | `Replica::InitiatePSync` (`:774`) | the **classic** PSYNC full sync, after the `IsPeerMode()` merge branch | **yes**, after each (the two calls sit in `else if` / `else` of the peer branch, so the peer merge is untouched) |
| `:1143` `FlushSlots` / `:1145` `FlushAll` | `Replica::InitiateDflySync` (`:941`) | the **DFLY** protocol full sync (a Dragonfly/drakeydb master, `DFLY SYNC`), again behind `IsPeerMode()` | no: no RREPLAY envelope or author watermark exists on that protocol |

## What changed

| File | Change |
|---|---|
| `src/server/classic_replay.h` | `AuthorDedup`, `kMaxAuthorDedupEntries`; `ClassicApplier::Leaf`, `ApplyCommand` returns it, `Dispatch(cmd, bool* ran)`, member `dedup_`; `ClassicProcessWideValues` takes `rreplay_deduped`; `ClassicRreplayDedupedTotal()`; `HandleRreplay` and `EnvelopeResult` comments. |
| `src/server/classic_replay.cc` | `AuthorDedup` implementation; `rreplay_deduped` atomic; `HandleRreplay` rewritten around the `outers` list; `Reservation` RAII guard; `ApplyCommand`/`Dispatch`. |
| `src/server/replica.cc` | two `Clear()` calls in `InitiatePSync`. |
| `src/server/server_family.cc`, `metrics.cc` | pass `ClassicRreplayDedupedTotal()` to the three render sites. |
| `src/server/classic_replay_test.cc` | 18 new tests, `Envelope()` default mvcc is now fresh per call, fixtures reset the dedup (`ClearAllForTest`), `ClassicAuthorMapTest` fixture sets `--active_replica`. |
| `tests/dragonfly/keydb_onboarding_test.py` | 2 new tests; `rreplay()` and `keydb_mvcc()` return a fresh mvcc per call; process-wide pins in two INFO/metrics tests. |

## Design notes

1. **Hook: `AuthorDedup::Instance()`**, not passed from `Replica`. It is process-wide state exactly
   like `ClassicTotals()` and `ForwardedAuthors`; passing it would change the `ClassicApplier`
   constructor and every test's `Link`. The constructor `AuthorDedup(max_entries, now_ms)` is public,
   so the unit tests build a private instance with a small bound and a fake clock.
2. **Where `Commit` sits.** `ApplyCommand` commits right after `Dispatch` returns with the command
   run (`reservation.Commit()`); the outer layers' and a skipped leaf's `Advance` are the last thing
   `HandleRreplay` does before `return`. `ConsumeRedisStream` (`replica.cc:1362-1365`) calls
   `HandleRreplay`, then `io_buf.ConsumeInput`, then `repl_offs_ += response->total_read`. So both
   are before the offset count. `ReplayAfterCommitBeforeCountIsDeduped` pins it (F-d).
3. **Order inside the scope: reserve, then stamp, then dispatch.** The brief lists "stamps set, then
   reserve". I reserve first. `IdxFor` takes a registry slot and can bump
   `multimaster_keydb_author_overflow`; a duplicate that is dropped, or a cancelled reserve, must
   cost neither (the plan's cancelled-reserve test asserts "no counter moved"). The scope object is
   still built before the reserve, so every exit restores, and Task 2.4/2.8 add their setters beside
   `Stamp` unchanged.
4. **A rejected leaf advances nothing, the enclosing layers included.** D-5 says an inner result of
   `kConsumed` advances the outer, and also that a rejected-before-run envelope "leaves the
   watermark alone, so the same envelope replayed by another link still applies once". They conflict
   for a nested envelope: if the outer B advanced, the same `B(A(m))` arriving on another link would
   hit B's early-out and the rejected `A(m)` would never get its retry. I followed the second
   sentence (no layer advances after `kRejected`); `RejectedDispatchCountsBytesDoesNotAdvance`
   asserts the outer is not advanced. Same for a leaf skipped because its layer's db could not be
   selected (`advance = false`). **Open question 1.**
5. **The early-out drops the inner layer too.** `Nested({A,100},{B,500})` after `A(100)` was applied:
   A's layer is dropped at its own `IsApplied` (not at the leaf), counted once in `rreplay_deduped`,
   not counted in `rreplay_unwrapped` (it was not taken apart); B's layer is unwrapped and advances.
   The leaf-level `kDrop` is the race path only (both links passed `IsApplied`), covered by the
   `AuthorDedupTest`s.
6. **`mvcc == 0` everywhere:** `Reserve` returns `kApply` without touching the map, `Commit`/`Advance`
   are no-ops, `IsApplied` is false (a plain `applied >= 0` would be true for every author). The
   `Reservation` guard holds nothing for mvcc 0, so a 0-mvcc `Release` cannot clear another link's
   reservation.
7. **Skipped commands reserve nothing** (control, KeyDB-only, unknown return before `Reserve`); the
   skipped leaf's author is advanced by `HandleRreplay`.
8. **`Clear()` erases the idle entries** instead of zeroing their `applied` (same observable effect,
   frees the memory) and zeroes `applied` of entries with `inflight != 0`, which it keeps.
9. **Cancellation.** `Reserve` calls `running()` after each 100 ms wait, with `mu_` released. A
   `kCancelled` leaf makes `HandleRreplay` return `kNotConsumed` before it counts `rreplay_unwrapped`;
   nothing advances. The layers' dbs may be selected (D-5 allows it, and the test asserts
   `db_index == 3` and that the same bytes land in db 3 on a fresh link). The outermost `running()`
   check still runs exactly once for an uncontended tree (`running_calls == 1` test unchanged).
10. **Tests needed fresh mvccs.** The watermark is monotone and process-wide, so the old test helpers
    (one default mvcc `7` / `1`, `ms << 20` without a counter) became duplicates of each other.
    `Envelope()` (gtest), `rreplay()` and `keydb_mvcc()` (pytest) now mint a higher mvcc per call, as
    a real KeyDB does. A first full pytest run found this: `test_unwrap_flushes_raw_batch_before_envelope`
    (2 params) and `test_plain_replica_of_active_keydb_keeps_a_ttl_less_orphan_...` (2 params) failed
    before the helpers were fixed; they pass now. Three more tests that stream explicit mvccs
    already passed.

## Lock acquisition sites of `mu_` and what runs under each

All in `classic_replay.cc`, all `fb2::Mutex` via `LockGuard`/`unique_lock`, none re-entrant.

| Site | Under `mu_` |
|---|---|
| `Reserve` (loop) | `EntryLocked` (find / insert / evict), `NowMs()`, field reads and writes. `cv_.wait_for` releases it while waiting. `running()` is called with `mu_` **released** (`lk.unlock(); running(); lk.lock();`), and the entry is looked up again on every pass. |
| `Commit` | `EntryLocked`, field writes, `cv_.notify_all()`, `NowMs()`. |
| `Release` | `find`, one field write, `notify_all`. |
| `Advance` | `EntryLocked`, field writes, `NowMs()`. |
| `IsApplied` | `find`, one compare. |
| `Clear` | one pass over the map (erase idle, zero the reserved). |
| `ClearAllForTest`, `size` | map clear / size. |

Never under it: a dispatch, `PeerRegistry::AddOrGet`, `AwaitBrief`, `IdxFor`, a log call, the
caller's `running()`. `Reservation::~Reservation` calls `Release` (takes `mu_`) after the dispatch has
returned.

## Eviction cost

`EntryLocked` scans the whole map (`O(n)`, `n <= 4096`) under `mu_` when it inserts a new key into a
full map (once per new author past 4096, never for a known one) and no idle entry is skipped by
`last_seen_ms` ties. It evicts one idle entry. If every entry is reserved nothing is evicted and the
map exceeds the bound until a commit (`EvictionSkipsInflight` pins this). Not measured: 4096 hash
iterations with a compare is on the order of microseconds, once per new author. A late duplicate of an
evicted author can get through (documented in D-5).

## Tests

gtest `classic_replay_test`: **116 -> 134** (all pass, three consecutive runs); `multi_master_test`
230 (229 pass, 1 skipped, unchanged). Pytest `keydb_onboarding_test.py`: **176 -> 178** collected.
One full run with `-m 'not slow'` (4 slow deselected): 172 passed, 2 failed (the two
`..._keeps_a_ttl_less_orphan_...` params, from `keydb_mvcc()` returning one mvcc for several
commands); after fixing it, those two plus the ttl-refresh, counter, mesh and master-switch tests:
14 passed.

New gtests (all names as in the plan):

- `AuthorDedupTest` (own 2-proactor pool, local `AuthorDedup`): `ConcurrentSameEnvelopeAppliesOnce`,
  `ReleaseLetsSecondLinkApply`, `CancelledWaiterReturnsNotConsumed` (the verdict is `kCancelled`; the
  applier-level `kNotConsumed` is the family test), `MvccZeroNeverDedupedNeverAdvances`,
  `AdvanceOnlyForward`, `EvictionSkipsInflight` (fake clock, bound 3), `ClearResetsAppliedKeepsInflight`.
- `ClassicApplyFamilyTest`: `NestedOuterAdvancesWhenInnerDeduped`, `AdvancesAfterSkippedInner`,
  `DoesNotAdvanceWhenMalformedOrSelf`, `Depth65MalformedStillAdvancesOuter` (65 distinct authors),
  `RunningFalseBeforeDispatchReturnsNotConsumedNoAdvance`,
  `CancelledReserveAfterSelectIsNotConsumedAndReplaysInTheSameDb`, `RejectedDispatchCountsBytesDoesNotAdvance`,
  `ReplayAfterCommitBeforeCountIsDeduped`, plus two of mine: `ErrorReplyOfARunCommandIsCommitted`
  (ran and replied WRONGTYPE: committed) and `EnvelopeWithoutMvccIsNeverDeduped`.
  `RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope` is changed (explicit 3-author tree,
  asserts the commit and both advances).
- `ClassicAuthorMapTest.DedupStillWorksForOverCapAuthor` (cap 0: overflow +1 once, deduped +2, the
  author never gets a registry slot).

Pytests: `test_keydb_mesh_forwarded_duplicates_deduped` (KeyDB A and B mutual, forwarding, one
`--active_replica --multi_master` node with **both** as peers, 2000 pipelined `INCR`: `c == N` on
the node, `multimaster_rreplay_deduped >= N` in INFO and `/metrics`; a marker write first proves both
links' full syncs are over, since "up" precedes the snapshot and a snapshot merged mid-run carries
counts the stream also delivers: my first version failed on that, c = 3878) and
`test_master_switch_full_sync_resets_author_dedup` (C1/C2 mesh, `Proxy` paused on C2's link to C1,
100 `INCR` on C1, plain D applies them, `REPLICAOF C2`, resume: `c == N`; the proxy leaks the chunk
already in flight when paused, so the test allows C2 to hold a few and asserts `< N` before). The
INFO/metrics pins: `assert_no_classic_fields` gains the two absence checks for `rreplay_deduped`;
`test_info_and_metrics_show_classic_counters` asserts both process-wide counters are shown at 0 as
counters. Exact pin extended from 2 to 3: `ClassicProcessWideValuesFollowTheRenderPredicate`.

## Falsification

Mutants applied to `classic_replay.cc` (and `.h` for d, `replica.cc` for e_replica) from
`/tmp/good23/`, rebuilt, `--gtest_filter='AuthorDedupTest.*:ClassicApplyFamilyTest.*:ClassicPeerApplyFamilyTest.*:ClassicAuthorMapTest.*'`
(58 tests), restored (`cmp` clean), rebuilt, 134 pass.

| # | Mutant | Fails |
|---|---|---|
| a | `Reserve` never sets `inflight`, never waits (check-only) | `AuthorDedupTest.ConcurrentSameEnvelopeAppliesOnce`, `CancelledWaiterReturnsNotConsumed`, `EvictionSkipsInflight`, `ClearResetsAppliedKeepsInflight`, `ClassicApplyFamilyTest.CancelledReserveAfterSelect...` |
| b | `Advance` before the `kNotConsumed` return | `ClassicApplyFamilyTest.RunningFalseBeforeDispatchReturnsNotConsumedNoAdvance` (only) |
| c | `Commit` also when the dispatcher rejected | `ClassicApplyFamilyTest.RejectedDispatchCountsBytesDoesNotAdvance` (only) |
| d | the leaf's `Commit` deferred to the start of the *next* `HandleRreplay` call | `ReplayAfterCommitBeforeCountIsDeduped` + the 5 family tests that assert `Applied` right after |
| e_unit | `Clear()` returns at once | `AuthorDedupTest.ClearResetsAppliedKeepsInflight` (only) |
| e_replica | the two `Clear()` calls in `replica.cc` removed | pytest `test_master_switch_full_sync_resets_author_dedup`, 3 of 3 runs |
| f | dedup off (`Reserve` always `kApply`, `IsApplied` false) | 17 tests, listed below |
| a, pytest | as a, mesh pytest, 5 runs | 5 of 5 failed |
| f, pytest | as f, mesh pytest | failed |

Verbatim, the discriminating lines:

(a) the proof (see fix round 1, M7, for how far "deterministic" goes):

```
/home/user/drakeydb/src/server/classic_replay_test.cc:2347: Failure
Expected equality of these values:
  applied.load()
    Which is: 2
  1
/home/user/drakeydb/src/server/classic_replay_test.cc:2348: Failure
Expected equality of these values:
  dropped.load()
    Which is: 0
  1
[  FAILED  ] AuthorDedupTest.ConcurrentSameEnvelopeAppliesOnce (12 ms)
```

(a) statistics, mesh pytest over 5 runs, each ended with the same line (**not** the full 2N: one
duplicate slips through at the start of the pipeline, the rest are caught by the early-out because
the links stay in lockstep after that; the gtest is the proof, as the plan says):

```
E           AssertionError: assert 2001 == 2000     (runs 1-5)
```

(b):

```
classic_replay_test.cc:1857: Failure
Value of: Applied(kAuthorB, 601)
  Actual: true
Expected: false
[  FAILED  ] ClassicApplyFamilyTest.RunningFalseBeforeDispatchReturnsNotConsumedNoAdvance (26 ms)
```

(c):

```
classic_replay_test.cc:1917: Failure
Value of: Applied(kAuthorA, 800)
  Actual: true
Expected: false
classic_replay_test.cc:1930: Failure
  deduped.moved()
    Which is: 2
  1u
[  FAILED  ] ClassicApplyFamilyTest.RejectedDispatchCountsBytesDoesNotAdvance (22 ms)
```

(d):

```
classic_replay_test.cc:1964: Failure
Expected equality of these values:
  deduped.moved()
    Which is: 0
  1u
[  FAILED  ] ClassicApplyFamilyTest.ReplayAfterCommitBeforeCountIsDeduped
```

My first version of that test (one nested envelope, replayed on a link that had made another call
in between) **passed** under (d): the outer's `Advance` dropped the replay whatever the leaf did, and
the flush-on-next-call mutant had already run. It now applies a single-layer envelope and a nested
one each on a link that goes away, and replays them on a new one.

(e_unit):

```
classic_replay_test.cc:2495: Failure
Value of: dedup.IsApplied(kAuthorA, 5)
  Actual: true
Expected: false
classic_replay_test.cc:2496: Failure
  dedup.size()
    Which is: 2
  1u
[  FAILED  ] AuthorDedupTest.ClearResetsAppliedKeepsInflight (104 ms)
```

(e_replica) pytest, 3 of 3: `E       AssertionError: assert equals failed` out of `c2_forwarded_to_d`
(`assert_eventually` gives up: `c` stays below N on D).

(f) gtest: `RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope`,
`NestedOuterAdvancesWhenInnerDeduped`, `AdvancesAfterSkippedInner`, `DoesNotAdvanceWhenMalformedOrSelf`,
`Depth65MalformedStillAdvancesOuter`, `RunningFalseBeforeDispatchReturnsNotConsumedNoAdvance`,
`CancelledReserveAfterSelect...`, `RejectedDispatchCountsBytesDoesNotAdvance`,
`ErrorReplyOfARunCommandIsCommitted`, `ReplayAfterCommitBeforeCountIsDeduped`,
`AuthorDedupTest.{ConcurrentSameEnvelopeAppliesOnce, ReleaseLetsSecondLinkApply, CancelledWaiterReturnsNotConsumed,
AdvanceOnlyForward, EvictionSkipsInflight, ClearResetsAppliedKeepsInflight}`,
`ClassicAuthorMapTest.DedupStillWorksForOverCapAuthor`. Mesh pytest:

```
E       assert 0 >= 2000       (multimaster_rreplay_deduped; with dedup off the counter never moves)
```

(The pytest asserts the counter, so its (f) failure is the counter, not the 2N value; `c` itself is
asserted after it. Not re-run with the counter assertion moved.)

### What each new test passes under with the feature removed

- Pass under **every** mutant, by design (they pin the opposite direction): `AuthorDedupTest.MvccZeroNeverDedupedNeverAdvances`
  (under f the whole feature is off, so it holds trivially), `EnvelopeWithoutMvccIsNeverDeduped`.
- `NestedOuterAdvancesWhenInnerDeduped`, `AdvancesAfterSkippedInner`, `DoesNotAdvanceWhenMalformedOrSelf`,
  `Depth65MalformedStillAdvancesOuter`, `ErrorReplyOfARunCommandIsCommitted` fail only under (f) and (d)
  (the latter three for the committed ones): they do not catch a/b/c/e.
- `RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope` fails under d and f.
- `ReleaseLetsSecondLinkApply`, `AdvanceOnlyForward` fail only under f: `Reserve`'s hand-off and the
  max rule are not pinned by any single-point mutant I tried (a leaves them passing).
- `DedupStillWorksForOverCapAuthor` fails only under f.

## Anchor drift

`replica.cc`: the plan's `:769/:771` are `:829`/`:835` now (P7-0..P7-2 moved them); the second
FlushAll/FlushSlots pair is `:1143/:1145` in `InitiateDflySync`. `ConsumeRedisStream` calls
`HandleRreplay` at `:1362`, `repl_offs_ += total_read` at `:1365`.

## Open questions

1. **Rejected leaf and its enclosing layers (note 4).** I chose "no layer advances", following D-5's
   replay sentence over its outer-layer sentence. If the spec's literal reading is wanted (outer
   advances after `kConsumed` even for a rejected inner), it is one `if` in `HandleRreplay`; the
   consequence is that the rejected inner's retry over another link is lost for a forwarded envelope.
2. **Reserve before Stamp** (note 3), against the brief's literal order.
3. The full non-slow pytest file ran once (172 passed, 2 failed, both from my test helpers, since
   fixed and re-run in isolation: 14 passed). The whole file was not re-run end to end after the fix.
4. `AuthorDedup::Clear()` also frees the idle entries; a test that counted entries after a `Clear`
   expecting the old shape (zeros kept) would differ. None does.

## Fix round 1

Review: `task-2.3-review.md` (0 Critical, 2 Important, 9 Minor). Rulings: `task-2.3-fix1.md`.

| Finding | Change |
|---|---|
| I1 | New `ClassicAuthorMapTest.ReservedAuthorCostsNoSlotAndNoOverflow` (new `PeerApplier` helper in the fixture): the test holds `Reserve(A, m)`; for cap 0 and cap 4096, (1) a link whose `running()` flips gets `kNotConsumed`, (2) a live link waits while a fiber on proactor 1 `Commit`s and gets a leaf `kDrop` (`deduped` +1); each asserts overflow total, `registry.Size()` and `FindIdx(A)` unchanged. |
| I2 | New pytest `test_resync_keeps_author_dedup_on_a_peer_merge_and_resets_it_on_a_flush` (peer_mode / plain_replica): `INCR c` at mvcc 5, link dropped, second full sync whose tail re-streams the same bytes, then a marker. Peer: `(c, deduped) == ("1", 1)` (merge keeps `c` and the watermark, the replay is dropped). Plain: `("1", 0)` (the flush emptied `c`, the replay re-applies it, nothing dropped because `Clear()` ran). |
| M1 | `--active_replica` removed from the `ClassicAuthorMapTest` fixture (SetUp and TearDown). |
| M2 | New `ClassicApplyFamilyTest.FailedSelectAdvancesNoLayer` (`--cluster_mode=yes`): own db 3 fails, or the inner layer's db 3 fails under a good outer: no layer advances; a db-0 control advances both. R14 recorded for Task 2.5 together with R10 and the KeyDB divergence (`replication.cpp:5476-5493`). |
| M3 | Comments on `Leaf::kRejected` and `Dispatch`'s `*ran`: "the dispatch result was not OK (refused before it ran, or it reported OOM)". |
| M4 | New `ClassicApplyFamilyTest.MvccZeroLeafLeavesAnotherLinksReservationAlone`. It applies a 0-mvcc leaf that runs and one that is rejected (wrong arity): the rejected path is the one that `Release`s. My first version used the running leaf only and did not catch the mutant, because `Commit()` clears `held_` before the destructor; the rejected leaf does. |
| M5, M6 | `AuthorMapTest` lead comment moved back above its class; `Reservation` is in `CommandScope`'s anonymous namespace. |
| M7 | `ConcurrentSameEnvelopeAppliesOnce`: the first link spins on `ThisFiber::Yield` until the second flags that it is about to `Reserve`, then sleeps 10 ms; the second is launched only after the first holds the reservation. Not a hard proof (the second link's `Reserve` could in principle run later than 10 ms), but the first can no longer finish before the second starts. |
| M8 | `FreshMvcc()` starts at `1 << 40`; pytest `rreplay()` counter at `1 << 40`; both comments name the trap. `keydb_mvcc()` keeps a low-bits counter (the milliseconds order it). |
| M9 | Nothing in code: the P7-2 gate runs the release perf bar (Task 2.4 Step 5). |

### Falsification (restored with `cmp`, rebuilt after each)

I1, `r11`: `Stamp(IdxFor)` moved above `Reserve` in `ApplyCommand`. Fails `ReservedAuthorCostsNoSlotAndNoOverflow` (and, as a bonus, two older tests):

```
classic_replay_test.cc:2849: Failure   (ClassicAuthorOverflowTotal() vs overflow_before, cap 0)
classic_replay_test.cc:2849: Failure
classic_replay_test.cc:2850: Failure   (node.registry.Size() 3 vs 2)
classic_replay_test.cc:2851: Failure   (FindIdx(kAuthorA).has_value() true, "cancelled cap 4096")
[  FAILED  ] ClassicPeerApplyFamilyTest.PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin
[  FAILED  ] ClassicAuthorMapTest.DedupStillWorksForOverCapAuthor
```

(Full first run: `Which is: 10 / overflow_before 9`, `registry.Size() Which is: 3 / size_before 2`, `Value of: node.registry.FindIdx(kAuthorA).has_value()  Actual: true  Expected: false  cancelled cap 4096`.)

I2, `peer_clear` (`Clear()` added to the peer merge branch of `InitiatePSync`), pytest:

```
E           AssertionError: (c, deduped) is ('2', 0), not ('1', 1)
FAILED ... test_resync_keeps_author_dedup_on_a_peer_merge_and_resets_it_on_a_flush[df_factory0-peer_mode]
================= 1 failed, 1 passed, 178 deselected in 4.08s ==================
```

I2, `e_replica` (the two flush-point `Clear()`s removed), plain twin:

```
E           AssertionError: (c, deduped) is (None, 1), not ('1', 0)
FAILED ... test_resync_keeps_author_dedup_on_a_peer_merge_and_resets_it_on_a_flush[df_factory0-plain_replica]
================= 1 failed, 1 passed, 178 deselected in 3.73s ==================
```

M2, `m2` (a db-failure skip treated as a skipped leaf, i.e. it advances):

```
classic_replay_test.cc:2031: Failure   Value of: Applied(kAuthorA, 1100)  Actual: true  Expected: false
classic_replay_test.cc:2037: Failure   Value of: Applied(kAuthorC, 1201)  Actual: true  Expected: false
classic_replay_test.cc:2038: Failure   Value of: Applied(kAuthorB, 1200)  Actual: true  Expected: false
[  PASSED  ] 136 tests.
[  FAILED  ] ClassicApplyFamilyTest.FailedSelectAdvancesNoLayer
```

M4, `m4` (`held_(true)`):

```
classic_replay_test.cc:2007: Failure
Expected equality of these values:
  dedup.Reserve(kAuthorA, 10, [] { return false; })
    Which is: 4-byte object <00-00 00-00>
  AuthorDedup::Verdict::kCancelled
    Which is: 4-byte object <02-00 00-00>
[  PASSED  ] 136 tests.
[  FAILED  ] ClassicApplyFamilyTest.MvccZeroLeafLeavesAnotherLinksReservationAlone
```

Mutant (a) re-run on the final tests still fails the same family (`CancelledReserveAfterSelect...`,
`MvccZeroLeafLeavesAnotherLinksReservationAlone`, `ConcurrentSameEnvelopeAppliesOnce`, ...).

### Final counts

`classic_replay_test` **137** passed (134 + 3: `ReservedAuthorCostsNoSlotAndNoOverflow`,
`MvccZeroLeafLeavesAnotherLinksReservationAlone`, `FailedSelectAdvancesNoLayer`), three runs;
`multi_master_test` 229 passed (1 skipped); pytest collected 180 (178 + the two params of the
resync test); `-k "dedup or author_slot or exact_classic_counters or master_switch or mesh_forwarded or resync_keeps"`:
7 passed. Pre-commit clean on all changed files. Whole pytest file not re-run (the review ran it
once: 175 passed, 3 perf-gated skips).
