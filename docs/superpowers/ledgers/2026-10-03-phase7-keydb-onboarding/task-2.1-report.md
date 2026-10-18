# Task 2.1 report: `ClassicAuthorMap`, the author cap, the uuid-change counter, the registry gauge

Branch `feat/phase7-2-author-stamps-dedup-guard`, HEAD `0f79bf5a` plus the uncommitted ledger edits.
Not committed (the orchestrator commits). Implementer: Sonnet 5.5. Plan: Task 2.1; spec D-4 "Author
cap", D-13. Scratch (mutant outputs, good copies) is in `/tmp/mut21/` and `/tmp/good21/`.

## Summary

- `ClassicAuthorMap::IdxFor(uuid)` resolves an author to a `PeerRegistry` index: the link's own uuid
  to `peer_origin_idx_` (never counted), a forwarded uuid through the process-wide
  `--classic_author_cap` (default 4096) and then `AddOrGet` plus `RegisterOriginHash` on every
  proactor (`AwaitBrief`), memoized per link. Past the cap: the link's idx (never `kSelfIdx`),
  `multimaster_keydb_author_overflow` bumped, `LOG_EVERY_T(WARNING, 60)`. A null registry turns
  stamping off (every author maps to the link idx).
- `classic_link_uuid_changes`: a seventh per-link counter (one `kClassicCounters` row), bumped by
  `Replica::Greet()` when a completed handshake of a classic link presents a different non-empty
  uuid than the previous completed one (`Replica::last_greeted_master_uuid_`, untouched by the
  per-Greet clear at `replica.cc:478`).
- Process-wide `multimaster_keydb_author_overflow` (counter) and `multimaster_peer_registry_size`
  (gauge over `PeerRegistry::Size()`), in INFO beside `multimaster_lww_dropped` (active-node block)
  and after the per-link fields (plain-replica block), and in `/metrics` as
  `multimaster_keydb_author_overflow_total` and `multimaster_peer_registry_size`.
- **`IdxFor` is called from nowhere in production yet.** `ConsumeRedisStream` builds the map and
  hands it to the `ClassicApplier` (`authors()`), and Task 2.2 will call it per command. Only the
  tests call it.
- gtest: `classic_replay_test` 99 -> 107 (8 new; the count before is 107 minus the 8 added, I did not
  run the old binary), `multi_master_test` 230 -> 230 (pins edited, none added). Pytest: 1 new case
  (`keydb_onboarding_test.py` collects 175), 2 exact pins extended.

## What changed

| File | Change |
|---|---|
| `src/server/classic_replay.h` | `ClassicLinkStats::classic_link_uuid_changes`; `NoteClassicLinkUuidChange`; `ClassicCounterValue::gauge`; `ClassicProcessWideValues`, `ClassicAuthorOverflowTotal`, `ResetClassicForwardedAuthorsForTest`; class `ClassicAuthorMap`; `ClassicApplier` takes an optional `ClassicAuthorMap` (last ctor parameter, default stamping off) and exposes `authors()`. |
| `src/server/classic_replay.cc` | `ABSL_FLAG classic_author_cap`; the `kClassicCounters` row; the process-wide forwarded-author set (`ForwardedAuthors`, an `fb2::Mutex` + `flat_hash_set`) and overflow atomic; `ClassicAuthorMap`; `ClassicProcessWideValues`; `NoteClassicLinkUuidChange`. |
| `src/server/replica_types.h` | `ClassicLinkCounts::classic_link_uuid_changes`. |
| `src/server/replica.{h,cc}` | `last_greeted_master_uuid_`; the bump at the end of `Greet()`; `ConsumeRedisStream` passes `ClassicAuthorMap(peer_mode_ ? peer_mode_->registry : nullptr, master_context_.master_node_uuid, peer_origin_idx_)`. |
| `src/server/server_family.cc` | INFO: the process-wide values in the active block (after `multimaster_lww_dropped`) and after the per-link loop of the plain-replica block; `/metrics` callback passes `peer_registry_.Size()`. |
| `src/server/metrics.{h,cc}` | `Metrics::Print(..., size_t peer_registry_size)`; emits the overflow counter (`_total`) and the gauge next to the classic totals. `Metrics` itself is unchanged (its size is `static_assert`ed). |
| `src/server/classic_replay_test.cc` | 8 new tests, the `Counts` helper and the exact lists of 6 counters extended to 7. |
| `src/server/multi_master_test.cc` | The two peer-line pins of `PeerReplicationInfo` extended with `classic_link_uuid_changes`. |
| `tests/dragonfly/keydb_onboarding_test.py` | `CLASSIC_COUNTERS` gains the new name; the exact dicts of `test_scripted_active_master_exact_classic_counters` (INFO and `/metrics`) gain `classic_link_uuid_changes: 0`; new `test_keydb_restart_consumes_one_author_slot`. |

`multi_master.cc` (listed in the plan) needs no change: the peer line renders from `kClassicCounters`.

## Design notes

1. **Owner of the map: the applier, built by `Replica`.** `ConsumeRedisStream` already constructs
   the applier per stream connection, after `Greet()`, so `master_context_.master_node_uuid` and
   `peer_origin_idx_` are final. The map goes in as a constructor argument: a 4-line hunk in
   `replica.cc`, no new `Replica` member for it, and the 8 existing test `Link`s are untouched
   (defaulted parameter). A map owned by `Replica` would have needed refreshing after each `Greet()`
   (a KeyDB restart changes both inputs). The memo is therefore per stream connection, which is a
   cache only: `AddOrGet` is idempotent.
2. **The cap is a process-wide set of admitted uuids**, not a counter of `AddOrGet` calls: two links
   that forward the same author share one slot (a counter would double count, and `FindIdx`-then-
   `AddOrGet` races). It holds at most `--classic_author_cap` strings. `Admit` runs before
   `AddOrGet`, so a refused author never touches the registry. The set is cleared by
   `ResetClassicForwardedAuthorsForTest` (the registry of a test is its own, the set is the
   process's). An author counted against the cap that later becomes a link's own uuid keeps its
   slot (harmless).
3. **Overflow is counted per resolution, not per distinct author, and is not memoized**: asking again
   asks the cap again. Task 2.2 will call `IdxFor` once per envelope, so the counter then reads
   "envelopes stamped as the link because of the cap". The `LOG_EVERY_T` is per call site.
4. **Hash registration** uses `shard_set->pool()->AwaitBrief`, as `Greet()` does for the link's own
   hash, and finishes before `IdxFor` returns, so `IncomingStamp`'s `DCHECK(hash != 0)` holds for
   everything stamped afterwards. A second link that resolves an already registered author registers
   the hash again (idempotent, once per author per link): another link's registration may still be in
   flight when its `AddOrGet` result is seen.
5. **The gauge's predicate.** Spec D-13 / the brief say `ActiveKeyDbMasterSeen() || value != 0`, but
   `PeerRegistry::Size()` is never 0 (it holds the node), so that would print the gauge on every node
   and break byte-identity with upstream for a stock master. I used `ActiveKeyDbMasterSeen() ||
   Size() > 1`: the registry has grown past the node itself. A stock replica of Redis (size 1) prints
   nothing; an active node with any peer prints it (it is the Risk 7 signal). **Open question 1.**
6. **`Metrics` cannot carry the size** (its `static_assert` sums every member), so the registry's
   owner passes it: `ServerFamily` reads `peer_registry_.Size()` for both INFO and the `/metrics`
   callback; `Print` gets one more parameter (its only caller is `ConfigureMetrics`).
7. **`classic_link_uuid_changes` counts only classic links** (`classic_master_`): the counter is a
   classic one in INFO and `/metrics`, and a DFLY peer that changes uuid would otherwise make
   `classic_link_uuid_changes_total` appear on a node that never met KeyDB. It is recorded at the
   end of a *completed* `Greet()` only; a failed `Greet()` neither bumps nor updates the stored uuid.
8. The per-link counter, like the others, is shown for an active KeyDB's link (zero included) and
   for any classic link whose counter moved; `/metrics` exports `classic_link_uuid_changes_total`
   under decision 25's predicate.

## Tests

### gtest (`cd /home/user/drakeydb/build-dbg && ./classic_replay_test && ./multi_master_test`)

`classic_replay_test.cc` (8 new, 99 -> 107; fixture `ClassicAuthorMapTest : BaseFamilyTest`, which
runs the registry calls on a proactor fiber and reads `MvccStamper::tlocal()` on every proactor):

- `ClassicAuthorMapTest.DistinctUuidsGetDistinctIdxAndRegisteredHashOnEveryProactor`: two authors:
  distinct, non-self, non-link idx; `GetUuid` round trip; registry size 4; each author's
  `NodeUuidHash` on all 3 proactors.
- `.LinkUuidMapsToPeerIdx`: the link's uuid maps to `link_idx`, registry unchanged, no hash registered.
- `.MemoizesPerLink`: a second ask of one map registers nothing (the hash wiped on every proactor
  stays wiped), a second map re-registers it and gets the same idx.
- `.CapFallsBackToLinkIdxAndCounts`: cap 2; the third author gets the link's idx, never
  `kSelfIdx`, is not in the registry, bumps the overflow total once per resolution; counted authors
  keep their idx in this map and in a fresh one. (The envelope's mvcc is the caller's; `IdxFor`
  does not see it, so "the mvcc is kept" is Task 2.2's test.)
- `.CapIgnoresLinkUuid`: cap 0 and cap 1 (taken by a forwarded author): the link's uuid still maps
  to its own idx with no overflow; the next forwarded author is refused.
- `.NullRegistryTurnsStampingOff`: null registry and a default map map every uuid to the link idx.
- `ClassicReplayTest.ClassicProcessWideValuesFollowTheRenderPredicate`: the gauge and counter
  predicates (design note 5), help texts, `gauge` flag.
- `.NoteClassicLinkUuidChangeFeedsTheLinkAndTheTotals`.

Changed: `ClassicLinkFieldsFollowTheRenderPredicate` and `ClassicTotalSeriesFollowTheRenderPredicate`
(seven counters in order); `multi_master_test` `PeerReplicationInfo.ShowsClassicFieldsOnlyForClassicLinks`
and `.OmitsClassicFieldsWhenMasterNotActiveAndCountersZero` (the peer line carries the field).

### pytest

- New `test_keydb_restart_consumes_one_author_slot` (real KeyDB, peer mode): before: link
  `classic_link_uuid_changes == 0`, registry size 2; after each of two KeyDB restarts: the link's
  `node_uuid` changed, `classic_link_uuid_changes == restarts`, `multimaster_peer_registry_size ==
  2 + restarts` in INFO and equal to the `/metrics` gauge, and `dragonfly_classic_link_uuid_changes`
  (`_total`) `== restarts`.
- Extended: `CLASSIC_COUNTERS`, the two exact dicts of `test_scripted_active_master_exact_classic_counters`.

## Falsification (Step 4)

All gtest mutants: edit `src/server/classic_replay.cc` from `/tmp/good21/`, then
`docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb && ninja -C build-dbg -j4 classic_replay_test && cd build-dbg && ./classic_replay_test --gtest_filter="ClassicAuthorMapTest.*"'`
(`RAW: Restore`/`RUN`/`OK` lines filtered). Restored by copying the good file back; the final run
below the table is on the restored sources.

| # | Mutant | Fails | Would it still pass without the feature? |
|---|---|---|---|
| M1 | `RegisterOriginHash(idx, hash);` -> `(void)idx; (void)hash;` | `DistinctUuids...` (hash `{0,0,0}`), `MemoizesPerLink` | n/a |
| M2 | no cap (the `size() >= cap` check removed) | `CapFallsBackToLinkIdxAndCounts`, `CapIgnoresLinkUuid` | n/a |
| M3 | link uuid counted against the cap (`Admit` for it) | `CapIgnoresLinkUuid` | n/a |
| M4 | overflow returns `PeerRegistry::kSelfIdx` | `CapFallsBackToLinkIdxAndCounts`, `CapIgnoresLinkUuid` | n/a |
| M6 | memo lookup removed | `MemoizesPerLink` | n/a |
| M5 | `last_greeted_master_uuid_` never stored (`replica.cc`) | pytest `test_keydb_restart_consumes_one_author_slot` | n/a |
| M7 | the gauge skipped in `/metrics` (`metrics.cc`) | same pytest | n/a |

Each mutant's output follows verbatim (the last lines of the run).

### M1_skip_register

```
[1/3] Building CXX object src/server/CMakeFiles/dragonfly_lib.dir/classic_replay.cc.o
[2/3] Linking CXX static library lib/libdragonfly_lib.a
[3/3] Linking CXX executable classic_replay_test
    Which is: { 0, 0, 0 }
  vector<uint64_t>(pp_->size(), NodeUuidHash(kAuthorA))
    Which is: { 7953565813638624897, 7953565813638624897, 7953565813638624897 }
/home/user/drakeydb/src/server/classic_replay_test.cc:1805: Failure
Expected equality of these values:
  HashesOf(b)
    Which is: { 0, 0, 0 }
  vector<uint64_t>(pp_->size(), NodeUuidHash(kAuthorB))
    Which is: { 16571850207653206743, 16571850207653206743, 16571850207653206743 }
[  FAILED  ] ClassicAuthorMapTest.DistinctUuidsGetDistinctIdxAndRegisteredHashOnEveryProactor (23 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1838: Failure
Expected equality of these values:
  HashesOf(a)
    Which is: { 0, 0, 0 }
  vector<uint64_t>(pp_->size(), NodeUuidHash(kAuthorA))
    Which is: { 7953565813638624897, 7953565813638624897, 7953565813638624897 }
[  FAILED  ] ClassicAuthorMapTest.MemoizesPerLink (20 ms)
[----------] 6 tests from ClassicAuthorMapTest (123 ms total)
[----------] Global test environment tear-down
[==========] 6 tests from 1 test suite ran. (123 ms total)
[  PASSED  ] 4 tests.
[  FAILED  ] 2 tests, listed below:
[  FAILED  ] ClassicAuthorMapTest.DistinctUuidsGetDistinctIdxAndRegisteredHashOnEveryProactor
[  FAILED  ] ClassicAuthorMapTest.MemoizesPerLink
 2 FAILED TESTS
```

### M2_no_cap

```
[1/3] Building CXX object src/server/CMakeFiles/dragonfly_lib.dir/classic_replay.cc.o
[2/3] Linking CXX static library lib/libdragonfly_lib.a
[3/3] Linking CXX executable classic_replay_test
    Which is: 0
  3u
    Which is: 3
[  FAILED  ] ClassicAuthorMapTest.CapFallsBackToLinkIdxAndCounts (20 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1889: Failure
Expected equality of these values:
  map.IdxFor(kAuthorB)
    Which is: 3
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1890: Failure
Expected equality of these values:
  ClassicAuthorOverflowTotal() - overflow_before
    Which is: 0
  1u
    Which is: 1
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid (18 ms)
[----------] 6 tests from ClassicAuthorMapTest (123 ms total)
[----------] Global test environment tear-down
[==========] 6 tests from 1 test suite ran. (123 ms total)
[  PASSED  ] 4 tests.
[  FAILED  ] 2 tests, listed below:
[  FAILED  ] ClassicAuthorMapTest.CapFallsBackToLinkIdxAndCounts
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid
 2 FAILED TESTS
```

### M3_link_counts_against_cap

```
[1/3] Building CXX object src/server/CMakeFiles/dragonfly_lib.dir/classic_replay.cc.o
[2/3] Linking CXX static library lib/libdragonfly_lib.a
[3/3] Linking CXX executable classic_replay_test
Expected equality of these values:
  ClassicAuthorOverflowTotal()
    Which is: 4
  overflow_before
    Which is: 3
/home/user/drakeydb/src/server/classic_replay_test.cc:1888: Failure
Expected equality of these values:
  ClassicAuthorOverflowTotal()
    Which is: 5
  overflow_before
    Which is: 3
/home/user/drakeydb/src/server/classic_replay_test.cc:1890: Failure
Expected equality of these values:
  ClassicAuthorOverflowTotal() - overflow_before
    Which is: 3
  1u
    Which is: 1
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid (19 ms)
[----------] 6 tests from ClassicAuthorMapTest (123 ms total)
[----------] Global test environment tear-down
[==========] 6 tests from 1 test suite ran. (123 ms total)
[  PASSED  ] 5 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid
 1 FAILED TEST
```

### M4_overflow_as_self

```
[1/3] Building CXX object src/server/CMakeFiles/dragonfly_lib.dir/classic_replay.cc.o
[2/3] Linking CXX static library lib/libdragonfly_lib.a
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1858: Failure
Expected: (map.IdxFor(kAuthorC)) != (PeerRegistry::kSelfIdx), actual: 0 vs 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1868: Failure
Expected equality of these values:
  other.IdxFor(kAuthorD)
    Which is: 0
  node.link_idx
    Which is: 1
[  FAILED  ] ClassicAuthorMapTest.CapFallsBackToLinkIdxAndCounts (19 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1889: Failure
Expected equality of these values:
  map.IdxFor(kAuthorB)
    Which is: 0
  node.link_idx
    Which is: 1
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid (20 ms)
[----------] 6 tests from ClassicAuthorMapTest (123 ms total)
[----------] Global test environment tear-down
[==========] 6 tests from 1 test suite ran. (123 ms total)
[  PASSED  ] 4 tests.
[  FAILED  ] 2 tests, listed below:
[  FAILED  ] ClassicAuthorMapTest.CapFallsBackToLinkIdxAndCounts
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid
 2 FAILED TESTS
```

### M6_no_memo

```
[1/3] Building CXX object src/server/CMakeFiles/dragonfly_lib.dir/classic_replay.cc.o
[2/3] Linking CXX static library lib/libdragonfly_lib.a
[3/3] Linking CXX executable classic_replay_test
Note: Google Test filter = ClassicAuthorMapTest.*
[==========] Running 6 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 6 tests from ClassicAuthorMapTest
/home/user/drakeydb/src/server/classic_replay_test.cc:1832: Failure
Expected equality of these values:
  HashesOf(a)
    Which is: { 7953565813638624897, 7953565813638624897, 7953565813638624897 }
  vector<uint64_t>(pp_->size(), 0)
    Which is: { 0, 0, 0 }
[  FAILED  ] ClassicAuthorMapTest.MemoizesPerLink (19 ms)
[----------] 6 tests from ClassicAuthorMapTest (123 ms total)
[----------] Global test environment tear-down
[==========] 6 tests from 1 test suite ran. (123 ms total)
[  PASSED  ] 5 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicAuthorMapTest.MemoizesPerLink
 1 FAILED TEST
```

### M5_no_uuid_memory

```
[3/4] Linking CXX static library lib/libdragonfly_lib.a
[4/4] Linking CXX executable drakeydb
FAILED 😰                                                                [100%]
E           AssertionError: {'host': 'localhost', 'port': 5555, 'link_status': 'up', 'last_io_seconds_ago': 1, ...}
E           assert 0 == 1
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_restart_consumes_one_author_slot[df_factory0]
====================== 1 failed, 174 deselected in 3.64s =======================
```

### M7_no_gauge_series

```
[3/4] Linking CXX static library lib/libdragonfly_lib.a
[4/4] Linking CXX executable drakeydb
FAILED 😰                                                                [100%]
E       KeyError: 'dragonfly_multimaster_peer_registry_size'
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_restart_consumes_one_author_slot[df_factory0]
====================== 1 failed, 174 deselected in 1.25s =======================
```

Restored sources, rebuilt, rerun:

```
$ ./classic_replay_test     -> [==========] 107 tests from 9 test suites ran.  [  PASSED  ] 107 tests.
$ ./multi_master_test       -> [==========] 230 tests from 18 test suites ran. [  PASSED  ] 229 tests.
                               [  SKIPPED ] NodeIdentityFile.UnwritableDirIsEphemeral (as before)
$ pytest keydb_onboarding_test.py -k author_slot -> 1 passed, 174 deselected
```

What each new test would still pass under with the feature removed (no feature = `IdxFor` always
returns the link idx and the registry/counter code is absent): none of the `ClassicAuthorMapTest`
cases passes then, except `LinkUuidMapsToPeerIdx` and `NullRegistryTurnsStampingOff`, which pin the
two no-op paths (they pass with the feature removed by construction; they would fail if the link
uuid or the null registry went through `AddOrGet`). `MemoizesPerLink` alone would pass under a map
that registered on every call if the hash were not wiped first; the wipe is what makes it bite (M6).
`ClassicProcessWideValues...` and `NoteClassicLinkUuidChange...` pass only with their functions.
The pytest passes only with the Greet bump and the gauge (M5, M7).

## Full-file pytest run

`keydb_onboarding_test.py` whole file, `KEYDB_REQUIRED=1`, pinned venv: **171 passed, 3 skipped,
1 failed** in 609 s. The failure is `test_keydb_active_live_write_during_full_sync[peer_mode]`
(`sync_in_progress` stays 1; the replica log says `Error while calling ec: Internal error when
loading RDB file 5` after 42 s of the peer full sync). **It fails identically on the unmodified
sources** (HEAD `replica.cc`/`classic_replay.*`/`metrics.*`/`server_family.cc`/`replica_types.h`
copied back, `dragonfly` rebuilt, same test: `1 failed`, same assertion), so it is not caused by this
task; it fails 4 times in 4 runs here, so it is not a flake. Not investigated further.

## Anchor drift

- `replica.cc`: the per-Greet clear is at `:478` (plan: `:436-437`; brief: `:478`), the peer admission
  `AddOrGet` at `:544`, the `AwaitBrief` registration at `:586-589`, `classic_master_ =` at `:678`.
- `server_family.cc`: `multimaster_lww_dropped` append at `:3180` (plan `:3138-3162` block);
  plain-replica loop at `:3190-3224`; `ConfigureMetrics` at `:1755`.
- `metrics.cc`: the replica-side branch and the classic totals loop are as Task 1.3 left them
  (`Print` at `:115`).
- `classic_replay_test.cc` had 99 gtests, not 34 (P7-1 and P7-0 added many since the Task 1.3 report).

## Open questions

1. **Gauge predicate** (note 5): `ActiveKeyDbMasterSeen() || Size() > 1`, not the brief's `|| value !=
   0`, because the value is never 0. Alternative: `ActiveKeyDbMasterSeen()` alone (then an active node
   of DFLY peers never shows it). Say which you want.
2. **Overflow counted per resolution**, not per distinct refused author (note 3). Once Task 2.2 calls
   `IdxFor` per envelope the counter is envelopes, not authors. A per-author count would need a second
   bounded set; I did not add one.
3. `ClassicAuthorMap` is process-wide-stateful through the cap set; a `ServerFamily`-owned set would
   remove the test reset hook, at the cost of a pointer through `Replica` into the map.
4. `test_keydb_active_live_write_during_full_sync[peer_mode]` fails on this docker/arm64 environment
   on unmodified HEAD (above).

## Fix round 1

Review: `task-2.1-review.md` (0 Critical, 2 Important, 6 Minor). Rulings: `task-2.1-fix1.md`.

| Finding | Change |
|---|---|
| I1 | `test_keydb_restart_consumes_one_author_slot` first forces a same-uuid reconnect (`CLIENT KILL TYPE replica` on the KeyDB, accepted by KeyDB; waits for `sync_full` to grow and the link to be up and not syncing), then asserts `node_uuid` unchanged, `classic_link_uuid_changes == 0`, registry size 2; then the two restarts as before. Docstring now lists exactly what is asserted and both falsifications. |
| I2 | `assert_no_classic_fields`: `multimaster_keydb_author_overflow` absent from INFO and `dragonfly_multimaster_keydb_author_overflow` from `/metrics` on every node; for a non-peer node the gauge is absent from both; for a peer-mode node with a master the gauge is pinned on purpose (INFO and `/metrics` `== 2`, R1). |
| m1, m2, m3, m5, m6 | Comment/help wording as the review suggests (`classic_replay.h` x2, `classic_replay.cc` help text, `replica.h`), and `drakeydb: P7-2 --` on the `replica.cc` Greet hunk. |
| m4 | `IdxFor` asks `registry_->FindIdx(uuid)` first; a uuid the registry already holds (another link's own, forwarded) skips `Admit`, takes its registry idx, and still registers its hash on every proactor. Stated in the `ClassicAuthorMap` class comment. New gtest `ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap` (cap 0: idx = its registry idx, no overflow, registry size unchanged, hash on every proactor, a stranger is still refused). |

### Falsifications (commands: mutate from `/tmp/good21b/`, `ninja` the target in `drakeydb-p7`, run, restore)

**m4** (`if (!known && !Admit(uuid))` -> `if (!Admit(uuid))`), `./classic_replay_test --gtest_filter='ClassicAuthorMapTest.*'`:

```
[3/3] Linking CXX executable classic_replay_test

/home/user/drakeydb/src/server/classic_replay_test.cc:1908: Failure
Expected equality of these values:
  HashesOf(other_link_idx)
    Which is: { 0, 0, 0 }
  vector<uint64_t>(pp_->size(), NodeUuidHash(kAuthorA))
    Which is: { 7953565813638624897, 7953565813638624897, 7953565813638624897 }

/home/user/drakeydb/src/server/classic_replay_test.cc:1911: Failure
Expected equality of these values:
  ClassicAuthorOverflowTotal() - overflow_before
    Which is: 2
  1u
    Which is: 1

[  FAILED  ] ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap (20 ms)
[----------] 7 tests from ClassicAuthorMapTest (160 ms total)

[----------] Global test environment tear-down
[==========] 7 tests from 1 test suite ran. (160 ms total)
[  PASSED  ] 6 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap

 1 FAILED TEST
```

**M8** (bump on any re-Greet: the `!=` dropped in `replica.cc`), pytest `-k author_slot`:

```
[4/4] Linking CXX executable drakeydb
FAILED 😰                                                                [100%]
E       AssertionError: {'host': 'localhost', 'port': 5555, 'link_status': 'up', 'last_io_seconds_ago': 0, ...}
E       assert 1 == 0
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_restart_consumes_one_author_slot[df_factory0]
====================== 1 failed, 174 deselected in 3.52s =======================
```

**M9** (`metrics.cc` passes `true` as `active_master_seen`), pytest `-k 'absent_for_stock_master or absent_for_keydb_that_is_not_active or no_classic_fields_between'` (`-x`, so the first failure only; the stock-master test is the one the ruling names):

```
[4/4] Linking CXX executable drakeydb
FAILED 😰                                                                [ 20%]
E       AssertionError: assert 'dragonfly_multimaster_keydb_author_overflow' not in {'dragonfly_version': Metric(dragonfly_version, , gauge, , [Sample(name='dragonfly_version', labels={'version': 'df-dev'}, value=1, timestamp=None, exemplar=None, native_histogram=None)]), 'dragonfly_master': Metric(dragonfly_master, 1 if master 0 if replica, gauge, , [Sample(name='dragonfly_master', labels={}, value=0, timestamp=None, exemplar=None, native_histogram=None)]), 'dragonfly_uptime_in_seconds': Metric(dragonfly_uptime_in_seconds, , counter, , [Sample(name='dragonfly_uptime_in_seconds_total', labels={}, value=0, timestamp=None, exemplar=None, native_histogram=None)]), 'dragonfly_max_clients': Metric(dragonfly_max_clients, Maximal number of clients, gauge, , [Sample(name='dragonfly_max_clients', labels={}, value=64000, timestamp=None, exemplar=None, native_histogram=None)]), ...}
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_info_and_metrics_absent_for_stock_master[df_factory0]
====================== 1 failed, 170 deselected in 2.86s =======================
```

Restored sources (`cmp` against `/tmp/good21b/`), rebuilt, rerun:

```
classic_replay_test   [  PASSED  ] 108 tests.        (107 + AnotherLinksOwnUuidDoesNotConsumeTheCap)
multi_master_test     [  PASSED  ] 229 tests.        (1 skipped, NodeIdentityFile.UnwritableDirIsEphemeral, as before)
pytest -k "author_slot or exact_classic_counters or absent_for or no_classic_fields_between or
  down_link_keeps_zero or series_outlive or show_classic_counters or raw_keydb_only_command"
                      17 passed, 158 deselected in 24.04s
pre-commit            all hooks Passed on the changed files
```

### Tests that pass with the feature removed

- I1: before this round `test_keydb_restart_consumes_one_author_slot` passed with the uuid-equality
  check removed (M8); it now fails on `classic_link_uuid_changes == 0` after the same-uuid reconnect.
- I2: the six absence tests passed with the `/metrics` predicate forced true (M9); the stock-master
  one now fails on the overflow series. The peer-mode gauge pin (`== 2`) is covered by the
  restart test and the peer-mode absence cases, not by a separate falsification.
- Still pass by construction: `LinkUuidMapsToPeerIdx`, `NullRegistryTurnsStampingOff`.

Final counts: `classic_replay_test` 108 (was 107), `multi_master_test` 230 (229 pass, 1 skipped),
`keydb_onboarding_test.py` still 175 collected; the reviewer's selection 17 passed.
