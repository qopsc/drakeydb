# Task 2.1 review: `ClassicAuthorMap`, the author cap, the uuid-change counter, the registry gauge

Reviewer: Opus 5.5. BASE `0f79bf5a`, diff `git diff 0f79bf5a -- src tests` (11 files, +501/-16), read
hunk by hunk. Sources restored after every probe (`cmp` against `/tmp/rev21/` copies; final diff stat
identical to the report's).

## Verdict: **Needs fixes** (0 Critical, 2 Important, 6 Minor)

The production code does what the task section and spec D-4/D-13 ask, and R1–R3 are implemented
as ruled. The two Important findings are test gaps: a falsification the new pytest's docstring
claims but does not deliver, and the stock-master `/metrics` byte-identity of the two new series,
which nothing pins. Both are test-only fixes.

## Findings

| ID | Sev | file:line | What | Why | Suggested fix |
|---|---|---|---|---|---|
| I1 | Important | `tests/dragonfly/keydb_onboarding_test.py:2684` | The docstring of `test_keydb_restart_consumes_one_author_slot` says "counting a reconnect with the same uuid would make it 1 before the first restart". It doesn't: nothing reconnects before the first assertion. Mutant M8 below (bump on *any* re-Greet, the `!=` dropped) **passes** the test. | The no-false-bump path (probe c: a re-Greet to the same KeyDB, including a failed Greet then a successful one) is the half of the counter most likely to regress, and no test covers it. The docstring overstates what the test checks. | Before the first restart, force a reconnect with the same uuid (on the KeyDB, `CLIENT KILL TYPE slave`, or drop the link from the drakeydb side), wait until it re-attaches with the same `node_uuid`, then assert `classic_link_uuid_changes == 0` and `size == 2`. Otherwise, delete the claim. |
| I2 | Important | `tests/dragonfly/keydb_onboarding_test.py:2567`, `:3010` | `classic_series`/`assert_no_classic_fields` look only at `CLASSIC_COUNTERS`. In `/metrics`, nothing checks that `multimaster_keydb_author_overflow_total` and `multimaster_peer_registry_size` are absent for a stock master. Mutant M9 below (`metrics.cc` predicate forced `true`) emits both series on a stock node, and `test_info_and_metrics_absent_for_stock_master` plus the 5 other absence tests **pass**. | Spec D-13: with a stock Redis master, `/metrics` is byte-for-byte upstream's. The gtest pins `ClassicProcessWideValues`, but not the wiring in `metrics.cc`. (INFO is pinned by accident: `"keydb"` in the overflow name and the `list(link)[-1] == "psync_successes"` check.) | In `assert_no_classic_fields`, for a non-peer node, assert that both names are absent from INFO and that `dragonfly_multimaster_keydb_author_overflow` / `dragonfly_multimaster_peer_registry_size` are absent from `node.metrics()`. In the peer-mode / DFLY-peer absence tests, pin R1's gauge explicitly (`== 2`, no overflow series), so the R1 behaviour is also tested on purpose. |
| m1 | Minor | `src/server/classic_replay.h:181` | The comment on `ClassicAuthorOverflowTotal()` opens with "The forwarded authors counted against --classic_author_cap so far, and …". The function returns only the overflow count. | The comment describes a value that isn't returned. | "How many resolutions of a forwarded author --classic_author_cap refused (multimaster_keydb_author_overflow)." |
| m2 | Minor | `src/server/classic_replay.h:109-111` | The comment on `ClassicLinkStats::classic_link_uuid_changes` says the new uuid "takes one more PeerRegistry slot … (ClassicAuthorMap)". | The slot is taken by `Greet()`'s peer admission (`replica.cc:544`), not by `ClassicAuthorMap`, and only in peer mode. A plain replica of a restarting KeyDB bumps the counter and takes no slot. | "…a new author; on a peer link, Greet()'s admission registers it, one more PeerRegistry slot for the life of the process." |
| m3 | Minor | `src/server/classic_replay.cc:116` | The help text says "Classic links whose master presented a new uuid …", but the value counts events (handshakes), not links. `_total` is a sum of events too. | Help text that describes the wrong unit. | "Completed handshakes of a classic link whose master presented a different uuid than the previous one (a KeyDB restart)." |
| m4 | Minor | `src/server/classic_replay.cc:138` (`Admit`) | Spec D-4 says "a link's *own* uuid never counts against [the cap]". `IdxFor` exempts only *this* link's uuid. In a KeyDB mesh where A and B are both linked to this node, A forwards B's writes, so B's uuid is admitted against the cap through A, even though it is a link's own uuid and costs no new registry slot. | Conservative (the cap fills slightly faster than the registry grows), harmless at 4096, but it differs from the spec's wording. `CapIgnoresLinkUuid` covers only the same-link case. | Document it in the `ClassicAuthorMap` comment ("another link's own uuid, forwarded, is counted"), or skip `Admit` when `registry_->FindIdx(uuid)` already has it (the set keeps the race harmless). The owner decides; this is not a blocker. |
| m5 | Minor | `src/server/replica.h:329` | "Greet() clears master_context_.master_node_uuid on entry". | The clear happens mid-Greet, right before `REPLCONF UUID` (`replica.cc:478`), after the PING/capa exchange. A Greet that fails earlier leaves the uuid in place. | "…clears it before the REPLCONF UUID exchange…" |
| m6 | Minor | `src/server/replica.cc:683` | The new Greet hunk in this upstream-owned file has no `drakeydb: P7-2 --` marker. Every other new hunk in upstream files (`replica.h`, `metrics.{h,cc}`, `server_family.cc`) has one. | Convention for keeping upstream syncs reviewable. | Prefix the comment with `drakeydb: P7-2 --`. |

## Probes (a)–(e)

- **(a) `ForwardedAuthors` lock scope.** `Admit` holds the `fb2::Mutex` for a `contains`/`size`/
  `GetFlag`/`emplace` only, and never yields. `IdxFor` calls `PeerRegistry::AddOrGet`, which has its
  own mutex, released before its `RunBlockingInParallel` ORIGIN fan-out. It then calls `AwaitBrief`
  and `LOG_EVERY_T`, all **after** `Admit` returns. No fiber-blocking call happens under either
  mutex. OK.
- **(b) Second link vs. a first link's in-flight registration.** `AddOrGet` publishes the idx under
  its lock *before* its ORIGIN fan-out, so a second link can get the idx while the first link is
  still inside `AddOrGet`, or inside its `AwaitBrief`. The second link then runs its own
  `AwaitBrief`, and only after that returns does its `IdxFor` return (the memo is per map,
  `MemoizesPerLink` pins the re-registration). So every proactor has the hash before anything the
  second link stamps. Both writers store the same value (`RegisterOriginHash` is a plain
  thread-local store, run on-thread). `IncomingStamp`'s `DCHECK(hash != 0)` cannot fire from this
  race. OK.
- **(c) `last_greeted_master_uuid_`.**
  - **Failed Greet, then success to the same master.** Every failure path returns before
    `replica.cc:683`. The stored uuid is neither bumped nor overwritten, and the next success with
    the same uuid compares equal, so there is no bump. Correct by inspection, but **untested** (I1).
  - **`REPLICAOF` to a different classic master.** `ServerFamily::ReplicaOfInternal` always builds
    a new `Replica` (`server_family.cc:3699`; `REPLICAOF ADD` likewise, `peer_replication.cc:202`),
    so the stored uuid starts empty and nothing is bumped. The per-link counter therefore counts
    only re-Greets of the same `host:port`, and "a KeyDB restart" is honest. A different server
    answering at the same address (VIP/DNS failover) would also count, which the comment does not
    say; not worth a finding.
- **(d) `Metrics::Print` signature.** One caller (`server_family.cc:1764`). With `--active_replica`
  off, `peer_mode_` is null, so the map never touches the registry or the cap set, and
  `Size() == 1`. `ActiveKeyDbMasterSeen()` is false and the overflow is 0, so neither INFO nor
  `/metrics` changes. Restored build: `rdb_test` 138 passed (it includes
  `RdbTest.NoMvccOpcodeOrAuxWhenInactive`), `server_family_test` 44 passed, and the absence/exact
  pytests pass (below). The `/metrics` half is correct but unpinned (I2).
- **(e) Pytest `2 + restarts`.** The node itself plus the KeyDB's peer-admission slot gives 2,
  asserted before the first restart (`assert size == 2`). Each restart adds the new uuid through
  `Greet()`'s `AddOrGet`, and the old uuid stays (append-only). Correct. Only peer mode is
  exercised; a plain replica's bump (counter only, no slot) has no pytest.

R1 (`ActiveKeyDbMasterSeen() || Size() > 1`), R2 (overflow counted on every resolution, never
memoized) and R3 (process-wide set plus `ResetClassicForwardedAuthorsForTest`) are implemented as
stated (`classic_replay.cc:284-299`, `:314-322`, `:306-308`). One consequence of R1: a peer-mode node
attached to a non-active KeyDB or to DFLY peers now shows `multimaster_peer_registry_size:2` in
INFO and `/metrics`. That is accepted, but no test pins it on purpose (see I2's fix).

`test_keydb_active_live_write_during_full_sync[peer_mode]`: untouched. The diff changes nothing on
the full-sync/RDB-load path. The Greet hunk runs after the handshake, and the `ConsumeRedisStream`
hunk runs only after the full sync completes, which is past where the test fails (RDB load error).

## Spec axis

- The interfaces match the plan: `IdxFor(string_view)`; the link uuid maps to `peer_origin_idx_`
  and is never counted; a forwarded uuid goes through `AddOrGet` plus `AwaitBrief`
  `RegisterOriginHash(idx, NodeUuidHash(uuid))`; the memo is per link; `--classic_author_cap` is
  4096 in `classic_replay.cc`; overflow returns the link idx, never `kSelfIdx`, with the counter
  and `LOG_EVERY_T(WARNING, 60)`; a null registry means stamping is off.
- Both uuids are normalized (`ParseReplconfUuidReply` and the envelope parser both use
  `NormalizeNodeUuid`), so `uuid == link_uuid_` is sound.
- Not done, and correctly deferred: "the envelope mvcc kept" (Step 1 of `CapFallsBackToLinkIdxAndCounts`)
  cannot be observed until Task 2.2 calls `IdxFor`; the report says so.
- Added beyond the brief: `ClassicCounterValue::gauge`, `ClassicApplier::authors()`, and
  `NoteClassicLinkUuidChange` (needed and small). `classic_link_uuid_changes` gated on
  `classic_master_` (report note 7) is a sound reading of "classic link".
- `multi_master.cc` is unchanged; the peer line renders from `kClassicCounters` (pins updated in
  `multi_master_test`).

## Quality axis

- `util::fb2` only. clang-format 14 `--dry-run -Werror` is clean on all 10 C++ files, and `black
  --check` is clean on the pytest.
- No process text in `src/` or `tests/`. The `P7-2` markers follow the repo convention (one is
  missing, m6).
- The upstream-owned hunks are minimal: `metrics.{h,cc}` (one parameter, one loop),
  `server_family.cc` (two loops, one argument), `replica.{h,cc}` (one member, one block, one
  constructor argument).

## Falsifications reproduced

All four mutants of the report and of this review are restored, and the final runs were on the
restored sources. Commands:
`docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb && ninja -C build-dbg -j4 <target> && …'`.

**Report M2 (cap check removed in `Admit`)**: fails as reported.

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1890: Failure
Expected equality of these values:
  ClassicAuthorOverflowTotal() - overflow_before
    Which is: 0
  1u
    Which is: 1
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid (18 ms)
[ RUN      ] ClassicAuthorMapTest.NullRegistryTurnsStampingOff
[       OK ] ClassicAuthorMapTest.NullRegistryTurnsStampingOff (19 ms)
[==========] 6 tests from 1 test suite ran. (123 ms total)
[  PASSED  ] 4 tests.
[  FAILED  ] 2 tests, listed below:
[  FAILED  ] ClassicAuthorMapTest.CapFallsBackToLinkIdxAndCounts
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid
 2 FAILED TESTS
```

**Report M4 (overflow returns `PeerRegistry::kSelfIdx`)**: fails as reported.

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1858: Failure
Expected: (map.IdxFor(kAuthorC)) != (PeerRegistry::kSelfIdx), actual: 0 vs 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1868: Failure
Expected equality of these values:
  other.IdxFor(kAuthorD)
    Which is: 0
  node.link_idx
    Which is: 1
[  FAILED  ] ClassicAuthorMapTest.CapFallsBackToLinkIdxAndCounts (22 ms)
[ RUN      ] ClassicAuthorMapTest.CapIgnoresLinkUuid
/home/user/drakeydb/src/server/classic_replay_test.cc:1889: Failure
Expected equality of these values:
  map.IdxFor(kAuthorB)
    Which is: 0
  node.link_idx
    Which is: 1
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid (19 ms)
[  PASSED  ] 4 tests.
[  FAILED  ] 2 tests, listed below:
[  FAILED  ] ClassicAuthorMapTest.CapFallsBackToLinkIdxAndCounts
[  FAILED  ] ClassicAuthorMapTest.CapIgnoresLinkUuid
 2 FAILED TESTS
```

**Report M5 (`last_greeted_master_uuid_` never stored)**: fails as reported.

```
[4/4] Linking CXX executable drakeydb
FAILED 😰                                                                [100%]
E           AssertionError: {'host': 'localhost', 'port': 5555, 'link_status': 'up', 'last_io_seconds_ago': 1, ...}
E           assert 0 == 1
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_restart_consumes_one_author_slot[df_factory0]
====================== 1 failed, 174 deselected in 3.19s =======================
```

**Review M8 + M9, one build.** M8: `replica.cc` bumps on any re-Greet (`if
(!last_greeted_master_uuid_.empty())`, the `!=` dropped). M9: `metrics.cc` calls
`ClassicProcessWideValues(…, peer_registry_size, true)`. Neither mutant can affect the other's test
(a stock Redis master sends no uuid). Run with `-k "author_slot or absent_for_stock_master or
absent_for_keydb_that_is_not_active or no_classic_fields_between_dfly"`: **everything passes
under both mutants** (I1, I2).

```
[5/5] Linking CXX executable drakeydb
PASSED 😃                                                                [100%]
====================== 6 passed, 169 deselected in 14.06s ======================
```

M9 does change the output: a stock, non-active node built with the mutant serves

```
# HELP dragonfly_multimaster_keydb_author_overflow_total Resolutions of an RREPLAY author forwarded by a KeyDB that --classic_author_cap refused; stamped as the forwarding link's.
# TYPE dragonfly_multimaster_keydb_author_overflow_total counter
dragonfly_multimaster_keydb_author_overflow_total 0
# HELP dragonfly_multimaster_peer_registry_size Nodes the append-only peer registry holds, this one included.
# TYPE dragonfly_multimaster_peer_registry_size gauge
dragonfly_multimaster_peer_registry_size 1
```

**Restored sources:**

```
classic_replay_test  [  PASSED  ] 107 tests.
multi_master_test    [  PASSED  ] 229 tests.   (1 skipped, NodeIdentityFile.UnwritableDirIsEphemeral, as before)
rdb_test             [  PASSED  ] 138 tests.
server_family_test   [  PASSED  ] 44 tests.
pytest -k "author_slot or exact_classic_counters or absent_for or no_classic_fields_between or
  down_link_keeps_zero or series_outlive or show_classic_counters or raw_keydb_only_command"
                     17 passed, 158 deselected in 26.91s
```

**Tests that pass with the feature removed.** The report correctly names `LinkUuidMapsToPeerIdx`
and `NullRegistryTurnsStampingOff`: both pass by construction. In addition, the absence pytests
pass with the predicate removed from the `/metrics` wiring (I2), and the restart pytest passes with
the uuid-equality check removed (I1). The report doesn't mention either.

## For the next task (2.2)

- When `ClassicApplier` starts calling `authors().IdxFor(env.uuid)`, update the comment
  "Nothing calls it for a command yet." (`classic_replay.h:261`).
- Call `IdxFor` only after the self-uuid drop. `IdxFor(self_uuid)` would `AddOrGet` itself to
  `kSelfIdx` and stamp a foreign envelope as self. Worth a `DCHECK` in `IdxFor`, or a test.
- Pin "the envelope's mvcc is kept" for an over-cap author (deferred from this task's Step 1).
- R2 wording: the overflow counter becomes "envelopes stamped as the link because of the cap".
- `IdxFor` blocks the replication fiber for one `AddOrGet` ORIGIN fan-out plus one `AwaitBrief`
  per new author per link. That is fine per author, but keep it out of any non-preemptible
  context.

## Re-review (fix round 1)

Scope: `task-2.1-fix1.md` rulings, the report's "## Fix round 1", current `git diff 0f79bf5a -- src
tests` (11 files, +564/-17). Sources restored after every probe (`cmp` against `/tmp/rev21b/`).

### Verdict: **Clean**

Every finding is addressed as ruled. The fix introduces nothing new at Critical or Important level.

| Finding | Status |
|---|---|
| I1 | Fixed. Before the restarts, the pytest now runs `CLIENT KILL TYPE replica` on the KeyDB, then waits for KeyDB's `sync_full` to grow and for the link to be up and no longer syncing. It then asserts that `node_uuid` is unchanged, `classic_link_uuid_changes == 0` and the registry size is 2. The docstring now lists exactly those assertions. Under M8 the test fails (below). |
| I2 | Fixed. `assert_no_classic_fields` checks that the overflow series is absent from INFO and `/metrics` on every node. On a non-peer node the gauge must also be absent from both. On a peer-mode node with a master the gauge is pinned at `== 2` in INFO and `/metrics` (R1). The report's M9 output shows the stock-master test now failing. I did not re-run M9; the ruling asked for M8 and m4. |
| m1 | Fixed: `classic_replay.h:181-182` uses the suggested wording. |
| m2 | Fixed: `classic_replay.h:109-112`. The slot is now attributed to `Greet()`'s admission, on a peer link only. The sentence reads a little awkwardly ("…a new author and, on a peer link, Greet()'s admission registers it: …") but is accurate. |
| m3 | Fixed: the help text is now "Completed handshakes of a classic link whose master presented a different uuid than the previous one (a KeyDB restart)." |
| m4 | Fixed as ruled. `IdxFor` asks `registry_->FindIdx` first, and a uuid the registry already holds skips `Admit`. The class comment says so. New test: `ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap`. |
| m5 | Fixed: `replica.h:329` now says the uuid is cleared "before the REPLCONF UUID exchange". |
| m6 | Fixed: the `replica.cc:683` hunk now starts with `drakeydb: P7-2 --`. |

clang-format 14 `--dry-run -Werror` is clean on the 5 changed C++ files, and black is clean. No
process text appears in `src/` or `tests/`.

### The FindIdx short-circuit

- **Hash registration on the known path.** A uuid can be in the registry before its hash is on
  every proactor: another link's `Greet()` publishes the idx inside `AddOrGet` before its own
  `AwaitBrief` runs. The known path does not rely on that other registration. It still runs its own
  `AwaitBrief(RegisterOriginHash(idx, NodeUuidHash(uuid)))` before `IdxFor` returns, so
  `IncomingStamp`'s `DCHECK(hash != 0)` holds for everything stamped afterwards. The value written
  is the same one `Greet()` writes, and the store is idempotent.
  - I tested this with a mutant whose known path returns before `AwaitBrief`. It is caught by 2
    tests (`MemoizesPerLink` and `AnotherLinksOwnUuidDoesNotConsumeTheCap`), so the registration is
    pinned.
- **What "known" covers.** It is any uuid ever registered: live links' own uuids, the old uuids of
  restarted KeyDBs, and forwarded authors admitted earlier. None of these costs a new slot, so
  skipping the cap is correct for all of them.
- **One edge case, a note rather than a finding.** This node's own uuid is also known, at
  `kSelfIdx`. With the fix, `IdxFor(self_uuid)` returns `kSelfIdx` whatever the cap. Before the fix
  it returned `kSelfIdx` too, except when the cap refused it, in which case it got the link idx.
  `ClassicApplier` drops self-authored envelopes before any stamping, so this can't be reached
  today. Task 2.2 should still keep `IdxFor` behind that drop, and should consider
  `if (known == kSelfIdx) return link_idx_;` or a `DCHECK`.
- **Pre-existing, not introduced by the fix.** A second caller can stamp with an idx while the
  first caller's `AddOrGet` is still writing the `Op::ORIGIN` entries to the shards. The short-circuit
  does not widen this window: before the fix, a second caller's `AddOrGet` also returned at once for
  an existing entry. It only matters for plain replicas downstream that have to resolve the
  `origin_idx`.

### Falsifications reproduced

**m4** (`if (!known && !GetForwardedAuthors().Admit(uuid))` → `if (!GetForwardedAuthors().Admit(uuid))`):

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
[  FAILED  ] ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap (25 ms)
[==========] 7 tests from 1 test suite ran. (209 ms total)
[  PASSED  ] 6 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap
 1 FAILED TEST
```

**Hash-skip on the known path** (this review's own mutant: `if (known) { resolved_.emplace(uuid, *known); return *known; }` placed before the `AwaitBrief`):

```
[3/3] Linking CXX executable classic_replay_test
  vector<uint64_t>(pp_->size(), NodeUuidHash(kAuthorA))
    Which is: { 7953565813638624897, 7953565813638624897, 7953565813638624897 }
[  FAILED  ] ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap (21 ms)
[==========] 7 tests from 1 test suite ran. (146 ms total)
[  PASSED  ] 5 tests.
[  FAILED  ] 2 tests, listed below:
[  FAILED  ] ClassicAuthorMapTest.MemoizesPerLink
[  FAILED  ] ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap
 2 FAILED TESTS
```

**M8** (bump on any re-Greet, the `!=` dropped in `replica.cc`), pytest `-k author_slot`:

```
[5/5] Linking CXX executable drakeydb
E       AssertionError: {'host': 'localhost', 'port': 5555, 'link_status': 'up', 'last_io_seconds_ago': 0, ...}
E       assert 1 == 0
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_restart_consumes_one_author_slot[df_factory0]
====================== 1 failed, 174 deselected in 3.04s =======================
```

### Flakiness of the CLIENT KILL reconnect, and the restored runs

```
pytest -k author_slot --count=5      5 passed, 870 deselected in 27.61s
classic_replay_test                  [  PASSED  ] 108 tests.
multi_master_test                    [  PASSED  ] 229 tests.   (1 skipped, NodeIdentityFile.UnwritableDirIsEphemeral)
pytest -k "author_slot or exact_classic_counters or absent_for or no_classic_fields_between or
  down_link_keeps_zero or series_outlive or show_classic_counters or raw_keydb_only_command"
                                     17 passed, 158 deselected in 26.71s
```

The wait in the same-uuid reconnect does not race. It waits for KeyDB's `sync_full` to grow, which
happens only once the new connection's full sync has started, and then for the link to be up and no
longer syncing. A stale "up" from before the kill can't satisfy it.

### For later tasks

- **P7-3 (`--classic_partial_psync`).** If a classic reconnect resumes with `+CONTINUE`, KeyDB's
  `sync_full` will not grow. `reattached_same_uuid` would then time out, unless the flag is off by
  default for this test or the wait also accepts a partial resync (`sync_partial_ok`, or the link's
  `classic_psync_partial_ok`).
- **Task 2.2.** Call `IdxFor` only after the self drop, and consider guarding `kSelfIdx` (see the
  short-circuit section above).
