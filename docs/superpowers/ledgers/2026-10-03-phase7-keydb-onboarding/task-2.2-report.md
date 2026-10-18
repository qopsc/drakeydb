# Task 2.2 report: per-command stamps on peer links

Branch `feat/phase7-2-author-stamps-dedup-guard`, on top of Task 2.1 (`1dd8d035`). Not committed.
Implementer: Sonnet 5.5. Plan: Task 2.2; spec D-4 (point 1, the restore in 3), D-13.

## Summary

- A peer-mode link's dispatched command now runs with `repl_mvcc = <innermost envelope's mvcc>` and
  `repl_origin_idx = authors().IdxFor(<innermost envelope's uuid>)`; both are put back on every exit.
  A plain link (no registry) touches nothing but the restore, which is a no-op for it.
- **Peer mode = `ClassicAuthorMap::stamping()`** (`registry_ != nullptr`). Equivalent in production:
  `Replica::ConsumeRedisStream` builds the map with `peer_mode_ ? peer_mode_->registry : nullptr`.
  No second flag; the plain gtest pins it (`authors().stamping()` false, mvcc zero, local mint).
- Author and mvcc travel as arguments: `ApplyCommand(args, env.uuid, env.mvcc)`. At the call site
  `env` is the innermost layer's (the one whose inner is not an RREPLAY), so no member is written
  per layer.
- The scope is `CommandScope` (anonymous namespace, `classic_replay.cc`): constructor takes the
  context and the link's origin idx; `Stamp(mvcc, origin_idx)` sets; the destructor restores
  `repl_mvcc = 0`, `repl_origin_idx = link idx`. Tasks 2.4 (guard override) and 2.8 (`repl_time_ms`)
  add a field to `Stamp` or a sibling setter and one line to the destructor.
- Review items honoured: `IdxFor` is called only for a command that is dispatched (after the control
  / KeyDB-only / unknown exits) and after `HandleRreplay` dropped self-authored layers;
  `IdxFor` returns the link idx for this node's own uuid (`known == kSelfIdx`), never `kSelfIdx`;
  an over-cap author keeps the envelope mvcc and takes the link origin (pinned); the comment
  "Nothing calls it for a command yet" is replaced; the overflow help text now says "Envelopes ...
  one per dispatched command".

## What changed

| File | Change |
|---|---|
| `src/server/classic_replay.h` | `ClassicAuthorMap::stamping()`, `IdxFor` comment (never self); `ClassicApplier` comment, `ApplyCommand(args, author, mvcc)`, member `link_origin_idx_`. |
| `src/server/classic_replay.cc` | `CommandScope`; `ApplyCommand` stamps before `Dispatch`; `HandleRreplay` passes `env.uuid`/`env.mvcc`; `link_origin_idx_(cntx->repl_origin_idx)` read at construction; `IdxFor` self guard; overflow help text. |
| `src/server/classic_replay_test.cc` | 6 new tests; `ClassicApplyFamilyTest` gains `SetUp`/`TearDown` (peer node for the stamp tests only), `PeerLink`, `PeerNode`, `OnPeerLink`, `StampOf`. |
| `tests/dragonfly/keydb_onboarding_test.py` | `mvcc_origin` helper, `test_keydb_write_stamped_with_author_hash`. |

`replica.cc` needs no change: the applier reads the link idx `ConsumeRedisStream` already set on the
context before building the applier (`:1217`, applier at `:1226`).

## Design notes

1. **The set is before `Dispatch`, not at the top.** Setting before the early exits would call
   `IdxFor` (registry slot, cap, overflow count) for PINGs and unknown commands. The scope object is
   created after the control / KeyDB-only return, so every exit after it restores; for the unknown
   exit nothing was set, so the restore there is trivially true. The restore is therefore falsifiable on the
   dispatched, error-reply and rejected-dispatch exits only (F3, F4); the early exits are covered
   by the same test but no mutant can fail on them alone.
2. **`mvcc == 0`** (a 3-argument envelope, no mvcc): the author's idx is still set and `repl_mvcc`
   is 0, so the write is a local mint attributed to the author's origin. Spec D-4 is silent. Open
   question 1.
3. **Overflow counter** counts per dispatched command (one `IdxFor` each, not memoized for an
   over-cap author), now said in the help text.
4. **Test fixture.** Stamp tests need `--active_replica` (boot-only, side table). Setting it for the
   whole `ClassicApplyFamilyTest` broke `DflyFlowOfALiveSessionIsAnErrorNotACrash` (an active node
   answers `REPLCONF capa dragonfly` differently), so `NeedsPeerNode()` enables it by test name
   (`Peer*` plus three names). A name list is crude; the alternative is a second fixture, which would
   not carry the suite name the brief fixes.

## Tests

gtest `classic_replay_test`: 108 -> 114 (all pass); `multi_master_test` 230 (229 pass, 1 skipped as
before). Pytest `keydb_onboarding_test.py`: 175 -> 176 collected.

- `ClassicApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash`: two authors, three
  keys: stamp == `{envelope mvcc, NodeUuidHash(author)}`, differs from the link's hash, registry 4.
- `.PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin` (cap 0): mvcc kept, origin hash
  is the link's, no slot taken, overflow +1.
- `.NestedEnvelopeStampsWithInnerAuthor`: outer B (mvcc B), inner A (mvcc A): stamp is A's; B never
  enters the registry (size 3).
- `.PlainApplierLeavesMvccZero`: `stamping()` false; `repl_mvcc` 0, origin 0 after; stamp is a local
  mint (mvcc != envelope's, origin == a client write's).
- `.RestoresRawContextAfterEnvelope`: after a dispatched write, an error reply, a rejected
  dispatch, a PING, a KeyDB-only command and an unknown command: `repl_mvcc == 0`,
  `repl_origin_idx == link idx`; counters 2 errors / 1 keydb / 1 unknown; a following envelope is
  stamped by its own author.
- `ClassicAuthorMapTest.SelfUuidIsNeverResolvedToSelfIdx`: `IdxFor(self)` is the link idx, no
  overflow, registry unchanged.
- pytest `test_keydb_write_stamped_with_author_hash`: KeyDB B written, KeyDB A replicates from B and
  forwards, a peer-mode drakeydb attached to A: B's two keys share one origin (nested), A's two keys
  share another, they differ, and the node's own key's differs from both.

## Falsification (verbatim output lines; commands: mutate `classic_replay.cc` from `/tmp/good22/`,
`ninja -C build-dbg -j4 classic_replay_test`, `./classic_replay_test --gtest_filter='ClassicApplyFamilyTest.*:ClassicAuthorMapTest.*'`, restore)

### F1_no_repl_mvcc

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1283: Failure
Expected equality of these values:
  a1->Mvcc()
    Which is: 1879346236853583872
  kMvccA
    Which is: 1782579200000000005
/home/user/drakeydb/src/server/classic_replay_test.cc:1285: Failure
Expected equality of these values:
  a2->Mvcc()
    Which is: 1879346236853583873
  kMvccB
    Which is: 1782579200104857609
/home/user/drakeydb/src/server/classic_replay_test.cc:1287: Failure
Expected equality of these values:
  b1->Mvcc()
    Which is: 1879346236853583872
  kMvccB
    Which is: 1782579200104857609
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash (23 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1305: Failure
Expected equality of these values:
  stamp->Mvcc()
    Which is: 1879346236877701120
  kMvccA
    Which is: 1782579200000000005
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin (19 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1325: Failure
Expected equality of these values:
  stamp->Mvcc()
    Which is: 1879346236899721216
  kMvccA
    Which is: 1782579200000000005
[  FAILED  ] ClassicApplyFamilyTest.NestedEnvelopeStampsWithInnerAuthor (22 ms)
[  PASSED  ] 36 tests.
[  FAILED  ] 3 tests, listed below:
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin
[  FAILED  ] ClassicApplyFamilyTest.NestedEnvelopeStampsWithInnerAuthor
 3 FAILED TESTS
```

### F2_link_origin

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1284: Failure
Expected equality of these values:
  a1->origin_hash
    Which is: 14262392837314142136
  hash_a
    Which is: 7953565813638624897
/home/user/drakeydb/src/server/classic_replay_test.cc:1286: Failure
Expected equality of these values:
  a2->origin_hash
    Which is: 14262392837314142136
  hash_a
    Which is: 7953565813638624897
/home/user/drakeydb/src/server/classic_replay_test.cc:1288: Failure
Expected equality of these values:
  b1->origin_hash
    Which is: 14262392837314142136
  hash_b
    Which is: 16571850207653206743
/home/user/drakeydb/src/server/classic_replay_test.cc:1291: Failure
Expected equality of these values:
  node.registry.Size()
    Which is: 2
    Which is: 4
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash (22 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1308: Failure
Expected equality of these values:
  ClassicAuthorOverflowTotal() - overflow_before
    Which is: 0
    Which is: 1
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin (22 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1326: Failure
Expected equality of these values:
  stamp->origin_hash
    Which is: 14262392837314142136
  NodeUuidHash(kAuthorA)
    Which is: 7953565813638624897
/home/user/drakeydb/src/server/classic_replay_test.cc:1327: Failure
Expected equality of these values:
  node.registry.Size()
    Which is: 2
    Which is: 3
[  FAILED  ] ClassicApplyFamilyTest.NestedEnvelopeStampsWithInnerAuthor (26 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:1382: Failure
Expected equality of these values:
```

### F3_no_restore_origin

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1357: Failure
Expected equality of these values:
  link.cntx.repl_origin_idx
    Which is: 2
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1357: Failure
Expected equality of these values:
  link.cntx.repl_origin_idx
    Which is: 2
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1357: Failure
Expected equality of these values:
  link.cntx.repl_origin_idx
    Which is: 2
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1357: Failure
Expected equality of these values:
  link.cntx.repl_origin_idx
    Which is: 2
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1357: Failure
Expected equality of these values:
  link.cntx.repl_origin_idx
    Which is: 2
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1357: Failure
Expected equality of these values:
  link.cntx.repl_origin_idx
    Which is: 2
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:1357: Failure
Expected equality of these values:
  link.cntx.repl_origin_idx
    Which is: 3
  node.link_idx
    Which is: 1
[  FAILED  ] ClassicApplyFamilyTest.RestoresRawContextAfterEnvelope (20 ms)
[  PASSED  ] 38 tests.
```

### F4_no_restore_mvcc

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1356: Failure
Expected equality of these values:
  link.cntx.repl_mvcc
    Which is: 1782579200000000005
    Which is: 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1356: Failure
Expected equality of these values:
  link.cntx.repl_mvcc
    Which is: 1782579200000000005
    Which is: 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1356: Failure
Expected equality of these values:
  link.cntx.repl_mvcc
    Which is: 1782579200000000005
    Which is: 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1356: Failure
Expected equality of these values:
  link.cntx.repl_mvcc
    Which is: 1782579200000000005
    Which is: 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1356: Failure
Expected equality of these values:
  link.cntx.repl_mvcc
    Which is: 1782579200000000005
    Which is: 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1356: Failure
Expected equality of these values:
  link.cntx.repl_mvcc
    Which is: 1782579200000000005
    Which is: 0
/home/user/drakeydb/src/server/classic_replay_test.cc:1356: Failure
Expected equality of these values:
  link.cntx.repl_mvcc
    Which is: 1782579200104857609
    Which is: 0
[  FAILED  ] ClassicApplyFamilyTest.RestoresRawContextAfterEnvelope (20 ms)
[  PASSED  ] 38 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicApplyFamilyTest.RestoresRawContextAfterEnvelope
 1 FAILED TEST
```

### F5_self_unguarded

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:2049: Failure
Expected equality of these values:
  map.IdxFor(kSelfUuid)
    Which is: 0
  node.link_idx
    Which is: 1
/home/user/drakeydb/src/server/classic_replay_test.cc:2050: Failure
Expected: (map.IdxFor(kSelfUuid)) != (PeerRegistry::kSelfIdx), actual: 0 vs 0
[  FAILED  ] ClassicAuthorMapTest.SelfUuidIsNeverResolvedToSelfIdx (19 ms)
[  PASSED  ] 38 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicAuthorMapTest.SelfUuidIsNeverResolvedToSelfIdx
 1 FAILED TEST
```

### F6_stamp_plain

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1346: Failure
Expected: (stamp->Mvcc()) != (kMvccA), actual: 1782579200000000005 vs 1782579200000000005
[  FAILED  ] ClassicApplyFamilyTest.PlainApplierLeavesMvccZero (19 ms)
[  PASSED  ] 38 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicApplyFamilyTest.PlainApplierLeavesMvccZero
 1 FAILED TEST
```

### F2 on the pytest (`dragonfly` rebuilt, `-k stamped_with_author_hash`)

```
[3/3] Linking CXX executable drakeydb
FAILED 😰                                                                [100%]
[2026-10-18 00:48:19.156 ERROR] Test failed dragonfly/keydb_onboarding_test.py::test_keydb_write_stamped_with_author_hash[df_factory0] with logs: 
E           AssertionError: ({'96b3299015b33b05'}, {'96b3299015b33b05'})
E           assert {'96b3299015b33b05'} != {'96b3299015b33b05'}
E             
E             Both sets are equal
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_write_stamped_with_author_hash[df_factory0]
====================== 1 failed, 175 deselected in 3.96s =======================
```

Mutants: F1 `repl_mvcc` not set; F2 `Stamp(mvcc, link_origin_idx_)` (the link's origin); F3 destructor
does not restore `repl_origin_idx`; F4 does not restore `repl_mvcc`; F5 the self guard removed; F6
`if (true)` for `stamping()` (the plain applier stamps). Each restored (`cmp` against `/tmp/good22/`),
rebuilt, rerun: `classic_replay_test` 114 passed; `multi_master_test` 229 passed + 1 skipped;
pytest `-k "stamped_with_author_hash or author_slot or unwraps or exact_classic_counters or
write_during or peer_mode"` 30 passed. `pre-commit` clean on the four files.

Passing without the feature: F1's mutant (no `repl_mvcc`) is caught only by the gtests (the pytest
cannot see the mvcc it compares nothing about); the "restore skipped on one early-exit path" mutant
the brief asks for cannot exist with an RAII scope that sets only before the dispatch (note 1).

## Anchor drift

`conn_context.h` fields at `:360-380`, `main_service.cc` `SetReplOrigin` at `:892` and `:1820`,
`replica.cc:1217`/`:1226` as the brief says. `multi_master_test.cc` `MvccStoreTest::StampOf` at `:915`
(copied, not lifted: a lift needs a shared header, and it is 5 lines).

## Open questions

1. A `mvcc == 0` envelope (3-argument form) is attributed to the author's origin with a local
   mint. Keep the link origin instead? One `if`.
2. `NeedsPeerNode()` by test name (note 4).
3. The full `keydb_onboarding_test.py` was not run; the subset above was.

## Fix round 1

Review: `task-2.2-review.md` (0 Critical, 2 Important, 5 Minor). Rulings: `task-2.2-fix1.md`.

| Finding | Change |
|---|---|
| I1 | `NeedsPeerNode()`, the `SetFlag` in the base `SetUp` and the `absl/strings/match.h` include are gone. New `ClassicPeerApplyFamilyTest : ClassicApplyFamilyTest` (sets `--active_replica`, then the base `SetUp`; the base `TearDown` resets it) holds the peer helpers (`PeerLink`, `PeerNode`, `OnPeerLink`, `StampOf`, constants) and the stamp tests, which keep their names. |
| I2 | New `ClassicPeerApplyFamilyTest.SkippedCommandsNeverResolveTheirAuthor`: cap 0, an unknown command, a KeyDB-only command and a `PING`, each from a fresh author: `FindIdx(author)` empty, registry size 2, overflow delta 0. |
| m1, m2 | `CommandScope` comment and `IdxFor` comment reworded as suggested. |
| m3 | New `PeerEnvelopeWithoutMvccIsALocalMintOfTheAuthor` (mvcc 0: non-zero local mint, author's origin hash, differs from a local write's, registry +1, context restored). |
| m4 | `link_origin_idx_` kept; its comment says it equals `ClassicAuthorMap`'s link idx in production and why the applier reads the context. |
| m5 | Design note 1 above corrected. |

### Falsifications (`ClassicPeerApplyFamilyTest.*`, mutate `classic_replay.cc`, rebuild, run, restore)

I2, EARLY (`Stamp`/`IdxFor` moved to right after the scope is built, before the unknown-command check):

```
[4/4] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1409: Failure
Expected equality of these values:
  ClassicAuthorOverflowTotal()
    Which is: 2
  overflow_before
    Which is: 1
[  FAILED  ] ClassicPeerApplyFamilyTest.SkippedCommandsNeverResolveTheirAuthor (20 ms)
[  PASSED  ] 6 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicPeerApplyFamilyTest.SkippedCommandsNeverResolveTheirAuthor
 1 FAILED TEST
```

m3 (`scope.Stamp(mvcc, mvcc == 0 ? link_origin_idx_ : authors_.IdxFor(author))`):

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1426: Failure
Expected equality of these values:
  stamp->origin_hash
    Which is: 14262392837314142136
  NodeUuidHash(kAuthorA)
    Which is: 7953565813638624897
/home/user/drakeydb/src/server/classic_replay_test.cc:1428: Failure
Expected equality of these values:
  node.registry.Size()
    Which is: 2
    Which is: 3
[  FAILED  ] ClassicPeerApplyFamilyTest.PeerEnvelopeWithoutMvccIsALocalMintOfTheAuthor (18 ms)
[  PASSED  ] 6 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicPeerApplyFamilyTest.PeerEnvelopeWithoutMvccIsALocalMintOfTheAuthor
 1 FAILED TEST
```

Restored (`cmp`), rebuilt: `classic_replay_test` 116 passed (114 + 2; 10 suites);
pytest `-k "stamped_with_author_hash or author_slot or exact_classic_counters"` 4 passed;
pre-commit clean on the three C++ files (the Python file is unchanged).
