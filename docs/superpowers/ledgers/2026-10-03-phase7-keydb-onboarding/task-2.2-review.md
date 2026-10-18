# Task 2.2 review: per-command stamps on peer links

Reviewer: Opus 5.5. Branch `feat/phase7-2-author-stamps-dedup-guard`, BASE `1dd8d035`, working tree
(not committed). Scope: plan Task 2.2, spec D-4 (points 1 and 3, author cap), decisions 42–46, the
Task 2.1 review's hand-off items, `task-2.2-report.md`, and every hunk of `git diff 1dd8d035 -- src
tests` (4 files, +369/−8). Sources were backed up to `/tmp/rev22/`. Every probe and mutant was
restored (`cmp` clean on all four files), then the targets were rebuilt and re-run: 114 passed, and
the pytest passed (below).

## Verdict: **Needs fixes**

There are no Critical findings. There are two Important ones: the R7 fixture, and an unpinned
"dispatched only" property. Both are test-only changes. The production code does what spec D-4
says, and every probe the brief asks for (a)–(e) came out correct.

## Findings

| ID | Sev | file:line | What | Why | Suggested fix |
|---|---|---|---|---|---|
| I1 | Important | `src/server/classic_replay_test.cc:670-688` | `NeedsPeerNode()` picks the node mode by test name (a `Peer` prefix plus three exact names). | R7 asks: is a second fixture a change of 30 lines or fewer that keeps the plan's test names reachable? It is. I built it as a throwaway: `class ClassicPeerApplyFamilyTest : public ClassicApplyFamilyTest` whose `SetUp` sets `--active_replica` and calls the base `SetUp`. The base's `TearDown` already resets the flag. I deleted `NeedsPeerNode`, the `SetFlag` line in `SetUp` and the `absl/strings/match.h` include, and moved the 5 `TEST_F`s to the new fixture. The diff is **30 changed lines** (`diff -u`, +/−). Result: `classic_replay_test` **114 passed, 10 suites**. All 5 tests keep their names, and `--gtest_filter='*ApplyFamilyTest.<plan name>'` reaches every one of them (output below). The name list is also fragile. A future test named `Peer…` would silently boot an active node. A new stamp test missing from the list would run on a plain node; `StampOf` then returns nullopt, which fails loudly but for the wrong reason. Only `DflyFlowOfALiveSessionIsAnErrorNotACrash` needs the plain node: with `--active_replica` forced on for the whole suite, 35/36 pass. | Use the second fixture, exactly as probed. The comment moves onto the new class. |
| I2 | Important | `src/server/classic_replay.cc:523-527`, test `:1354` | "Only a command that is dispatched resolves its author (a skipped one costs no registry slot), so the cap counts dispatched commands" is **untested**. My mutant EARLY moves `scope.Stamp(mvcc, authors_.IdxFor(author))` up, right after the scope is built and *before* the unknown-command check. All 39 `ClassicApplyFamilyTest.*:ClassicAuthorMapTest.*` pass under it. | This placement is documented in the code comment and in the overflow counter's help text ("one per dispatched command"), and the brief singles it out (probe b). `RestoresRawContextAfterEnvelope` sends its unknown and KeyDB-only commands from `kAuthorA`, which the first envelope has already registered, so no mutant that resolves early can show. The report's note 1 says the early exits "cannot" be falsified. That holds for the restore, but not for the resolve. | Add an unknown command, a KeyDB-only command and a `PING`, each from a **fresh** author (e.g. `3333…`), under `--classic_author_cap=0`: assert `registry.Size()` is unchanged, the overflow delta is 0, and `FindIdx(fresh)` is empty. Under EARLY this fails on the overflow and on `Size`. Moving `IdxFor` to the top of `ApplyCommand` is caught too. |
| m1 | Minor | `src/server/classic_replay.cc:470-474` | The `CommandScope` comment says it "puts back on every exit of ApplyCommand (an early return …)". The scope is built *after* the control / KeyDB-only return. It also says "the link-wide ones (repl_origin_idx, repl_lww_guard) are what the scope restores to", but the scope never touches `repl_lww_guard`, and `repl_origin_idx` is both set and restored. | The comment overstates what the code does. | Suggested: "Sets `repl_mvcc` and `repl_origin_idx` on the stream's apply context for one dispatch, and on every exit after it is built puts back the link's values (mvcc 0, the link's origin), which the raw commands of the stream run with." |
| m2 | Minor | `src/server/classic_replay.h:211-213` | "kSelfIdx is never returned". A map without a registry returns `link_idx_`, which on a plain link *is* `kSelfIdx` (0). | The comment is only true with a registry. | "With a registry, kSelfIdx is never returned …". |
| m3 | Minor | (no test) | Ruling R6, a 3-argument envelope (`mvcc == 0`), is not pinned. My probe `PeerProbeMvccZeroEnvelope` shows how it behaves today: `mvcc=1879347325499867137` (a local mint), `origin=7953565813638624897` (`NodeUuidHash(kAuthorA)`, not the local `16186957841148020401`), registry grows by 1, and the context is restored. That matches R6. | A ruling with no test can regress silently, and Task 2.3's `MvccZeroNeverDedupedNeverAdvances` sits right next to it. | Add the probe as a test (`PeerEnvelopeWithoutMvccIsALocalMintOfTheAuthor`). |
| m4 | Minor | `src/server/classic_replay.h:340-342` | `link_origin_idx_` duplicates `ClassicAuthorMap::link_idx_`: both are `peer_origin_idx_`, reached by two routes (the context at construction, and the map's ctor argument). | Two copies of one value. | Optional: `authors_.link_idx()`, or keep it and say in the comment that the two are equal. |
| m5 | Minor | report, design note 1 | "The scope object is created at the top so every exit restores" is not accurate: it is created after the first early exit. | The report is inaccurate. The code is fine. | None in code. m1 fixes the comment. |

Formatting: neither `clang-format` nor `black` is installed on the host or in `drakeydb-p7`, so I
could not re-run them. I am relying on the report's "pre-commit clean". No process text appears in
the `src/` or `tests/` hunks; I grepped the added lines for finding IDs, `fix round`, `Task N` and
`review`.

## Spec axis

- **D-4.1.** Each dispatched inner command gets `repl_origin_idx = IdxFor(innermost uuid)` and
  `repl_mvcc = innermost mvcc`. `HandleRreplay` passes `env` of the layer whose inner command is not
  an RREPLAY, and that is the innermost one. The over-cap case keeps the mvcc and takes the link's
  idx. This is pinned, and also probed nested (see (e)).
- **Peer mode only.** The gate is `ClassicAuthorMap::stamping()` (`registry_ != nullptr`).
  Production builds that map from `peer_mode_ ? peer_mode_->registry : nullptr`
  (`replica.cc:1229`). The only `ReplicaPeerMode` construction is `peer_replication.cc:203`
  (`PeerReplicationManager`, reached only through `ReplicaOfActive`). So the gate is equivalent to
  `IsPeerMode()`, and an `--active_replica`-off node never stamps.
- **The restore.** Spec D-4.3 says to restore "in peer mode". The code restores unconditionally. On
  a plain link that writes `repl_mvcc = 0` and `repl_origin_idx = kSelfIdx`, which are the values
  the context already has: no field ever changes, so the behaviour is byte-identical. Accepted.
- **`replica.cc` untouched** (the plan lists it). This is justified: the context's origin is set at
  `:1217`, before the applier is built at `:1226`.
- **Task 2.1 hand-off.** Every item landed:
  - `IdxFor` is called only after the self drop and only for a dispatched command.
  - The `kSelfIdx` guard is in, with a test (F5).
  - Over-cap keeps the mvcc, pinned in `PeerEnvelopeOfAnOverCapAuthor…`.
  - The "Nothing calls it" comment is replaced.
  - The overflow help text is reworded (R2).
- **R6.** Implemented as ruled (see m3 for the probe).
- **R7.** See I1.
- **R8.** Done below.
- **Beyond the brief.** `ClassicAuthorMap::stamping()` (one accessor) and
  `ResetClassicForwardedAuthorsForTest()` in the fixture's `SetUp`/`TearDown`. Both are harmless.

## Probes (a)–(e)

- **(a) `CommandScope` vs the raw path.**
  - The context and the applier are locals of `ConsumeRedisStream`, used by that fiber alone.
    `flush_batch()` drains synchronously before an envelope is applied. The raw commands queued
    after it are dispatched later, and the squasher copies `repl_origin_idx`/`repl_mvcc` per batch
    (`multi_command_squasher.cc:114`) from the restored context.
  - `RestoresRawContextAfterEnvelope` checks the fields after each of six exits, and F3/F4 fail it.
  - **Exceptions.** `DispatchCommand` wraps `cid->Invoke` in `try/catch (std::exception&)`
    (`main_service.cc:1747-1752`). Server code is built with exceptions on (`-fno-exceptions`
    appears only for a third-party target), so anything else that throws unwinds through the RAII
    destructor, which still restores.
  - **Lua.** Lua's `longjmp` is bounded by `lua_pcall` inside the interpreter and never crosses
    `ApplyCommand`'s frame.
- **(b) Between `Stamp` and `SetReplOrigin`.**
  - `IdxFor` can yield (the `AddOrGet` ORIGIN fan-out and `AwaitBrief`), but it is evaluated as the
    argument, *before* `Stamp` writes. Between `Stamp` and `PrepareTransaction`'s copy
    (`main_service.cc:892`), `DispatchCommand` may yield.
  - **The context is single-fiber per link:** the only other uses of `&conn_context` in
    `ConsumeRedisStream` are `ctx->Init` and `DispatchSquashedBatch`, both on the same fiber, and
    the ACK fiber never touches it.
  - So nothing else can observe the stamp.
- **(c) `link_origin_idx_` at construction.** `peer_origin_idx_` is assigned only in `Greet()`
  (`replica.cc:544`), before the stream phase. Every reconnect re-enters `ConsumeRedisStream`, which
  builds a new context, sets `:1217` again, and builds a new applier. Nothing re-sets the context's
  origin while an applier is alive. This is correct.
- **(d) The pytest compares by inequality only.** The venv has no `xxhash`
  (`ModuleNotFoundError`), so a by-value check would need new plumbing. Inequality is enough here.
  - Under F2 (the link's origin for everything), `from_b == from_a`.
  - Under my mutant OUTER (stamp with the **outermost** envelope's author and mvcc, i.e. the
    forwarder), the pytest also fails with both sets equal (output below). That proves B's writes
    really arrive as `RREPLAY(A, RREPLAY(B, …))`, so the nested half of the assertion is not
    vacuous.
- **(e) Nested envelopes across the cap** (`--classic_author_cap=1`, throwaway tests; both pass):
  - **Outer over the cap, inner admitted.** The stamp is `{inner mvcc, hash(inner)}`, the overflow
    delta is 0, and the outer author is not in the registry (it is never resolved).
  - **Outer admitted, inner over the cap.** The stamp is `{inner mvcc, hash(link)}`, which is
    neither the outer author's nor the inner author's hash, and the overflow delta is 1. This
    matches spec D-4 ("beyond the cap … the link's idx").
  - Also probed:
    - The link's own uuid as the author under cap 0 maps to the link's idx, with no overflow and no
      slot.
    - A self-authored inner layer under a foreign outer one leaves no key and no registry growth.

## Falsifications reproduced

The report's own mutants F1 and F2, plus two of mine, each applied to `classic_replay.cc`, built,
run with `--gtest_filter='ClassicApplyFamilyTest.*:ClassicAuthorMapTest.*'`, and restored.

**F1** (`cntx_->repl_mvcc = mvcc;` removed): 3 tests fail, as the report says.
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1283: Failure
    Which is: 1879348400747446272
  kMvccA
    Which is: 1782579200000000005
...
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash (19 ms)
[  FAILED  ] ClassicApplyFamilyTest.PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin (19 ms)
[  FAILED  ] ClassicApplyFamilyTest.NestedEnvelopeStampsWithInnerAuthor (19 ms)
[  PASSED  ] 36 tests.
 3 FAILED TESTS
```

**F2** (`scope.Stamp(mvcc, link_origin_idx_)`): 4 tests fail. The report's output was cut off at
`:1382` and its count was not shown; here `RestoresRawContextAfterEnvelope` fails as well.
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1284: Failure
    Which is: 14262392837314142136
  hash_a
    Which is: 7953565813638624897
...
/home/user/drakeydb/src/server/classic_replay_test.cc:1383: Failure
    Which is: 14262392837314142136
  NodeUuidHash(kAuthorB)
    Which is: 16571850207653206743
[  FAILED  ] ClassicApplyFamilyTest.RestoresRawContextAfterEnvelope (20 ms)
[  PASSED  ] 35 tests.
 4 FAILED TESTS
```

**OUTER** (mine: `ApplyCommand(inner_args, outer_uuid, outer_mvcc)`, where `outer_*` are taken from
the first layer): the gtest fails 1, and the pytest fails.
```
/home/user/drakeydb/src/server/classic_replay_test.cc:1326: Failure
  stamp->Mvcc()
    Which is: 1782579200104857609
  kMvccA
    Which is: 1782579200000000005
/home/user/drakeydb/src/server/classic_replay_test.cc:1327: Failure
  stamp->origin_hash
    Which is: 16571850207653206743
  NodeUuidHash(kAuthorA)
    Which is: 7953565813638624897
[  FAILED  ] ClassicApplyFamilyTest.NestedEnvelopeStampsWithInnerAuthor (19 ms)
[  PASSED  ] 38 tests.
```
```
[3/3] Linking CXX executable drakeydb
E           AssertionError: ({'eaa5962753967718'}, {'eaa5962753967718'})
E           assert {'eaa5962753967718'} != {'eaa5962753967718'}
E             Both sets are equal
====================== 1 failed, 175 deselected in 6.93s =======================
```

**EARLY** (mine: `Stamp`/`IdxFor` moved before the unknown-command check): **survives**. This is
I2.
```
[3/3] Linking CXX executable classic_replay_test
[  PASSED  ] 39 tests.
```

**R7 second fixture** (throwaway, 30 changed lines):
```
[==========] 114 tests from 10 test suites ran. (2149 ms total)
[  PASSED  ] 114 tests.
[       OK ] ClassicPeerApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash (21 ms)
[       OK ] ClassicPeerApplyFamilyTest.PeerEnvelopeOfAnOverCapAuthorKeepsItsMvccAndTakesTheLinkOrigin (20 ms)
[       OK ] ClassicPeerApplyFamilyTest.NestedEnvelopeStampsWithInnerAuthor (19 ms)
[       OK ] ClassicPeerApplyFamilyTest.PlainApplierLeavesMvccZero (20 ms)
[       OK ] ClassicPeerApplyFamilyTest.RestoresRawContextAfterEnvelope (19 ms)
[  PASSED  ] 5 tests.
```
For comparison, with `--active_replica` forced on for the whole of `ClassicApplyFamilyTest`:
```
[  FAILED  ] ClassicApplyFamilyTest.DflyFlowOfALiveSessionIsAnErrorNotACrash (18 ms)
[  PASSED  ] 35 tests.
```

### Vacuity

- **Feature removed entirely** (no `Stamp`):
  - `PeerEnvelopeStamps…`, `OverCap…` and `Nested…` fail (F1/F2).
  - `RestoresRawContextAfterEnvelope` fails only on its last line, the stamp of `e`. Its field
    checks would pass, because a context that is never stamped is trivially "raw".
  - `PlainApplierLeavesMvccZero` passes by design (it pins absence), and F6 is its falsifier.
  - The pytest fails (every origin equals `own`).
- **What the report says.** It covers F1 not being visible to the pytest, and the early exits not
  being falsifiable for the restore. It does not say that the dispatched-only resolve is unpinned
  (I2).

## Runs (R8)

```
classic_replay_test             [  PASSED  ] 114 tests.
multi_master_test               [  PASSED  ] 229 tests.  (1 skipped: NodeIdentityFile.UnwritableDirIsEphemeral)
keydb_onboarding_test.py (KEYDB_REQUIRED=1, whole file)
                                ================== 173 passed, 3 skipped in 371.46s (0:06:11) ==================
  skips: all DRAKEYDB_PERF=1 (:4485 the 1.5x bar; :4530 the reference setups, 2 params)
after restore: classic_replay_test 114 passed; -k stamped_with_author_hash 1 passed, 175 deselected
```

## For the next task (2.3) and later

- **2.3 (`AuthorDedup`).**
  - The reservation wraps the leaf dispatch, so put it with the stamp: after the unknown-command
    check, keyed on the same innermost `(env.uuid, env.mvcc)` that `ApplyCommand` already receives.
  - An `mvcc == 0` envelope resolves its author and takes a registry slot (R6, m3), but must never
    be deduplicated.
  - I2's new test is where `DedupStillWorksForOverCapAuthor` and "a skipped command reserves
    nothing" belong.
- **2.4 (the guard, and EVAL unguarded).**
  - `CommandScope` is the place for the per-dispatch `repl_lww_guard = false` override. Its
    destructor must then restore the link's guard bit. m1's comment should name it once it exists.
- **2.6 (`KEYDB.MVCCRESTORE`).**
  - It is currently dropped by the unknown-command check *before* stamping. When it becomes a known
    command, its `Stamp` has to use its own `<mvcc>` argument, not `env.mvcc` (spec D-4.1).
- **2.8 (`repl_time_ms`).**
  - Add one field to `Stamp` and one line to the destructor, as the report says.

## Re-review (fix round 1)

Scope: the rulings in `task-2.2-fix1.md`, the report's "## Fix round 1" section, and the current
`git diff 1dd8d035 -- src tests` (4 files, +415/−8). I backed the sources up to `/tmp/rev22b/` and
restored them after every probe (`cmp` clean on all three C++ files). After the restore I rebuilt
`classic_replay_test` and `dragonfly`: **116 passed**.

### Verdict: **Clean**

Every ruling is implemented. The fix introduces nothing at Critical or Important level. Three
Minor notes follow the table; none of them blocks.

| Finding | Status |
|---|---|
| I1 | **Fixed.** `ClassicPeerApplyFamilyTest : ClassicApplyFamilyTest` sets `--active_replica`, then runs the base `SetUp`; the base `TearDown` resets the flag. `NeedsPeerNode`, the per-test `SetFlag` and the `match.h` include are gone. The 5 tests keep their names and two new ones were added. Beyond the probe, the implementer also moved the peer-only helpers (`PeerLink`, `PeerNode`, `OnPeerLink`, `StampOf`, the constants) into the new class. They are used only there, so this is accepted. Leak check below. |
| I2 | **Fixed.** `SkippedCommandsNeverResolveTheirAuthor` sends an unknown command, a KeyDB-only command and a `PING`, each from a fresh author, under cap 0. EARLY is caught (reproduced below). My own TOP mutant, which resolves at the very top of `ApplyCommand` before the control/KeyDB-only return, is caught too (overflow delta 3). |
| m1 | **Fixed** with the suggested wording (`classic_replay.cc:470-472`). |
| m2 | **Fixed:** "with a registry, kSelfIdx is never returned" (`classic_replay.h:213`). |
| m3 | **Fixed:** `PeerEnvelopeWithoutMvccIsALocalMintOfTheAuthor`. It uses the explicit `"0"` mvcc, while R6 is about the 3-argument form; `ParseRreplayEnvelope` gives `mvcc = 0` for both, so the test covers the ruling. The report's falsification output (the link's origin for mvcc 0) fails 2 assertions. I did not re-run it. |
| m4 | **Fixed.** The comment says the two values are equal in production and why the applier reads the context (`classic_replay.h:340-344`). |
| m5 | **Fixed:** design note 1 now says "created after the control / KeyDB-only return". |

Minor notes, none blocking:

- **n1.** In `SkippedCommandsNeverResolveTheirAuthor`, only the overflow assertion can catch the
  mutants. With `--classic_author_cap=0` a refused author never gets a registry slot, so
  `FindIdx(author)` and `registry.Size() == 2` hold even under EARLY: in the output below, only
  `:1409` fails. My own I2 wording ("fails on the overflow and on Size") was wrong on this point.
  - If wanted: a second pass at the default cap would make the slot assertions fail too.
- **n2.** The m4 comment says "a default map, a plain link's, has no link idx". In production a
  plain link's map is not a default one: it is built with `peer_origin_idx_`, which is 0 on a plain
  link, and a default map's `link_idx_` is 0 as well. The comment is harmless but loose.
- **n3.** I1's ruling asked for the fixture comment to move onto the new class. The new comment no
  longer says why the other tests stay plain (a node of the other kind answers
  `REPLCONF capa dragonfly` differently). That reason now lives only in this ledger.

### EARLY, reproduced against the new test

The mutant moves `if (authors_.stamping()) scope.Stamp(mvcc, authors_.IdxFor(author));` to right
after `CommandScope scope(...)`, before the unknown-command check. Run with
`--gtest_filter='ClassicPeerApplyFamilyTest.*:ClassicApplyFamilyTest.*:ClassicAuthorMapTest.*'`:

```
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:1409: Failure
  ClassicAuthorOverflowTotal()
    Which is: 2
  overflow_before
    Which is: 1
[  FAILED  ] ClassicPeerApplyFamilyTest.SkippedCommandsNeverResolveTheirAuthor (20 ms)
[  PASSED  ] 40 tests.
 1 FAILED TEST
```

TOP is the same `Stamp` line, moved above the control/KeyDB-only return:

```
/home/user/drakeydb/src/server/classic_replay_test.cc:1409: Failure
  ClassicAuthorOverflowTotal()
    Which is: 4
  overflow_before
    Which is: 1
[  FAILED  ] ClassicPeerApplyFamilyTest.SkippedCommandsNeverResolveTheirAuthor (29 ms)
[  PASSED  ] 40 tests.
```

### `--active_replica` does not leak

The two shuffled runs of the whole binary, as asked:

```
[==========] 116 tests from 10 test suites ran. (2274 ms total)
[  PASSED  ] 116 tests.
[==========] 116 tests from 10 test suites ran. (2245 ms total)
[  PASSED  ] 116 tests.
```

In those two runs, and in seeds 7, 11 and 5, `ClassicPeerApplyFamilyTest` ran *after*
`ClassicApplyFamilyTest`, so they could not show a leak. `DflyFlowOfALiveSessionIsAnErrorNotACrash`
is the one test that fails on an active node. I therefore searched for a seed where the peer suite
runs first.

- **Seed 2, whole binary.** Suite order: `ClassicReplayTest`, `ClassicPeerApplyFamilyTest`,
  `ClassicAuthorMapTest`, `ClassicEmulatedClusterTest`, `ReplicaActiveExpiryTest`,
  `ClassicApplyFamilyTest`. Result: `[  PASSED  ] 116 tests.`
- **Seed 2, the two apply suites only.** The 7 peer tests run first, then the 26 plain ones;
  `[       OK ] ClassicApplyFamilyTest.DflyFlowOfALiveSessionIsAnErrorNotACrash`, 33 passed.

To check that a leak would be caught, I removed the reset (`absl::SetFlag(&FLAGS_active_replica,
false)`) from the base `TearDown` and ran seed 2:

```
[  FAILED  ] ClassicApplyFamilyTest.DflyFlowOfALiveSessionIsAnErrorNotACrash (18 ms)
[  PASSED  ] 115 tests.
 1 FAILED TEST
```

So the reset is what keeps later suites plain, and it is pinned by an ordering that seed 2
reproduces. The default (unshuffled) order runs `ClassicApplyFamilyTest` first, because it is
defined first, which is why a leak would go unseen there.

### Formatting

I could not re-run formatting: neither `clang-format` nor `black` is on the host's PATH or in the
container. The report says host pre-commit (`~/.venvs/precommit`) is clean on the three C++ files;
I did not re-run it. No process text appears in the added lines.
