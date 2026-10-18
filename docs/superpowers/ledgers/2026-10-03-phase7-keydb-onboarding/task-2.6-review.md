# Task 2.6 review: `KEYDB.MVCCRESTORE` applied, LWW-guarded

Reviewer: Opus 5.5. BASE `68612a5f` (HEAD is now `d62f1e7c`, which only adds a ledger file). I read
`git diff 68612a5f -- src tests` hunk by hunk, plus the two untracked files. Container
`drakeydb-p7`, `build-dbg`, `-j4`. I edited no source. Every probe and mutant was applied, run and
then restored: `cmp` against my backups prints `ALL-RESTORED`, `git diff --stat` matches the
implementer's tree, and `classic_replay_test` is back to 167 passing.

## Verdict: **Needs fixes** (no Critical; 2 Important, 6 Minor)

The feature is correct. It matches spec D-7a on every point I probed:
- the translation, the arity check, the `<expire>` table and the `<mvcc>` classes;
- the hook's position;
- the stamp (own mvcc, innermost author's hash) and the dedup on the envelope's `(uuid, mvcc)`;
- the accounting split.

Every mutant I applied to the feature code is caught. The two Important findings are a counter-order
deviation from D-13, which is visible to operators and pinned by tests, and one test assertion that
can never fail. The Minors are comments and docs that are now incomplete.

## Findings

| ID | Sev | file:line | What | Why | Suggested fix |
|---|---|---|---|---|---|
| I-1 | Important | `src/server/classic_replay.cc:122` (row), `src/server/classic_replay_test.cc:825`, `src/server/multi_master_test.cc:457`, `tests/dragonfly/keydb_onboarding_test.py:2591` | `keydb_mvccrestore_failed` renders after `classic_apply_errors`. Spec D-13 puts it right after `keydb_cmds_dropped`: `rreplay_unwrapped, rreplay_malformed, rreplay_self_dropped, keydb_cmds_dropped, keydb_mvccrestore_failed, classic_unknown_cmds_dropped, classic_apply_errors, classic_link_uuid_changes`. | This order is the INFO plain block, the peer line and the `/metrics` order, and the exact pins freeze it (`list(classic_fields(...)) == CLASSIC_COUNTERS`, the peer-line string). The gtest still says "every counter, in the order of spec D-13", which is now false. Moving it later means churn in output operators can see. | Move the `kClassicCounters` row (and the `ClassicLinkStats` / `ClassicLinkCounts` fields, for readability) to just after `keydb_cmds_dropped`. Reorder the four pins to match. |
| I-2 | Important | `src/server/classic_replay_test.cc:2143` (`PlainApplierAppliesMvccRestoreVerbatimUnstamped`) | `EXPECT_EQ(link.cntx.repl_mvcc, 0u)` runs after `Apply` returns. By then `CommandScope`'s destructor has already reset `repl_mvcc` to 0 whatever happened during dispatch. The assertion cannot fail. | R19 accepted this test's deviation on the premise that it asserts `repl_mvcc == 0`. That assertion pins nothing. My mutant M7 stamps every MVCCRESTORE on a plain link (`else if (mvccrestore) scope.Stamp(..., link_origin_idx_)`) and all 20 MVCCRESTORE tests pass under it (output below). The report's vacuity notes do not mention this. Not rated Critical: on a plain node the stamp table does not exist and the guard bit is off, so a stray stamp is probably unobservable (I did not check the journal path), and R19 already waived pinning the stamp. | Either capture `repl_mvcc` at dispatch time (swap RESTORE's handler with the fixture's `SetHandler` helper and record `cntx->repl_mvcc`), or delete the assertion and drop "Unstamped" from the name. Record whichever is chosen in the report's vacuity list. |
| M-1 | Minor | `src/server/classic_replay.h:135`, `src/server/classic_replay.cc:113` | The `keydb_cmds_dropped` comment ("KeyDB-only commands (IsKeyDbOnlyCommand), skipped") and its help text no longer cover everything counted: a type-64 MVCCRESTORE payload is not an `IsKeyDbOnlyCommand`. | The comment understates the counter. | Add "and KEYDB.MVCCRESTOREs of a KeyDB cron job". |
| M-2 | Minor | `classic_replay.h:141-143` (`classic_apply_errors`), `classic_replay.cc:120` (its help), `classic_replay.h:125-127` (`rreplay_unwrapped`), `classic_replay.h:421` (`Leaf::kSkipped`), `classic_replay.h:429-432` (`ApplyCommand` doc) | Comments that list the outcomes of an inner command were not updated. `classic_apply_errors` says "inner commands that did not apply", but a failed MVCCRESTORE no longer counts there. `rreplay_unwrapped`, `kSkipped` and the `ApplyCommand` doc list control, KeyDB-only and unknown commands, but not an untranslatable MVCCRESTORE. | The comments are no longer true to the code. | Add the MVCCRESTORE case to each, or "except a KEYDB.MVCCRESTORE (keydb_mvccrestore_failed)" for the error counter. |
| M-3 | Minor | `src/server/classic_replay_test.cc:2117-2118` | The comment on `MvccRestoreIsTranslatedAndApplied` says "A raw one is not (spec D-7a)", but nothing tests the raw path. The old `KeyDbMvccRestoreCounted...` test was replaced, and no other test streams a raw `KEYDB.MVCCRESTORE`. | The behavior is correct (my probe below: raw is not translated, lands in `unknown_KEYDB.MVCCRESTORE`, classic counters unmoved, while an enveloped one in the same stream applies). It is correct by construction, since the hook lives only in `ApplyCommand`, but a binding D-7a sentence is unpinned and the comment reads as if it were pinned. | Add `resp_command("KEYDB.MVCCRESTORE", "k2", 500, -1, b"\x00\x01v" + footer(9))` to the raw-path test at `keydb_onboarding_test.py:3478`. Assert `k2` is absent and `unknown_keydb.mvccrestore` is in commandstats. Its exact `classic_fields == {"keydb_cmds_dropped": 2}` pin then also shows it is not a `keydb_mvccrestore_failed`. Or drop the sentence from the comment. |
| M-4 | Minor | `tests/dragonfly/data/README.md:18`, the new section, `classic_replay_test.cc:2164` | The README still says "Both files are the stream only" next to three files. The frame table and the gtest's "frames 0 and 1" assume a fixed frame order, which does not hold. I regenerated the capture and got the same 774 bytes but the order `hash, ttl_key, plain_key` instead of `plain_key, hash, ttl_key`: KeyDB's RDB order follows its seeded dict. | Regenerating the file reorders the frames, so the README table and the gtest comment go stale. The gtest embeds the payloads by content and is unaffected. | Say "The files"; note in the section that frame order follows KeyDB's dict order and changes between captures; name the frames in the gtest comment by key (`plain_key`, `hash`). |
| M-5 | Minor | `src/server/classic_replay.cc:981`, `:390-391` | On the success path, which a resync runs once per key, `NoteMvccRestore` copies the key into a `std::string` before translating, but the key is only used on a failure. `TranslateMvccRestore` copies the payload into `vector<string>`, and `Assign` copies it again (three copies of each DUMP counting `FillBackedArgs`). | Avoidable allocations on the burst path. The comment ("read before the translation, which leaves `cmd` unchanged on a failure") argues for reading the key *after* a failure, which is what makes the copy unnecessary. | Read `cmd->at(1)` after a non-`kOk` result, since the args are untouched then. Optionally swap the arguments into a local `BackedArguments` and `Assign` from views of it, which saves one payload copy. |
| M-6 | Minor | `src/server/classic_replay.cc:337-343` | `ParseSignedDecimal` and `ParseUnsignedDecimal` accept leading zeros (`05`, `007`), and the signed one accepts `-0` (it maps to `1`, born expired). KeyDB's `string2ll` rejects all three. | Only matters for input KeyDB never emits, and it errs toward applying. Recorded as an observation, not a change request. | None needed. Mention it in the comment if desired. |

**R18 (confirmed).** `LoaderFailureCountsAsMvccRestoreFailedNotApplyError` pins what its comment
claims: `failed 1`, `apply_errors 0`, and `Applied(kAuthorA, 830)` (the envelope commits). Mutant M4
(error reply sent to `NoteApplyError`) fails exactly this test. The test does not assert the
resident key, which matches the ruling. ISSUE-REGISTER D-31 already describes this shape: "`Add`
then fails — `INVALID_VALUE` for a malformed/untrusted body … the old value is genuinely gone".
**No docs Minor.**

**R19.** The deviation stands as ruled, but the `repl_mvcc == 0` half of the test is vacuous (I-2).

## Probes

### (a) Byte-exactness and arity

Throwaway gtest, reverted. Verbatim:
```
PROBE arity 3 parse=1
PROBE arity 4 parse=1
PROBE arity 6 parse=1
PROBE arity 7 parse=1
PROBE bytes parse=0 exact=true
```
The `bytes` case is a lowercase `keydb.mvccrestore`, key `"\0 \r\n"`, and a payload with `\r\n\0\0\r\n`
inside. The output is exactly `RESTORE <key> 77 <payload> REPLACE ABSTTL`. `OutputIsByteExact` and
`RejectsMalformed` (4, 1 and 6 arguments, args untouched) cover the plan's cases.

### (b) `<expire>`

Parse codes: 0 = kOk, 3 = kBadExpire.
```
PROBE expire [+5] parse=3 ttl=-
PROBE expire [ 5] parse=3 ttl=-
PROBE expire [5 ] parse=3 ttl=-
PROBE expire [5x] parse=3 ttl=-
PROBE expire [-0] parse=0 ttl=1
PROBE expire [05] parse=0 ttl=5
PROBE expire [9223372036854775808] parse=3 ttl=-
PROBE expire [-9223372036854775808] parse=0 ttl=0
PROBE expire [-9223372036854775809] parse=3 ttl=-
PROBE expire [9223372036854775807] parse=0 ttl=0
PROBE expire [9223372036854775806] parse=0 ttl=9223372036854775806
PROBE expire [0] parse=0 ttl=1
PROBE expire [-1] parse=0 ttl=0
```
The mapping table is exact: negative → 0, `LLONG_MAX` → 0, `0` → 1, anything else kept. Overflow is
`kBadExpire`. On `-0` and `05` see M-6.

### (c) `<mvcc>`

Parse codes: 0 = kOk, 2 = kBadMvcc.
```
PROBE mvcc [0] parse=0 own=0
PROBE mvcc [9223372036854775808] parse=0 own=0
PROBE mvcc [18446744073709551615] parse=0 own=0
PROBE mvcc [18446744073709551616] parse=2 own=0
PROBE mvcc [abc] parse=2 own=0
PROBE mvcc [+5] parse=2 own=0
PROBE mvcc [ 5] parse=2 own=0
PROBE mvcc [007] parse=0 own=7
PROBE mvcc [-0] parse=2 own=0
PROBE mvcc [5x] parse=2 own=0
PROBE mvcc [9223372036854775807] parse=0 own=9223372036854775807
```
This matches the plan:
- `0`, `1<<63` and all-ones give `kOk` with `own == 0` (fallback to the envelope's mvcc);
- overflow and non-numeric give `kBadMvcc`;
- `LLONG_MAX` has bit 63 clear, so it is a usable own mvcc, as the spec says.

### (d) Hook placement and the raw path

The hook sits after `IsControlCommand || SkipKeyDbOnly` and before `FindExtended`
(`classic_replay.cc:862-885`). Raw-path probe: a throwaway pytest, reverted. The stream is enveloped
`SET k good`, raw `KEYDB.MVCCRESTORE k2 …` with a loadable payload, enveloped
`KEYDB.MVCCRESTORE k3 …`, then `SET after 1`. Verbatim:
```
ZPROBE active False k2 0 k3 v {'rreplay_unwrapped': 3} ['unknown_KEYDB.MVCCRESTORE']
ZPROBE active True k2 0 k3 v {'rreplay_unwrapped': 3, 'rreplay_malformed': 0, 'rreplay_self_dropped': 0, 'keydb_cmds_dropped': 0, 'classic_unknown_cmds_dropped': 0, 'classic_apply_errors': 0, 'keydb_mvccrestore_failed': 0, 'classic_link_uuid_changes': 0} ['unknown_KEYDB.MVCCRESTORE']
====================== 2 passed, 191 deselected in 2.00s =======================
```
The raw one is not translated and stays on upstream's `unknown_` accounting, as D-7a says. It is
**not pinned** (M-3).

### (e) Stamp, dedup, replay on a second link

Throwaway peer gtest, reverted. Verbatim:
```
PROBE second link deduped moved=1 link2 unwrapped=0 failed2=0
PROBE nested applied A=true B=true
PROBE dk=v1
PROBE nk stamp mvcc=777 originA=true originB=false
PROBE stale vetoed: applied(env)=true failed=0 apply_errors=0
PROBE sk=local
```
- **Second link.** The same `(author, envelope mvcc)` on a second link, carrying a different payload
  and a *newer* own mvcc, is deduped at the envelope early-out: the value stays `v1`.
- **Nested.** A forwarded MVCCRESTORE (B wraps A) is stamped with its own `<mvcc>` and the
  **innermost** author's hash, and both layers advance.
- **Stale veto.** A stale restore vetoed by the guard does not count as a failure, and its envelope
  commits (`ran` is true on a veto, as in Task 2.4).
- **Dedup on the envelope's pair.** `MvccRestoreStampsOwnMvccWithAuthorHash` pins it (`Applied(A,
  900)`, `!Applied(A, 901)`). Mutant M6, which reserves on the own mvcc instead, fails it.

### (f) Counter rendering

`ClassicLinkFields`, `ClassicTotalSeries` and the peer line all render from `kClassicCounters`, so
`server_family.cc`, `multi_master.cc` and `metrics.cc` needed no edit (as the report says). The exact
pins are extended:
- the gtest `ClassicLinkFieldsFollowTheRenderPredicate`, which includes a stock master shown only
  through `keydb_mvccrestore_failed 4`;
- `ClassicTotalSeries…`;
- `PeerReplicationInfo.*`;
- pytest `test_scripted_active_master_exact_classic_counters`, both modes, INFO and `/metrics`;
- the fake-master test's `/metrics` series.

The order is wrong (I-1).

### (g) Golden vector

The `.bin` parses into three frames. The two payloads the gtest embeds are byte-equal to frames 0
and 1 of the file (`True True 774`). I re-ran `capture_keydb_mvccrestore.py --out-dir /tmp/r26cap`
in the container. It finished cleanly and wrote 774 bytes again, with the same keys and `<expire>`
classes but a different frame order (M-4) and, as the README's intro already warns, new uuids, mvccs
and deadline. The ports are free ports and the uuids are KeyDB's own, so the structure regenerates
but the bytes do not. The README row explains the three frames.

### (h) Mesh-merge pytest ×3

Run with `KEYDB_REQUIRED=1` under `flock`, `-k "mvccrestore or exact_classic_counters"`, three times:
```
KEYDB.MVCCRESTORE mesh merge of 2005 keys reached D and D2 in 0.32 s
====================== 4 passed, 187 deselected in 7.47s =======================
KEYDB.MVCCRESTORE mesh merge of 2005 keys reached D and D2 in 0.96 s
====================== 4 passed, 187 deselected in 6.73s =======================
KEYDB.MVCCRESTORE mesh merge of 2005 keys reached D and D2 in 0.84 s
====================== 4 passed, 187 deselected in 7.20s =======================
```
3/3. It asserts everything the brief lists, on both D and D2:
- `3_000_000 <= PTTL ttl_key <= 3_600_000` and `PTTL plain_key == -1`;
- `stale` is `newer-on-D` on D and `from-A` on D2;
- `multimaster_lww_dropped >= 1` on D;
- `keydb_mvccrestore_failed == 0`, unknown 0 and apply errors 0.

## Falsifications reproduced

Each mutant was applied with `/tmp/r26/mutate.py` against the good source, built, run, and restored.
Filter: `*MvccRestore*:*NoTtlRestore*:*AbsoluteTtl*:*BadPayload*:*CronPayload*`, which selects the
20 new tests.

**Environment caveat.** In the first loop, M2's run printed M1's failures. The bind mount served the
container the previous mutant's content. I re-ran M2 alone after checking the in-container source
with `grep` (output below). The other mutants' outputs are distinct and match their own mutation.
Whoever falsifies next in this container should `grep` the mutation inside the container before
trusting a run.

**(a) stamp with the envelope's mvcc.** `scope.Stamp(mvcc, …)`.
```
    Which is: 900
    Which is: 500
[  FAILED  ] ClassicPeerApplyFamilyTest.MvccRestoreStampsOwnMvccWithAuthorHash (21 ms)
    Which is: 'from-keydb'
...
    Which is: 0
    Which is: 1
[  FAILED  ] ClassicPeerApplyFamilyTest.StaleMvccRestoreLosesToNewerLocalWrite (21 ms)
[  FAILED  ] ClassicPeerApplyFamilyTest.NewerMvccRestoreWinsOverOlderLocalWrite (20 ms)
[  PASSED  ] 17 tests.
```
pytest (`dragonfly` relinked with the mutant):
```
>                   assert await c.get("stale") == stale
E                   AssertionError: assert equals failed
E                      -'from-A'       +'newer-on-D'
====================== 1 failed, 190 deselected in 5.12s =======================
```

**(c) drop the type-byte check.** `if (false && !rdbIsObjectTypeDF(type))`.
```
    Which is: 4-byte object <00-00 00-00>
    Which is: 4-byte object <04-00 00-00>
[  FAILED  ] ClassicReplayTest.TranslateMvccRestoreRejectsUnloadablePayload (0 ms)
    Which is: nil
    Which is: 0xaaaadaa2df68
[  FAILED  ] ClassicApplyFamilyTest.BadPayloadLeavesResidentKeyCountsAndAdvancesWatermark (24 ms)
[  PASSED  ] 18 tests.
```
`k` is nil: the D-31 delete, as the plan predicts. The `failed == 1` assertion still passes under
this mutant (the dispatched RESTORE replies an error), so `k == good` is the load-bearing assertion.

**My own mutants:**

M1, no bit-63 fallback: `*own_mvcc = (mvcc == 0 && kMvccInvalid) ? 0 : mvcc;`.
```
    Which is: 9223372036854775808
    Which is: 0
[  FAILED  ] ClassicReplayTest.TranslateMvccRestoreExtractsOwnMvcc (0 ms)
    Which is: 9223372036854775807
    Which is: 900
[  FAILED  ] ClassicPeerApplyFamilyTest.MvccRestoreFallsBackToEnvelopeMvccWhenOwnInvalid (21 ms)
[  FAILED  ] ClassicPeerApplyFamilyTest.MvccRestoreFallbackUnderEveryUnusableOwnMvcc (22 ms)
[  PASSED  ] 17 tests.
```

M2, no `0 → 1` mapping (re-run alone, in-container source checked first).
```
387:  else if (false && expire == 0)
394:  *own_mvcc = (mvcc == 0 || (mvcc & kMvccInvalid) != 0) ? 0 : mvcc;
/home/user/drakeydb/src/server/classic_replay_test.cc:697: Failure
    Which is: { "RESTORE", "k", "0", "\0xxxx\t\0\0\0\0\0\0\0\0\0", "REPLACE", "ABSTTL" }
    Which is: { "RESTORE", "k", "1", "\0xxxx\t\0\0\0\0\0\0\0\0\0", "REPLACE", "ABSTTL" }
[  FAILED  ] ClassicReplayTest.TranslateMvccRestoreMapsExpire (0 ms)
/home/user/drakeydb/src/server/classic_replay_test.cc:2213: Failure
[  FAILED  ] ClassicApplyFamilyTest.AbsoluteTtlRestoredAsDeadline (21 ms)
[  PASSED  ] 18 tests.
```

M4, an error reply counted as `classic_apply_errors`.
```
    Which is: 0
    Which is: 1
    Which is: 1
    Which is: 0
[  FAILED  ] ClassicApplyFamilyTest.LoaderFailureCountsAsMvccRestoreFailedNotApplyError (20 ms)
[  PASSED  ] 19 tests.
```

M6, reservation keyed on the own mvcc.
```
[  FAILED  ] ClassicApplyFamilyTest.NoTtlRestoreHasNoTtl (21 ms)
Value of: Applied(kAuthorA, 830)
  Actual: false
[  FAILED  ] ClassicApplyFamilyTest.LoaderFailureCountsAsMvccRestoreFailedNotApplyError (19 ms)
Value of: Applied(kAuthorA, 900)
  Actual: false
[  FAILED  ] ClassicPeerApplyFamilyTest.MvccRestoreStampsOwnMvccWithAuthorHash (23 ms)
[  PASSED  ] 17 tests.
```

M7, stamp every MVCCRESTORE on a plain link. **Survives** (I-2):
```
919:  else if (mvccrestore)
[3/3] Linking CXX executable classic_replay_test
[       OK ] ClassicApplyFamilyTest.PlainApplierAppliesMvccRestoreVerbatimUnstamped (23 ms)
[  PASSED  ] 20 tests.
```

Final state:
```
918:    scope.Stamp(own_mvcc != 0 ? own_mvcc : mvcc, authors_.IdxFor(author));
====================== 2 passed, 189 deselected in 6.10s =======================
[  PASSED  ] 167 tests.
ALL-RESTORED
```
`multi_master_test`: 229 passed, 1 skipped. clang-format 14 `--dry-run -Werror` and black `--check`
(the pre-commit cache binaries) are clean on every changed file.

### Vacuity

The report's list is accurate, with these additions:
- `PlainApplier…Unstamped`'s `repl_mvcc` assertion is vacuous under every mutant (I-2).
- `BadPayloadLeaves…`'s `failed == 1` holds under (c); only `k == good` is load-bearing there.
- In `NoTtlRestoreHasNoTtl`, the `-1` iteration passes without the INVALID_EXPIRE mapping. Only the
  `LLONG_MAX` iteration is load-bearing for (b).

The fake-master pytest's `k == good`, `after == 1` and `dbsize == 2` are vacuous, as the report says.

## Spec axis: missing or extra

- Every step of the plan is delivered. The deviations are the ones the report lists (R19, the
  future/old mvccs, the PTTL rounding bound) and they are sound. The `ClassicApplyFamilyTest.*`
  stamp tests live in `ClassicPeerApplyFamilyTest`, which is the peer applier fixture, as intended.
- Nothing extra beyond `IsMvccRestore` and the two `Note*` helpers.
- `IsKeyDbOnlyCommand` is unchanged.
- No upstream file is touched, `generic_family.cc` included.
- `--active_replica` off: a plain node now dispatches a `RESTORE … REPLACE ABSTTL` where it used to
  drop an unknown command. That is the feature, and only a classic link can carry the input, so the
  byte-identity constraint is not engaged.
- Untested but specified: D-7a's "`--multi_master_stream_lww=false` applies it in arrival order,
  still stamped". It follows from the existing `stamping()` / guard-bit split. The plan did not ask
  for a test.

## For the next task

- The spec D-13 order rule (I-1) applies to Task 2.8's seventh process-wide counter
  (`multimaster_envelope_time_fallback`) as well: put it where D-13 / decision 45 list it, and keep
  the pins honest about the order.
- `ClassicApplier::ApplyCommand` now has three outcomes for an MVCCRESTORE: a translation failure
  (`kSkipped`, which advances), a dispatched error (`kApplied`, which commits) and a dispatcher
  rejection (`kRejected`, no advance, decision 47). Task 2.8's envelope-time path must keep the stamp
  as `own_mvcc != 0 ? own_mvcc : mvcc` for MVCCRESTORE. Its time source is the key's mvcc, not the
  envelope's, if Task 2.8 derives a deadline or time from the stamp.
- Container gotcha: the bind mount can serve a stale view of a file that was rewritten seconds
  earlier. `grep` the in-container source before trusting a mutant run.

## Re-review (fix round 1)

Scope: the rulings in `task-2.6-fix1.md` and the "Fix round 1" section of the report. I re-read
`git diff 68612a5f -- src tests` and diffed it against the snapshots from my first pass (`/tmp/r26/*.good`).
The untracked `.bin` and capture script were not changed in this round. I edited no source. The M7
mutant was applied and then restored: `cmp` prints `ALL-RESTORED`, the in-container `grep` for the
mutation finds 0 matches, and the good binaries were rebuilt.

### Verdict: **Clean**

| ID | Ruling | Status |
|---|---|---|
| I-1 | Move to D-13's position. Fields and the four pins. | **Fixed.** The `kClassicCounters` row, the `ClassicLinkStats` field and the `ClassicLinkCounts` field all sit right after `keydb_cmds_dropped`. The pins were reordered: the gtest `ClassicLinkFields…` and `ClassicTotalSeries…` lists, both `multi_master_test` peer-line strings (`…keydb_cmds_dropped=2,keydb_mvccrestore_failed=6,classic_unknown_cmds_dropped=3…`), pytest `CLASSIC_COUNTERS`, and the two exact dicts in `test_scripted_active_master_exact_classic_counters`. The gtest comment "in the order of spec D-13" is true again. |
| I-2 | Capture `repl_mvcc` at dispatch time; falsify with M7. | **Fixed.** A second phase swaps RESTORE's handler, which records `server_conn_cntx()->repl_mvcc` and the call count. It runs last because it replaces the real handler for the rest of the test. The vacuous after-the-fact assertion is gone. M7 now fails this test (below). |
| M-1 | `keydb_cmds_dropped` comment and help. | **Fixed.** Both now mention MVCCRESTOREs of a cron job. |
| M-2 | Outcome-listing comments. | **Fixed:** `classic_apply_errors` (comment and help), `rreplay_unwrapped`, `Leaf::kSkipped`, and the `ApplyCommand` doc. Cosmetic nit, no action needed: the doc's reflow leaves a short line ("…like any other command. `author` (normalized) and"). clang-format accepts it. |
| M-3 | Raw `KEYDB.MVCCRESTORE` in the raw-path pytest. | **Fixed.** `test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter` streams a raw MVCCRESTORE of `k2` with a loadable payload. It asserts `unknown_keydb.mvccrestore` is in commandstats and `EXISTS k2 == 0`, and the exact `{"keydb_cmds_dropped": 2}` pins (INFO and `/metrics`) are unchanged, so it is not a `keydb_mvccrestore_failed` either. The gtest comment now names the pytest. |
| M-4 | README wording; gtest names frames by key. | **Fixed.** "The files"; the README says frame order follows KeyDB's dict order and that the table describes this file only; the gtest names `plain_key` and `hash`. |
| M-5 | Read the key only on a non-`kOk` result. | **Fixed.** The optional payload swap was not done, which the ruling allowed. The new comment ("A failure leaves `cmd` unchanged, so the key is still there to name") is accurate. |
| M-6 | One sentence on the parser. | **Fixed.** The `ParseSignedDecimal` comment covers both parsers. |

Nothing outside the rulings changed. The `classic_replay.cc` diff against my snapshot touches only
the row move, the two help strings, the parser comment and the key read.

### M7 against the fixed plain test (reproduced)

Mutation, added after the `authors_.stamping()` stamp: `else if (mvccrestore) scope.Stamp(own_mvcc
!= 0 ? own_mvcc : mvcc, link_origin_idx_);`. The in-container source was checked first.
```
923:  else if (mvccrestore)
[3/3] Linking CXX executable classic_replay_test
/home/user/drakeydb/src/server/classic_replay_test.cc:2163: Failure
    Which is: 500
    Which is: 0
[  FAILED  ] ClassicApplyFamilyTest.PlainApplierAppliesMvccRestoreVerbatimUnstamped (26 ms)
[  PASSED  ] 19 tests.
[  FAILED  ] ClassicApplyFamilyTest.PlainApplierAppliesMvccRestoreVerbatimUnstamped
```
This matches the implementer's recorded run. My first restore stalled on an interactive `cp -i`
alias on the host. I restored with `/bin/cp -f` and verified it (`cmp` clean, `grep -c` 0 in the
container) before rebuilding.

### Pins and suites, after the rebuild on the good source

```
[4/4] Linking CXX executable drakeydb
[  PASSED  ] 167 tests.                                   # classic_replay_test, full
[  PASSED  ] 229 tests.                                   # multi_master_test, full (1 skipped)
[       OK ] ClassicReplayTest.TranslateMvccRestoreOutputIsByteExact (0 ms)
[       OK ] ClassicReplayTest.ClassicLinkFieldsFollowTheRenderPredicate (0 ms)
[       OK ] ClassicReplayTest.ClassicTotalSeriesFollowTheRenderPredicate (0 ms)
[       OK ] ClassicApplyFamilyTest.PlainApplierAppliesMvccRestoreVerbatimUnstamped (24 ms)
[       OK ] PeerReplicationInfo.RendersCountsAndPeerLines (0 ms)
[       OK ] PeerReplicationInfo.ShowsClassicFieldsOnlyForClassicLinks (0 ms)
[       OK ] PeerReplicationInfo.OmitsClassicFieldsWhenMasterNotActiveAndCountersZero (0 ms)
```
Pytest, with `KEYDB_REQUIRED=1` under `flock`, `-k "mvccrestore or exact_classic_counters or
raw_keydb_only or show_classic_counters"`. The seven tests are `test_info_and_metrics_show_classic_counters`
(both modes), the mesh-merge test, the unloadable-payload test, `test_scripted_active_master_exact_classic_counters`
(both modes) and `test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter`:
```
====================== 7 passed, 184 deselected in 12.17s ======================
```
clang-format 14 `--dry-run -Werror` and black `--check` are clean on every changed file.
