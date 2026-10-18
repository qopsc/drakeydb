# Task 2.6 report: `KEYDB.MVCCRESTORE` applied, LWW-guarded

Implementer: Sonnet 5.5. Not committed. Container `drakeydb-p7`, `build-dbg`, `-j4`.

## Result

| | before | after |
|---|---|---|
| `classic_replay_test` | 148 (166 minus the 18 tests added; the old `KeyDbMvccRestoreCountedUnknownUntilItIsApplied` was replaced) | **167 pass** |
| `multi_master_test` | 229 pass, 1 skipped | 229 pass, 1 skipped (pins extended) |
| `keydb_onboarding_test.py` (whole file) | n/a | **187 passed, 4 skipped** (perf, `DRAKEYDB_PERF`), 424 s |

The "before" for `classic_replay_test` is derived (166 - 18), not measured at HEAD.
The pre-commit hooks (clang-format, black, pyflakes) pass on all changed files.

Mesh-merge resync time (`REPLICAOF A` on B until all 2005 keys are on D and D2): **0.55 s**
(logged by the test; the Task 1.5 release bar is for the stream, not this resync).

## Changed files

- `src/server/classic_replay.{h,cc}`: `MvccRestoreParse`, `TranslateMvccRestore`, `IsMvccRestore`,
  `ClassicLinkStats::keydb_mvccrestore_failed`, a `kClassicCounters` row (after
  `classic_apply_errors`), `ClassicApplier::NoteMvccRestore`/`NoteMvccRestoreFailed`, and the hook
  in `ApplyCommand`: after the KeyDB-only skip, before the unknown-command lookup. A non-`kOk`
  result returns `kSkipped`, so the envelope is consumed and `Advance`d only. The stamp is
  `own_mvcc != 0 ? own_mvcc : envelope mvcc`. The dedup reservation is still the envelope's. An
  error reply counts as `keydb_mvccrestore_failed`, not `classic_apply_errors`.
- `src/server/replica_types.h`: `ClassicLinkCounts::keydb_mvccrestore_failed`. INFO (plain block
  and peer line) and `/metrics` render from the `kClassicCounters` table, so they needed no edit.
- `src/server/classic_replay_test.cc`, `src/server/multi_master_test.cc`: tests and pins.
- `tests/dragonfly/keydb_onboarding_test.py`: the two pytests; `CLASSIC_COUNTERS` and the
  exact-counter pins (the scripted stream's `KEYDB.MVCCRESTORE "payload"` is now
  `keydb_mvccrestore_failed 1`, `classic_unknown_cmds_dropped 1`).
- `tests/dragonfly/tools/capture_keydb_mvccrestore.py` (new): the Step 0 probes.
- `tests/dragonfly/data/keydb_v6.3.4_mvccrestore_stream.bin` (774 bytes) and a README row/section.

`TranslateMvccRestore` reimplements the length/version check because `GetRdbVersion` is anonymous
in `generic_family.cc` (no edits there, per the brief).

## Step 0 probes (verbatim, from `capture_keydb_mvccrestore.py`)

Setup: KeyDB 6.3.4 (`/root/keydb-bin/keydb-server`).

### (a) Golden vector provenance

Active KeyDB X (`active-replica yes`, `multi-master yes`, forwarding on) with a raw replica
attached, then `REPLICAOF Z` on X. Z is an active KeyDB holding `ttl_key` (`PX 3600000`),
`plain_key` and a hash. The stream the raw replica saw after X's empty RDB is the golden file.

```
merge sync of 3 keys reached X's replica in 0.58 s
=== probe (a): 774 bytes after X's RDB (0 before REPLICAOF Z) ===
X uuid: 9193f791-7934-457c-b3fb-03ff1eeb9810; handshake: [('PING', '+PONG'), ('REPLCONF listening-port 6399', '+OK'), ('REPLCONF capa eof capa psync2', '+OK active-replica'), ('REPLCONF UUID df953c20-ac8b-4d69-9730-dbe592b92616', '+9193f791-7934-457c-b3fb-03ff1eeb9810'), ('PSYNC ? -1', '+FULLRESYNC d6dd04c48e6f6f7be612e3152d6227d1b88ef9e7 0')]
[0] offset=0 length=259
    RREPLAY uuid=9193f791-... db=0 mvcc=1879414190994620416 inner=[KEYDB.MVCCRESTORE plain_key 1879414190390640643 9223372036854775807 <payload>]
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\n9193f791-7934-457c-b3fb-03ff1eeb9810\r\n$158\r\n*5\r\n$17\r\nKEYDB.MVCCRESTORE\r\n$9\r\nplain_key\r\n$19\r\n1879414190390640643\r\n$19\r\n9223372036854775807\r\n$56\r\n\000\013plain-value\372\013mvcc-tstamp\0231879414190390640643\011\000M\264\210Z\233\333A\202\r\n\r\n$1\r\n0\r\n$19\r\n1879414190994620416\r\n"
[1] offset=259 length=266   KEYDB.MVCCRESTORE hash 1879414190390640644 9223372036854775807 <68-byte payload>
[2] offset=525 length=249   KEYDB.MVCCRESTORE ttl_key 1879414190390640642 1792352643265 <54-byte payload>
```

Findings:
- One envelope per key, three keys.
- A key without a TTL carries `9223372036854775807` (INVALID_EXPIRE); a TTL'd key carries an
  absolute ms deadline.
- The command's own `<mvcc>` (`...0640643`) is older than the envelope's (`...4620416`).
- The payload is KeyDB's DUMP form: it carries an `mvcc-tstamp` aux before the footer. drakeydb's
  `RESTORE` accepts it (`GoldenKeyDbMvccRestoreEnvelopesApply`).

### (b) KeyDB `DUMP` then drakeydb `RESTORE key 0 payload REPLACE ABSTTL`

```
string: type byte 0, footer version 9, len 46 -> b'OK'; drakeydb type b'string'
int: type byte 0, footer version 9, len 47 -> b'OK'; drakeydb type b'string'
list: type byte 14, footer version 9, len 97 -> b'OK'; drakeydb type b'list'
biglist: type byte 14, footer version 9, len 18880 -> b'OK'; drakeydb type b'list'
set: type byte 2, footer version 9, len 51 -> b'OK'; drakeydb type b'set'
intset: type byte 11, footer version 9, len 59 -> b'OK'; drakeydb type b'set'
zset: type byte 12, footer version 9, len 71 -> b'OK'; drakeydb type b'zset'
hash: type byte 13, footer version 9, len 68 -> b'OK'; drakeydb type b'hash'
stream: type byte 15, footer version 9, len 104 -> b'OK'; drakeydb type b'stream'
```

### (c) `RESTORE` TTL edge values on drakeydb

```
ABSTTL 0: b'OK'; PTTL -1; EXISTS 1
ABSTTL 1: b'OK'; PTTL -2; EXISTS 0
ABSTTL -1: ERR value is not an integer or out of range; EXISTS 0
ABSTTL 9223372036854775807: b'OK'; PTTL 268435455000; EXISTS 1
ABSTTL 1792352646890: b'OK'; PTTL 3599996; EXISTS 1
ABSTTL 1792349045890: b'OK'; PTTL -2; EXISTS 0
relative PTTL 3600000: b'OK'; PTTL 3600000
relative 0: b'OK'; PTTL -1
```

This confirms the spec table:
- INVALID_EXPIRE passed through is a deadline about 8.5 years out (`PTTL 268435455000`).
- `ABSTTL 0` means no TTL, so KeyDB's 0 must become 1 (born expired).
- A negative `ABSTTL` is an error, so negatives must map to 0.

## Tests

- `ClassicReplayTest.TranslateMvccRestore{MapsExpire,ExtractsOwnMvcc,RejectsMalformed,
  RejectsUnloadablePayload,ClassifiesCronPayloadAsDrop,OutputIsByteExact}`.
- `ClassicApplyFamilyTest`: `MvccRestoreIsTranslatedAndApplied`,
  `PlainApplierAppliesMvccRestoreVerbatimUnstamped`, `NoTtlRestoreHasNoTtl`,
  `GoldenKeyDbMvccRestoreEnvelopesApply` (frames 0 and 1 of the golden vector),
  `AbsoluteTtlRestoredAsDeadline`, `BadPayloadLeavesResidentKeyCountsAndAdvancesWatermark`,
  `CronPayloadDroppedCountedAsKeydbCmdsDropped`, `MalformedMvccRestoreIsCountedAndConsumed`,
  `LoaderFailureCountsAsMvccRestoreFailedNotApplyError`.
- `ClassicPeerApplyFamilyTest`: `MvccRestoreStampsOwnMvccWithAuthorHash`,
  `MvccRestoreFallsBackToEnvelopeMvccWhenOwnInvalid`,
  `MvccRestoreFallbackUnderEveryUnusableOwnMvcc`, `StaleMvccRestoreLosesToNewerLocalWrite`,
  `NewerMvccRestoreWinsOverOlderLocalWrite`.
- pytest: `test_keydb_mvccrestore_from_keydb_mesh_merge_applies` (keydb marker) and
  `test_keydb_mvccrestore_unloadable_payload_skipped_and_counted` (plain link; run under `flock`).

Deviations from the brief:
- `.PlainApplierAppliesMvccRestoreVerbatimUnstamped` asserts a verbatim apply over a newer local
  key and `repl_mvcc == 0`, not the stamp. The plain fixture has no stamp reader.
- The stale and newer tests use a future envelope mvcc and an old own mvcc (and the reverse)
  instead of 700/500/800. A real local stamp is ms-based, far above those small numbers, so small
  numbers could not tell falsification (a) apart.
- `AbsoluteTtlRestoredAsDeadline` allows PTTL up to 3'601'000 because PTTL rounds up. The first run
  of the test, with a 3'600'000 bound, failed on exactly that and was fixed.

## Falsification

Each was done one at a time against the good source (backup `/tmp/p26/classic_replay.cc.good`),
rebuilt, run, then restored. The build is back on the good source (`cmp` clean; 167 pass).
Failing lines are from the `grep -aE "Failure|Which is|FAILED"` output of the runs.

| | Mutation | Fails |
|---|---|---|
| a | stamp with `mvcc` (envelope's) | gtest `MvccRestoreStampsOwnMvccWithAuthorHash` (900 vs 500); `StaleMvccRestoreLosesToNewerLocalWrite` (local overwritten by `from-keydb`, `multimaster_lww_dropped` 0 vs 1); `NewerMvccRestoreWinsOverOlderLocalWrite`. pytest mesh-merge: `assert equals failed -'from-A' +'newer-on-D'` on the `stale` check |
| b | `if (expire < 0)` (no INVALID_EXPIRE mapping) | gtest `TranslateMvccRestoreMapsExpire` (`"9223372036854775807"` vs `"0"`), `NoTtlRestoreHasNoTtl` (PTTL 268435455000), `GoldenKeyDbMvccRestoreEnvelopesApply`. pytest mesh-merge: `-268435454947 +-1` on `plain_key`'s PTTL |
| c | drop the `rdbIsObjectTypeDF` check | gtest `TranslateMvccRestoreRejectsUnloadablePayload` (`kOk(0)` vs `kBadPayload(4)`); `BadPayloadLeavesResidentKey...` (`k` gone: D-31) |
| c2 | type 64 returns `kBadPayload` | gtest `TranslateMvccRestoreClassifiesCronPayloadAsDrop` (4 vs 5); `CronPayloadDroppedCountedAsKeydbCmdsDropped` (dropped 0 vs 1, failed 1 vs 0) |
| d | `Unguard()` for MVCCRESTORE | gtest `StaleMvccRestoreLosesToNewerLocalWrite` only (value `from-keydb`, stamp replaced, `lww_dropped` 0 vs 1) |
| e | hook off (`mvccrestore = false && ...`) | gtest `MvccRestoreIsTranslatedAndApplied`, `PlainApplier...Verbatim...`, `NoTtlRestoreHasNoTtl`, golden, peer stamping tests. pytest: mesh-merge `1 == 2005` (D stays empty); fake-master `assert 0 == 1`; scripted exact-counters (both modes) |
| f | no `keydb_mvccrestore_failed` bump | gtest `BadPayloadLeavesResidentKey...` (0 vs 1), `MalformedMvccRestoreIsCountedAndConsumed` (0 vs 3), `LoaderFailureCounts...` (0 vs 1). pytest fake-master `assert 0 == 3` and the exact-counters pins |

What each test would still pass under without the feature:
- The fake-master pytest's `k == good`, `after == 1`, `dbsize == 2`, link up and exact offsets pass
  vacuously: the unknown-command path skips the four envelopes too. The load-bearing assertions
  are `keydb_cmds_dropped == 1`, `keydb_mvccrestore_failed == 3` and `classic_unknown_cmds_dropped
  == 0` (it fails under (e) with `assert 0 == 1`, though that run stopped at the first assert).
- The plain-link gtests (`PlainApplier...`, `NoTtl`) pass under (a), (c) and (d): none of those
  touch plain behaviour.
- `LoaderFailureCountsAsMvccRestoreFailedNotApplyError` passes under (a)-(e) except where the hook
  is off. It exercises only the error-reply accounting.
- (c)'s resident-key assertion is gtest only. The fake-master pytest has no resident `k` that a
  type-99 payload could erase *and* be observed after, since `k == good` is also true when the
  skip happens at the unknown path.

## Notes for review

- A garbage-bodied type-0 payload with a valid footer passes the pre-checks and fails in the loader.
  `RESTORE` replies an error cleanly (no abort), and with `REPLACE` the resident key is deleted
  first, as D-31 describes. The test pins the accounting only (`failed 1`, `apply_errors 0`,
  watermark committed). The key loss is the documented residual.
- Dedup is process-wide and monotone per author, so the tests give each envelope its own mvcc
  (a fixed 900 is used once per test).

## Fix round 1

Rulings of `task-2.6-fix1.md` (review `task-2.6-review.md`), applied as ruled. Not committed.

- **I-1.** `keydb_mvccrestore_failed` now follows `keydb_cmds_dropped` (the `kClassicCounters` row, the
  `ClassicLinkStats` and `ClassicLinkCounts` fields). Reordered the pins: the two gtest lists in
  `classic_replay_test.cc`, the two peer-line strings in `multi_master_test.cc`, pytest
  `CLASSIC_COUNTERS` and the two exact dicts.
- **I-2.** `PlainApplierAppliesMvccRestoreVerbatimUnstamped` keeps its name and its verbatim-apply
  half. A second phase swaps RESTORE's handler (`SetHandler`, last, since it replaces the real one)
  and records `server_conn_cntx()->repl_mvcc` at dispatch time: it must be called once and see 0.
  The old after-the-fact `repl_mvcc == 0` assertion is gone.
- **M-1, M-2.** Updated the `keydb_cmds_dropped` comment and help (cron-job MVCCRESTOREs),
  `classic_apply_errors` comment and help, `rreplay_unwrapped`, `Leaf::kSkipped` and the
  `ApplyCommand` doc.
- **M-3.** The raw-path pytest (`test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter`)
  now streams a raw `KEYDB.MVCCRESTORE k2` with a loadable payload. It asserts `k2` is absent,
  `unknown_keydb.mvccrestore` is in commandstats, and the exact `{"keydb_cmds_dropped": 2}` pin is
  unchanged. The gtest comment points at it.
- **M-4.** README "The files"; frame order follows KeyDB's dict order and changes between captures;
  the gtest comment names the frames by key.
- **M-5.** The key is read only on a non-`kOk` result. I did not do the optional payload-copy swap.
- **M-6.** One sentence on `ParseSignedDecimal` (leading zeros and `-0` accepted; KeyDB never emits them).

### I-2 falsification (reviewer's M7)

Mutation, added after the `authors_.stamping()` stamp in `ApplyCommand`:

```
  else if (mvccrestore)
    scope.Stamp(own_mvcc != 0 ? own_mvcc : mvcc, link_origin_idx_);
```

In-container source checked first (`923:  else if (mvccrestore)`). Run of
`--gtest_filter="*MvccRestore*:*NoTtlRestore*:*AbsoluteTtl*:*BadPayload*:*CronPayload*"`:

```
/home/user/drakeydb/src/server/classic_replay_test.cc:2163: Failure
Expected equality of these values:
    Which is: 500
    Which is: 0
[  FAILED  ] ClassicApplyFamilyTest.PlainApplierAppliesMvccRestoreVerbatimUnstamped (24 ms)
[  PASSED  ] 19 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicApplyFamilyTest.PlainApplierAppliesMvccRestoreVerbatimUnstamped
```

Restored (`cmp` clean) and rebuilt. Vacuity list: the old assertion was vacuous under every
mutant; it is replaced, and this is now the only test that fails under M7. The other 19 MVCCRESTORE
tests pass under M7.

### Final state

- pre-commit (clang-format, black, pyflakes): clean on all changed files.
- `classic_replay_test`: **167 passed** (no test added or removed). `multi_master_test`: **229
  passed**, 1 skipped.
- pytest `-k "mvccrestore or exact_classic_counters or raw_keydb_only or show_classic_counters"`:
  **7 passed**, 184 deselected.
