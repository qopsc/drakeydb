# Task 2.7 report: clock-skew estimate on classic links

Implementer: Sonnet 5.5. Not committed. Container `drakeydb-p7`, `build-dbg`, `-j4`.

## Summary

A depth-1 RREPLAY envelope with `mvcc != 0` samples `(mvcc >> 20) - now_ms` at most once a second
(`ClockSkewSampler`, owned by the applier). The link's `clock_skew_ms_` holds the maximum of the
last 60 samples. The warning goes through the one existing `IsClockSkewConcerning` / `LOG_EVERY_T`
site, which moved into `Replica::PublishClockSkew` so Greet's handshake echo and the applier share it.
A DFLY peer's handshake echo is unchanged (same store, same warning, same text).

| | before | after |
|---|---|---|
| `classic_replay_test` | 167 | **172 pass** (+5) |
| pytest `-k clock_skew` | n/a | 2 passed (one `keydb` marker) |

Pre-commit (clang-format, black, pyflakes) passes on all changed files. `multi_master_test` and the
whole pytest file were not rerun (no INFO/metrics shape changed).

## What changed

| File | Change |
|---|---|
| `src/server/classic_replay.h` | `ClockSkewSampler` (60 slots, 1000 ms gap, `Sample`/`EstimateMs`); `ClassicApplier` gets an `on_skew` callback ctor arg (last, defaulted), `skew()` accessor, members `on_skew_`, `skew_` |
| `src/server/classic_replay.cc` | sampler implementation; the sample in `HandleRreplay` after the `kOk`, `running()` and self checks and before the dedup read and `ParseSingleCommand` (the shared 2.8/2.9 site), at `depth == 1 && env.mvcc != 0` |
| `src/server/replica.{h,cc}` | `PublishClockSkew(int64_t)` (store + the existing warning, moved verbatim from Greet); Greet calls it; `ConsumeRedisStream` passes `[this](int64_t s) { PublishClockSkew(s); }` |
| `src/server/classic_replay_test.cc` | 2 sampler tests + the 3 planned family tests; helper `MvccAhead` |
| `tests/dragonfly/keydb_onboarding_test.py` | `test_keydb_clock_skew_estimate_from_envelopes`, `test_keydb_clock_skew_estimate_against_real_keydb` (keydb marker), helper `minted_ahead` |

## Design notes

- Callback, not a pointer to the atomic: the callback keeps the warning in the one log site.
- Sample at the top of the loop body, so a deduped or later-malformed-inner envelope still samples
  (its stamp is a clock reading either way). Nested layers and kBadMvcc / `mvcc == 0` never sample.
- Clock going backwards (`now < last`) takes a sample instead of waiting.
- **Deviation:** the plan says "plain link", but a plain replica's INFO has no `clock_skew_ms`
  (only the peer `masterN:` line renders it). Both pytests use a peer-mode (`--active_replica`) node.
- `GetSummary` / `Greet`'s reset to 0 are unchanged: a reconnect clears the estimate, and the new
  applier starts with an empty window.
- The `skew != 0` assertion in the real-KeyDB test could in principle hit exactly 0; it passed.

## Tests

- `ClockSkewSamplerTest.SamplesMillisecondsAtMostOncePerSecond`, `.EstimateIsMaxOfLastSixty`
  (fake clock, table-driven).
- `ClassicApplyFamilyTest.SkewSampleUsesMvccMilliseconds` (0 before, then in (4000, 5000] for
  `now + 5000`), `.SkewSampleIgnoresNestedAndZeroMvcc` (outer current + inner +5000 gives |est| <=
  100; mvcc 0 and a depth-2 call give 0), `.SkewEstimateIsMaxOfWindow` (a second envelope inside one
  second does not sample; the window itself is the sampler test).
- pytest: fake master 60 s ahead gives `55000 <= clock_skew_ms <= 60000` and the log line
  `clock differs by N ms`; real KeyDB: `-2000 < clock_skew_ms <= 100`.
- The callback wiring (`Replica::PublishClockSkew`) is covered by the pytests only.

## Falsification (command: rebuild `classic_replay_test`, run `--gtest_filter=*Skew*`)

| Mutant | Result |
|---|---|
| read `mvcc`, not `mvcc >> 20` | fail: `SkewSampleUsesMvccMilliseconds` (`actual: 1879422901640446026 vs 5000`), `SkewSampleIgnoresNestedAndZeroMvcc`, both sampler tests |
| sample nested envelopes (drop `depth == 1`) | fail: `SkewSampleIgnoresNestedAndZeroMvcc` (`Which is: 5000` vs 0); other 4 pass |
| no once-a-second limit | fail: `SkewEstimateIsMaxOfWindow` (`9000` vs `3000`), `SamplesMillisecondsAtMostOncePerSecond` |
| last sample instead of max | fail: `SamplesMillisecondsAtMostOncePerSecond` (0 vs 5000), `EstimateIsMaxOfLastSixty` (-40 vs 3000) |
| restored | `[  PASSED  ] 5 tests.` |

Not falsified: the pytests (no mutant run against them; the fake-master one fails without the
feature since the field stays 0).

## Anchor drift

`replica.cc` Greet's skew block moved from `:510-521` to `:522-526` (a call). None other.

## Open questions

None.
