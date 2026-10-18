# Task 2.7 brief: clock-skew estimate on classic links (implementer: Sonnet 5.5)

You are the implementer for **Task 2.7** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
The orchestrator commits; **you do not commit or push**. Tasks 2.1–2.6 are committed; read
`task-2.6-report.md` and the "For the next task" notes of the 2.4 and 2.6 reviews first.

## Read, in order

1. Plan: Global Constraints; **Task 2.7** (interfaces: a depth-1 envelope with `mvcc != 0` samples
   `(mvcc >> 20) - now_ms` at most once a second; the link keeps the **maximum of the last 60**
   samples in the existing `clock_skew_ms_` atomic; warns through `IsClockSkewConcerning`; a DFLY
   peer's handshake echo is untouched; every test name).
2. Spec D-13 "Clock skew" paragraph, D-1.17.
3. `task-2.1-brief.md` "Environment". Same rules; the container is free.
4. Code: `replica.cc` `clock_skew_ms_` (set at `:512` from the DFLY handshake, reset at `:403`),
   `IsClockSkewConcerning` and its `LOG_EVERY_T` site, `GetSummary` (`clock_skew_ms`),
   `HandleRreplay`'s depth-1 point where Task 2.8 will evaluate the envelope time once (after
   `kOk`, `running()`, the self check; before `ParseSingleCommand`): **take the sample there**, so
   2.8 and 2.9 can share the same site. `MvccClock` layout (`ms << 20 | counter`, `mvcc.h`) and
   KeyDB's (`server.h:960`).

## Deliver

Exactly Task 2.7 Steps 1–5, with:

- The sampler lives in `classic_replay.{h,cc}` (a small `ClockSkewSampler` the applier owns:
  `Sample(mvcc, now_ms)` at most once per second, a 60-slot ring, `EstimateMs()` = max of the
  window; pure and table-testable), and the applier publishes `EstimateMs()` into the link's
  atomic through a callback `Replica` hands it (`std::function<void(int64_t)>` or a pointer to the
  atomic — pick the smaller `replica.cc` hunk). The warning goes through the existing path; do not
  add a second log site.
- Only depth-1 envelopes with `mvcc != 0` sample; nested layers and raw commands do not.
- Not in scope: the envelope-time apply (2.8), the stream clock (2.9).

## Tests

The plan's names: `ClassicApplyFamilyTest.SkewSampleUsesMvccMilliseconds`,
`.SkewSampleIgnoresNestedAndZeroMvcc`, `.SkewEstimateIsMaxOfWindow` (table-drive the sampler with a
fake clock too), pytest `test_keydb_clock_skew_estimate_from_envelopes` (fake master: an envelope
minted 60 s ahead → `clock_skew_ms` ≈ +60000 and the warning in the log; real KeyDB on the same
host → `-2000 < clock_skew_ms <= 100`). Falsify per Step 4 (read `mvcc` instead of `mvcc >> 20`;
sample nested envelopes), plus: sample more than once a second (the window test fails) and keep
the last instead of the max.

## Report

`task-2.7-report.md`, same shape. Reply with changed files, test counts before/after, report path.
Do not commit.
