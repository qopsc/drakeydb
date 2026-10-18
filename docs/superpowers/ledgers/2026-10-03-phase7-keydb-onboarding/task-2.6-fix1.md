# Task 2.6 fix round 1 (implementer: the Task 2.6 Sonnet agent, resumed)

Read `task-2.6-review.md` in full. Verdict: Needs fixes (0 Critical, 2 Important, 6 Minor).
Rulings — implement all, nothing else:

- **I-1**: move `keydb_mvccrestore_failed` to D-13's position, right after `keydb_cmds_dropped`
  (the `kClassicCounters` row, and the `ClassicLinkStats`/`ClassicLinkCounts` fields for
  readability); reorder the four exact pins (gtest, `multi_master_test` peer line, pytest
  `CLASSIC_COUNTERS` and the exact dicts) to match.
- **I-2**: capture `repl_mvcc` at dispatch time in `PlainApplierAppliesMvccRestoreVerbatimUnstamped`
  (swap RESTORE's handler through the fixture's `SetHandler` helper and record `cntx->repl_mvcc`
  inside it), keep the name; falsify with the reviewer's M7 (stamp every MVCCRESTORE on a plain
  link): the test must now fail. Record it in the report's vacuity list.
- **M-1, M-2**: comment/help wording as the reviewer suggests (the `keydb_cmds_dropped` help and
  comment, `classic_apply_errors`, `rreplay_unwrapped`, `Leaf::kSkipped`, the `ApplyCommand` doc).
- **M-3**: add the raw `KEYDB.MVCCRESTORE` to the raw-path pytest at the line the reviewer names;
  assert the key is absent, `unknown_keydb.mvccrestore` in commandstats, and the exact
  `classic_fields` pin unchanged.
- **M-4**: README wording ("The files"; frame order follows KeyDB's dict order and differs between
  captures); the gtest comment names the frames by key.
- **M-5**: read the key only on a non-`kOk` result; optionally the swap to save a payload copy —
  do the first, do the second only if it is ≤ 15 lines and you re-run `OutputIsByteExact`.
- **M-6**: observation only; add one sentence to the parser comment (leading zeros and `-0` are
  accepted, erring toward applying; KeyDB never emits them).

Then host pre-commit; rebuild `classic_replay_test multi_master_test dragonfly`; run both gtests
in full and the pytest `-k "mvccrestore or exact_classic_counters or raw_keydb_only or
show_classic_counters"`. Append "## Fix round 1" to `task-2.6-report.md` with the I-2 falsification
verbatim and final counts. Reply with the summary. Do not commit.
