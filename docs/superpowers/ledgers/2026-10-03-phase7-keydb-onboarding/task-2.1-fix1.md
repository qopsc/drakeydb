# Task 2.1 fix round 1 (implementer: the Task 2.1 Sonnet agent, resumed)

Read `task-2.1-review.md` in full. Verdict: Needs fixes (0 Critical, 2 Important, 6 Minor).
Orchestrator rulings — implement all of these, nothing else:

- **I1** (pytest `test_keydb_restart_consumes_one_author_slot`): before the first KeyDB restart,
  force a same-uuid reconnect (prefer `CLIENT KILL TYPE replica`/`slave` on the KeyDB; fall back to
  the drakeydb side if KeyDB refuses), wait for the re-attach with the **same** `node_uuid`, assert
  `classic_link_uuid_changes == 0` and `multimaster_peer_registry_size == 2`. Then the restarts as
  before. Make the docstring say exactly what is asserted. Falsify with the reviewer's M8 (bump on
  any re-Greet): the test must now fail.
- **I2**: in `assert_no_classic_fields` for a non-peer node, assert both
  `multimaster_keydb_author_overflow` and `multimaster_peer_registry_size` are absent from INFO and
  `dragonfly_multimaster_keydb_author_overflow_total` / `dragonfly_multimaster_peer_registry_size`
  absent from `node.metrics()`. In the peer-mode / DFLY-peer absence tests pin R1 on purpose:
  gauge `== 2`, no overflow series. Falsify with the reviewer's M9 (predicate forced true in
  `metrics.cc`): the stock-master absence test must now fail.
- **m1, m2, m3, m5, m6**: apply the reviewer's suggested wording / marker exactly.
- **m4**: adopt the second option: in `IdxFor`, when `registry_->FindIdx(uuid)` already has the
  uuid (another link's own uuid, forwarded), skip `Admit` — it costs no new slot, so it must not
  consume the cap. Say so in the `ClassicAuthorMap` class comment. Add a gtest
  `ClassicAuthorMapTest.AnotherLinksOwnUuidDoesNotConsumeTheCap` (register uuid X in the registry
  as a second link would, set cap 0, resolve X through this map: its registry idx, no overflow) and
  falsify it (drop the `FindIdx` short-circuit).

Also, from the report's gaps: record in the report which tests pass with the feature removed for
I1/I2 as the review lists them, and the new falsifications (M8, M9, m4) verbatim.

Then: `~/.venvs/precommit/bin/pre-commit run --files <changed>`; rebuild `classic_replay_test
dragonfly`; run `classic_replay_test`, `multi_master_test`, and the pytest selection the reviewer
used (`-k "author_slot or exact_classic_counters or absent_for or no_classic_fields_between or
down_link_keeps_zero or series_outlive or show_classic_counters or raw_keydb_only_command"`).
Append a "## Fix round 1" section to `task-2.1-report.md` (what changed per finding, verbatim
falsification outputs, final counts). Reply with the summary. Do not commit.
