# Task 2.2 fix round 1 (implementer: the Task 2.2 Sonnet agent, resumed)

Read `task-2.2-review.md` in full. Verdict: Needs fixes (0 Critical, 2 Important, 5 Minor).
Rulings — implement all, nothing else:

- **I1**: replace `NeedsPeerNode()` with the reviewer's second fixture exactly as probed:
  `class ClassicPeerApplyFamilyTest : public ClassicApplyFamilyTest` whose `SetUp` sets
  `--active_replica` then calls the base `SetUp`; base `TearDown` resets the flag. Move the 5 peer
  `TEST_F`s to it; delete `NeedsPeerNode`, the `SetFlag` line and the `absl/strings/match.h` include;
  the fixture comment moves onto the new class. The plan's test names stay reachable by filter.
- **I2**: new test `ClassicPeerApplyFamilyTest.SkippedCommandsNeverResolveTheirAuthor` (or the
  reviewer's name): under `--classic_author_cap=0`, an unknown command, a KeyDB-only command and a
  `PING`, each from a fresh author: `registry.Size()` unchanged, overflow delta 0, `FindIdx(fresh)`
  empty. Falsify with the reviewer's EARLY mutant (resolve before the unknown check): it must fail.
- **m1, m2**: the reviewer's suggested comment wording.
- **m3**: add the probe as `ClassicPeerApplyFamilyTest.PeerEnvelopeWithoutMvccIsALocalMintOfTheAuthor`
  (mvcc-0 envelope: local mint, author's origin hash, registry +1, context restored). Falsify by
  stamping the link's origin for mvcc 0.
- **m4**: keep `link_origin_idx_`; add to its comment that it equals `ClassicAuthorMap::link_idx_`
  in production (both are `peer_origin_idx_`) and why the applier reads the context rather than
  the map (a default map has no link idx).
- **m5**: correct design note 1 in the report ("created after the control/KeyDB-only return").

Then host `~/.venvs/precommit/bin/pre-commit run --files <changed>`; rebuild `classic_replay_test
dragonfly`; run `classic_replay_test` in full and the pytest `-k "stamped_with_author_hash or
author_slot or exact_classic_counters"`. Append "## Fix round 1" to `task-2.2-report.md` (per
finding, verbatim falsification outputs for I2 and m3, final counts). Reply with the summary. Do
not commit.
