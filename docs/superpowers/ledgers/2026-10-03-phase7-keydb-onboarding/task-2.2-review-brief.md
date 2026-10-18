# Review brief template (reviewer: Opus 5.5)

You are the **reviewer** for P7-2 Task 2.2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
You do not edit source. You read, build, run, probe, and write findings. The orchestrator decides.

## Inputs

- The task section in `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` (the spec of
  the task), the spec sections it names, `decisions.md` (all rows, esp. 42–46), and the
  implementer's `task-2.2-report.md`.
- The diff: `git diff 1dd8d035` (BASE given below) — every hunk, not the report's summary of it.

## Environment

Builds and tests run in Docker container `drakeydb-p7` where this worktree is `/home/user/drakeydb`:
`docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb && ninja -C build-dbg -j4 <targets>'`.
Pytest: `DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly KEYDB_SERVER_PATH=/root/keydb-bin/keydb-server
KEYDB_REQUIRED=1 flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest …`.
Never more than `-j4`; no `pgrep` loops. You may write throwaway probe tests, but revert them.

## Two axes, every finding tagged Critical / Important / Minor

1. **Spec:** does the code do exactly what the task section and the spec say (interfaces, counters,
   predicates, gating on `IsActiveReplica()` / peer mode, byte-identity of `--active_replica` off)?
   What did the brief ask for that is missing? What was done that the brief did not ask for?
2. **Quality:** CLAUDE.md style; `util::fb2` only; yields and lock scopes on the fiber paths;
   comments true to the code (a comment that overstates is a finding); no process text ("fix
   round", finding IDs) in src/ or tests/; upstream-owned hunks minimal.

## Falsification audit

Re-run at least two of the report's falsifications yourself (apply mutant, build, run, restore). A
test that passes under its mutant is a Critical. Say which tests would be vacuous with the feature
removed, and whether the report says so.

## Output

`docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-2.2-review.md`: verdict
(**Clean** / **Needs fixes**), findings table (ID, severity, file:line, what, why, suggested fix),
the falsifications you reproduced with verbatim output, and anything the next task must know.
Reply with the verdict and the Critical/Important list.

## Task 2.2 specifics

- BASE is `1dd8d035` (Task 2.1 + sync-window + CI commits are in). `git diff 1dd8d035 -- src tests`.
- Read `task-2.1-review.md`'s "For the next task (2.2)" and "For later tasks" first and check each
  item landed (IdxFor after the self drop, kSelfIdx guard, over-cap mvcc kept, comment updated).
- Orchestrator rulings (check they are implemented, do not re-litigate):
  - R6 (report OQ1): a 3-argument envelope (`mvcc == 0`) keeps the author's origin with a local mint,
    as spec D-4.1 literally says ("0 means local mint and never guarded"; the origin is the author's).
    No-forward still holds (origin != self). Accepted as built.
  - R7 (OQ2): `NeedsPeerNode()` by test name is accepted **only if** you find no cheaper correct
    alternative; if a second fixture (`ClassicPeerApplyFamilyTest : ClassicApplyFamilyTest` with
    `--active_replica`) is a ≤ 30-line change that keeps the plan's test names reachable, say so as an
    Important and the fix round does it. Your call on the evidence.
  - R8 (OQ3): run the whole `keydb_onboarding_test.py` once (`KEYDB_REQUIRED=1`, ~6 min here) and
    `classic_replay_test` + `multi_master_test` in full; the implementer ran a subset.
- Probe especially: (a) `CommandScope` vs the raw path: a raw (non-envelope) command of the same
  stream after an envelope sees `repl_mvcc == 0` and the link origin (the restore), including when
  `ApplyCommand` throws or `Dispatch` longjmps (does anything in `DispatchCommand` escape by
  exception?); (b) the stamp is set **after** the unknown-command check but **before** `Dispatch`:
  is anything between `Stamp` and the transaction's `SetReplOrigin` copy (`PrepareTransaction`) able
  to yield and let another command of this context run? (the context is single-fiber per link, say
  so if true); (c) `link_origin_idx_` read at construction vs `ConsumeRedisStream` re-setting
  `repl_origin_idx` later on the same context (a reconnect builds a new applier? verify); (d) the
  pytest: does it assert origins by *value* (`NodeUuidHash` of the known KeyDB uuids) or only by
  inequality? If only inequality, is that enough to catch "stamped with the forwarder" (F2 shows
  it is caught: confirm why); (e) a nested envelope whose **outer** author is over the cap and inner
  is not, and vice versa.
