# Review brief template (reviewer: Opus 5.5)

You are the **reviewer** for P7-2 Task <N> on branch `feat/phase7-2-author-stamps-dedup-guard`.
You do not edit source. You read, build, run, probe, and write findings. The orchestrator decides.

## Inputs

- The task section in `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` (the spec of
  the task), the spec sections it names, `decisions.md` (all rows, esp. 42–46), and the
  implementer's `task-<N>-report.md`.
- The diff: `git diff <BASE>` (BASE given below) — every hunk, not the report's summary of it.

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

`docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-<N>-review.md`: verdict
(**Clean** / **Needs fixes**), findings table (ID, severity, file:line, what, why, suggested fix),
the falsifications you reproduced with verbatim output, and anything the next task must know.
Reply with the verdict and the Critical/Important list.
