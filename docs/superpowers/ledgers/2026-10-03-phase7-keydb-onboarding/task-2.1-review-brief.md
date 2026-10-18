# Review brief template (reviewer: Opus 5.5)

You are the **reviewer** for P7-2 Task 2.1 on branch `feat/phase7-2-author-stamps-dedup-guard`.
You do not edit source. You read, build, run, probe, and write findings. The orchestrator decides.

## Inputs

- The task section in `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` (the spec of
  the task), the spec sections it names, `decisions.md` (all rows, esp. 42–46), and the
  implementer's `task-2.1-report.md`.
- The diff: `git diff 0f79bf5a` (BASE given below) — every hunk, not the report's summary of it.

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

`docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-2.1-review.md`: verdict
(**Clean** / **Needs fixes**), findings table (ID, severity, file:line, what, why, suggested fix),
the falsifications you reproduced with verbatim output, and anything the next task must know.
Reply with the verdict and the Critical/Important list.

## Task 2.1 specifics

- BASE is `0f79bf5a` (`git diff 0f79bf5a -- src tests`); the ledger .md edits are the orchestrator's.
- Orchestrator rulings already made, do not re-litigate, do check they are implemented as stated:
  - R1: the gauge `multimaster_peer_registry_size` renders on `ActiveKeyDbMasterSeen() || Size() > 1`
    (report design note 5 / open question 1): accepted, byte-identity for a stock master wins.
  - R2: overflow is counted per resolution (open question 2): accepted; Task 2.2 wording will say
    "envelopes stamped as the link because of the cap".
  - R3: the cap set stays process-wide with the test reset hook (open question 3): accepted.
- Probe especially: (a) the `ForwardedAuthors` `fb2::Mutex` critical section — is `AwaitBrief`
  ever called while it is held (a fiber-blocking call under a mutex other fibers need)? (b) the
  per-link memo vs a second link resolving the same author while the first's `AwaitBrief` is in
  flight: can `IncomingStamp`'s `DCHECK(hash != 0)` fire? (c) `last_greeted_master_uuid_` across
  a failed `Greet()` followed by a successful one to the same master (no false bump) and across a
  `REPLICAOF` to a different classic master (bump or not? the brief says "a different uuid" and the
  counter's comment says "a KeyDB restart" — is the comment honest?); (d) the `Metrics::Print`
  signature change: every caller, and whether `--active_replica` off output is byte-identical
  (run `RdbTest.NoMvccOpcodeOrAuxWhenInactive`-style existing pins and the INFO/metrics exact
  tests); (e) the pytest's `multimaster_peer_registry_size == 2 + restarts` — is 2 right for a
  single-link peer-mode node (self + link) and is it asserted before the first restart?
- The report's "full-file pytest" failure `test_keydb_active_live_write_during_full_sync[peer_mode]`
  is being triaged separately on the unmodified binary; do not spend time on it beyond confirming
  it is not touched by this diff.
