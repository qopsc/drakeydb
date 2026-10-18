# Review brief template (reviewer: Opus 5.5)

You are the **reviewer** for P7-2 Task 2.4 on branch `feat/phase7-2-author-stamps-dedup-guard`.
You do not edit source. You read, build, run, probe, and write findings. The orchestrator decides.

## Inputs

- The task section in `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` (the spec of
  the task), the spec sections it names, `decisions.md` (all rows, esp. 42–46), and the
  implementer's `task-2.4-report.md`.
- The diff: `git diff 52b35e47` (BASE given below) — every hunk, not the report's summary of it.

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

`docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-2.4-review.md`: verdict
(**Clean** / **Needs fixes**), findings table (ID, severity, file:line, what, why, suggested fix),
the falsifications you reproduced with verbatim output, and anything the next task must know.
Reply with the verdict and the Critical/Important list.

## Task 2.4 specifics

- BASE `52b35e47`. `git diff 52b35e47 -- src tests docs` (8 files, ~+712/−98). Spec D-4.2–4.4, D-6.
- Read `task-2.3-review.md` "For the next task (2.4)" and check each item landed (setters after
  `Reserve`; a rewrite never changes the reserved `(author, mvcc)`).
- Orchestrator rulings:
  - R15: the peer-mode release perf failure (report "Step 5") is **out of this review's verdict**:
    the code is reviewed on spec and quality; the perf finding is handled by a separate Task 1.6
    investigation after this review. But **confirm the finding's attribution**: on the existing
    `build-opt/dragonfly` (release, this tree) run the peer-mode perf test once and the plain-mode
    control once (`DRAKEYDB_PERF=1`, the Task 1.5 harness, nothing else running: check `uptime`
    load < 0.5 first), and read KeyDB's log for the `omem` drop. Then, with
    `--multi_master_stream_lww=false`, once more in peer mode. Record the three outcomes verbatim.
    Do **not** rebuild `build-opt` with diagnostic edits (the implementer did; take its word for
    the compiled-out variants unless your runs contradict it).
  - R16: `test_unwrap_flushes_raw_batch_before_envelope[peer_mode]`'s adjustment (envelope mvccs an
    hour ahead so the guard does not veto them) is accepted **if** the test's purpose (raw batch
    flushed before the envelope) is still what it asserts; verify it still fails under the P7-1
    mutant it was written for (no pre-envelope flush; see `task-1.2-report.md` falsification (a)).
- Probe especially: (a) `ClassicApplyRewrites` table completeness vs D-6: `SET k v GET` alone,
  `SET k v NX PX 100` (PX value is not an option), `SET k v KEEPTTL NX` (order), a value literally
  `"nx"`, `MSETNX` with an odd arg count (must not crash, just rewrite the name), `SET` with fewer
  than 3 args; (b) `Unguard` scope: after an EVAL that throws or is rejected, is the link's guard
  bit restored (the destructor path)? Is `repl_lww_guard` copied onto the Transaction at
  `PrepareTransaction` time, so the per-dispatch override is what the stub sees (`transaction.cc`
  stub copy)? (c) the `multimaster_lww_dropped` accounting on a vetoed envelope: counted once, and
  the dedup `Commit`s a vetoed command (it "ran", the veto is the command's own outcome) so the
  author's watermark advances — is that right per D-5/D-6? Say what the code does and whether a
  vetoed stale write replayed on another link is dropped by dedup or re-vetoed; (d) the raw path:
  a raw `SET k v NX` on the same stream is **not** rewritten (D-6: raw streams untouched); pinned?
  (e) `docs/multi-master.md` rewritten section: accurate against the code (guard needs peer mode +
  `--active_replica` + flag; plain never; EVAL stamped unguarded; raw never)?
