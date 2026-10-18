# Review brief template (reviewer: Opus 5.5)

You are the **reviewer** for P7-2 Task 2.6 on branch `feat/phase7-2-author-stamps-dedup-guard`.
You do not edit source. You read, build, run, probe, and write findings. The orchestrator decides.

## Inputs

- The task section in `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` (the spec of
  the task), the spec sections it names, `decisions.md` (all rows, esp. 42–46), and the
  implementer's `task-2.6-report.md`.
- The diff: `git diff 68612a5f` (BASE given below) — every hunk, not the report's summary of it.

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

`docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-2.6-review.md`: verdict
(**Clean** / **Needs fixes**), findings table (ID, severity, file:line, what, why, suggested fix),
the falsifications you reproduced with verbatim output, and anything the next task must know.
Reply with the verdict and the Critical/Important list.

## Task 2.6 specifics

- BASE `68612a5f` (ledger-only commits since `e9af87a4`; the code base is `b79bd492`). Diff:
  `git diff 68612a5f -- src tests` plus the two untracked files
  `tests/dragonfly/data/keydb_v6.3.4_mvccrestore_stream.bin` and
  `tests/dragonfly/tools/capture_keydb_mvccrestore.py`. Spec D-7a is binding.
- Rulings:
  - R18 (report "Notes for review", the garbage-bodied type-0 payload): the resident key being
    deleted by `RESTORE … REPLACE` before the loader fails is **the documented D-31 residual**, not
    a Task 2.6 defect: the brief's pre-checks (length, footer version, type byte) are what D-7a
    asks for, and a CRC check was ruled out. Confirm the accounting test pins what it claims, and
    confirm `docs/ISSUE-REGISTER.md` D-31 already describes this shape (if not, that is a Minor for
    the docs commit). Do not ask for a CRC check.
  - R19: the `PlainApplierAppliesMvccRestoreVerbatimUnstamped` deviation (asserts the verbatim
    apply over a newer local key and `repl_mvcc == 0`, not the stamp) is accepted: a plain node
    has no side table to read a stamp from.
- Probe especially: (a) `TranslateMvccRestore` byte-exactness on a payload containing `\r\n`, NULs
  and a key with a space (the plan's `OutputIsByteExact`), and the **arity**: exactly 5 arguments,
  4 and 6 rejected; (b) `<expire>` parsing edge cases: `+5`, ` 5`, `5x`, `-0`, `9223372036854775808`
  (overflow), and the mapping table (negative → 0, `LLONG_MAX` → 0, `0` → 1, else kept); (c)
  `<mvcc>` edge cases: `0`, `1<<63`, all-ones, overflow, non-numeric → `kBadMvcc` vs `*own_mvcc == 0`
  with `kOk`; which is which per the plan; (d) the hook placement: before the unknown-command
  lookup and after the KeyDB-only skip; a **raw-path** `KEYDB.MVCCRESTORE` (not in an envelope) is
  NOT translated and stays on upstream's unknown accounting (D-7a); pinned? (e) the stamp:
  `own_mvcc` with the envelope author's hash; the dedup reservation keyed on the **envelope's**
  `(uuid, mvcc)`; a replayed MVCCRESTORE envelope on a second link is deduped; (f) the counter
  rendering: `keydb_mvccrestore_failed` appears in INFO plain block, peer line and `/metrics` with
  the exact pins extended; (g) the golden vector's provenance script is deterministic enough to
  regenerate (ports, uuids) and the README row explains the three frames; (h) the mesh-merge
  pytest: it asserts `PTTL ttl_key` in a real window, `plain_key == -1`, `stale` differs on D vs
  D2, and `keydb_mvccrestore_failed == 0` on both; run it ×3 for flakiness (it starts two KeyDBs
  and two drakeydbs).
