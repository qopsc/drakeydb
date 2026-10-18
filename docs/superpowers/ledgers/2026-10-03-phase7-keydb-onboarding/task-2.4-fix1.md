# Task 2.4 fix round 1 (implementer: the Task 2.4 Sonnet agent, resumed)

Read `task-2.4-review.md` in full. Verdict: Needs fixes (0 Critical, 1 Important, 8 Minor).
Rulings — implement all, nothing else:

- **I1**: retire the stale invariants exactly where the reviewer lists them, with its suggested
  wording: `conn_context.h:371-377` (second setter `ConsumeRedisStream`; true on a classic peer
  link; "never reset per command, except `ClassicApplier`'s per-dispatch `CommandScope`"),
  `test_utils.h:119-125`, `transaction.h:396-398` and `:739-741` ("raw classic command"),
  `docs/multi-master.md:140-142`.
- **M1**: EVAL sentence in `docs/multi-master.md` peer-only; "two things on every link" for the
  rewrites alone.
- **M2**: the two comments: single-shard script → SQUASHED_STUB → tripwire, as suggested.
- **M3**: `absl::StartsWithIgnoreCase(name, "EVAL")` so `EVAL_RO`/`EVALSHA_RO` are covered; a
  one-line comment that KeyDB 6.3.4 never sends them.
- **M4**: pin the raw path: FakeClassicMaster, plain and peer: raw `SET k old`, raw `SET k new NX`,
  marker; `k == old`. Falsify by routing raw commands through `ClassicApplyRewrites` (or by a
  throwaway rewrite in `DispatchSquashedBatch`'s input): it must fail.
- **M5**: `RewriteSet` builds `kept` only when the first pass found something to drop.
- **M6**: replace the three "(Task 2.4)" references in the pytest with what they mean.
- **M7**: move the bare-`MSETNX` row to `ClassicApplyRewritesMsetnxBecomesMset`.
- **M8**: add the behavioural check after the EVAL in `EvalStamped…` (a stale `SET e x` on the same
  link is vetoed: `e == from-script`).

Environment note: the worktree's git store was rebuilt (same path, same files; `git status` works
again) and `helio/` was re-cloned, so the first `ninja` rebuilds helio objects (a few minutes).
`build-opt` is untouched. Do **not** run the perf test in this round.

Then host pre-commit; rebuild `classic_replay_test dragonfly`; `classic_replay_test` in full,
`multi_master_test`, and the pytest `-k "lww or nx or msetnx or eval or not_forwarded or
conditional_set or raw"`. Append "## Fix round 1" to `task-2.4-report.md` with the verbatim M4
and M8 falsification outputs and final counts. Reply with the summary. Do not commit.
