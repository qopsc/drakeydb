# Review brief template (reviewer: Opus 5.5)

You are the **reviewer** for P7-2 Task 2.3 on branch `feat/phase7-2-author-stamps-dedup-guard`.
You do not edit source. You read, build, run, probe, and write findings. The orchestrator decides.

## Inputs

- The task section in `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` (the spec of
  the task), the spec sections it names, `decisions.md` (all rows, esp. 42–46), and the
  implementer's `task-2.3-report.md`.
- The diff: `git diff 5f1e011c` (BASE given below) — every hunk, not the report's summary of it.

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

`docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-2.3-review.md`: verdict
(**Clean** / **Needs fixes**), findings table (ID, severity, file:line, what, why, suggested fix),
the falsifications you reproduced with verbatim output, and anything the next task must know.
Reply with the verdict and the Critical/Important list.

## Task 2.3 specifics

- BASE `5f1e011c`. `git diff 5f1e011c -- src tests` (7 files, ~+1106/−46). Spec D-5 is binding.
- Orchestrator rulings (check they are implemented; do not re-litigate):
  - R10 (report OQ1, design note 4): a **rejected** leaf advances no layer, enclosing ones included,
    so that `B(A(m))` replayed on another link still gets its retry. Accepted. D-5's "an inner result
    of kConsumed advances the outer" is narrowed to "consumed and not rejected"; Task 2.5's docs
    commit amends the spec. Check the test pins the outer.
  - R11 (OQ2, note 3): reserve **before** stamp (a dropped or cancelled duplicate must cost no
    registry slot and no overflow count). Accepted; check the plan's cancelled-reserve test asserts
    "no counter moved" including `multimaster_keydb_author_overflow` and registry size.
  - R12 (OQ3): run the whole `keydb_onboarding_test.py` (`KEYDB_REQUIRED=1`, not `-m 'not slow'`
    unless a slow test needs a release binary: then say which were deselected) and the full
    `classic_replay_test` ×3 and `multi_master_test`.
  - R13 (OQ4): `Clear()` erasing idle entries is accepted.
- Probe especially: (a) **the reservation protocol under concurrency**: two fibers, same
  `(uuid, mvcc)`, one slow dispatch: exactly one `kApply` (reproduce the report's (a) gtest
  falsification yourself); a waiter whose `running()` flips mid-wait; `Release` from a leaf whose
  `mvcc == 0` must not touch another link's reservation; (b) **deadlock surface**: is `mu_` ever
  acquired while the caller holds another `fb2::Mutex` (e.g. `ForwardedAuthors`' or `PeerRegistry`'s)?
  Is `cv_.wait_for` called with the lock held correctly (`unique_lock`) and does `Reserve` re-look-up
  the entry after each wake (the map is not pointer-stable)? (c) **`HandleRreplay` rewrite**: with
  the "outers" list, is the layer-order of `SelectDb` unchanged vs Task 2.2's code (the db selected
  before the self check, D-3.3), and is `rreplay_unwrapped` still counted exactly as before for
  non-deduped layers (the P7-1 exact-counter pytests pass?); (d) the early-out at the layer level
  (design note 5): a nested `A(B(m))` where A is applied but B is not: is B's command lost? D-5 says
  outer early-out is what KeyDB has; confirm KeyDB's behaviour from `replication.cpp:5435-5485` and
  that the test `NestedOuterAdvancesWhenInnerDeduped` is the right direction; (e) the two `Clear()`
  sites: after `FlushAll`/`FlushSlots` in `InitiatePSync` only; confirm the DFLY-sync sites are
  correctly left alone and that a peer-mode classic sync (merge, no flush) does not clear (a test?);
  (f) eviction when every entry is in flight (`EvictionSkipsInflight`): does the map grow unbounded
  under a pathological N links × distinct authors? Bound reasoning only; (g) the test-helper change
  (fresh mvcc per `Envelope()`/`rreplay()`/`keydb_mvcc()`): did it weaken any P7-1 test that relied
  on a fixed mvcc (grep for literal mvcc expectations)? (h) why does the `ClassicAuthorMapTest`
  fixture now set `--active_replica`? Is it needed, and is it reset?
