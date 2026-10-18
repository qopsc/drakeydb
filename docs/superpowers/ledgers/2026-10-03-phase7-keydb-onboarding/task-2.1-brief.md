# Task 2.1 brief: `ClassicAuthorMap` (implementer: Sonnet 5.5)

You are the implementer for **Task 2.1** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`
(worktree: this checkout; HEAD is `origin/main` @ `0f79bf5a` plus uncommitted ledger edits).
The orchestrator commits; **you do not commit or push**.

## Read first, in this order

1. `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` — "Global Constraints" (lines
   30–93), "Verified anchors" (94–153), and **Task 2.1** (the section under "## P7-2"). The task
   section is the spec of your work: interfaces, tests, falsifications, commit subject.
2. `docs/superpowers/specs/2026-10-03-phase7-keydb-onboarding-design.md` — D-4 (apply context,
   authors, stamps; the "Author cap"), D-13 (observability: how a counter is rendered).
3. `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/decisions.md` — decisions 4, 7, 9,
   15, 18, 19, 42–46; and `task-1.3-report.md` for how counters were plumbed (`kClassicCounters`,
   `ClassicTotals()`, `ClassicLinkCounts`, the per-field predicate), since you add two process-wide
   values (`multimaster_keydb_author_overflow`, the gauge `multimaster_peer_registry_size`) and one
   per-link counter (`classic_link_uuid_changes`).
4. `src/server/classic_replay.{h,cc}`, `src/server/replica.cc:430-600` (Greet: uuid, admission,
   `peer_origin_idx_`, the `AwaitBrief` registration at `:586-589`), `src/server/multi_master.h`
   (`PeerRegistry`), `src/server/mvcc.{h,cc}` (`RegisterOriginHash`, `OriginHash`, `NodeUuidHash`).
5. `CLAUDE.md` (style; read before edit; `util::fb2` only; no `std::regex`).

Anchors in the plan were taken at `c60dfdb`; `src/` has moved since (P7-0, P7-1). Re-verify every
line number before editing and note drift in your report.

## Environment (this is new: read carefully)

- Your shell runs on macOS in the worktree. **All builds and tests run inside the Docker container
  `drakeydb-p7`**, where this worktree is mounted at `/home/user/drakeydb`. Prefix every build/test
  command with `docker exec drakeydb-p7 bash -lc '...'`, e.g.

  ```
  docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb && ninja -C build-dbg -j4 classic_replay_test dragonfly'
  docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb/build-dbg && ./classic_replay_test --gtest_filter="ClassicAuthorMapTest.*"'
  docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb && DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly KEYDB_SERVER_PATH=/root/keydb-bin/keydb-server KEYDB_REQUIRED=1 flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k author_slot -x -q'
  ```

- Never more than `-j4`. Run ninja in the **foreground**; never poll with `pgrep` loops.
- A full build may still be finishing when you start: `docker exec drakeydb-p7 tail -1 /root/ninja-full.log`
  shows progress; `NINJA-EXIT=0` is the end. Wait for it before your first build.
- Formatting runs on the host: `~/.venvs/precommit/bin/pre-commit run --files <changed files>`
  (clang-format 14, black). Run it before you report.
- The gtest fixture you need (`MultiMasterFamilyTest`-style with proactors) exists in
  `src/server/multi_master_test.cc`; `classic_replay_test.cc` already has `ClassicApplyFamilyTest`
  (a `BaseFamilyTest`). Reuse; do not invent a new harness.

## What to deliver

Exactly the Task 2.1 section of the plan, Steps 1–5, with these clarifications:

- `ClassicAuthorMap` lives in `classic_replay.{h,cc}`, owned by the link's `ClassicApplier` (or by
  `Replica` and handed to the applier; pick the one with the smaller `replica.cc` hunk and say why).
  It needs the link's own uuid and `peer_origin_idx_`, the `PeerRegistry*` (null in tests ⇒ stamping
  off, `IdxFor` returns the link idx), and access to `shard_set->pool()->AwaitBrief`.
- Only **Task 2.1** is in scope: resolving authors, the cap, the counters, the uuid-change counter,
  the gauge. Do **not** set `repl_mvcc`/`repl_origin_idx` per command yet (that is Task 2.2).
  `IdxFor` is called from nowhere in production yet except where you need it for the tests; say so.
- `--classic_author_cap`: `ABSL_FLAG(uint32_t, classic_author_cap, 4096, "...")` in
  `classic_replay.cc`. Overflow bumps `multimaster_keydb_author_overflow`, returns the link idx,
  `LOG_EVERY_T(WARNING, 60)`.
- Counters: follow the Task 1.3 pattern exactly (one row in `kClassicCounters` for the per-link
  one; the process-wide counter and the gauge rendered beside `multimaster_lww_dropped` in the
  active-node INFO block and in the plain-replica block, and in `/metrics` as
  `multimaster_keydb_author_overflow_total` and `multimaster_peer_registry_size` (gauge keeps its
  name), under the `ActiveKeyDbMasterSeen() || value != 0` predicate of decision 25). Extend the
  existing exact-equality INFO/metrics pins accordingly and say which tests you touched.
- `classic_link_uuid_changes`: `Replica` keeps the uuid of the previous completed `Greet()` in a
  member that the per-Greet clear at `replica.cc:478` does not touch; bump when a completed
  `Greet()` presents a different non-empty uuid.

## Falsification (Step 4) — mandatory, recorded verbatim

For each listed mutant: apply it, rebuild only the affected targets, run the named test, paste the
failing output, restore, rebuild, rerun, paste the passing output. Also record for each new test
what it would still pass under with the feature removed.

## Report

Write `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-2.1-report.md` in the
shape of `task-1.3-report.md`: Summary, What changed (file table), Design notes, Tests
(name → what it pins), Falsification table (verbatim commands and outputs), Anchor drift,
Open questions. Keep the diff list exact; the reviewer works from it.

When done, reply with: the list of changed files, the test counts before/after, and the path of the
report. Do not commit.
