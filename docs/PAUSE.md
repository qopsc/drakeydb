# drakeydb: pause / resume file

> **Snapshot as of 2026-10-14.** This file tells you what is paused, how to resume each item, and
> what else is left to do. It is uncommitted on purpose: as an untracked file it survives branch
> switches.
>
> **The canonical docs are on the newest branch, not this checkout.** Read them with
> `git show origin/feat/phase7-1-rreplay-unwrap:docs/PLAN.md` (also `docs/ISSUE-REGISTER.md` and
> `docs/UPSTREAM-SYNC.md`). Where this file and those docs disagree, they win, except where §2a
> says otherwise.

| Ref | SHA | What |
|---|---|---|
| `origin/main` | `c60dfdb0` | P4-4 merged (#9). P0 through P4-4 are all merged (#1–#9) |
| `origin/feat/phase7-0-closeout-and-harness` | `dbc7e6c4` | PR #10, open. 18 commits on main |
| `origin/feat/phase7-1-rreplay-unwrap` | `55dadc32` | PR #11, open. Stacked: 61 commits on top of #10 |
| `feat/phase4-5-tombstone-lifecycle` (**local only**) | `369fb09a` | P4-5 / PR-B, paused. 63 commits on `c60dfdb0` |
| `p4-3-final-wave` (this checkout) | `2516d09e` | Stale. Its tree is identical to the P4-3 squash `31c6b136` and it has no P7 docs |

---

## 1. ⚠ At-risk work: secure it first

Some of this work exists in one place only. If that disk or directory goes, it is gone.

1. **`feat/phase4-5-tombstone-lifecycle` @ `369fb09a`** has 63 commits, 41 files, +11880/−1378.
   It is **not on origin**, has no PR, and is **not** contained in P7-0 or P7-1 (`git cherry`
   shows 63/63 missing). It is checked out in the worktree
   `.claude/worktrees/phase4-4-stream-lww`, which sits inside this checkout's untracked
   `.claude/` directory.
2. **The P4-5 SDD workspace** (162 files, gitignored, the only copy):
   `.claude/worktrees/phase4-4-stream-lww/.superpowers/sdd/p4-3-is-done-lets-drifting-duckling/`
   holds the PR-A ledger, `prb-replan.md`, `progress.md`, `global-constraints.md`, and the
   briefs and reports for B1–B18.
3. **The roasted-moth SDD workspaces** (the only copies):
   `~/.paseo/worktrees/2wtglncc/roasted-moth/.superpowers/sdd/`
   - `2026-08-23-phase3-origin-journal`
   - `2026-08-25-phase4-mvcc-lww`
   - `2026-08-29-phase4-2-rdb-mvcc-persistence`
   - `2026-08-30-phase4-3-merge-lww-tombstones` (marked "PRESERVING", needed for P4-4/P4-5)

Suggested actions. Nothing here has been done yet:

```bash
# Back up the branch only. Do not open a PR (see §2a for sequencing).
git push origin feat/phase4-5-tombstone-lifecycle

# Copy the ledgers somewhere outside the repo.
tar czf ~/drakeydb-sdd-backup-2026-10-14.tgz \
  -C .claude/worktrees/phase4-4-stream-lww .superpowers/sdd \
  -C ~/.paseo/worktrees/2wtglncc/roasted-moth .superpowers/sdd
```

The lock on the P4-5 worktree is stale: the pid holding it (1827) is not running.

---

## 2. Paused work and how to resume it

### 2a. P4-5 / PR-B: tombstone lifecycle (paused inside B15, fix round 5)

**Where everything is**
- **Branch:** `feat/phase4-5-tombstone-lifecycle` @ `369fb09a`, already rebased onto `c60dfdb0`
  (0 behind origin/main as of today).
- **Worktree:** `.claude/worktrees/phase4-4-stream-lww`. It has `build-dbg/`, `clang-dbg/` and
  `genfiles/`, and its working tree is clean.
- **Plan:** `~/.claude/plans/p4-3-is-done-lets-drifting-duckling.md`. Its §5 is superseded by
  `prb-replan.md` in the SDD workspace.
- **Scope:**
  - deadline-bearing expiry tombstones stored in the mvcc key's TTL word
  - `RDB_OPCODE_DF_TOMBSTONES_V2` (=230)
  - repl version 69
  - the D-20 fix, generalized to replayed removals of absent keys
  - the D-27 expired-delta veto, which drops only, applies to STRING tombstones only, and uses a
    string-delta allow-list
  - per-fiber MVCC arm ownership

**Task status** (the order follows ruling R36; there is no B6)

| Task | Status |
|---|---|
| B1, B2–B5, B7–B14 | ✅ complete (several took 2–3 fix rounds) |
| **B15** | 🟡 **Fix rounds 1–4 committed (ends `369fb09a`). Round 5 was dispatched (`task-B15-fix5.md`, base `369fb09a`) but produced no commits and no report** |
| B15b | ⬜ Brief written: no duplicate prime entry after an `AddOrFindInternal` yield |
| B16 | ⬜ Brief written: GC under load (idle task only) |
| B17 | ⬜ Brief written: heartbeat GC |
| B18 | ⬜ Brief written: docs and register, plus every deferred "MUST FIX before merge" minor and the R51/R53 comment hygiene |
| Whole-branch review, adversarial pass, gate, PR | ⬜ |

**B15 fix round 5 must deliver** (ruling R56, last ledger lines of `progress.md`)
- **F2 (Important):** move the `kKeyLive` check in `InstallAbsentKeyTombstone` to *first*, right
  after the db lookup. It currently sits after the `!TombstonesEnabled` and born-expired
  refusals, so the loader takes `kRefused` as final and an older revive survives (probes P2/P3).
- **F1 (Important):** streaming callers *can* get `kKeyLive`. The affected callers are
  `RecordAbsentKeyRemoval` for OpDelV2/OpRen/SerializeSrc, `generic_family.cc` ~:789/:942 and
  `string_family.cc` ~:1023. The fix:
  - correct the false "unreachable" comments
  - add a stat `mvcc_live_key_install_skips` (INFO + DEBUG MVCC)
  - register the class "a peer-link tx whose own lazy-expiry lookup yields can be overtaken by a
    concurrent loader install". The structural fix goes in as an owner-level follow-up after PR-B:
    the loader honours tx key locks, or peer-link applies pause per shard during a merge load.
- **Minors:**
  - N-a: the duplicate-key check copies every key into a `flat_hash_set` on live servers; use
    `prime.Find(key) != it`.
  - N-b: `rdb_load.h:622` is wrong.
  - N-c: the S>T comment overstates.
  - N-d: unconditional `kKeyLive` Disarm vs the "scoped to found_mutable" comment.
  - N-e: the report's line numbers are stale.

**To resume**
1. Secure the branch and the SDD dir (§1).
2. Re-read `prb-replan.md`, `global-constraints.md`, the tail of `progress.md`, and
   `task-B15-fix5.md`.
3. Re-dispatch fix round 5 to a fresh Sonnet implementer, then have Opus 5.5 re-review it, then
   continue B15b → B18 and the review passes.

**Conflicts to settle before or at resume**
- **The plan contradicts this branch.** P7-1's `docs/PLAN.md:20` says "the design spec's planned
  P4-5 (tombstones) shipped inside P4-3, so there is no separate P4-5". This branch disproves
  that.
- **Sequencing.** The register gives D-14, D-16, D-20, D-27 and D-29 to a *tombstone-lifecycle
  phase* that runs after P7 and after the upstream sync (P7 Task 4.5). That phase should
  **resume from this branch**, not start fresh. Fix PLAN.md when that is decided.
- **Rebase cost.** 17 files overlap with P7-1, including `db_slice.cc`, `rdb_load.cc`,
  `rdb_test.cc`, `multi_master_test.cc`, `mvcc.h`, `generic_family.cc`, `ISSUE-REGISTER.md` and
  `PLAN.md`. P4-5's `b86f0372` and P7-1's `6a28474d` fix the same RDB byte-scan flake in
  different ways: keep one.
- **Recheck at rebase:**
  - repl version 69 vs whatever P7 bumped
  - opcode 230; commit `8fb44243` flags 225 as the likely collision
  - the mixed-version notes

**Env trap recorded in the ledger:** never use `pgrep` wait loops (`until ! pgrep -x ninja`), not
even without `-f`. Defunct `[ninja]` zombies match, so the loop never ends. Run ninja in the
foreground.

### 2b. Watch on PR #10

Check-ins were stopped on 2026-10-04 after three quiet ones. They resume on the next GitHub event
or owner message.

---

## 3. Blocked on Jon

### PRs
1. **Review and squash-merge #10 (P7-0).** It is `MERGEABLE`, with no reviews submitted. Every
   check that can run is green. The one `test_replicaof_reject_on_load` failure was a documented
   race and passed on re-run.
2. **Then #11 (P7-1).** Its base is `main`, so it carries all of P7-0's commits (79 total).
   *Inference, not recorded in the ledger:* after #10 squash-merges it will need
   `git rebase --onto origin/main dbc7e6c4 feat/phase7-1-rreplay-unwrap`, the same as P4 needed.
   All 4 CodeRabbit threads are resolved.
3. **CI noise on both PRs:**
   - `fuzz-pr` and `large-tests-arm` are cancelled after a 24 h wait for a runner. The fork has
     no `CI-LARGE-86` or `CI-LARGE-ARM` runners, and these cancellations make the PRs `UNSTABLE`.
   - On #11, `cgroup-thread-detection` was cancelled by a GitHub hosted-runner failure. Re-run
     it.
   - **Todo:** gate or skip the runner-less jobs in the fork's CI so PRs stop showing
     `UNSTABLE`.

### Decisions pending
- **D-13:** is the `SORT STORE` divergence under a `BY` pattern worth the larger journal entry?
  D-18 is parked behind the same call.
- **D-11 vs D-12:** narrow the classic ctime exposure, or keep accepting it.
- **D-21, D-28, D-30, D-31:** each has "Owner: open". Fix only if wanted.
- **At the upstream sync:** which SORT journaling model to keep (see the `generic_family.cc` row
  in UPSTREAM-SYNC.md). The D-5 byte-identity exception flips there.
- **Whether to file upstream:**
  - U-1 through U-7 (not fixed, not filed)
  - D-25 (candidate report)
  - the 13 fork-fixed U-items: U-9 through U-15 and U-17 through U-22
- **P7-3 hazard (j):** count drops at flush time, or accept and document the double count.
- **§2a:** how to reconcile P4-5 with the PLAN.

---

## 4. Next up, in roadmap order

Each item cites the P7 plan's task numbers. The full detail is in
`docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` on the P7-1 branch.

1. **Merge #10 and #11** (§3).
2. **P7-2:** `feat/phase7-2-author-stamps-dedup-guard`
   - 2.1 `ClassicAuthorMap` with a `--classic_author_cap` flag. ← **first task**
   - 2.2 Per-command stamps on peer links.
   - 2.3 `AuthorDedup`: process-wide dedup per author uuid.
   - 2.4 LWW guard on classic peer links: `ClassicApplyRewrites` and EVAL handling, then a
     peer-mode perf re-run. A perf failure reopens Task 1.6, the squasher fallback.
   - 2.5 Close D-1.
   - 2.6 Apply `KEYDB.MVCCRESTORE`, LWW-guarded (decision 22).
   - 2.7 Clock-skew estimate on classic links.
   - 2.8 Envelope-time apply (decisions 27 and 30).
   - 2.9 Sweep clock and read-path hide, covering `SCAN`/`KEYS`/`RANDOMKEY` (decisions 29, 30
     and 33). This narrows D-32.
   - Gate, then PR.
3. **P7-3:** `feat/phase7-3-classic-partial-psync`
   - 3.1 `--classic_partial_psync`, covering hazards (a) through (k).
   - 3.2 A peer-mode partial resync skips the merge load.
   - Gate: also run `replication_test.py` and the hazard tests ×10.
4. **P7-4:** `feat/phase7-4-keydb-rdb-extras-docs-exit`
   - 4.1 Skip RDB type 64 (`KEYDB.CRON`).
   - 4.2 Member-TTL aux, aux noise, and a throttle for bit-63 stamps.
   - 4.3 D-8 precedence test.
   - 4.4 Operator docs: the "Onboarding from KeyDB" section, `differences.md`, the UPSTREAM-SYNC
     rows, and register updates.
   - 4.5 Phase exit gate, including a release-build perf run in plain and peer mode.
5. **Upstream-sync PR.** Overdue: there has been no sync since the fork point `29e6bea`
   (2026-08-19). The D-5 exceptions flip here, and U-16 is planned to land here.
6. **Tombstone-lifecycle phase.** Resume §2a.
7. **P8, mesh hardening and ops.**
   - *The stub is stale.* The reciprocal tiebreak (P3), the FAILOVER/REPLTAKEOVER/DFLY TAKEOVER
     refusal (P2) and `clock_skew_ms` (P4-0) have already shipped.
   - Still left:
     - `--replica_quorum` serve-stale gating
     - per-peer lag metrics and per-peer Prometheus metrics (deferred from P2)
     - a 4-node chaos pytest
8. **P9, CI, docs and release.**
   - the full `drakeydb-ci.yml` (a minimal one landed in P7-0)
   - helm mesh values
   - the first `drakey-0.1.0` tag

---

## 5. Small follow-ups (no phase owns these)

**P7 loose ends**
- **M-8** from P7-1's whole-branch review: the fake-master tests duplicate their settle/ACK
  scaffolding, and the SORT pytest never asserts its two-shard placement.
- **Flaky test:** make `test_simultaneous_reciprocal_replicaof_converges` deterministic. It also
  flakes on `main`.
- **Task 0.9:** the 200/200-under-load run and falsification (b) were never run.
- **Stale checkboxes** in the P7 plan file: "P7-0 gate and PR" (3 boxes), "P7-1 gate and PR" (2),
  and Task 0.1's "Reviewer pass". The ledger shows all of these as done.

**Other follow-ups**
- Confirm the `MarkForClose` null-`conn()` finding is registered (P7 Task 4.4).
- Convert member TTLs to native ones (Task 4.4 registers this).
- An optional 7th counter for envelope-time fallback, if wanted.
- Task 2.4's `SET XX KEEPTTL` on plain links: "revisit if the review asks".

**Phase-3 limitations never marked closed**
- The sticky `apply_failed_` stops LSN adoption. `ReportError` would make it prompt.
- The guard against plain-replica misconfiguration is only a `DCHECK`.
- The legacy classic loader never gets `SetApplyOrigin`. P7 makes it carry real data.
- The real redis-binary interop test skips locally ("watch CI").

---

## 6. Issue-register snapshot

`docs/ISSUE-REGISTER.md` on the P7-1 branch is canonical. It has 52 entries. The D-numbers
D-2, D-3, D-6, D-7 and D-17 do not exist; D-17 (flush epoch) was moved out of P4.

**Open, to fix: 19**

| Owner | Items |
|---|---|
| P7 | D-1 (no mvcc for classic links, Task 2.5), D-8 (both stamp forms for one key untested, Task 4.3), D-32 (TTL-less orphan on a plain replica of an active KeyDB; Tasks 2.8/2.9 narrow it) |
| Tombstone-lifecycle phase (§2a) | D-14 (cross-shard tombstone dispatch steals an arm), D-16 (expiry tombstone born GC-eligible), D-20 (newer DEL of an absent key doesn't advance an older tombstone), D-27 (pre-deadline delta re-creates the key without a TTL), D-29 (an unstamped value's expiry leaves no tombstone, so it can be resurrected) |
| No owner, fix path known | D-11, D-13\*, D-15, D-18\*, D-19, D-21\*, D-22, D-28\*, D-30\*, D-31\* (\* = waiting on an owner call, §3) |
| After P7, with the upstream sync | U-16 (a blocking command in a classic stream stalls the link and shutdown) |

**Everything else**
- **Upstream bugs, not fixed and not filed (7):** U-1 through U-7.
- **Accepted / documented (8):**
  - D-5: byte-identity exceptions, which flip at the upstream sync
  - D-9
  - D-10: narrow multi-shard coverage
  - D-12: classic ctime resurrection
  - D-23: identical-stamp divergence, for future CRDT work
  - D-24
  - D-25: PFMERGE phantom
  - D-26
- **Fixed, resolved or withdrawn (18):**
  - fixed in the fork: U-9 through U-15 and U-17 through U-22
  - withdrawn: U-8
  - resolved: D-4, D-33, D-34, D-35 (D-34 and D-35 keep documented SORT residuals)

---

## 7. Cleanup (after #10 and #11 merge; none of it is urgent)

**Local branches** whose trees are identical to merged squashes. They are safe to delete.
`git switch` off `p4-3-final-wave` first.

| Branch | Tip | Same tree as |
|---|---|---|
| `p4-3-final-wave` | `2516d09e` | `31c6b136` (#8) |
| `pr-8` | `fb3d6ebe` | ancestor of `2516d09e` |
| `feat/phase4-mvcc-lww` | `77e801e3` | `1b6a2e82` (#5) |
| `feat/phase4-1-mvcc-store` | `aac40cce` | `5c470c91` (#6) |
| `feat/phase4-2-rdb-mvcc-persist` | `fc0dcb1e` | `e3ccbc8a` (#7) |
| `feat/phase4-3-merge-lww-tombstones` | `8f4c1ec9` | content is in `ad581c62` ⊂ main |
| `feat/phase4-4-stream-lww` | `eefc24fb` | `c60dfdb0` (#9) |

**Other cleanup**
- Local `main` (`e3ccbc8a`) is 2 commits behind `origin/main`. Fast-forward it.
- **roasted-moth worktree:**
  - Its `.git` file points to the deleted
    `/private/tmp/drakeydb-clean/repo/.git/worktrees/p43-wt-meta`, so plain git fails inside it.
  - Its 35 dirty entries match `fb3d6ebe` exactly, so there is no unique code in it.
  - Back up its SDD workspaces (§1), then `git worktree prune` or remove it.
- `origin/claude/relaxed-franklin-b0qhqn` is redundant. Its 3 commits are patch-equivalent to
  P7-1's `dfbe84d3`, `c6ebff72` and `32a63f89`. Delete it after #11 merges.
- Optional: delete the remote branches of merged phases (`feat/phase3-*`, `feat/phase4-*`).
- After the merges, move this checkout onto `main` so the P7 docs are on disk.

---

## 8. Future / v2 (non-goals for now)

- Per-peer auth and TLS.
- Serving reads and writes during a merge (`allow-write-during-load`).
- `--multi_master_protect_flush`.
- Forwarding topologies; v1 is mesh-only with no forwarding.
- CRDT semantics (the D-23 class).
- A per-key TTL stamp (D-23/D-24).
- Partial resume on peer REMOVE+ADD; today it always does a full resync.
- KeyDB outbound interop. It is one-way by design: KeyDB length-checks the `REPLCONF UUID`
  reply.

---

## 9. Resuming: environment and process

**Build and test environment**
- **Linux builds:** OrbStack (`orbctl start`), arm64, image `drakeydb-build:deps-p2`.
  - Containers **need** `--security-opt seccomp=unconfined`, or io_uring aborts.
- **pytest:**
  - `DRAGONFLY_PATH` **must be absolute**.
  - Run a full `ninja` before `ctest -L DFLY`.
  - redis-py is pinned below 8 (version 8's retry behaviour caused the baseline failures).
- **pre-commit:** the venv at `~/.venvs/precommit` has pre-commit, clang-format 14 and black.

**Git and GitHub**
- `gh` defaults to the upstream parent. **Always** pass
  `--repo darkspadez/drakeydb --base main --head <branch>`.
- Paseo worktrees start without the helio submodule: run `git submodule update --init`.
- `KeyDB/` is a gitignored reference clone that lives only in this main checkout.

**Process**
- SDD: Sonnet writes the code. Opus 5.5 does every review: per task, whole-branch, and one
  **independent adversarial pass per PR** briefed only to refute the convergence claim.
- **Every LWW test must assert *which value survived*,** not just that the nodes agree.
- **Treat every new test as vacuous until a falsification proves otherwise.** P3 had 7 vacuous
  tests and P4-0 had 3.
- Non-active nodes stay byte-identical to upstream. Every behaviour change is gated on
  `IsActiveReplica()`.

---

## 10. Where things live

| What | Where |
|---|---|
| Canonical plan + phase status | `docs/PLAN.md` (read it on `origin/feat/phase7-1-rreplay-unwrap`) |
| Issue register | `docs/ISSUE-REGISTER.md` (same branch) |
| Upstream-sync guide | `docs/UPSTREAM-SYNC.md` (same branch) |
| P7 plan | `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` (P7 branches) |
| P7 spec | `docs/superpowers/specs/2026-10-03-phase7-keydb-onboarding-design.md` (P7 branches) |
| P7 ledger (decisions 1–41, progress, task reports) | `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/` (P7 branches) |
| P4-4/P4-5 plan | `~/.claude/plans/p4-3-is-done-lets-drifting-duckling.md` (§5 superseded) |
| P4-5 replan + ledger | `.claude/worktrees/phase4-4-stream-lww/.superpowers/sdd/p4-3-is-done-lets-drifting-duckling/` |
| Older SDD ledgers (P3–P4-3) | `~/.paseo/worktrees/2wtglncc/roasted-moth/.superpowers/sdd/` |
| Branding policy | `BRANDING.md` |
