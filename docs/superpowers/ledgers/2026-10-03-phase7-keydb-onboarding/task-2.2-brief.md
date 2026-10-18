# Task 2.2 brief: per-command stamps on peer links (implementer: Sonnet 5.5)

You are the implementer for **Task 2.2** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
The orchestrator commits; **you do not commit or push**. Task 2.1 is committed on this branch
(`ClassicAuthorMap`, `ClassicApplier::authors()`); read its report `task-2.1-report.md` first.

## Read, in order

1. Plan `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md`: Global Constraints,
   Verified anchors, **Task 2.2**, and the first paragraph of Task 2.8 ("whichever of the three
   lands first adds one RAII scope in `ApplyCommand` that sets the fields before the dispatch and
   puts them back on every exit"). **You are the first of the three: add that RAII scope.**
2. Spec D-4 (points 1 and the restore in 3; point 2's guard bit and point 3's rewrites and point 4's
   EVAL are **Task 2.4, not yours**), D-13.
3. `task-2.1-brief.md` for the environment (container `drakeydb-p7`, `/home/user/drakeydb`, `-j4`,
   flock, pinned venv, pre-commit on the host). Same rules.
4. `src/server/classic_replay.{h,cc}` (`HandleRreplay`'s layer loop, `ApplyCommand`),
   `src/server/conn_context.h:360-380` (`repl_origin_idx`, `repl_mvcc`, `repl_lww_guard` and their
   comments), `src/server/replica.cc` around `:1217` (`ConsumeRedisStream` sets
   `conn_context.repl_origin_idx = peer_origin_idx_`), `src/server/main_service.cc` where
   `PrepareTransaction` copies `repl_*` onto the transaction (`SetReplOrigin`), `src/server/mvcc.cc`
   `:203` (how a stamp is minted from `{repl_mvcc, OriginHash(origin_idx)}`), `debugcmd.cc` `DEBUG
   MVCC <key>` output format, and `MvccStoreTest` in `multi_master_test.cc` (an `--active_replica`
   fixture with `StampOf`-style helpers; reuse or lift the helper).

## Deliver

Exactly Task 2.2 Steps 1–5, with:

- **Peer mode only.** The applier learns whether it stamps: a `bool peer_mode` (or the presence of a
  registry in `authors_`, if you can show that is equivalent in production: `Replica` builds the
  map with `peer_mode_ ? registry : nullptr`) — pick one, state it, and make the plain-mode gtest
  pin it.
- **Author = innermost layer's uuid; mvcc = innermost layer's mvcc.** `HandleRreplay` re-declares
  `env` per layer; carry the innermost author/mvcc into `ApplyCommand` (an argument, not a member
  written per layer, unless you can show the member is simpler and as safe).
- **RAII scope in `ApplyCommand`**: sets `cntx_->repl_mvcc = env.mvcc` and `cntx_->repl_origin_idx
  = authors_.IdxFor(uuid)` before the dispatch, restores `repl_mvcc = 0` and `repl_origin_idx =
  <link idx>` on every exit (early returns for control/KeyDB-only/unknown commands, rejected
  dispatch, error reply). The link idx to restore to is what `ConsumeRedisStream` set: read it once
  at applier construction (`cntx_->repl_origin_idx`), do not hardcode `peer_origin_idx_`.
  The scope is the one Tasks 2.4 and 2.8 will extend (guard override, `repl_time_ms`); name and
  place it so they can.
- `IdxFor` is called once per dispatched inner command (so `multimaster_keydb_author_overflow`
  counts envelopes stamped as the link; say so in the counter's help text if it does not already).
- Not in scope: the guard bit, rewrites, EVAL handling (2.4); `KEYDB.MVCCRESTORE`'s own mvcc (2.6);
  envelope time (2.8). Do not touch `transaction.*`, `multimaster_lww.*`.

## Tests (Step 1 of the plan, exactly those names)

`ClassicApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash`,
`.NestedEnvelopeStampsWithInnerAuthor`, `.PlainApplierLeavesMvccZero`,
`.RestoresRawContextAfterEnvelope` (also after each early-exit path: a skipped `PING`, a KeyDB-only
command, an unknown command, a rejected dispatch — one test or a table, your call, but every exit
path is covered), and pytest `test_keydb_write_stamped_with_author_hash` (two real KeyDBs A and B,
B forwarding A, one peer-mode drakeydb attached to B: a key written on A arriving nested through B
shows A's origin hash in `DEBUG MVCC`, not B's; same-author keys share an origin; both differ from
the node's own). Falsify per the plan's Step 4 (not setting `repl_mvcc`; stamping with the link
origin) plus: restore skipped on one early-exit path (the restore test fails).

## Report

`task-2.2-report.md`, same shape as `task-2.1-report.md` (summary, file table, design notes, tests,
verbatim falsifications, anchor drift, open questions). Reply with changed files, test counts
before/after, report path. Do not commit.
