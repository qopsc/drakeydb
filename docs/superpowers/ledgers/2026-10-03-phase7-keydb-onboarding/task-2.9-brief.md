# Task 2.9 brief: sweep clock and read-path hide (implementer: Sonnet 5.5)

You are the implementer for **Task 2.9** of P7-2 on branch `feat/phase7-2-author-stamps-dedup-guard`.
The orchestrator commits; **you do not commit or push**. Tasks 2.1–2.8 are committed; read
`task-2.8-report.md` (the single depth-1 evaluation of `EnvelopeTimeMs` you feed the stream clock
from, its Step 0 clock-read audit, the flip table) and the "For the next task" notes of the 2.7
and 2.8 reviews first.

## Read, in order

1. Plan: Global Constraints; **Task 2.9 in full** — every interface paragraph is binding (the
   process-wide `stream_ms` with a single writer = the main link's non-peer applier,
   `publish_clock`; the advance at depth 1 at Task 2.8's evaluation point; the reset only on a
   master **uuid** change or the flag clearing, never on a reconnect or `+CONTINUE`; `SweepClockMs`
   pure with the unsigned-safe 60 s floor; `StreamClockHides` on the read path (`FindInternal`,
   `kReadStats` only) and in `ScanCb` (decision 33); INFO `replica_stream_clock_lag_ms`; the floor
   warning in `RedisStreamAcksFb`; the bounds written in code and docs), Step 0's audit table, every
   test name, the four pytest flips and the orphan pytest flip, the harness note
   (`repl_ping_replica_period=1`).
2. Spec D-9; decisions 29, 30, 31, 33.
3. `task-2.1-brief.md` "Environment". Same rules; the container is free.
4. Code: `engine_shard.cc` `RetireExpiredAndEvict` (`:885` region, one `GetCurrentTimeMs()` kept
   for an unflagged shard), `db_slice.cc` `FindInternal`'s expiry block (`:947-953` region) and
   `ExpireAllIfNeeded`, `generic_family.cc` `ScanCb` (`:813-814` region), `replica.cc`
   `ApplyReplicaActiveExpiry`, `MainReplicationFb` (`:279`, `:341` Greet sites), `RedisStreamAcksFb`,
   `GetSummary`, `MasterContext::master_node_uuid`; `ReplicaActiveExpiryTest`.

## Deliver

Exactly Task 2.9 Steps 0–5. Three hunks land in upstream-hot files (`engine_shard.cc`,
`db_slice.cc`, `generic_family.cc`): one call each to a fork-only predicate, with a `drakeydb: P7-2`
marker and the `docs/UPSTREAM-SYNC.md` rows the plan names. `DbSlice::ExpireAllIfNeeded` stays on
the local clock with a comment. Narrow ISSUE-REGISTER D-32 as its Status says and update D-9's
"until Tasks 2.8 and 2.9 land" wording in the spec.

## Tests

All names in the plan's Step 1 (the `SweepClock*` table, the `StreamClock*` applier cases, the
`ReplicaActiveExpiryTest` sweep/hide/mutable/scan cases, pytests (a)–(g) and the flips). Falsify
per Step 4 (a)–(n), each restored and recorded verbatim; (k) is redundant by construction: record
it, do not count it. The new pytests run ×5 under three busy loops (Step 5).

## Report

`task-2.9-report.md`, same shape, with the Step 0 audit table, the flip table and the ×5 results.
Reply with changed files, test counts before/after, report path. Do not commit.
