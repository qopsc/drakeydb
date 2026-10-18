# Task 1.6 Step 2: design note (advisor: Fable 5.1, 2026-10-18) — STOP point, owner call needed

Input: `task-1.6-step1-report.md` (release build, 12-cpu arm64 container, Task 1.5 harness).

## What the measurement says

| | plain replica | peer node | gap |
|---|---|---|---|
| applied ops/s at KeyDB's max (~750k offered) | 745–758k (keeps up, 84 % cpu) | 519–562k (saturated) | 69–75 % |
| cpu per applied write | 1.1 µs | 1.8–1.9 µs | +0.75 µs |
| `journal::RecordEntry` (encode into a `StringSink`, ring-buffer push) | not run (a plain replica has no journal) | 18.9 % of samples ≈ 0.36 µs | the largest single item |
| mvcc side-table lookups (`FindMutable` → `FindByFp`), `LaunderIfNeeded`, clock reads | not run | ≈ 0.3 µs | the rest |
| LWW guard, tombstones, `SetMvcc`, `ShouldDropForLww` | — | within noise | not the cause |

Skipping `RecordEntry` for applied writes (diagnostic (b)) gives 95 % of plain (ratio 0.985) — but
it is **not a fix**: on an active node the mvcc stamp is committed *inside* `RecordEntry`
(`MvccStamper::Commit`, journal.cc; see `server_family.cc:1360-1395` for why the journal runs from
boot), so (b) leaves every applied key unstamped and trips the dense mvcc invariant.

## Why the squasher micro-batch (the original Task 1.6 idea) is moot

The cost is per **applied write** (encode + stamp lookups), not per dispatch. Batching envelope
commands through the squasher would not remove one `RecordEntry` per command; it would only
amortize dispatch overhead that the profile shows is small (`DispatchCommand` self < 1 %). Dropping
that design.

## Options

- **O1. Encode lazily until a consumer has ever attached.** `JournalSlice` keeps a per-slice
  `bool had_consumer_`; while false (a node that has never had a peer, replica or streamer since
  boot or since its last `ClearBuffer`), `AddLogRecord` still assigns the LSN and commits the stamp
  but skips `JournalWriter::Write`, the `StringSink` copy and the ring push. Once a consumer
  registers it stays on forever (a peer that drops and reconnects needs the backlog for a partial
  sync). Gain: the whole 0.36 µs, **only** on an un-meshed node — the harness's case and a node's
  pre-mesh life. Zero gain for a node with peers. Risk: a reconnecting peer after a `ClearBuffer`
  gets a full sync instead of partial (already the case today after `ClearBuffer`). ~40 lines in
  `journal_slice.{h,cc}` (upstream-owned, one hunk). Test: an `--active_replica` node with no
  peers keeps `GetRingBufferSize() == 0` and stamps every write (the P4-1 Task 10 regression test
  must still pass); after `REPLICAOF ADD` the ring fills.
- **O2. Cheaper encode.** `AddLogRecord` sizes the entry first (`payload` arg lengths + 32) and
  encodes into a pre-reserved `std::string` through a sink that appends without the
  `iovec`/`WriteSomeBytes` indirection (`WriteSomeBytes` + `Sink::Write` + `StringSink::WriteSome`
  are 8.5 % of samples). Expected: about half of the encode cost, ~0.15–0.2 µs, **on every node
  that journals, meshed included**. ~60 lines in `journal_slice.cc` + a small writer in
  `serializer.{h,cc}` (upstream-owned; the writer is additive). Byte output identical: pinned by
  the P3 golden-buffer test (`Journal.WriteLegacyFramingIsByteIdentical`) and a new v2 twin.
- **O3. One lookup instead of two for the stamp.** `FindMutable` does the prime lookup and then
  the side-table `FindByFp` per write (P4-1's side-table design). Fusing them is a `db_slice.cc` /
  `dash.h` change the P7 constraints forbid (`src/core/dash.h` untouched) and belongs to a P4
  follow-up, not P7. Not proposed here.

O1 + O2 together: a meshed peer node gains O2 only (≈ 1.7 µs/op, ~80 % of plain); an un-meshed one
reaches ~95 %. Neither makes a meshed peer node keep up with a KeyDB writing at its own maximum
(~750k ops/s on these cores): that is 2.3× the per-write work a plain replica does, by design
(stamp + journal for the mesh). KeyDB's own active replica has the same shape (apply + propagate).

## Recommendation (owner call — the three decisions below)

1. **The bar.** Decision 12 ("must keep up with KeyDB") was written for a plain replica. For a
   peer node I recommend a stated ceiling instead: *applied ≥ 70 % of KeyDB's maximum offered rate
   with no link drop*, with the operator doc saying that a KeyDB that writes faster than ~70 % of
   its own capacity into a drakeydb peer needs `client-output-buffer-limit replica` raised (the
   256 MB default drops the link at ~9 s of such load) or the load spread over the mesh. The
   perf test's `peer_mode` parametrization asserts that ceiling, not the plain bar.
2. **O1 and O2 in P7-2** as `perf: …` commits after Task 2.9 (both small, both measurable with the
   existing harness; O2 first since it helps every node), each falsified by the harness numbers
   and the byte-identity pins. Expected outcome: un-meshed ≥ 0.95, meshed ≈ 0.8.
3. **Register the residual** in `docs/ISSUE-REGISTER.md` (peer-mode apply ceiling; O3 as the P4
   follow-up that would close most of the rest), and close Task 1.6 with this note instead of the
   squasher design.

Until the owner answers, P7-2 continues with Tasks 2.6–2.9; the peer-mode perf parametrization
stays failing-by-design and is excluded from the gate with a note. Nothing in O1/O2 blocks or is
blocked by 2.6–2.9.
