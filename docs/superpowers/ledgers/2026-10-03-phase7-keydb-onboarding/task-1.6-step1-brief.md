# Task 1.6 Step 1: measure where a peer-mode node's time goes (investigator: Sonnet 5.5)

Reopened by Task 2.4 Step 5 (`task-2.4-report.md`, "Step 5"): on a release build an
`--active_replica --multi_master` (peer-mode) drakeydb attached to an active KeyDB cannot keep up
with ~775k ops/s of `SET`/`INCR` (KeyDB drops it at its 256 MB output buffer after ~9 s, ~74% of
the offered load applied), while a plain replica of the same KeyDB keeps up at ratio 1.0001. The
implementer showed it also fails with the LWW guard off, the rewrites removed and the stamping
compiled out. Plain mode and peer mode share the envelope parse, the per-command dispatch and the
process-wide dedup, so the gap is in what an active node does per applied write that a plain one
does not: the MVCC side table (`DbSlice::SetMvcc`, P4-1), the local journal with extended framing
(P3/P4), `ShouldDropForLww`/`IncomingStamp` on every transaction, tombstone arming, per-write
`FindMutable` through the stamper, ... **Your job is to measure, not to fix.**

Environment: `task-2.1-brief.md` "Environment"; the release binary is `build-opt/dragonfly` of the
current tree (`CCACHE_DISABLE=1 ninja -C build-opt -j4 dragonfly` to rebuild after a diagnostic
edit; always restore the sources and rebuild the real binary at the end, `cmp` against a copy). The
perf harness is `test_keydb_onboarding_keeps_up_under_load[peer_mode|plain_replica]` with
`DRAKEYDB_PERF=1` (`task-1.5-report.md` has the method; pin the cpus as it does; `uptime` load
< 0.5 before each run; one run at a time, under `flock`). Nothing else may run in the container
while you measure. **Do not commit. Do not edit anything but `src/` diagnostics you revert.**

Deliver, in `task-1.6-step1-report.md`:

1. **Bisection by flag** (no rebuild): peer mode with each of `--multi_master_stream_lww=false`,
   `--multi_master_tombstone_ttl=0`, and (if the harness allows) `--active_replica` without
   `--multi_master`; record applied-rate (from KeyDB's log omem growth or the sampler) per variant.
2. **Bisection by compile-out** (release rebuild each): (a) `DbSlice::SetMvcc` a no-op; (b) the
   journal's `RecordEntry` early-return for applied writes on this link (or the cheapest equivalent
   that skips journaling); (c) `ShouldDropForLww` returning false before any lookup; (d) all three.
   Record the rate per variant. Say which single change brings peer mode to the plain rate, if any.
3. **A profile**, if a profiler is available: try `perf` (`apt-get install -y linux-perf` or
   `linux-tools-generic`; if the OrbStack kernel refuses `perf record`, say so), else gperftools
   (`WITH_GPERF` in the release configure, `HEAPPROFILE`/`CPUPROFILE` env) — a 10 s sample of the
   peer node under load with the top 30 symbols. If neither works in 20 minutes, skip and say so.
4. **Attribution table**: variant → ops/s applied → % of plain; your reading of where the time goes
   and what the cheapest fix would be (Task 1.6 Step 2 is the design note; do not write it).
5. The exact commands for every run, verbatim outputs, and the `cmp` proof that the tree and
   `build-opt` are restored.

Reply with a 6-line summary: plain rate, peer rate, the dominant cost, the one variant that
closed the gap (or none), profiler availability, restore proof.
