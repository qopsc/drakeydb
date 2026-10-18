# Task 2.3 fix round 1 (implementer: the Task 2.3 Sonnet agent, resumed)

Read `task-2.3-review.md` in full. Verdict: Needs fixes (0 Critical, 2 Important, 9 Minor).
Rulings — implement all, nothing else:

- **I1**: add the reviewer's peer-link variant(s) pinning R11 (reserve before stamp): with a registry
  and `--classic_author_cap=0` (and once with cap > 0), hold `Reserve(A, m)` from the test, then
  (1) a link whose `running()` flips → `kNotConsumed`, (2) a live link while the test `Commit`s →
  leaf `kDrop`; each asserts `ClassicAuthorOverflowTotal()`, `registry.Size()` and `FindIdx(A)`
  unchanged. Falsify with the reviewer's mutant (Stamp moved above Reserve): it must fail.
- **I2**: the deterministic `FakeClassicMaster` pytest in **peer mode**: stream `INCR c` at mvcc 5,
  drop the link, script a second full sync (a merge, no flush) whose tail re-streams the same
  envelope: `c == 1`, `multimaster_rreplay_deduped == 1`. Plus the plain-mode twin (flush + replay:
  `c == 1` because the flush emptied it and the replay re-applied it — assert what actually holds
  and say why in the docstring; the point is that `Clear()` ran there). Falsify with the reviewer's
  mutant (`Clear()` on the peer merge): the peer-mode test must fail (`c == 2`).
- **M1**: drop the `--active_replica` line from the `ClassicAuthorMapTest` fixture.
- **M2**: add the db-select-failure case (`--cluster_mode=yes`, `SELECT 3` fails) to
  `DoesNotAdvanceWhenMalformedOrSelf` or its own test, asserting the enclosing layers do not advance.
  Ruling **R14**: a leaf skipped because its layer's db could not be selected advances no layer
  (same retry reasoning as R10). Task 2.5 records R10 + R14 in the spec amendment, and notes the
  divergence from KeyDB the reviewer cites (`replication.cpp:5476-5493`).
- **M3**: comment wording as the reviewer suggests (`Leaf::kRejected`, `Dispatch`'s `*ran`).
- **M4**: add the mvcc-0 `Release` test the reviewer sketches (reservation survives a 0-mvcc leaf);
  falsify with `held_(true)`.
- **M5, M6**: move the comment; put `Reservation` in `CommandScope`'s anonymous namespace.
- **M7**: make `ConcurrentSameEnvelopeAppliesOnce` deterministic: the holder waits on a latch the
  second fiber sets after it entered `Reserve` (a `fb2::Done`/`EventCount`, or an atomic flag polled
  with `ThisFiber::Yield`); fix the report's "deterministic" wording accordingly.
- **M8**: `FreshMvcc()` starts at `1 << 40` and its comment names the trap; same for the pytest
  `rreplay()`/`keydb_mvcc()` counters (start above any explicit literal).
- **M9**: nothing in code; the P7-2 gate runs the release perf bar (already Task 2.4 Step 5).

Then host pre-commit; rebuild `classic_replay_test dragonfly`; `classic_replay_test` ×3 in full,
`multi_master_test`, and the pytest `-k "dedup or author_slot or exact_classic_counters or
master_switch or mesh_forwarded"`. Append "## Fix round 1" to `task-2.3-report.md` with verbatim
falsification outputs for I1, I2, M2, M4 and the final counts. Reply with the summary. Do not commit.
