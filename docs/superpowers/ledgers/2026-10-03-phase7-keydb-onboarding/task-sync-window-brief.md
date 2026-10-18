# Side task: make the KeyDB full-sync window tests independent of timer granularity (Sonnet 5.5)

Context: `triage-live-write-peer-mode.md` (read it first) and ruling R4 in `progress.md`. On this
arm64 OrbStack VM `usleep(100)` ≈ 2.2 ms, so KeyDB's `rdb-key-save-delay 400` × 5105 keys makes the
test's full sync take ~50 s while `wait_for_peer_link` / `assert_full_sync_arrived` allow ~44 s.
Environment rules: `task-2.1-brief.md` "Environment" (container `drakeydb-p7`, flock, pinned venv).
**Only `tests/dragonfly/keydb_onboarding_test.py` (and a helper it imports, if any) changes. No
C++. Do not commit.** The orchestrator commits as
`test: size the KeyDB full-sync window by writes, not by host timer granularity (P7)`.

Deliver:
1. `wait_for_peer_link` and `assert_full_sync_arrived`: a 120 s `timeout=` like
   `wait_available_async`, instead of `times=300`.
2. Size the window: lower `BULK_KEYS` and set `RDB_KEY_SAVE_DELAY_US` so the sync lasts about 2–5 s
   on both a fast-timer host (`usleep(100)` ≈ 0.15 ms) and this one (≈ 2.2 ms), keeping
   `MIN_WRITES_IN_SYNC_WINDOW` (20) in-window writes with margin in both regimes. Show the arithmetic
   for both regimes in a comment beside the constants (KeyDB slices the delay into ≤100 µs
   `usleep`s: `rdb.cpp:1175-1181`, so the cost per key is `ceil(delay/100)` sleeps).
3. The failure message of the window wait reports the measured sync duration and the in-window
   write count, so a slow host reads as "sync took N s, M writes" rather than an RDB error.
4. Check every other test in the file that uses `rdb-key-save-delay`/`BULK_KEYS` or `times=300`
   waits on a full sync (grep), and apply the same treatment where the same arithmetic shows it is
   exposed; list what you touched and what you left and why.

Verify: run `-k "live_write_during_full_sync or full_sync"` here (both parametrizations green, with
times), then the whole `keydb_onboarding_test.py` once (`KEYDB_REQUIRED=1`; expect 0 failed; record
the duration). Falsify: set the timeout back to 44 s equivalent with the old constants and show the
peer-mode case fails again; restore. Write `task-sync-window-report.md` (what changed, arithmetic,
runs, falsification verbatim). Reply with a 5-line summary.
