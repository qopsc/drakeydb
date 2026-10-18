# Report: size the KeyDB full-sync window by writes, not timer granularity (R4)

Only `tests/dragonfly/keydb_onboarding_test.py` changed. No C++, no commit.

## What changed
- `assert_full_sync_arrived` and `wait_for_peer_link.link_up`: `times=300` -> `timeout=SYNC_WAIT_TIMEOUT_S` (120).
- `BULK_KEYS` 5000 -> 2000, `RDB_KEY_SAVE_DELAY_US` 400 -> 100 (1 sleep/key instead of 4),
  `WRITE_PAUSE_S` 0.01 -> 0.002 (new constant for the in-window write pause).
- `write_during_full_sync` failure message: `the sync took N s with M writes inside its window, need 20`.

## Arithmetic
sync ~= (BULK_KEYS + 105 seeded keys) * ceil(delay/100) * t, t = real `usleep(100)`.
- fast host (t ~0.15 ms): 2105 * 1 * 0.15 = ~0.3 s; one write round ~3 ms -> ~100 writes (>= 20, 5x margin).
- coarse host (t ~2.2 ms): 2105 * 1 * 2.2 = ~4.6 s; hits the 1000-write cap; well under 120 s.
- Old: 5105 * 4 * 2.2 = ~45-50 s (> the 44 s the waits allowed).
The hosts differ ~15x in t, so "2-5 s on both" is unattainable; this gives ~4.6 s coarse and a safe window on fast.
Measured here: peer_mode 5.7 s, plain_replica 6.1 s per test (incl. startup).

## Other tests (grep)
Touched: none beyond the above. Only `seed_before_attach` uses the bulk constants, used by 4 tests
(plain_master_full_sync_and_stream, active_handshake_and_full_sync, active_handshake_peer_mode,
live_write_during_full_sync); all now sized by the shared constants. Left the other `times=300`
waits (~30 s): they poll small datasets or run after `assert_full_sync_arrived`/`wait_available_async`
returned, so they are not exposed to the sync duration.

## Runs (container drakeydb-p7, pinned venv)
- `-k "live_write_during_full_sync or full_sync"`: 14 passed in 43.36 s (live_write plain 6.11 s, peer 5.67 s;
  handshake_and_full_sync 7.45 s; plain_master 6.06 s).
- Whole file, `KEYDB_REQUIRED=1`: 172 passed, 3 skipped (perf/release-bar, DRAKEYDB_PERF) in 370.24 s, 0 failed.
- pre-commit (black etc.): passed.

## Falsification
Old constants (5000 / 400 / 0.01) and `times=300` waits, `-k live_write_during_full_sync`:
```
E       AssertionError: {'host': 'localhost', 'port': 5556, 'link_status': 'up', 'last_io_seconds_ago': 42, ...}
E       assert (equals failed
E         'up'  'up'  and 1 == 0)
FAILED ... test_keydb_active_live_write_during_full_sync[df_factory0-peer_mode]
=========== 1 failed, 1 passed, 173 deselected in 102.38s (0:01:42) ============
```
Matches the triage (peer_mode fails at `link_up`, `sync_in_progress` 1 at ~42 s). New code restored and re-verified by grep.
