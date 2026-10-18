# Triage: `test_keydb_active_live_write_during_full_sync[peer_mode]` fails on arm64 container

**Verdict: not a fork bug and not an RDB-format problem. The test's poll budget is shorter than the
sync takes on this machine; the "RDB error 5" is the harness killing KeyDB mid-transfer.**
Environment-specific (arm64 OrbStack VM timer granularity), but the test is fragile by design.

## Reproduction (verbatim command from the brief, 1 run, HEAD binary)

```
docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb && DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly KEYDB_SERVER_PATH=/root/keydb-bin/keydb-server KEYDB_REQUIRED=1 flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k "test_keydb_active_live_write_during_full_sync" -x -q --log-cli-level=INFO'
```
Result: plain_replica passes, peer_mode fails at `wait_for_peer_link` (`keydb_onboarding_test.py:147`):
`peer['sync_in_progress'] == 1`, `last_io_seconds_ago: 42`; 1 failed, 1 passed in 104.68 s.

## What "file 5" is

`rdb::errc` 5 = `rdb_file_corrupted` (`src/server/error.h:76`), returned with the log line
`Error while calling FetchType()` (`rdb_load.cc:2484`): the type-byte read hit end of stream.
Not an opcode: no unknown opcode/aux is logged. The only AUX warning is the benign
`Unrecognized RDB AUX field: 'repl-masters'`.

Timeline (replica `drakeydb.INFO` / `keydb-server-5556.log`):
- 23:06:17.388 KeyDB starts BGSAVE for the replica; 23:06:19.966 replica starts loading
  (`db_size=5105`).
- Entries grow ~251 per 2.5-3 s (one `Took ~2.4-2.9 s to load rdb_type 0` per chunk), 4022 of 5105
  at the end.
- 23:07:01.690 KeyDB log: `Received SIGTERM ... User requested shutdown ... There is a child saving
  an .rdb. Killing it!`. 23:07:01.694 replica: FetchType() fails -> errc 5. The SIGTERM is the pytest
  teardown after `link_up` exhausted its 300 attempts (300 x 0.1 s sleep + RPC time ~= 42-44 s).

So the stream was truncated by the harness, not corrupted by the sender.

## Why the sync takes > 44 s here

The test slows KeyDB's RDB child on purpose: `rdb-key-save-delay 400` (us) x `BULK_KEYS 5000`,
documented as "at least 2 s" (`keydb_onboarding_test.py:37-41`). KeyDB implements that delay as
4 slices of `usleep(100)` per key (`rdb.cpp:1175-1181`, `debugDelay` = `usleep`).
In this OrbStack arm64 VM (kernel 7.2.9-orbstack) sleeps are coarse:
a C probe measured `usleep(400)` = 2.18 ms (5x), so `usleep(100)` ~2.2 ms and ~9-10 ms/key
=> ~100 keys/s observed => 5105 keys ~ 50 s. The plain-mode run in the same session took 51 s
(`23:05:21 -> 23:06:12`), confirming the whole sync is ~50 s regardless of mode.

Plain mode passes because `wait_available_async` has `timeout=120` (`utility.py:151`); peer mode
uses `wait_for_peer_link` -> `assert_eventually(times=300)` ~= 44 s. Peer mode additionally reports
the link `up` + `sync_in_progress==1` for the entire transfer (see the docstring on
`wait_for_peer_link`), so the wait cannot return before the load finishes, and the load takes ~50 s.
Merge-load overhead (P4-3 LWW hooks) was not isolated: peer-mode per-chunk times (2.3-2.9 s) are
indistinguishable from plain (2.4-3.0 s); the bottleneck is the sender.

## Not the cause (checked)

- RDB bytes: same KeyDB, same flags, both modes load the same stream (`Took ... rdb_type 0` lines,
  `RESIZEDB db_size=5105` in both). Plain mode loads it to completion, so no arm64 / MALLOC=libc
  byte difference is involved. (Task 0.7 report only records `malloc=libc` as a build flag; nothing
  suggests a different RDB encoding.)
- Timing of the live write: writes happen inside `write_during_full_sync` before the wait; the
  failure is a poll timeout, not a live-write interleaving.
- `docs/ISSUE-REGISTER.md` U-11 (stream bytes arriving with the end of the RDB) and D-10 do not
  describe this; no new loader tolerance is needed.

## Why P7-1's gate was green

x86_64 cloud sandbox: `usleep(100)` is ~0.15 ms there, so 5000 keys take a few seconds and the
44 s budget is never close. The test passes with ample margin on fast-timer hosts and fails on any
host where `usleep(100)` > ~0.8 ms (5105 keys x 4 sleeps x t > ~44 s).

## Proposed fix

Make the test independent of timer granularity:
1. `wait_for_peer_link`: `assert_eventually(timeout=120)` (match `wait_available_async`'s 120 s)
   instead of `times=300`; same for `assert_full_sync_arrived` (also `times=300`).
2. Better: bound the sender's delay to the window actually needed. `write_during_full_sync` needs
   only `MIN_WRITES_IN_SYNC_WINDOW = 20` writes in the window; lower `BULK_KEYS` (e.g. 1000) and set
   `RDB_KEY_SAVE_DELAY_US` to 2000+ (one slice) so the sync lasts ~2-3 s on both fast and
   coarse-timer hosts. KeyDB slices to <=100 us, so any value costs `ceil(delay/100)` sleeps/key;
   choosing the delay in multiples of 100 keeps this predictable.
3. Pin: add to the test a sanity assertion that the observed in-window write count stays >= 20
   (already there) and record the measured sync duration in the failure message so a
   slow-timer host reads as "sync took N s" rather than "RDB error 5".

Validation (not run here; read-only brief): apply (1) or (2), re-run the command above on
`drakeydb-p7`; expect both parametrizations green, peer_mode in ~50 s (1) or ~5 s (2).
