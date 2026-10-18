# Triage brief: `test_keydb_active_live_write_during_full_sync[peer_mode]` fails on unmodified HEAD here

Role: investigator (read-only on source). **Do not edit any file under src/ or tests/, do not run
ninja, do not commit.** Another agent is reviewing a diff in this worktree; the build tree must stay
as it is. You may write files only under `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/`
and `/tmp`.

Finding to explain: in container `drakeydb-p7` (this worktree mounted at `/home/user/drakeydb`,
arm64, Ubuntu 24.04, debug build), the P7-1 pytest
`tests/dragonfly/keydb_onboarding_test.py::test_keydb_active_live_write_during_full_sync[peer_mode]`
fails 4/4 runs with `sync_in_progress` staying 1 and the replica log saying
`Error while calling ec: Internal error when loading RDB file 5` ~42 s into the peer full sync.
P7-1's gate (x86_64 cloud sandbox, 2026-10-05) recorded the whole file green. The other
parametrization (plain mode) passes here. The implementer showed it fails identically with the
Task 2.1 changes reverted, so treat the binary as HEAD `0f79bf5a`.

Run it (always wrapped in flock, pinned venv):

```
docker exec drakeydb-p7 bash -lc 'cd /home/user/drakeydb && DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly KEYDB_SERVER_PATH=/root/keydb-bin/keydb-server KEYDB_REQUIRED=1 flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k "test_keydb_active_live_write_during_full_sync" -x -q --log-cli-level=INFO'
```

Logs land under `/tmp/dragonfly_logs` (and `/tmp/failed` on failure) inside the container; the
KeyDB log is beside them. Find: which RDB opcode/type/aux the loader chokes on ("file 5" is an
errc value: map it via `rdb_load.cc`'s error enum), whether KeyDB v6.3.4 on arm64 built with
`MALLOC=libc` emits something the x86 build did not (compare the Task 0.7 report's capture notes
and `tests/dragonfly/data/README.md`), whether it is timing (the live write during the sync), and
whether `plain` mode avoids the same bytes or merely tolerates them (peer mode = merge load path,
`rdb_load.cc` merge-LWW hooks from P4-3). Read `docs/ISSUE-REGISTER.md` for anything already
registered (D-10, U-11, the loader tolerance rows of spec D-10).

Deliver `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/triage-live-write-peer-mode.md`:
reproduction (verbatim), the exact failing byte/opcode and the code path, root cause hypothesis
with evidence, whether it is environment-specific (arm64 / libc malloc / timing) or a real fork
bug that P7-1's gate missed, and a proposed fix with the test that would pin it. Reply with a
5-line summary.
