#!/usr/bin/env python3
"""Captures what an active KeyDB v6.3.4 streams for a merge full sync (`KEYDB.MVCCRESTORE`), and
probes how drakeydb's `RESTORE` takes KeyDB's `DUMP` payloads (plan Task 2.6 Step 0).

Probe (a): an active KeyDB X with a raw replica attached merge-syncs from an active KeyDB Z that holds
a TTL'd key, a TTL-less key and a hash. X loads each key from Z's RDB and emits one RREPLAY-wrapped
`KEYDB.MVCCRESTORE key <mvcc> <expire> <DUMP payload>` per key to its replica. The bytes the replica
saw after X's own (empty) RDB are written to --out-dir as keydb_v6.3.4_mvccrestore_stream.bin.

Probe (b): `DUMP` of each type from KeyDB, `RESTORE ... REPLACE ABSTTL` onto a drakeydb.
Probe (c): `RESTORE` on drakeydb with ABSTTL 0, 1, -1, 9223372036854775807 and with a relative PTTL.

Run it from the repository root, with tests/dragonfly/requirements.txt installed:

  KEYDB_SERVER_PATH=/path/to/keydb-server DRAGONFLY_PATH=build-dbg/dragonfly \\
      python3 tests/dragonfly/tools/capture_keydb_mvccrestore.py
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from capture_keydb_rreplay import (  # noqa: E402
    RawReplica,
    c_escape,
    client,
    describe,
    free_port,
    new_keydb,
    parse_frames,
    wait_for,
)
from tests.dragonfly.instance import KeyDBServer  # noqa: E402

STREAM_NAME = "keydb_v6.3.4_mvccrestore_stream.bin"
LLONG_MAX = 9223372036854775807


def probe_a(servers, log_dir, out_dir):
    flags = dict(active_replica=True, multi_master=True, no_forward=False)
    x = new_keydb(servers, log_dir, **flags, repl_ping_replica_period=3600)
    z = new_keydb(servers, log_dir, **flags, repl_ping_replica_period=3600)
    kx, kz = client(x), client(z)
    kz.set("ttl_key", "ttl-value", px=3_600_000)
    kz.set("plain_key", "plain-value")
    kz.hset("hash", mapping={"f1": "1", "f2": "2"})

    replica = RawReplica(x.port)
    replica.full_sync_start()
    replica.read_rdb()
    replica.drain_until(lambda frames: True, deadline=2.0)
    before = len(replica.stream)

    started = time.monotonic()
    kx.execute_command("REPLICAOF", "localhost", z.port)
    wait_for(lambda: kx.dbsize() == 3, "X to merge Z's keys")

    def restores(frames):
        return sum("MVCCRESTORE" in describe(f[2]) for f in frames) >= 3

    replica.drain_until(restores)
    print(f"merge sync of 3 keys reached X's replica in {time.monotonic() - started:.2f} s")

    data = bytes(replica.stream)
    (out_dir / STREAM_NAME).write_bytes(data)
    print(f"=== probe (a): {len(data)} bytes after X's RDB ({before} before REPLICAOF Z) ===")
    print(f"X uuid: {replica.master_uuid}; handshake: {replica.handshake_replies}")
    for i, (start, end, args) in enumerate(parse_frames(data)):
        print(f"[{i}] offset={start} length={end - start}")
        print(f"    {describe(args)[:300]}")
        print(f'    "{c_escape(data[start:end])[:400]}"')
    replica.close()


def start_drakeydb(binary, tmp):
    port = free_port()
    proc = subprocess.Popen(
        [binary, f"--port={port}", "--proactor_threads=2", f"--dir={tmp}", "--logtostderr"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    d = redis.Redis(port=port)
    for _ in range(100):
        try:
            d.ping()
            return proc, d
        except redis.ConnectionError:
            time.sleep(0.1)
    proc.kill()
    raise SystemExit("drakeydb did not start")


def probe_b_c(servers, log_dir, binary):
    k = new_keydb(servers, log_dir)
    kr = redis.Redis(port=k.port)
    kr.set("string", "v")
    kr.set("int", 12345)
    kr.rpush("list", *[f"e{i}" for i in range(10)])
    kr.rpush("biglist", *[f"element-{i}" * 4 for i in range(2000)])
    kr.sadd("set", "a", "b", "c")
    kr.sadd("intset", 1, 2, 3)
    kr.zadd("zset", {"m1": 1, "m2": 2.5})
    kr.hset("hash", mapping={"f1": "1", "f2": "2"})
    kr.execute_command("XADD", "stream", "*", "f", "v")

    with tempfile.TemporaryDirectory() as tmp:
        proc, d = start_drakeydb(binary, tmp)
        try:
            print(
                "=== probe (b): KeyDB DUMP -> drakeydb RESTORE key <ttl> payload REPLACE ABSTTL ==="
            )
            for key in [
                "string",
                "int",
                "list",
                "biglist",
                "set",
                "intset",
                "zset",
                "hash",
                "stream",
            ]:
                payload = kr.dump(key)
                footer = payload[-10:-8]
                try:
                    reply = d.execute_command("RESTORE", key, 0, payload, "REPLACE", "ABSTTL")
                except redis.ResponseError as e:
                    reply = f"ERR {e}"
                print(
                    f"{key}: type byte {payload[0]}, footer version {int.from_bytes(footer, 'little')},"
                    f" len {len(payload)} -> {reply}; drakeydb type {d.type(key)}"
                )
                if key in ("string", "int"):
                    assert d.get(key) == kr.get(key)

            print("=== probe (c): RESTORE ABSTTL edge values, then PTTL/EXISTS ===")
            payload = kr.dump("string")
            now_ms = int(time.time() * 1000)
            for ttl in [0, 1, -1, LLONG_MAX, now_ms + 3_600_000, now_ms - 1000]:
                try:
                    reply = d.execute_command("RESTORE", "t", ttl, payload, "REPLACE", "ABSTTL")
                    print(f"ABSTTL {ttl}: {reply}; PTTL {d.pttl('t')}; EXISTS {d.exists('t')}")
                except redis.ResponseError as e:
                    print(f"ABSTTL {ttl}: ERR {e}; EXISTS {d.exists('t')}")
                d.delete("t")
            reply = d.execute_command("RESTORE", "t", 3_600_000, payload, "REPLACE")
            print(f"relative PTTL 3600000: {reply}; PTTL {d.pttl('t')}")
            d.delete("t")
            reply = d.execute_command("RESTORE", "t", 0, payload, "REPLACE")
            print(f"relative 0: {reply}; PTTL {d.pttl('t')}")
        finally:
            proc.terminate()
            proc.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--keydb-server")
    parser.add_argument(
        "--dragonfly", default=os.environ.get("DRAGONFLY_PATH", "build-dbg/dragonfly")
    )
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "tests/dragonfly/data")
    parser.add_argument("--log-dir")
    args = parser.parse_args()
    if args.keydb_server:
        os.environ["KEYDB_SERVER_PATH"] = args.keydb_server
    if KeyDBServer.find_binary() is None:
        raise SystemExit("keydb-server not found: pass --keydb-server or set KEYDB_SERVER_PATH")

    servers = []
    try:
        probe_a(servers, args.log_dir, args.out_dir)
        probe_b_c(servers, args.log_dir, args.dragonfly)
    finally:
        for server in servers:
            server.stop()


if __name__ == "__main__":
    main()
