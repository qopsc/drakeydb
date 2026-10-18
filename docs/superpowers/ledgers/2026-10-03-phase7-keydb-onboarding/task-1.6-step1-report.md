# Task 1.6 Step 1 report: where a peer-mode node's time goes

Measured, not fixed. Release `build-opt/dragonfly`, 12-cpu container, harness pinned as Task 1.5
(KeyDB cpu 0, replica cpus 1-2, loader cpu 3), 30 s window, `DRAKEYDB_PERF=1`, `flock`, load < 0.5.
Scratch (all JSON, logs, scripts) is `/root/p16` in `drakeydb-p7`.

## Summary

- Plain replica: **~745-760k ops/s applied**, 82-84% of its cpu (1.1 us/op), ratio 1.000.
- Peer mode: **~520-560k ops/s applied**, 100% cpu (1.8-1.9 us/op), ratio 0.78-0.80. It is
  CPU-saturated; capacity is ~70-72% of plain's applied rate (and plain has headroom).
- Dominant cost: **the local journal write per applied command**: `journal::RecordEntry` is 18.9% of the
  peer node's cpu (`AddLogRecord` 15.6%, `JournalWriter::Write` into a `StringSink` ~9%), the whole
  `Transaction::LogJournalOnShard` 20.5%. Second: `FindMutable`/mvcc side-table lookups (~12% and
  the ExpireTablePolicy `FindByFp` 4.2%).
- Variant that closes (almost) the gap: **(b) skipping `RecordEntry` for applied writes**: 714k applied,
  ratio 0.985, 1.41 us/op, no drop. None of the flags, `SetMvcc` no-op (a) or the LWW veto (c) moves it.
- Profiler: **`perf` works** (use `/usr/lib/linux-tools/6.8.0-147-generic/perf`; the `perf` wrapper refuses
  the OrbStack 7.2.9 kernel). Top-30 below.
- Restore: `cmp` OK for the three files against `/root/p16/orig`, no `DIAG` left, `ninja -C build-opt -n
  dragonfly` says "no work to do", binary rebuilt from the restored tree (18:07Z).

## Note on method

- A peer run cannot complete under the stock harness: KeyDB drops a replica at its 256 MB output
  buffer after ~9 s. `lift.py` (an external process, not a repo edit) issues `CONFIG SET
  client-output-buffer-limit "replica 0 0 0"` on KeyDB (port 5555) right after it starts, so the run
  completes and the harness records its numbers (it then fails its own bounds: expected). Applied
  rate = the harness's offset delta over the steady window; `us/op` = replica cpu% / applied rate.
- The tree changed under the work (Task 2.6 edits in `classic_replay.*`, `replica_types.h`, tests, per
  the orchestrator's decision to proceed). `build-opt` was rebuilt from the tree as it was then; the
  first-round rows (before) used the earlier binary (07:50Z). Rows from both binaries agree.
- `--active_replica` without `--multi_master` is what the harness's peer mode already is
  (`multi_master: 0` in INFO), so there is no separate run for it.
- `base_plain` (403k) was taken right after the rebuild, with the box's load tail from the build; it is
  noise (offered load was 404k), re-run twice as `base_plain2/3`.

## Attribution table

Plain rate for % : 752k (mean of base_plain2/3 on the current binary).

| variant | offered ops/s | applied ops/s | % of plain | replica cpu | us/op | max lag | verdict |
|---|---|---|---|---|---|---|---|
| plain_replica (base_plain2, 3) | 745,663 / 757,729 | 745,590 / 757,661 | 100% | 84% | 1.11-1.13 | 1 MB | keeps up |
| peer_mode, baseline (old binary: peer2, peer3) | 671,017 / 721,541 | 536,400 / 561,757 | 71-75% | 100% | 1.87 / 1.78 | 546 / 651 MB | falls behind |
| peer_mode, baseline (current binary: base_peer) | 664,763 | 518,680 | 69% | 99% | 1.91 | 591 MB | falls behind |
| flag `multi_master_stream_lww=false` | 645,483 | 514,474 | 68% | 101% | 1.97 | 532 MB | no change |
| flag `multi_master_tombstone_ttl=0` | 659,729 | 542,736 | 72% | 101% | 1.86 | 485 MB | no change |
| both flags | 629,374 | 517,109 | 69% | 101% | 1.95 | 467 MB | no change |
| (a) `DbSlice::SetMvcc` no-op | 669,137 | 536,637 | 71% | 100% | 1.86 | 545 MB | no change |
| (b) `RecordEntry` returns for applied writes (mvcc != 0) | 725,008 | 713,982 | 95% | 101% | 1.41 | 54 MB | ratio 0.985: passes 0.95 |
| (c) `ShouldDropForLww` returns false first | 665,395 | 522,463 | 69% | 100% | 1.91 | 587 MB | no change |
| (d) a+b+c | 677,266 | 664,211 | 88% | 101% | 1.52 | 70 MB | ratio 0.981 |

Single runs per variant (the peer baseline repeated three times: 69-75%, so +-3 points is noise).
"% of plain" for (b) and (d) is ratio-limited by the offered load (the replica kept up; its capacity
is higher than shown). The offered load itself varies 630-725k because the pinned loader/KeyDB share
the box with a saturated replica. Variant (d) is slightly worse than (b) alone: noise (offered 677k vs
725k, 1.52 vs 1.41 us/op), a and c add nothing.

## Reading

- The gap is ~0.75 us of cpu per applied write (1.9 vs 1.1 us). Flags that gate the LWW guard and
  tombstones, the MVCC side-table write (a) and the veto lookup (c) account for none of it, each within noise.
- Skipping the journal for applied writes recovers ~0.45-0.5 us/op and keeps the node up to the plain
  rate in ratio terms (0.985, lag 54 MB vs a bar of 191 MB). The profile agrees: `RecordEntry` is
  18.9% of the peer node's samples (`LogJournalOnShard` 20.5%), i.e. about 0.36 us/op of 1.9.
- What stays after (b): 1.41 vs 1.11 us/op. The profile's remaining peer-only candidates:
  `FindMutable`/`FindMutableInternal` (12.0% inclusive, from the stamper's per-write mvcc lookup:
  `Segment<..MvccStamp, ExpireTablePolicy>::FindByFp` 4.2% self), `AutoUpdater` (Cancel 2.5%, Run 1.2%),
  `LaunderIfNeeded` 5.2% (the top self symbol), `__kernel_clock_gettime` 4.1% (clock reads for mint/stamps).
  I did not isolate these (no per-symbol compile-out); (a) alone showed no gain because the lookups
  that are left (the side-table `Find` in `FindMutable`, the stamp read) are not in `SetMvcc`.
- Cheapest fix to design in Step 2 (not written here): do not serialize a journal entry for a write
  applied on a peer link when no consumer needs it (no replica/downstream peer/journal-streamer
  attached), or make `RecordEntry` lazy (queue the raw args instead of encoding through
  `JournalWriter` into a `StringSink` per command; ~9% of cpu is that encoding alone). The
  extended-framing encode and the `StringSink` copy are the targets. A second, smaller target is the
  doubled key lookup in `FindMutable` plus the side-table `FindByFp`.
  Caveat: variant (b) is a diagnostic; journal state (lsn, mvcc arms, epoch) was not kept consistent,
  and nothing downstream was attached.

## Profile (perf, 10 s at ~15 s into a peer-mode run, 9,869 samples, on the real (restored) binary)

Top 30 by self time (`perf report --no-children --sort symbol`, cut to 170 columns):

```
     5.21%  [.] dfly::DbSlice::IteratorT<dfly::DashTable<dfly::CompactKey, dfly::CompactValue, dfly::detail::PrimeTablePolicy>::Iterator<false, false> >::LaunderIfNeeded(
     4.77%  [.] unsigned char dfly::detail::Segment<dfly::CompactKey, dfly::CompactValue, dfly::detail::PrimeTablePolicy>::Bucket::FindByFp<dfly::DashTable<dfly::CompactK
     4.59%  [.] dfly::DbSlice::FindMutableInternal(dfly::DbContext const&, std::basic_string_view<char, std::char_traits<char> >, std::optional<unsigned int>)
     4.19%  [.] unsigned char dfly::detail::Segment<dfly::CompactKey, dfly::MvccStamp, dfly::detail::ExpireTablePolicy>::Bucket::FindByFp<dfly::DashTable<dfly::CompactKey
     4.10%  [.] __kernel_clock_gettime
     3.49%  [.] io::(anonymous namespace)::WriteSomeBytes(iovec const*, unsigned int, io::Sink*)
     3.11%  [.] dfly::DbSlice::FindInternal(dfly::DbContext const&, std::basic_string_view<char, std::char_traits<char> >, std::optional<unsigned int>, dfly::DbSlice::Upd
     2.56%  [.] io::Sink::Write(iovec const*, unsigned int)
     2.48%  [.] dfly::DbSlice::AutoUpdater::Cancel()
     2.44%  [.] io::StringSink::WriteSome(iovec const*, unsigned int)
     2.04%  [.] absl::lts_20250512::numbers_internal::safe_strto64_base(std::basic_string_view<char, std::char_traits<char> >, long*, int)
     1.89%  [.] facade::RedisParser::Parse(absl::lts_20250512::Span<unsigned char const>, unsigned int*, std::vector<facade::RespExpr, std::allocator<facade::RespExpr> >*
     1.77%  [.] facade::RedisParser::ConsumeBulk(absl::lts_20250512::Span<unsigned char const>)
     1.54%  [.] memcmp
     1.36%  [.] facade::RedisParser::ParseLen(absl::lts_20250512::Span<unsigned char const>, long*)
     1.24%  [.] facade::RedisParser::ParseArg(absl::lts_20250512::Span<unsigned char const>)
     1.20%  [.] dfly::DbSlice::AutoUpdater::RunInternal(bool)
     1.13%  [.] dfly::CompactObj::MallocUsed(bool) const
     1.10%  [.] facade::OpResult<long> absl::lts_20250512::functional_internal::InvokeObject<dfly::(anonymous namespace)::IncrByGeneric(dfly::CommandContext*, std::basic_
     1.08%  [.] dfly::CommandRegistry::FindExtended(facade::ParsedArgs) const
     1.06%  [.] absl::lts_20250512::numbers_internal::safe_strtou64_base(std::basic_string_view<char, std::char_traits<char> >, unsigned long*, int)
     1.01%  [.] dfly::IsValidNodeUuid(std::basic_string_view<char, std::char_traits<char> >)
     0.91%  [.] dfly::Service::DispatchCommand(facade::ParsedArgs, facade::ParsedCommand*, facade::AsyncPreference)
     0.85%  [.] dfly::ClassicApplier::HandleRreplay(std::vector<facade::RespExpr, std::allocator<facade::RespExpr> > const&, unsigned int)
     0.79%  [.] dfly::DbSlice::AutoUpdater::AutoUpdater(unsigned short, std::basic_string_view<char, std::char_traits<char> >, dfly::DbSlice::IteratorT<dfly::DashTable<df
     0.76%  [.] dfly::UniqueSlotChecker::GetUniqueSlotId() const
     0.75%  [.] dfly::Transaction::LogJournalOnShard(dfly::journal::Entry::Payload&&) const
     0.70%  [.] absl::lts_20250512::strings_internal::memcasecmp(char const*, char const*, unsigned long)
     0.69%  [.] absl::lts_20250512::EqualsIgnoreCase(std::basic_string_view<char, std::char_traits<char> >, std::basic_string_view<char, std::char_traits<char> >)
     0.67%  [.] mi_new
     0.66%  [.] 0x00000000000a1408
     0.65%  [.] facade::FillBackedArgs(std::vector<facade::RespExpr, std::allocator<facade::RespExpr> > const&, cmn::BackedArguments*)
```

Top inclusive: `HandleRreplay` 91%, `ApplyCommand` 80.5%, `Service::DispatchCommand` 72%,
`Transaction::ScheduleSingleHop` 60%, `CmdIncr` 33%, `CmdSet` 29%, `LogJournalOnShard` 20.5%,
`journal::RecordEntry` 18.9% (`JournalSlice::AddLogRecord` 15.6%, `JournalWriter::Write` 9.1-9.5%,
`SetCmd::RecordJournal` 9.5%), `FindMutable` 12.2%, `RedisParser::Parse` 11.6%, `AddOrFind` 10.9%.

## Commands (verbatim scripts) and outputs

Per-run summary lines (`sum.py`), all runs:

```
plain1                 offered= 743023 applied= 743060 ratio=1.0001 maxlag_MB=1 cpu(m/r)=99.5/82.0 us_per_op=1.10
plain2                 offered= 762547 applied= 762380 ratio=0.9998 maxlag_MB=1 cpu(m/r)=99.8/82.1 us_per_op=1.08
peer2                  offered= 671017 applied= 536400 ratio=0.7994 maxlag_MB=546 cpu(m/r)=99.6/100.1 us_per_op=1.87
peer3                  offered= 721541 applied= 561757 ratio=0.7786 maxlag_MB=651 cpu(m/r)=99.9/100.2 us_per_op=1.78
peer_lww_off           offered= 645483 applied= 514474 ratio=0.797 maxlag_MB=532 cpu(m/r)=99.8/101.3 us_per_op=1.97
peer_tomb0             offered= 659729 applied= 542736 ratio=0.8227 maxlag_MB=485 cpu(m/r)=99.7/100.7 us_per_op=1.86
peer_both_off          offered= 629374 applied= 517109 ratio=0.8216 maxlag_MB=467 cpu(m/r)=99.4/100.8 us_per_op=1.95
base_plain             offered= 403591 applied= 403530 ratio=0.9998 maxlag_MB=4 cpu(m/r)=98.1/81.0 us_per_op=2.01
base_plain2            offered= 745663 applied= 745590 ratio=0.9999 maxlag_MB=1 cpu(m/r)=99.8/84.1 us_per_op=1.13
base_plain3            offered= 757729 applied= 757661 ratio=0.9999 maxlag_MB=1 cpu(m/r)=99.7/84.1 us_per_op=1.11
base_peer              offered= 664763 applied= 518680 ratio=0.7802 maxlag_MB=591 cpu(m/r)=99.5/99.3 us_per_op=1.91
var_a                  offered= 669137 applied= 536637 ratio=0.802 maxlag_MB=545 cpu(m/r)=99.9/99.7 us_per_op=1.86
var_b                  offered= 725008 applied= 713982 ratio=0.9848 maxlag_MB=54 cpu(m/r)=99.7/100.7 us_per_op=1.41
var_c                  offered= 665395 applied= 522463 ratio=0.7852 maxlag_MB=587 cpu(m/r)=99.9/100.0 us_per_op=1.91
var_abc                offered= 677266 applied= 664211 ratio=0.9807 maxlag_MB=70 cpu(m/r)=99.6/100.9 us_per_op=1.52
```

Scripts used (`run.sh label mode [--df flags]`, `lift.py`, `variant.py`, `var.sh`, `prof.sh`):

```
#!/bin/bash
# usage: run.sh label mode(plain_replica|peer_mode) [extra --df flags...]
label=$1; mode=$2; shift 2
while awk "{exit !(\$1>=0.5)}" /proc/loadavg; do sleep 5; done
dfargs=(); for f in "$@"; do dfargs+=(--df "$f"); done
cd /home/user/drakeydb; /root/drakey-venv-pinned/bin/python /root/p16/lift.py > /root/p16/$label.lift 2>&1 &
rm -f /root/p16/$label.json
uptime > /root/p16/$label.meta
DRAKEYDB_PERF=1 DRAKEYDB_PERF_OUT=/root/p16/$label.json DRAGONFLY_PATH=/home/user/drakeydb/build-opt/dragonfly KEYDB_SERVER_PATH=/root/keydb-bin/keydb-server KEYDB_REQUIRED=1 flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest "tests/dragonfly/keydb_onboarding_test.py::test_keydb_onboarding_keeps_up_under_load" -k "$mode" -x -q -s --basetemp=/root/p16/tmp-$label "${dfargs[@]}" > /root/p16/$label.log 2>&1
echo "exit=$?" >> /root/p16/$label.log
import redis, time
end = time.time() + 60
while time.time() < end:
    try:
        r = redis.Redis(port=5555, socket_timeout=2)
        r.config_set("client-output-buffer-limit", "replica 0 0 0")
        print("lifted", r.config_get("client-output-buffer-limit"), flush=True)
        break
    except Exception:
        time.sleep(0.05)
import sys
root="/home/user/drakeydb/src/server/"
orig={"db_slice.cc":None,"journal/journal.cc":None,"transaction.cc":None}
for f in orig: orig[f]=open(f"/root/p16/orig/{f.replace('/','_')}").read()
def sub(s,a,b):
    assert s.count(a)==1,(a,s.count(a)); return s.replace(a,b)
v=sys.argv[1]
t=dict(orig)
if "a" in v:
    t["db_slice.cc"]=sub(t["db_slice.cc"],"void DbSlice::SetMvcc(DbIndex db_ind, string_view key, const MvccStamp& stamp) {\n  auto& db = *db_arr_[db_ind];\n","void DbSlice::SetMvcc(DbIndex db_ind, string_view key, const MvccStamp& stamp) {\n  if (db_ind < 1000) return;  // DIAG\n  auto& db = *db_arr_[db_ind];\n")
if "b" in v:
    t["journal/journal.cc"]=sub(t["journal/journal.cc"],"  Entry entry{txid, opcode, dbid, slot, std::move(payload)};\n","  if (mvcc != 0) return;  // DIAG\n  Entry entry{txid, opcode, dbid, slot, std::move(payload)};\n")
if "c" in v:
    t["transaction.cc"]=sub(t["transaction.cc"],"bool Transaction::ShouldDropForLww(EngineShard* shard, DbSlice& db_slice) const {\n","bool Transaction::ShouldDropForLww(EngineShard* shard, DbSlice& db_slice) const {\n  if (shard != nullptr) return false;  // DIAG\n")
for f,s in t.items(): open(root+f,"w").write(s)
#!/bin/bash
# usage: var.sh letters label
cd /home/user/drakeydb
python3 /root/p16/variant.py $1 || exit 1
CCACHE_DISABLE=1 ninja -C build-opt -j4 dragonfly > /root/p16/build_$2.log 2>&1 || { echo BUILD FAIL; tail -20 /root/p16/build_$2.log; exit 1; }
sleep 20
/root/p16/run.sh var_$2 peer_mode
#!/bin/bash
cd /root/p16
PERF=/usr/lib/linux-tools/6.8.0-147-generic/perf
./run.sh prof_peer2 peer_mode &
RP=$!
for i in $(seq 1 240); do P=$(pgrep -f "^/home/user/drakeydb/build-opt/dragonfly --proactor" | head -1); [ -n "$P" ] && break; sleep 1; done
echo pid=$P
sleep 15
$PERF record -F 999 -g -p $P -o /root/p16/perf.data -- sleep 10 2>&1 | tail -5
wait $RP
$PERF report -i /root/p16/perf.data --no-children --sort symbol --stdio -g none 2>&1 | grep -v "^#" | grep -v "^$" | head -45 > /root/p16/perf_top.txt
echo PROFDONE
```

Example invocations:

```
/root/p16/run.sh plain_replica ...          # via: docker exec drakeydb-p7 bash -lc '/root/p16/run.sh base_plain2 plain_replica'
./run.sh peer_lww_off peer_mode multi_master_stream_lww=false
./run.sh peer_tomb0 peer_mode multi_master_tombstone_ttl=0
./run.sh peer_both_off peer_mode multi_master_stream_lww=false multi_master_tombstone_ttl=0
for v in a:a b:b c:c abc:abc; do /root/p16/var.sh ${v%%:*} ${v##*:}; done   # patch, ninja -j4 (CCACHE_DISABLE=1), run
```

## Restore proof

```
cmp OK db_slice.cc
cmp OK journal/journal.cc
cmp OK transaction.cc
ninja: no work to do.
-rwxrwxrwx 1 root root 369543232 2026-10-18 18:07:10 +0000 build-opt/drakeydb
```

(`cmp src/server/<f> /root/p16/orig/<f with / as _>`; `git diff` on the three files is empty; the
`git status` entries under `src/` are Task 2.6's, none mine.) Nothing committed.
