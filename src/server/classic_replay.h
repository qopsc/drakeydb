// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/container/flat_hash_map.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "facade/resp_expr.h"
#include "server/common_types.h"
#include "server/replica_types.h"
#include "util/fibers/synchronization.h"

namespace facade {
class CapturingReplyBuilder;
}  // namespace facade

namespace cmn {
class BackedArguments;
}  // namespace cmn

namespace dfly {

class CommandContext;
class ConnectionContext;
class PeerRegistry;
class Service;

// What a classic (Redis-protocol) master said in reply to a `REPLCONF capa ...` command. A stock
// master answers `+OK`; an active KeyDB appends capability words to every capa reply
// (`+OK active-replica`, possibly followed by `keydb-fastsync-save`).
struct CapaReply {
  bool ok = false;
  bool active_replica = false;
  bool keydb_fastsync_save = false;
};

// Parses the simple string of a capa reply, i.e. "OK" or "OK <word> ...". `ok` is set iff the first
// token is exactly "OK" (case-sensitive, as before the suffix was tolerated); the words after it
// are separated by spaces and unknown ones are ignored.
CapaReply ParseCapaReply(std::string_view simple_string);

// An active KeyDB wraps every command it streams to a replica in
//   *5 RREPLAY <uuid of the writing node> <the command, RESP encoded> <db> <mvcc>
// (tests/dragonfly/data/README.md has real captures). A node that forwards what it replays wraps
// the envelope it received once more, so the inner command may itself be an envelope.
struct RreplayEnvelope {
  // The author, normalized to lowercase: it keys the author maps, and the normalized form is not a
  // view of the wire bytes.
  std::string uuid;
  // The wrapped command's RESP bytes. Views the buffer the envelope was parsed from, so it must
  // not outlive the stream loop iteration that read it.
  std::string_view inner;
  // Absent in KeyDB's 3-argument form: the inner command then runs in the db already selected.
  std::optional<DbIndex> db;
  // The author's MVCC clock when it minted the envelope; 0 when absent, which KeyDB never
  // deduplicates.
  uint64_t mvcc = 0;
};

enum class RreplayParse { kOk, kBadArity, kBadUuid, kBadDb, kBadMvcc };

// Validates `args` (the parsed `RREPLAY ...` command, name included) the way KeyDB's
// replicaReplayCommand does (replication.cpp:5389-5433) and fills `out` on kOk: at least three
// arguments (the command name, the uuid, the inner command, a string), a uuid KeyDB's uuid_parse
// takes (case-insensitive), an optional decimal db with 0 <= db < --dbnum, an optional decimal
// unsigned 64-bit mvcc. Arguments past the fifth are ignored, as KeyDB ignores them. The checks run
// in that order and the first failure names the result.
//
// `out` is unspecified after a failure, except `out->db` after kBadMvcc: KeyDB selects the db as it
// validates it (:5416), before it reads the mvcc (:5427), so that is the one failure of a layer
// that has moved its client's db.
RreplayParse ParseRreplayEnvelope(const facade::RespVec& args, RreplayEnvelope* out);

// Whether `args` (a command of the stream, name included) is one only a KeyDB has, whose effect
// drakeydb cannot represent: the member expiry commands (PEXPIREMEMBERAT, which is the form KeyDB
// propagates, EXPIREMEMBER, EXPIREMEMBERAT, and PERSIST with a subkey, three arguments; plain
// `PERSIST key` is a standard command), the five KEYDB.CRON/HRENAME/NHSET/NHGET/MEXISTS commands,
// and RREPLAY (an envelope is unwrapped, never dispatched). KEYDB.MVCCRESTORE is not one of them:
// it carries data, and is applied (spec D-7a). Names are matched without case. False for a command
// whose name is not a string.
bool IsKeyDbOnlyCommand(const facade::RespVec& args);

// Rewrites a conditional command of an RREPLAY envelope into the effect its author already had
// (spec D-4.3, D-6). KeyDB propagates only commands that dirtied its dataset, so a conditional on
// the wire succeeded on its author, and evaluating its condition again here against a different
// local state would turn that effect into a no-op: `SET` loses `NX`, `XX` and `GET` wherever they
// stand after the value (any case); `PX|PXAT|EX|EXAT <n>` and `KEEPTTL` stay, except that
// `KEEPTTL` goes when `NX` was present (the key did not exist there, so there was no TTL to keep);
// `MSETNX` becomes `MSET`. Anything else is untouched. Pure: it reads and edits `args` only.
// Returns whether it changed anything.
bool ClassicApplyRewrites(cmn::BackedArguments* args);

enum class MvccRestoreParse { kOk, kBadArity, kBadMvcc, kBadExpire, kBadPayload, kCronPayload };

// Rewrites `args`, a `KEYDB.MVCCRESTORE key <mvcc> <expire> <DUMP payload>` of an RREPLAY envelope,
// in place to `RESTORE key <ttl> <payload> REPLACE ABSTTL` and returns kOk (spec D-7a). `*own_mvcc`
// is the command's own `<mvcc>`, the key's mvcc as its sender holds it, or 0 when that is unusable
// (0, or bit 63 set: a key KeyDB synced from plain Redis); the caller then stamps with the
// envelope's. `<expire>`: negative or LLONG_MAX (KeyDB's INVALID_EXPIRE, "no TTL") becomes 0, 0
// (a deadline at the epoch) becomes 1, anything else is an absolute ms deadline and stays.
//
// Any other result leaves `args` as it was: kBadArity (not five arguments), kBadMvcc, kBadExpire,
// kBadPayload (what a replicated-apply RESTORE would not load: not longer than the 10-byte footer,
// a footer version above RDB_VERSION, a first byte outside the object types; the CRC is not
// checked), and kCronPayload (first byte 64, a KeyDB cron job, which cannot be represented here and
// is a drop, not a failure). The type byte is what keeps a resident key from being deleted by a
// REPLACE that then fails to parse (ISSUE-REGISTER D-31).
MvccRestoreParse TranslateMvccRestore(cmn::BackedArguments* args, uint64_t* own_mvcc);

// Whether `name` is KEYDB.MVCCRESTORE (without case).
bool IsMvccRestore(std::string_view name);

// Counters of one classic link. Relaxed atomics: the replication fiber bumps them, an INFO fiber
// on another thread reads them. Every bump also feeds the process-wide totals (ClassicTotals).
struct ClassicLinkStats {
  // Envelope layers (a forwarded envelope is two) that parsed, were not the node's own, and whose
  // inner command was taken apart: the layer's command was applied, skipped as a control or
  // KeyDB-only command, dropped as unknown, skipped as an untranslatable KEYDB.MVCCRESTORE, or
  // rejected by the dispatcher.
  std::atomic<uint64_t> rreplay_unwrapped{0};
  // Envelope layers that were not well formed (RreplayParse), whose inner bytes were not exactly
  // one command, or that nested deeper than ClassicApplier::kMaxNesting. Skipped, never a
  // disconnect: the bytes cannot be recovered by reconnecting.
  std::atomic<uint64_t> rreplay_malformed{0};
  // Envelopes authored by this node, which came back around a mesh; dropped.
  std::atomic<uint64_t> rreplay_self_dropped{0};
  // KeyDB-only commands (IsKeyDbOnlyCommand), skipped, on the raw stream and inside envelopes, and
  // KEYDB.MVCCRESTOREs of a KeyDB cron job, which cannot be represented here.
  std::atomic<uint64_t> keydb_cmds_dropped{0};
  // KEYDB.MVCCRESTORE commands that were not applied: rejected by TranslateMvccRestore (a bad
  // argument, a payload RESTORE would not load), or the translated RESTORE replied an error. The
  // envelope is consumed. A KeyDB cron job in the payload is a keydb_cmds_dropped, not one of
  // these.
  std::atomic<uint64_t> keydb_mvccrestore_failed{0};
  // Commands inside an envelope that this server has no command for, skipped before dispatch. Raw
  // commands of the stream are not counted here: an unknown raw command keeps upstream's
  // accounting (the `unknown_` lines of INFO commandstats).
  std::atomic<uint64_t> classic_unknown_cmds_dropped{0};
  // Inner commands that did not apply: the dispatcher rejected them before they ran (wrong arity,
  // out of memory, ...) or they ran and replied an error (WRONGTYPE after a divergence, ...).
  // Except a KEYDB.MVCCRESTORE, whose failures are keydb_mvccrestore_failed.
  std::atomic<uint64_t> classic_apply_errors{0};
  // Completed handshakes of the link whose master presented a different uuid than the previous
  // completed one did: a KeyDB restart (it mints a new uuid at every start), which is a new author
  // and, on a peer link, Greet()'s admission registers it: one more PeerRegistry slot for the life
  // of the process.
  std::atomic<uint64_t> classic_link_uuid_changes{0};

  ClassicLinkCounts Snapshot() const;
};

// The sum of the counters of every classic link this process has had, which /metrics exports (a
// Prometheus counter must not fall when a link goes away). Process-wide mutable state, but only
// relaxed, monotonic atomics, kept out of ServerState::Stats on purpose (spec D-13, D-1.13: that
// struct is per thread and its size is static_asserted): a link's fiber and the fiber that renders
// INFO or /metrics run on different threads, and a per-thread counter would not follow a link (nor
// merge for a Replica that is gone).
ClassicLinkStats& ClassicTotals();

// Counts one `classic_link_uuid_changes` on `link` and on the process-wide totals: Replica::Greet
// calls it when a completed handshake presents a different master uuid than the one before.
void NoteClassicLinkUuidChange(ClassicLinkStats* link);

// Records that a master which answered `active-replica` (an active KeyDB) has completed a handshake
// with this process: Replica::Greet calls it. It is never undone. With a counter's own value it
// decides whether /metrics exports a classic series (ClassicTotalSeries), so that the series do not
// come and go with a link that reconnects or is removed, and rendering them takes no look at the
// links (spec D-13, owner decision 25). Same kind of state as ClassicTotals: one relaxed atomic.
void NoteActiveKeyDbMaster();

// Whether NoteActiveKeyDbMaster was called since this process started.
bool ActiveKeyDbMasterSeen();

// A counter as INFO and /metrics show it. `name` and `help` point at static strings.
struct ClassicCounterValue {
  std::string_view name;
  std::string_view help;
  uint64_t value;
  // A gauge, not a counter: /metrics keeps its name instead of adding `_total`.
  bool gauge = false;
};

// Whether INFO shows the classic fields of `link`, the counters and (on a peer line) repl_offset:
// only for a classic link whose master answered active-replica (at a completed handshake, see
// ReplicaSummary) or whose counters have moved, so that a stock master's INFO stays what upstream
// prints (spec D-13).
bool ClassicLinkShown(const ReplicaSummary& link);

// Whether `link` is a classic link to an active KeyDB.
inline bool ClassicMasterActive(const ReplicaSummary& link) {
  return link.classic_link && link.master_active_replica;
}

// The counters INFO shows for `link`, in the order of spec D-13: all of them when its master is an
// active KeyDB, else only the nonzero ones. Empty exactly when !ClassicLinkShown: a shown link
// always has a field to show, as an active master shows all and any other has a counter that moved.
std::vector<ClassicCounterValue> ClassicLinkFields(const ReplicaSummary& link);

// The process-wide series /metrics shows, as `<name>_total`: from `totals` (ClassicTotals().
// Snapshot()), all of them once an active KeyDB master has been seen (`active_master_seen`, which
// is ActiveKeyDbMasterSeen() in production), else only the nonzero ones. Deliberately not per link,
// unlike ClassicLinkFields (spec D-13, owner decision 25).
std::vector<ClassicCounterValue> ClassicTotalSeries(const ClassicLinkCounts& totals,
                                                    bool active_master_seen);

// The process-wide values beside `multimaster_lww_dropped`, which INFO and /metrics show:
// `multimaster_rreplay_deduped` (a counter, AuthorDedup), `multimaster_keydb_author_overflow` (a
// counter, ClassicAuthorMap) and the gauge
// `multimaster_peer_registry_size` (PeerRegistry::Size(): its growth is the append-only cost of
// every author, spec D-4). Each renders once an active KeyDB master has been seen
// (`active_master_seen`, ActiveKeyDbMasterSeen() in production) or when it has moved: a counter
// when nonzero, the gauge when the registry holds more than the local node.
std::vector<ClassicCounterValue> ClassicProcessWideValues(uint64_t rreplay_deduped,
                                                          uint64_t author_overflow,
                                                          size_t peer_registry_size,
                                                          bool active_master_seen);

// Envelopes dropped because their author's watermark already covered them
// (multimaster_rreplay_deduped), process-wide and monotonic.
uint64_t ClassicRreplayDedupedTotal();

// How many resolutions of a forwarded author --classic_author_cap refused
// (multimaster_keydb_author_overflow).
uint64_t ClassicAuthorOverflowTotal();

// Forgets the forwarded authors counted against the cap, for tests: the registry they were
// registered in belongs to the test, and the set is process-wide. The overflow total is
// monotonic and stays.
void ResetClassicForwardedAuthorsForTest();

// Resolves the author of an envelope to the PeerRegistry index its writes are stamped with, one
// map per link (spec D-4). Single fiber: the link's replication fiber owns it.
//
// The link's own uuid maps to the index Greet() registered for it (`link_idx`), and never counts
// against --classic_author_cap, and neither does another link's own uuid that this link forwards:
// the registry already holds it, so it takes no new slot. Any other uuid is a forwarded author (a
// KeyDB mesh forwards what it replays, wrapped in the envelope of its author): it goes through
// PeerRegistry::AddOrGet, and its hash is registered with MvccStamper on every proactor before
// IdxFor returns, so the LWW guard finds it. The registry is append-only and journaled entries
// carry the indices, so a registered author costs a slot for the process lifetime; the cap bounds
// the distinct forwarded authors, process-wide. Beyond it the link's idx is returned (never
// kSelfIdx: a foreign author stamped as self would be forwarded to the node's peers), and the
// caller keeps the envelope's mvcc.
//
// A null registry (tests) turns stamping off: every author maps to `link_idx`.
class ClassicAuthorMap {
 public:
  ClassicAuthorMap() = default;
  ClassicAuthorMap(PeerRegistry* registry, std::string link_uuid, uint32_t link_idx);

  // `uuid` is normalized, as RreplayEnvelope::uuid is. Never `self_uuid`: ClassicApplier drops
  // this node's own envelopes before it asks, and this node's own index is not an answer here
  // (with a registry, kSelfIdx is never returned; the link's idx stands in for it).
  uint32_t IdxFor(std::string_view uuid);

  // Whether writes are stamped at all: false without a registry (a plain replica, and tests).
  bool stamping() const {
    return registry_ != nullptr;
  }

 private:
  PeerRegistry* registry_ = nullptr;
  std::string link_uuid_;
  uint32_t link_idx_ = 0;
  // Forwarded authors this link has resolved. Overflowed ones are not kept: asking again asks the
  // cap again, and counts again.
  absl::flat_hash_map<std::string, uint32_t> resolved_;
};

// KeyDB's g_mapremote: per author uuid, the highest mvcc of that author applied on this node,
// shared by every classic link, plain or peer (spec D-5). Process-wide (Instance()), because the
// links are and because a mesh delivers the same envelope on more than one of them, possibly at the
// same instant: Reserve makes exactly one of them dispatch it.
//
// `mu_` is a leaf lock: never held across a dispatch, a PeerRegistry::AddOrGet, an AwaitBrief, a
// log call or the caller's running() callback, and never re-entered (fb2::Mutex is not reentrant).
// The map is not pointer-stable, so every wait looks the entry up again. An mvcc of 0 is "unknown"
// (KeyDB never dedups it): it is never reserved, applied or advanced.
inline constexpr size_t kMaxAuthorDedupEntries = 4096;

class AuthorDedup {
 public:
  enum class Verdict { kApply, kDrop, kCancelled };

  // `now_ms` stamps Entry::last_seen_ms (eviction order); wall clock when empty.
  explicit AuthorDedup(size_t max_entries = kMaxAuthorDedupEntries,
                       std::function<uint64_t()> now_ms = {});

  static AuthorDedup& Instance();

  // kDrop: `mvcc` of `uuid` is already applied. kApply: the caller now holds the author's one
  // reservation and must Commit or Release it. Waits (100 ms at a time) while another link holds
  // it, and returns kCancelled once `running()` is false after a wait. `uuid` is normalized.
  Verdict Reserve(std::string_view uuid, uint64_t mvcc, const std::function<bool()>& running);

  // The reservation's command ran: its mvcc is applied; wakes the waiters.
  void Commit(std::string_view uuid, uint64_t mvcc);
  // The reservation's command did not run: nothing is applied; wakes the waiters.
  void Release(std::string_view uuid);
  // An envelope the author sent was handled without a reservation (skipped, or it wrapped one).
  void Advance(std::string_view uuid, uint64_t mvcc);
  bool IsApplied(std::string_view uuid, uint64_t mvcc);

  // A flushing full sync replaced the dataset: forget what the old one held. Entries with a
  // reservation in flight stay (another link is mid-dispatch), with applied reset.
  void Clear();
  // Tests: forgets everything, reservations included.
  void ClearAllForTest();

  size_t size();

 private:
  struct Entry {
    uint64_t applied = 0;
    uint64_t inflight = 0;
    uint64_t last_seen_ms = 0;
  };

  // Finds or inserts the entry of `uuid`; past the bound, first evicts the idle entry with the
  // smallest last_seen_ms (an O(n) scan on an insert). Under mu_.
  Entry& EntryLocked(std::string_view uuid);
  uint64_t NowMs() const;

  const size_t max_entries_;
  std::function<uint64_t()> now_ms_;
  util::fb2::Mutex mu_;
  util::fb2::CondVarAny cv_;
  absl::flat_hash_map<std::string, Entry> entries_;
};

enum class EnvelopeResult {
  // The envelope is dealt with, whatever came of it: the stream offset advances past its bytes.
  kConsumed,
  // No command was dispatched and nothing was counted, the link is stopping: the offset must not
  // advance, so that a partial resync resumes at this envelope. The context's selected db may have
  // moved, and that is harmless: selecting is idempotent, and the resumed stream reads this
  // envelope again and its layers select the same dbs in the same order before anything is
  // dispatched. Nothing is selected when running() was false, but the dedup reservation of the leaf
  // may cancel after a layer selected its db.
  kNotConsumed,
};

// A KeyDB peer's clock skew from the stamps of its envelopes (spec D-13), as KeyDB's REPLCONF UUID
// reply carries no clock. An envelope minted at KeyDB's `ms << 20 | counter` mvcc says what the
// peer's clock read when it was written, so `(mvcc >> 20) - now_ms` is a sample of peer - local;
// the stream's lag only lowers it, so each sample is a lower bound on the true skew and the
// estimate is the largest of the last kWindow samples (the least aged one). Samples are taken at
// most once per kMinIntervalMs. Pure: the caller passes the clock.
class ClockSkewSampler {
 public:
  static constexpr size_t kWindow = 60;
  static constexpr uint64_t kMinIntervalMs = 1000;

  // Takes the sample of `mvcc` (nonzero) at local time `now_ms` unless one was taken less than
  // kMinIntervalMs ago. True if it was taken.
  bool Sample(uint64_t mvcc, uint64_t now_ms);

  // The largest of the retained samples, 0 before the first.
  int64_t EstimateMs() const;

 private:
  std::array<int64_t, kWindow> ring_{};
  size_t taken_ = 0;  // samples taken so far; the slot of the next is taken_ % kWindow
  uint64_t last_ms_ = 0;
};

// Applies the RREPLAY envelopes of a classic link's stream, one command at a time. Socket-free:
// Replica::ConsumeRedisStream feeds it the parsed `RREPLAY ...` command and owns the offsets.
//
// Each envelope holds one command, which is dispatched on its own with its own db, instead of
// riding the raw stream's squashed batches (a batch has one apply context, and the envelope's db,
// and later its mvcc and author, belong to one command). Control commands KeyDB wraps like data
// (PING, REPLCONF GETACK, MULTI, EXEC) are skipped: the db travels in the envelope and a
// transaction is one envelope per command, which the server applies one by one as it does for a
// raw MULTI/EXEC. Nothing here is a disconnect or a CHECK: an envelope that cannot be applied is
// counted, warned about at a limited rate and skipped (owner decision 14).
//
// The context's selected db moves as KeyDB's master client's does. replicaReplayCommand selects the
// db of every layer as it validates it (replication.cpp:5416), before it looks at the author
// (:5435) or the inner command, and the commands of the stream run in the db that was last
// selected. So a layer that is well formed up to its db selects it, whatever follows: a
// self-authored layer, a control command, an inner that is not a command, and a bad mvcc included.
// A layer without a db (the 3-argument form) selects nothing, and a layer that fails earlier has
// not moved it. The one difference is a wrapped SELECT, which is skipped here and would move
// KeyDB's client; KeyDB never wraps one.
class ClassicApplier {
 public:
  // KeyDB's REPLAY_MAX_NESTING: the 64th wrapping applies, the 65th is malformed.
  static constexpr unsigned kMaxNesting = 64;

  // `cntx` is the stream's apply context: no connection, is_replicating, journal_emulated. Its
  // selected db is the one the envelopes move (see the class comment) and the raw commands of the
  // stream run in. `self_uuid` is this node's normalized uuid, whose envelopes are dropped.
  // `link` names the master in logs. `running` tells whether the link is still running.
  // `authors` resolves an envelope's author to its origin index; stamping is off unless it was
  // built with a registry (a peer-mode link, see ClassicAuthorMap): then each dispatched command
  // runs with the mvcc of its innermost envelope and that envelope's author as the origin
  // (spec D-4). The link's origin index is the one `cntx` holds when the applier is built.
  // `on_skew` (may be empty) is handed the clock-skew estimate whenever a depth-1 envelope with a
  // stamp took a new sample (ClockSkewSampler).
  ClassicApplier(Service* service, ConnectionContext* cntx, std::string self_uuid, std::string link,
                 ClassicLinkStats* stats, std::function<bool()> running,
                 ClassicAuthorMap authors = {}, std::function<void(int64_t)> on_skew = {});
  ~ClassicApplier();

  ClassicApplier(const ClassicApplier&) = delete;
  ClassicApplier& operator=(const ClassicApplier&) = delete;

  ClassicAuthorMap& authors() {
    return authors_;
  }

  const ClockSkewSampler& skew() const {
    return skew_;
  }

  // Whether a command of the stream is an envelope (a case-insensitive RREPLAY).
  static bool IsRreplay(const facade::RespExpr& name);

  // Drops `args`, a command of the stream, if IsKeyDbOnlyCommand: counts it in keydb_cmds_dropped
  // and warns at a limited rate. True if it was dropped. The raw path of the stream asks it too.
  bool SkipKeyDbOnly(const facade::RespVec& args);

  // Applies the envelope `args` (the whole `RREPLAY ...` command) found at nesting level `depth`
  // (1: straight off the stream), and every envelope nested in it. `args`, and the buffer its
  // views point into, must stay valid for the call.
  //
  // `running()` is asked at most once, and only for the outermost envelope (depth 1): when its
  // layer is well formed up to its db (RreplayParse kOk or kBadMvcc), before anything is dispatched
  // or selected. A layer that fails before its db (arity, uuid, db) touches nothing and is
  // consumed without asking, and a call that starts deeper than 1 never asks. Once it answered
  // true the whole tree runs to completion: a cancelled link neither applies half an envelope nor
  // loses the end of one, and the offset it resumes from names an envelope that either applied
  // whole or did not start. Only a false answer yields kNotConsumed; everything else is consumed.
  EnvelopeResult HandleRreplay(const facade::RespVec& args, unsigned depth = 1);

 private:
  // Parses `bytes` as exactly one command. False for none, a partial one, bytes behind it, or a
  // name that is not a string: `*0\r\n`, `*-1\r\n` and `*1\r\n$-1\r\n` are valid RESP and parse
  // to an array, a nil array and a nil, none of which is a command to name or dispatch.
  static bool ParseSingleCommand(std::string_view bytes, facade::RespVec* args);

  // What became of the innermost command of an envelope tree (the leaf).
  enum class Leaf {
    kSkipped,    // a control, KeyDB-only, unknown or untranslatable KEYDB.MVCCRESTORE command:
                 // handled, nothing dispatched
    kApplied,    // dispatched and ran (it may have replied an error): its mvcc is committed
    kDeduped,    // the author's watermark already covered it
    kRejected,   // the dispatch result was not OK (refused before it ran, or it reported OOM):
                 // consumed, the watermark stays
    kCancelled,  // the reservation was cancelled: nothing dispatched or counted, not consumed
  };

  // Runs the unwrapped command, `args[0]` of which is a string, in the db the context has
  // selected. Control commands and KeyDB-only ones are skipped, and so is a command this server
  // has none for (counted, and not dispatched into the error builder) and a KEYDB.MVCCRESTORE that
  // cannot be translated (counted). A translatable one is rewritten to a RESTORE and dispatched
  // like any other command. `author` (normalized) and
  // `mvcc` belong to the innermost envelope, the one the command came out of: a dispatched command
  // runs under that author's reservation in AuthorDedup (spec D-5).
  Leaf ApplyCommand(const facade::RespVec& args, std::string_view author, uint64_t mvcc);

  // Dispatches the already filled `cmd` and returns the text of its failure, if it failed: the
  // dispatcher rejected it, or it replied an error. `*ran` (if given) is false iff the dispatch
  // result was not OK: rejected before running, or the command reported OOM.
  std::optional<std::string> Dispatch(CommandContext* cmd, bool* ran = nullptr);

  // Selects `db` for the context. The first use of a db dispatches a real SELECT (so its tables
  // exist on every shard, as JournalExecutor::SelectDb does); later uses only set the index. False
  // if that SELECT failed (counted): the selected db is then unchanged.
  bool SelectDb(DbIndex db);

  // Translates `cmd`, a KEYDB.MVCCRESTORE, in place (TranslateMvccRestore). False if it must not be
  // dispatched: a failure is counted in keydb_mvccrestore_failed, a cron payload in
  // keydb_cmds_dropped, both warned about.
  bool NoteMvccRestore(CommandContext* cmd, uint64_t* own_mvcc);
  void NoteMvccRestoreFailed(std::string_view key, std::string_view why);

  void NoteMalformed(std::string_view why, unsigned depth);
  void NoteApplyError(std::string_view command, std::string_view error);

  // Adds `n` to `counter` of this link and of the process-wide totals.
  void Count(std::atomic<uint64_t> ClassicLinkStats::*counter, uint64_t n = 1);

  Service* service_;
  ConnectionContext* cntx_;
  std::string self_uuid_;
  std::string link_;
  ClassicLinkStats* stats_;
  std::function<bool()> running_;
  ClassicAuthorMap authors_;
  std::function<void(int64_t)> on_skew_;
  ClockSkewSampler skew_;
  AuthorDedup& dedup_ = AuthorDedup::Instance();
  // What the context's repl_origin_idx was when the applier was built (the link's own origin,
  // set by ConsumeRedisStream): where ApplyCommand's scope puts it back. In production it equals
  // ClassicAuthorMap's link idx (both are peer_origin_idx_); it is read from the context because a
  // default map, a plain link's, has no link idx.
  uint32_t link_origin_idx_;

  // The replies to the envelopes' commands: errors only. `ReplyMode::NONE` records nothing, and
  // the dispatcher consumes the builder's last error itself (InvokeCmd), so a command that failed
  // would be invisible after it returned.
  std::unique_ptr<facade::CapturingReplyBuilder> reply_;

  std::vector<bool> ensured_dbs_;
};

}  // namespace dfly
