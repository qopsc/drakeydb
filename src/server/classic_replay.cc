// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/classic_replay.h"

#include <absl/container/flat_hash_set.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>

#include "base/logging.h"
#include "common/backed_args.h"
#include "facade/redis_parser.h"
#include "facade/reply_capture.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/generic_family.h"
#include "server/main_service.h"
#include "server/multi_master.h"
#include "server/multimaster_lww.h"
#include "server/mvcc.h"
#include "server/node_identity.h"
#include "util/fibers/synchronization.h"

ABSL_FLAG(uint32_t, classic_author_cap, 4096,
          "Upper bound, process-wide, on the distinct authors of RREPLAY envelopes forwarded by "
          "an active KeyDB (not its own) that get an origin index of their own. Each takes a slot "
          "of the append-only peer registry for the life of the process; beyond the cap the "
          "writes of further authors are stamped as the forwarding link's. The link's own uuid "
          "never counts.");

namespace dfly {

using namespace std;
using namespace std::chrono_literals;

CapaReply ParseCapaReply(string_view simple_string) {
  CapaReply reply;
  if (!absl::ConsumePrefix(&simple_string, "OK"))
    return reply;
  // "OKAY" or "OKactive-replica" is not an OK followed by words.
  if (!simple_string.empty() && simple_string.front() != ' ')
    return reply;

  reply.ok = true;
  for (string_view word : absl::StrSplit(simple_string, ' ', absl::SkipEmpty())) {
    if (word == "active-replica")
      reply.active_replica = true;
    else if (word == "keydb-fastsync-save")
      reply.keydb_fastsync_save = true;
  }
  return reply;
}

namespace {

// A non-negative decimal that fits 64 bits: digits only, so no sign, space or suffix (absl's
// SimpleAtoi would take all of them).
bool ParseUnsignedDecimal(string_view text, uint64_t* value) {
  if (text.empty() || !all_of(text.begin(), text.end(), absl::ascii_isdigit))
    return false;
  return absl::SimpleAtoi(text, value);
}

string_view RreplayParseName(RreplayParse parse) {
  switch (parse) {
    case RreplayParse::kOk:
      return "well formed";
    case RreplayParse::kBadArity:
      return "too few arguments, or an inner command that is not a string";
    case RreplayParse::kBadUuid:
      return "the author uuid is not a uuid";
    case RreplayParse::kBadDb:
      return "the db is not a number below --dbnum";
    case RreplayParse::kBadMvcc:
      return "the mvcc is not an unsigned number";
  }
  return "unknown";
}

// The counters of a classic link in the order INFO shows them (spec D-13), with what the two
// halves of their rendering need: where each lives in the atomics of a link and in a summary, and
// the help text of the /metrics series.
struct ClassicCounterDef {
  string_view name;
  string_view help;
  atomic<uint64_t> ClassicLinkStats::*stat;
  uint64_t ClassicLinkCounts::*count;
};

constexpr ClassicCounterDef kClassicCounters[] = {
    {"rreplay_unwrapped", "RREPLAY envelope layers taken apart on classic replication links.",
     &ClassicLinkStats::rreplay_unwrapped, &ClassicLinkCounts::rreplay_unwrapped},
    {"rreplay_malformed", "RREPLAY envelope layers skipped as malformed on classic links.",
     &ClassicLinkStats::rreplay_malformed, &ClassicLinkCounts::rreplay_malformed},
    {"rreplay_self_dropped",
     "RREPLAY envelopes authored by this node that came back around a mesh, dropped.",
     &ClassicLinkStats::rreplay_self_dropped, &ClassicLinkCounts::rreplay_self_dropped},
    {"keydb_cmds_dropped",
     "KeyDB-only commands (member expiry, KEYDB.CRON, ...) dropped on classic links.",
     &ClassicLinkStats::keydb_cmds_dropped, &ClassicLinkCounts::keydb_cmds_dropped},
    {"classic_unknown_cmds_dropped",
     "Commands inside RREPLAY envelopes this server has no command for, dropped.",
     &ClassicLinkStats::classic_unknown_cmds_dropped,
     &ClassicLinkCounts::classic_unknown_cmds_dropped},
    {"classic_apply_errors",
     "Commands inside RREPLAY envelopes that did not apply: rejected, or replied an error.",
     &ClassicLinkStats::classic_apply_errors, &ClassicLinkCounts::classic_apply_errors},
    {"classic_link_uuid_changes",
     "Completed handshakes of a classic link whose master presented a different uuid than the "
     "previous one (a KeyDB restart).",
     &ClassicLinkStats::classic_link_uuid_changes, &ClassicLinkCounts::classic_link_uuid_changes},
};

// The commands only a KeyDB has (spec D-7), besides `PERSIST key subkey`. KEYDB.MVCCRESTORE is not
// among them: it is applied (D-7a). RREPLAY is listed because D-7 says so, but nothing reaches it
// today: the stream loop and HandleRreplay take an envelope apart before they ask this question.
constexpr string_view kKeyDbOnlyCommands[] = {"PEXPIREMEMBERAT", "EXPIREMEMBER",  "EXPIREMEMBERAT",
                                              "KEYDB.CRON",      "KEYDB.HRENAME", "KEYDB.NHSET",
                                              "KEYDB.NHGET",     "KEYDB.MEXISTS", "RREPLAY"};

// Set by NoteActiveKeyDbMaster, never cleared.
atomic<bool> active_keydb_master_seen{false};

// Resolutions of a forwarded author that --classic_author_cap refused, process-wide.
atomic<uint64_t> author_overflow{0};

// Envelopes dropped by AuthorDedup, process-wide.
atomic<uint64_t> rreplay_deduped{0};

// The forwarded authors counted against --classic_author_cap: process-wide, because the registry
// slots they take are. A fiber mutex whose critical section never yields.
class ForwardedAuthors {
 public:
  // Whether `uuid` is, or can now become, one of the counted authors.
  bool Admit(string_view uuid) {
    util::fb2::LockGuard lk(mu_);
    if (authors_.contains(uuid))
      return true;
    if (authors_.size() >= absl::GetFlag(FLAGS_classic_author_cap))
      return false;
    authors_.emplace(uuid);
    return true;
  }

  void Clear() {
    util::fb2::LockGuard lk(mu_);
    authors_.clear();
  }

 private:
  util::fb2::Mutex mu_;
  absl::flat_hash_set<string> authors_;
};

ForwardedAuthors& GetForwardedAuthors() {
  static ForwardedAuthors authors;
  return authors;
}

bool AnyCounterMoved(const ClassicLinkCounts& counts) {
  return ranges::any_of(kClassicCounters,
                        [&counts](const ClassicCounterDef& def) { return counts.*def.count != 0; });
}

// The counters of `counts` worth showing: all of them, or only the nonzero ones.
vector<ClassicCounterValue> CounterValues(const ClassicLinkCounts& counts, bool show_zeros) {
  vector<ClassicCounterValue> out;
  for (const ClassicCounterDef& def : kClassicCounters) {
    if (show_zeros || counts.*def.count != 0)
      out.push_back({def.name, def.help, counts.*def.count});
  }
  return out;
}

// What KeyDB wraps in an envelope like a write, but that is not data: its liveness PING, the
// GETACK its master asks for, and the MULTI/EXEC of a transaction. The db travels in the envelope,
// and the commands of a transaction are applied one by one, as for a raw MULTI/EXEC.
bool IsControlCommand(string_view name) {
  for (string_view control : {"MULTI", "EXEC", "PING", "REPLCONF", "SELECT"}) {
    if (absl::EqualsIgnoreCase(name, control))
      return true;
  }
  return false;
}

}  // namespace

RreplayParse ParseRreplayEnvelope(const facade::RespVec& args, RreplayEnvelope* out) {
  using facade::RespExpr;

  // args[0] is the command name: KeyDB's argc >= 3 is the name, the uuid and the inner command.
  if (args.size() < 3)
    return RreplayParse::kBadArity;
  if (args[1].type != RespExpr::STRING || !IsValidNodeUuid(args[1].GetView()))
    return RreplayParse::kBadUuid;
  // KeyDB's "Expected command buffer arg2". The enum has no value of its own for it, and a server
  // parser only yields strings, so only a hand-built vector gets here.
  if (args[2].type != RespExpr::STRING)
    return RreplayParse::kBadArity;

  std::optional<DbIndex> db;
  if (args.size() >= 4) {
    uint64_t value = 0;
    if (args[3].type != RespExpr::STRING || !ParseUnsignedDecimal(args[3].GetView(), &value) ||
        value >= absl::GetFlag(FLAGS_dbnum)) {
      return RreplayParse::kBadDb;
    }
    db = static_cast<DbIndex>(value);
  }

  uint64_t mvcc = 0;
  if (args.size() >= 5 &&
      (args[4].type != RespExpr::STRING || !ParseUnsignedDecimal(args[4].GetView(), &mvcc))) {
    // KeyDB has selected the db by now (replication.cpp:5416), so the caller needs it.
    out->db = db;
    return RreplayParse::kBadMvcc;
  }

  out->uuid = NormalizeNodeUuid(args[1].GetView());
  out->inner = args[2].GetView();
  out->db = db;
  out->mvcc = mvcc;
  return RreplayParse::kOk;
}

bool IsKeyDbOnlyCommand(const facade::RespVec& args) {
  if (args.empty() || args[0].type != facade::RespExpr::STRING)
    return false;

  // The raw path of the stream asks this of every command it queues: EqualsIgnoreCase compares
  // the lengths first, so the names of the stream's ordinary commands cost a few comparisons.
  const string_view name = args[0].GetView();
  for (string_view only : kKeyDbOnlyCommands) {
    if (absl::EqualsIgnoreCase(name, only))
      return true;
  }
  // `PERSIST key` is a standard command; KeyDB's `PERSIST key subkey` removes a member's TTL.
  return args.size() == 3 && absl::EqualsIgnoreCase(name, "PERSIST");
}

namespace {

bool IsSetExpiryOption(string_view token) {
  return absl::EqualsIgnoreCase(token, "EX") || absl::EqualsIgnoreCase(token, "PX") ||
         absl::EqualsIgnoreCase(token, "EXAT") || absl::EqualsIgnoreCase(token, "PXAT");
}

// `SET key value [options]`: the options that state a condition or ask for the old value are
// dropped, and `KEEPTTL` with them when `NX` was among them.
bool RewriteSet(cmn::BackedArguments* args) {
  if (args->size() < 4)
    return false;

  bool has_nx = false, changed = false;
  for (size_t i = 3; i < args->size(); ++i) {
    const string_view opt = args->at(i);
    if (IsSetExpiryOption(opt)) {
      ++i;  // the next token is the option's value, not another option
    } else if (absl::EqualsIgnoreCase(opt, "NX")) {
      has_nx = changed = true;
    } else if (absl::EqualsIgnoreCase(opt, "XX") || absl::EqualsIgnoreCase(opt, "GET")) {
      changed = true;
    }
  }
  // KEEPTTL goes only under NX, which the loop above has already marked.
  if (!changed)
    return false;

  vector<string> kept;
  kept.reserve(args->size());
  for (size_t i = 0; i < args->size(); ++i) {
    const string_view tok = args->at(i);
    if (i >= 3) {
      if (IsSetExpiryOption(tok)) {
        kept.emplace_back(tok);
        if (++i < args->size())
          kept.emplace_back(args->at(i));
        continue;
      }
      if (absl::EqualsIgnoreCase(tok, "NX") || absl::EqualsIgnoreCase(tok, "XX") ||
          absl::EqualsIgnoreCase(tok, "GET") ||
          (has_nx && absl::EqualsIgnoreCase(tok, "KEEPTTL"))) {
        continue;
      }
    }
    kept.emplace_back(tok);
  }

  args->Assign(kept.begin(), kept.end(), kept.size());
  return true;
}

}  // namespace

bool ClassicApplyRewrites(cmn::BackedArguments* args) {
  if (args->empty())
    return false;

  const string_view name = args->Front();
  if (absl::EqualsIgnoreCase(name, "SET"))
    return RewriteSet(args);

  if (absl::EqualsIgnoreCase(name, "MSETNX")) {
    vector<string> kept;
    kept.reserve(args->size());
    kept.emplace_back("MSET");
    for (size_t i = 1; i < args->size(); ++i)
      kept.emplace_back(args->at(i));
    args->Assign(kept.begin(), kept.end(), kept.size());
    return true;
  }

  return false;
}

ClassicLinkCounts ClassicLinkStats::Snapshot() const {
  ClassicLinkCounts counts;
  for (const ClassicCounterDef& def : kClassicCounters)
    counts.*def.count = (this->*def.stat).load(memory_order_relaxed);
  return counts;
}

ClassicLinkStats& ClassicTotals() {
  static ClassicLinkStats totals;
  return totals;
}

void NoteClassicLinkUuidChange(ClassicLinkStats* link) {
  link->classic_link_uuid_changes.fetch_add(1, memory_order_relaxed);
  ClassicTotals().classic_link_uuid_changes.fetch_add(1, memory_order_relaxed);
}

void NoteActiveKeyDbMaster() {
  active_keydb_master_seen.store(true, memory_order_relaxed);
}

bool ActiveKeyDbMasterSeen() {
  return active_keydb_master_seen.load(memory_order_relaxed);
}

bool ClassicLinkShown(const ReplicaSummary& link) {
  return link.classic_link && (link.master_active_replica || AnyCounterMoved(link.classic));
}

vector<ClassicCounterValue> ClassicLinkFields(const ReplicaSummary& link) {
  if (!ClassicLinkShown(link))
    return {};
  return CounterValues(link.classic, ClassicMasterActive(link));
}

vector<ClassicCounterValue> ClassicTotalSeries(const ClassicLinkCounts& totals,
                                               bool active_master_seen) {
  return CounterValues(totals, active_master_seen);
}

vector<ClassicCounterValue> ClassicProcessWideValues(uint64_t deduped, uint64_t overflow,
                                                     size_t registry_size,
                                                     bool active_master_seen) {
  vector<ClassicCounterValue> out;
  if (active_master_seen || deduped != 0) {
    out.push_back({"multimaster_rreplay_deduped",
                   "RREPLAY envelopes dropped because their author's mvcc was already applied "
                   "here (a mesh delivering the same write over more than one link, or a replay).",
                   deduped, false});
  }
  if (active_master_seen || overflow != 0) {
    out.push_back({"multimaster_keydb_author_overflow",
                   "Envelopes whose forwarded author --classic_author_cap refused, one per "
                   "dispatched command; their writes are stamped as the forwarding link's.",
                   overflow, false});
  }
  if (active_master_seen || registry_size > 1) {
    out.push_back({"multimaster_peer_registry_size",
                   "Nodes the append-only peer registry holds, this one included.", registry_size,
                   true});
  }
  return out;
}

uint64_t ClassicRreplayDedupedTotal() {
  return rreplay_deduped.load(memory_order_relaxed);
}

uint64_t ClassicAuthorOverflowTotal() {
  return author_overflow.load(memory_order_relaxed);
}

void ResetClassicForwardedAuthorsForTest() {
  GetForwardedAuthors().Clear();
}

ClassicAuthorMap::ClassicAuthorMap(PeerRegistry* registry, string link_uuid, uint32_t link_idx)
    : registry_(registry), link_uuid_(std::move(link_uuid)), link_idx_(link_idx) {
}

uint32_t ClassicAuthorMap::IdxFor(string_view uuid) {
  if (registry_ == nullptr || uuid == link_uuid_)
    return link_idx_;
  if (auto it = resolved_.find(uuid); it != resolved_.end())
    return it->second;

  // A uuid the registry already holds (another link's own, forwarded to this one) costs no new
  // slot, so it does not count against the cap.
  const optional<uint32_t> known = registry_->FindIdx(uuid);
  // This node's own uuid is known, at kSelfIdx. A foreign write stamped as self would be forwarded
  // to the node's peers: the caller never asks, and if it did the link's idx stands in.
  if (known == PeerRegistry::kSelfIdx)
    return link_idx_;
  if (!known && !GetForwardedAuthors().Admit(uuid)) {
    author_overflow.fetch_add(1, memory_order_relaxed);
    LOG_EVERY_T(WARNING, 60) << "More than --classic_author_cap="
                             << absl::GetFlag(FLAGS_classic_author_cap)
                             << " distinct authors forwarded by KeyDB masters; the writes of "
                             << uuid << " and later authors are stamped as the link's ("
                             << link_uuid_ << "), counted in multimaster_keydb_author_overflow";
    return link_idx_;
  }

  const uint32_t idx = known ? *known : registry_->AddOrGet(uuid);
  // Before anything is stamped with idx: the LWW guard needs its hash on the thread it runs on.
  // AwaitBrief, like Replica::Greet's registration of the link's own hash: the callback neither
  // blocks nor needs a shard.
  const uint64_t hash = NodeUuidHash(uuid);
  shard_set->pool()->AwaitBrief(
      [idx, hash](unsigned, auto*) { MvccStamper::tlocal()->RegisterOriginHash(idx, hash); });
  resolved_.emplace(uuid, idx);
  return idx;
}

AuthorDedup::AuthorDedup(size_t max_entries, function<uint64_t()> now_ms)
    : max_entries_(max_entries), now_ms_(std::move(now_ms)) {
}

AuthorDedup& AuthorDedup::Instance() {
  static AuthorDedup dedup;
  return dedup;
}

uint64_t AuthorDedup::NowMs() const {
  if (now_ms_)
    return now_ms_();
  return static_cast<uint64_t>(absl::ToUnixMillis(absl::Now()));
}

AuthorDedup::Entry& AuthorDedup::EntryLocked(string_view uuid) {
  if (auto it = entries_.find(uuid); it != entries_.end())
    return it->second;

  if (entries_.size() >= max_entries_) {
    auto oldest = entries_.end();
    for (auto it = entries_.begin(); it != entries_.end(); ++it) {
      if (it->second.inflight == 0 &&
          (oldest == entries_.end() || it->second.last_seen_ms < oldest->second.last_seen_ms)) {
        oldest = it;
      }
    }
    // Every entry reserved: the map exceeds the bound until one of them commits.
    if (oldest != entries_.end())
      entries_.erase(oldest);
  }
  return entries_[string(uuid)];
}

AuthorDedup::Verdict AuthorDedup::Reserve(string_view uuid, uint64_t mvcc,
                                          const function<bool()>& running) {
  if (mvcc == 0)
    return Verdict::kApply;

  std::unique_lock lk(mu_);
  for (;;) {
    // Looked up afresh on every pass: the map may have rehashed or lost this entry while we waited.
    Entry& entry = EntryLocked(uuid);
    entry.last_seen_ms = NowMs();
    if (entry.applied >= mvcc)
      return Verdict::kDrop;
    if (entry.inflight == 0) {
      entry.inflight = mvcc;
      return Verdict::kApply;
    }

    cv_.wait_for(lk, 100ms);
    // running() is the caller's: not under the leaf lock.
    lk.unlock();
    const bool alive = running();
    lk.lock();
    if (!alive)
      return Verdict::kCancelled;
  }
}

void AuthorDedup::Commit(string_view uuid, uint64_t mvcc) {
  if (mvcc == 0)
    return;
  util::fb2::LockGuard lk(mu_);
  Entry& entry = EntryLocked(uuid);
  entry.inflight = 0;
  entry.applied = max(entry.applied, mvcc);
  entry.last_seen_ms = NowMs();
  cv_.notify_all();
}

void AuthorDedup::Release(string_view uuid) {
  util::fb2::LockGuard lk(mu_);
  if (auto it = entries_.find(uuid); it != entries_.end())
    it->second.inflight = 0;
  cv_.notify_all();
}

void AuthorDedup::Advance(string_view uuid, uint64_t mvcc) {
  if (mvcc == 0)
    return;
  util::fb2::LockGuard lk(mu_);
  Entry& entry = EntryLocked(uuid);
  entry.applied = max(entry.applied, mvcc);
  entry.last_seen_ms = NowMs();
}

bool AuthorDedup::IsApplied(string_view uuid, uint64_t mvcc) {
  if (mvcc == 0)
    return false;
  util::fb2::LockGuard lk(mu_);
  auto it = entries_.find(uuid);
  return it != entries_.end() && it->second.applied >= mvcc;
}

void AuthorDedup::Clear() {
  util::fb2::LockGuard lk(mu_);
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (it->second.inflight == 0) {
      entries_.erase(it++);
    } else {
      it->second.applied = 0;
      ++it;
    }
  }
}

void AuthorDedup::ClearAllForTest() {
  util::fb2::LockGuard lk(mu_);
  entries_.clear();
  cv_.notify_all();
}

size_t AuthorDedup::size() {
  util::fb2::LockGuard lk(mu_);
  return entries_.size();
}

bool ClassicApplier::SkipKeyDbOnly(const facade::RespVec& args) {
  if (!IsKeyDbOnlyCommand(args))
    return false;

  Count(&ClassicLinkStats::keydb_cmds_dropped);
  LOG_EVERY_T(WARNING, 60) << "Dropping a KeyDB-only command from " << link_ << ": "
                           << absl::CHexEscape(args[0].GetView().substr(0, 32))
                           << " (KeyDB member TTLs and cron jobs have no equivalent here); the "
                              "drops are counted in keydb_cmds_dropped";
  return true;
}

void ClassicApplier::Count(atomic<uint64_t> ClassicLinkStats::*counter, uint64_t n) {
  (stats_->*counter).fetch_add(n, memory_order_relaxed);
  (ClassicTotals().*counter).fetch_add(n, memory_order_relaxed);
}

ClassicApplier::ClassicApplier(Service* service, ConnectionContext* cntx, string self_uuid,
                               string link, ClassicLinkStats* stats, function<bool()> running,
                               ClassicAuthorMap authors)
    : service_(service),
      cntx_(cntx),
      self_uuid_(std::move(self_uuid)),
      link_(std::move(link)),
      stats_(stats),
      running_(std::move(running)),
      authors_(std::move(authors)),
      link_origin_idx_(cntx->repl_origin_idx),
      reply_(make_unique<facade::CapturingReplyBuilder>(facade::ReplyMode::ONLY_ERR)) {
}

ClassicApplier::~ClassicApplier() = default;

bool ClassicApplier::IsRreplay(const facade::RespExpr& name) {
  return name.type == facade::RespExpr::STRING && absl::EqualsIgnoreCase(name.GetView(), "RREPLAY");
}

EnvelopeResult ClassicApplier::HandleRreplay(const facade::RespVec& args, unsigned depth) {
  DCHECK_GE(depth, 1u);

  // Nested envelopes are unwrapped in a loop, not by recursion: a fiber's stack is 40 KB in a
  // release build, and 64 levels of a frame holding a parser and an envelope could exceed it.
  // Each layer's views point at the bytes of the stream buffer, not into the vector they were
  // parsed into, so `inner_args` can be reused for the next layer.
  facade::RespVec inner_args;
  const facade::RespVec* current = &args;
  // False once the db of a layer could not be selected, until a deeper layer selects one: the
  // command then has no db to run in and is skipped.
  bool db_selected = true;
  uint64_t layers = 0;
  // The enclosing layers of the one being looked at that took their inner command apart, outermost
  // first: each advances its author's watermark when its inner was handled (spec D-5). The layer at
  // hand is `env`; the leaf commits its own reservation, and `env` advances only for a skipped one.
  vector<pair<string, uint64_t>> outers;
  bool advance = true;
  bool leaf_skipped = false;
  RreplayEnvelope env;

  for (;; ++depth) {
    // KeyDB's order (replicaReplayCommand): validate the layer, select its db, look at its author,
    // count its nesting. Its mvcc is validated after the db is selected, so a bad one leaves the db
    // selected.
    env = RreplayEnvelope{};
    const RreplayParse parse = ParseRreplayEnvelope(*current, &env);

    if (parse == RreplayParse::kOk || parse == RreplayParse::kBadMvcc) {
      // The point of no return: from the first dispatch or select of the outermost envelope on, the
      // whole tree is applied, whatever happens to the link. Nothing above has touched the context.
      if (depth == 1 && !running_())
        return EnvelopeResult::kNotConsumed;

      if (env.db)
        db_selected = SelectDb(*env.db);
    }

    if (parse != RreplayParse::kOk) {
      NoteMalformed(RreplayParseName(parse), depth);
      break;
    }

    if (env.uuid == self_uuid_) {
      Count(&ClassicLinkStats::rreplay_self_dropped);
      break;
    }

    if (depth > kMaxNesting) {
      NoteMalformed(absl::StrCat("nested deeper than ", kMaxNesting, " envelopes"), depth);
      break;
    }

    // A read only: two links can pass it together, the leaf's reservation is what closes that.
    if (dedup_.IsApplied(env.uuid, env.mvcc)) {
      rreplay_deduped.fetch_add(1, memory_order_relaxed);
      break;
    }

    if (!ParseSingleCommand(env.inner, &inner_args)) {
      NoteMalformed("the inner command is not exactly one command", depth);
      break;
    }
    ++layers;

    if (!IsRreplay(inner_args[0])) {
      if (!db_selected) {
        advance = false;
        break;
      }
      switch (ApplyCommand(inner_args, env.uuid, env.mvcc)) {
        case Leaf::kCancelled:
          // Nothing dispatched, nothing counted, nothing advanced: the link resumes at this
          // envelope.
          return EnvelopeResult::kNotConsumed;
        case Leaf::kRejected:
          advance = false;
          break;
        case Leaf::kSkipped:
          leaf_skipped = true;
          break;
        case Leaf::kApplied:
        case Leaf::kDeduped:
          break;
      }
      break;
    }
    outers.emplace_back(std::move(env.uuid), env.mvcc);
    current = &inner_args;
  }

  // Before the caller counts the envelope's bytes (Replica::ConsumeRedisStream), so that a resume
  // offset never covers an envelope whose watermark was not recorded.
  if (advance) {
    for (const auto& [uuid, mvcc] : outers)
      dedup_.Advance(uuid, mvcc);
    if (leaf_skipped)
      dedup_.Advance(env.uuid, env.mvcc);
  }

  Count(&ClassicLinkStats::rreplay_unwrapped, layers);
  return EnvelopeResult::kConsumed;
}

bool ClassicApplier::ParseSingleCommand(string_view bytes, facade::RespVec* args) {
  if (bytes.empty())
    return false;

  // No array or string in the bytes can be longer than the bytes.
  const uint32_t limit =
      static_cast<uint32_t>(min<size_t>(bytes.size(), numeric_limits<uint32_t>::max()));
  facade::RedisParser parser(facade::RedisParser::Mode::SERVER, limit, limit);
  uint32_t consumed = 0;
  facade::RedisParser::Result result = parser.Parse(
      facade::RedisParser::Buffer{reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()},
      &consumed, args);
  if (result != facade::RedisParser::OK || consumed != bytes.size() || args->empty())
    return false;
  // `*0`, `*-1` and `*1 $-1` are one RESP value each, and their first element is an array or a nil,
  // which has no name to read.
  return args->front().type == facade::RespExpr::STRING;
}

namespace {

// Sets `repl_mvcc`, `repl_origin_idx` and `repl_lww_guard` on the stream's apply context for one
// dispatch, and on every exit after it is built puts back the link's values (mvcc 0, the link's
// origin, the link's guard bit, which ConsumeRedisStream set once and the raw commands of the
// stream run with).
class CommandScope {
 public:
  CommandScope(ConnectionContext* cntx, uint32_t link_origin_idx)
      : cntx_(cntx), link_origin_idx_(link_origin_idx), link_guard_(cntx->repl_lww_guard) {
  }
  ~CommandScope() {
    cntx_->repl_mvcc = 0;
    cntx_->repl_origin_idx = link_origin_idx_;
    cntx_->repl_lww_guard = link_guard_;
  }

  CommandScope(const CommandScope&) = delete;
  CommandScope& operator=(const CommandScope&) = delete;

  // The command that is about to be dispatched is authored at `origin_idx` and carries `mvcc`.
  void Stamp(uint64_t mvcc, uint32_t origin_idx) {
    cntx_->repl_mvcc = mvcc;
    cntx_->repl_origin_idx = origin_idx;
  }

  // The command that is about to be dispatched runs unguarded whatever the link's bit is.
  void Unguard() {
    cntx_->repl_lww_guard = false;
  }

 private:
  ConnectionContext* cntx_;
  uint32_t link_origin_idx_;
  bool link_guard_;
};

// The leaf's reservation in AuthorDedup: whatever way the leaf is left, it is committed or
// released, so another link waiting on the author never waits for nothing. An mvcc of 0 reserves
// nothing.
class Reservation {
 public:
  Reservation(AuthorDedup* dedup, string_view uuid, uint64_t mvcc)
      : dedup_(dedup), uuid_(uuid), mvcc_(mvcc), held_(mvcc != 0) {
  }
  ~Reservation() {
    if (held_)
      dedup_->Release(uuid_);
  }

  Reservation(const Reservation&) = delete;
  Reservation& operator=(const Reservation&) = delete;

  void Commit() {
    if (held_)
      dedup_->Commit(uuid_, mvcc_);
    held_ = false;
  }

 private:
  AuthorDedup* dedup_;
  string_view uuid_;  // the innermost envelope's, which outlives the leaf
  uint64_t mvcc_;
  bool held_;
};

}  // namespace

ClassicApplier::Leaf ClassicApplier::ApplyCommand(const facade::RespVec& args, string_view author,
                                                  uint64_t mvcc) {
  const string_view name = args[0].GetView();
  if (IsControlCommand(name) || SkipKeyDbOnly(args))
    return Leaf::kSkipped;

  CommandScope scope(cntx_, link_origin_idx_);

  CommandContext cmd;
  cmd.Init(reply_.get(), cntx_);
  facade::FillBackedArgs(args, &cmd);
  // The dispatcher would reject a command it has no entry for as an unknown one, into the error
  // builder and upstream's `unknown_` accounting. Inside an envelope it is a command of a KeyDB
  // (KEYDB.MVCCRESTORE until P7-2 applies it, or one a newer KeyDB added), so it is counted on its
  // own. The lookup is the dispatcher's, which reads `ACL <sub>` as one name.
  if (service_->mutable_registry()->FindExtended(facade::ParsedArgs{cmd}).first == nullptr) {
    Count(&ClassicLinkStats::classic_unknown_cmds_dropped);
    LOG_EVERY_T(WARNING, 60) << "Dropping a command of an RREPLAY envelope from " << link_
                             << " that this server has no command for: "
                             << absl::CHexEscape(name.substr(0, 32))
                             << "; the drops are counted in classic_unknown_cmds_dropped";
    return Leaf::kSkipped;
  }

  // Only a command that is dispatched reserves its author's mvcc (spec D-5), and a duplicate or a
  // cancelled one costs nothing else: no registry slot, no overflow count.
  switch (dedup_.Reserve(author, mvcc, running_)) {
    case AuthorDedup::Verdict::kDrop:
      rreplay_deduped.fetch_add(1, memory_order_relaxed);
      return Leaf::kDeduped;
    case AuthorDedup::Verdict::kCancelled:
      return Leaf::kCancelled;
    case AuthorDedup::Verdict::kApply:
      break;
  }
  Reservation reservation(&dedup_, author, mvcc);

  // After the reservation, so a duplicate or a cancelled leaf runs no rewrite. On every envelope
  // link, plain or peer, whether or not the guard is on (spec D-4.3, D-6): the guard only decides
  // whether the dispatch can be vetoed. The reservation is by the envelope's (author, mvcc) and
  // the rewrites do not change it.
  ApplyLwwRewrites(&cmd);
  ClassicApplyRewrites(&cmd);

  // Only a command that is dispatched resolves its author (a skipped one costs no registry slot),
  // so the cap counts dispatched commands. `author` is never this node's own: HandleRreplay dropped
  // those.
  if (authors_.stamping())
    scope.Stamp(mvcc, authors_.IdxFor(author));
  // A script's inner commands share one stamp; a single-shard script runs them through a
  // SQUASHED_STUB, where the guard cannot compare per command (the RunSquashedMultiCb tripwire),
  // so an EVAL is stamped, never vetoed. KeyDB 6.3.4 never sends the _RO variants, but the prefix
  // covers them.
  if (absl::StartsWithIgnoreCase(name, "EVAL"))
    scope.Unguard();
  bool ran = true;
  if (optional<string> error = Dispatch(&cmd, &ran); error)
    NoteApplyError(name, *error);
  if (!ran)
    return Leaf::kRejected;  // released by the guard
  reservation.Commit();
  return Leaf::kApplied;
}

optional<string> ClassicApplier::Dispatch(CommandContext* cmd, bool* ran) {
  facade::DispatchResult result =
      service_->DispatchCommand(facade::ParsedArgs{*cmd}, cmd, facade::AsyncPreference::ONLY_SYNC);
  if (ran)
    *ran = result == facade::DispatchResult::OK;
  facade::CapturingReplyBuilder::Payload reply = reply_->Take();
  if (auto error = facade::CapturingReplyBuilder::TryExtractError(reply); error)
    return string(error->first);
  if (result != facade::DispatchResult::OK)
    return "the command was not dispatched";
  return nullopt;
}

bool ClassicApplier::SelectDb(DbIndex db) {
  if (ensured_dbs_.size() <= db)
    ensured_dbs_.resize(db + 1);

  if (ensured_dbs_[db]) {
    cntx_->conn_state.db_index = db;
    return true;
  }

  CommandContext select;
  select.Init(reply_.get(), cntx_);
  const string index = absl::StrCat(db);
  const array<string_view, 2> parts = {"SELECT", index};
  select.Assign(parts.begin(), parts.end(), parts.size());
  if (optional<string> error = Dispatch(&select); error) {
    NoteApplyError("SELECT", *error);
    return false;
  }
  ensured_dbs_[db] = true;
  return true;
}

void ClassicApplier::NoteMalformed(string_view why, unsigned depth) {
  Count(&ClassicLinkStats::rreplay_malformed);
  LOG_EVERY_T(WARNING, 60) << "Skipping a malformed RREPLAY envelope from " << link_
                           << " (nesting level " << depth << "): " << why;
}

void ClassicApplier::NoteApplyError(string_view command, string_view error) {
  Count(&ClassicLinkStats::classic_apply_errors);
  LOG_EVERY_T(WARNING, 10) << "A command of an RREPLAY envelope from " << link_
                           << " did not apply and is skipped: "
                           << absl::CHexEscape(command.substr(0, 32)) << ": " << error;
}

}  // namespace dfly
