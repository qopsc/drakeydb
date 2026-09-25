// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/rdb.h"
}

//  Custom types: Range 30-36 is used by DF RDB types.
constexpr uint8_t RDB_TYPE_JSON = 30;
constexpr uint8_t RDB_TYPE_HASH_WITH_EXPIRY = 31;
constexpr uint8_t RDB_TYPE_SET_WITH_EXPIRY = 32;
constexpr uint8_t RDB_TYPE_SBF = 33;
constexpr uint8_t RDB_TYPE_SBF2 = 34;
constexpr uint8_t RDB_TYPE_CMS = 35;
constexpr uint8_t RDB_TYPE_TOPK = 36;
constexpr uint8_t RDB_TYPE_CUCKOO = 37;

constexpr bool rdbIsObjectTypeDF(uint8_t type) {
  return __rdbIsObjectType(type) || (type == RDB_TYPE_JSON) ||
         (type == RDB_TYPE_HASH_WITH_EXPIRY) || (type == RDB_TYPE_SET_WITH_EXPIRY) ||
         (type == RDB_TYPE_SBF) || (type == RDB_TYPE_SBF2) || (type == RDB_TYPE_CMS) ||
         (type == RDB_TYPE_TOPK) || (type == RDB_TYPE_CUCKOO);
}

//  Opcodes: Range 200-240 is used by DF extensions.

// This opcode is sent by the master Dragonfly instance to a replica
// to notify that it finished streaming static data and is ready
// to switch to the stable state replication phase.
constexpr uint8_t RDB_OPCODE_FULLSYNC_END = 200;

constexpr uint8_t RDB_OPCODE_COMPRESSED_ZSTD_BLOB_START = 201;
constexpr uint8_t RDB_OPCODE_COMPRESSED_LZ4_BLOB_START = 202;
constexpr uint8_t RDB_OPCODE_COMPRESSED_BLOB_END = 203;

constexpr uint8_t RDB_OPCODE_JOURNAL_BLOB = 210;

// A full sync will continue to send information in journal blobs until the replica
// sends a `DFLY STARTSTABLE` to the master.
// We use this opcode to synchronize the journal offsets at the end of the full sync,
// so it is always sent at the end of the RDB stream.
constexpr uint8_t RDB_OPCODE_JOURNAL_OFFSET = 211;

constexpr uint8_t RDB_OPCODE_DF_MASK = 220; /* Mask for key properties */

// RDB_OPCODE_DF_MASK define 4byte field with next flags
constexpr uint32_t DF_MASK_FLAG_STICKY = (1 << 0);
constexpr uint32_t DF_MASK_FLAG_MC_FLAGS = (1 << 1);

// drakeydb: P4-2 Task 1 -- {mvcc, origin_hash}, 16 raw LE bytes. Emitted per stamped key
// (active-replica save side only), immediately before the key's type byte, exactly like
// RDB_OPCODE_DF_MASK above. The read side is unconditional (any node can load it); the write side
// is active-only (spec D-7). A zero stamp is never emitted -- it means unstamped/absent.
constexpr uint8_t RDB_OPCODE_DF_MVCC = 221; /* {mvcc, origin_hash}, 16 raw LE bytes */

// Opcode to store HNSW vector index node data for global indices.
// Format: [index_name, enterpoint_node, elements_number,
//          then for each node in ascending internal_id 0..elements_number-1:
//            internal_id, global_id, level, zero_level_links_num, zero_level_links,
//            higher_level_links_num (only if level > 0), higher_level_links (only if level > 0)]
constexpr uint8_t RDB_OPCODE_VECTOR_INDEX = 222;

// Opcode to store ShardDocIndex key-to-DocId mapping for search indices
// Format: [shard_id, index_name, mapping_count, then for each mapping: key_string, doc_id]
constexpr uint8_t RDB_OPCODE_SHARD_DOC_INDEX = 223;

// Used to tag a chunk of serialized data with its stream id
constexpr uint8_t RDB_OPCODE_TAGGED_CHUNK = 224;

// drakeydb: P4-3 Task 5 -- persists tombstones (bit-63-flagged mvcc slots, Task 2/11) through the
// RDB so a peer's delete survives our own restart and can still be applied on the next merge
// (Task 6). Payload: [db_index][count][count x {key, packed, origin_hash}] -- db_index and count
// are RDB-length-encoded (SaveLen/LoadLen); key is an RDB string (SaveString/FetchGenericString);
// packed and origin_hash are 16 raw LE bytes, exactly like RDB_OPCODE_DF_MVCC's per-key stamp
// above -- packed retains bit 63 on the wire.
//
// Emitted from the per-shard PROLOGUE (SliceSnapshot::Start, snapshot.cc, beside
// SearchSerializer::Serialize), not an epilogue: RdbSaver has no per-shard epilogue hook
// (SaveEpilog, rdb_save.cc, is global -- it only sends EOF+checksum). The section's position
// relative to the key stream and any concurrent journal blob carries no meaning: tombstone
// application is itself LWW-guarded (MergeAccepts, mvcc.h), so whichever of the three lands last
// for a given key resolves the same way regardless of ordering.
//
// Write side is active-only and skipped for an empty table (D-7); read side (like
// RDB_OPCODE_DF_MVCC) is UNCONDITIONAL -- every loader must consume this section's bytes in
// full, whether or not it ends up installing anything (RdbLoader::HandleTombstones).
//
// drakeydb: P4-5 -- superseded as a WRITE-side format by RDB_OPCODE_DF_TOMBSTONES_V2 below (an
// active node no longer emits this opcode at all); the constant and the read side stay, so a file
// or a peer stream written before this record existed still loads (meta = kTombMetaNone).
constexpr uint8_t RDB_OPCODE_DF_TOMBSTONES = 225;

// drakeydb: P4-5 -- carries a tombstone's meta word (mvcc.h's kTombMetaNone / kTombMetaAnchorOnly
// / a whole-key deadline D) alongside the stamp, so the GC deadline a deleted key's expiry earned
// survives a full sync or restart instead of reverting to stamp.MsPart()+ttl (D-16, which recurs
// on every restart/sync without this). Payload: [db_index][count][count x {key, packed u64 LE,
// origin_hash u64 LE, meta u64 LE}] -- identical framing to RDB_OPCODE_DF_TOMBSTONES above, with
// one more 8-byte LE word appended per entry.
//
// Write side: active nodes emit V2 ONLY, using the same IsActiveReplica() gate and per-chunk
// framing RDB_OPCODE_DF_TOMBSTONES used. RDB_OPCODE_DF_TOMBSTONES is never written again -- an
// old loader that only understands it is refused at the replication-version handshake instead
// (kDrakeydbReplVersion, node_identity.h), the same D-7 compatibility rule opcode 221 and 225
// established: don't extend an already-shipped record in place, or an old loader parsing the
// extra bytes as something else desyncs silently instead of failing loudly.
//
// Read side: UNCONDITIONAL on every node, exactly like 225's own read-unconditional contract --
// both opcodes are parsed and, on a non-active node, discarded.
//
// UPSTREAM-SYNC hazard: chosen well clear of 226, the number upstream's own next sequential DF
// extension would most likely take next (its own most recent is 224,
// RDB_OPCODE_TAGGED_CHUNK) -- an upstream opcode later claiming this exact number would silently
// misparse against this fork's tombstone-V2 reader, or vice versa. Re-verify this constant
// against upstream's rdb.h/rdb_load.cc before every merge (docs/UPSTREAM-SYNC.md's watchlist).
constexpr uint8_t RDB_OPCODE_DF_TOMBSTONES_V2 = 230;
