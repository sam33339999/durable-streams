# Durable Streams — Operations & Architecture Guide

> 📖 **繁體中文版**：[operations.zh-TW.md](./operations.zh-TW.md)

This guide covers the internal architecture, data structures, storage mechanisms, capacity handling, and operational considerations for self-hosting Durable Streams.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Internal Data Structures & Algorithms](#2-internal-data-structures--algorithms)
3. [How Data Is Written and Stored](#3-how-data-is-written-and-stored)
4. [What Happens When Storage Gets Full](#4-what-happens-when-storage-gets-full)
5. [Self-Hosting Checklist](#5-self-hosting-checklist)
6. [In-Memory vs File Store: Persistence Behaviour](#6-in-memory-vs-file-store-persistence-behaviour)
7. [Operational Concerns](#7-operational-concerns)

---

## 1. Architecture Overview

```
Client (TS / Python / Go / Elixir / .NET / Swift / PHP / Java / Rust / Ruby)
         │
         ▼  HTTP (GET / POST / PUT / DELETE / HEAD)
┌────────────────────────────────────────┐
│           Caddy HTTP Handler           │
│         (packages/caddy-plugin)        │
├────────────────────────────────────────┤
│              Store Layer               │
│  ┌─────────────────┬─────────────────┐ │
│  │  MemoryStore    │   FileStore     │ │
│  │  (dev/testing)  │  (production)   │ │
│  └─────────────────┴─────────────────┘ │
└────────────────────────────────────────┘
         │                    │
    (volatile RAM)   bbolt metadata DB
                       + .seg segment files
```

There is no special consensus layer or distributed coordination. The server is **single-node** with optional file-backed persistence for durability.

---

## 2. Internal Data Structures & Algorithms

### 2.1 Offset — Lexicographically Sortable Position

Every position in a stream is encoded as an `Offset`:

```
Format: "RRRRRRRRRRRRRRRR_BBBBBBBBBBBBBBBB"
         │── 16 digits ──│ │── 16 digits ──│
              ReadSeq          ByteOffset
```

| Field        | Description                                             |
|--------------|---------------------------------------------------------|
| `ReadSeq`    | Reserved for future log-rotation (always `0` currently) |
| `ByteOffset` | Cumulative bytes of **payload data** written (excludes framing headers) |

The zero-padded decimal representation makes offsets **lexicographically comparable** using simple string comparison — no integer parsing required. This is important for HTTP range semantics and for clients storing the last-seen offset as a plain string.

Special values:

| Offset string | Meaning                                   |
|---------------|-------------------------------------------|
| `"0000000000000000_0000000000000000"` | Start of stream (replay all) |
| `"-1"` | Alias for start of stream |
| `"now"` | Skip all existing data; subscribe to future writes only |

### 2.2 Segment File — Append-Only Length-Prefixed Log

Stream data is persisted in binary **segment files** (`data.seg`):

```
┌────────────────────────────────────────────────────────────┐
│  [4-byte BE length] [payload bytes]                        │
│  [4-byte BE length] [payload bytes]                        │
│  ...                                                       │
└────────────────────────────────────────────────────────────┘
```

- Each message is prefixed with a 4-byte big-endian unsigned integer indicating the payload length.
- Messages are concatenated directly; there are no separators or checksums.
- The maximum message size is **64 MB** (`MaxMessageSize = 64 * 1024 * 1024`).
- The `ByteOffset` in an `Offset` is the byte position of the **start of the next message's payload** (i.e., cumulative payload bytes, not file bytes).
- Random access is achieved with `SeekToOffset(byteOffset)`: since the offset tracks data bytes (not file bytes), the reader must scan from the start to find the correct file position. For large streams, avoid seeking to arbitrary old offsets in hot paths.

**JSON mode**: when the `Content-Type` is `application/json`, each stored message is a single JSON value. Arrays appended to the stream are **flattened** (each element becomes its own message). Reads reconstruct the array by wrapping all returned messages in `[…]`.

### 2.3 Metadata Store — bbolt (Embedded Key-Value DB)

The file store uses **bbolt** (a pure-Go embedded B-tree database) to store stream metadata:

```
bbolt file: {dataDir}/metadata/metadata.db
```

Each stream record stores:

| Field           | Type              | Description                            |
|-----------------|-------------------|----------------------------------------|
| `Path`          | string            | Stream URL path (primary key)          |
| `ContentType`   | string            | Locked after creation                  |
| `CurrentOffset` | Offset            | Latest tail offset                     |
| `ExpiresAt`     | *time.Time        | Absolute expiry (if set)               |
| `TTLSeconds`    | *int64            | Relative TTL in seconds (if set)       |
| `Producers`     | map[id]→ProducerState | Idempotent producer tracking       |
| `Closed`        | bool              | Whether the stream is sealed           |
| `ClosedBy`      | *ClosedByProducer | Producer that sealed the stream        |

bbolt transactions are **serialised writes with concurrent reads**, providing ACID semantics per-key. The metadata store is loaded entirely into an in-memory cache (`metaCache`) at startup for low-latency reads.

### 2.4 File Handle Pool — LRU Eviction

Opening a file descriptor for every write or read is expensive. Durable Streams maintains two LRU pools:

| Pool            | Direction | Backing structure            |
|-----------------|-----------|------------------------------|
| `FilePool`      | Writers   | `map[path]→entry` + `list.List` (LRU) |
| `ReaderPool`    | Readers   | Same pattern                 |

When the pool reaches its configured limit (`max_file_handles`, default **100**), the **least recently used** entry is closed and evicted. The next access for that file simply reopens it.

The TypeScript reference server uses a **SIEVE cache** instead of a pure LRU — a more recently published eviction algorithm that has higher hit rates on mixed workloads by giving new entries a "trial period" before they become evictable.

### 2.5 In-Memory Store — Map + Channel Notification

```
MemoryStore
├── streams: map[path] → memoryStream
│   ├── metadata: StreamMetadata
│   ├── messages: []Message        ← JSON mode
│   └── data: []byte               ← binary mode
│
├── longPoll: longPollManager
│   └── waiters: map[path] → []chan struct{}
│
└── producerLocks: map["path:producerId"] → *sync.Mutex
```

Long-poll clients block on an `<-channel`. When a new message is written, the writer calls `notify(path)` which does a non-blocking send to every waiter channel, unblocking them immediately.

### 2.6 Idempotent Producer State Machine

Every producer (identified by `Producer-Id`, `Producer-Epoch`, `Producer-Seq` request headers) is tracked per stream:

```
State: { Epoch: int64, LastSeq: int64, LastUpdated: unix-timestamp }
```

Rules enforced on every append:

| Condition                              | Result                          |
|----------------------------------------|---------------------------------|
| `epoch < state.Epoch`                  | `ErrStaleEpoch` (zombie fencing)|
| `epoch > state.Epoch`                  | New epoch accepted; resets seq to 0 |
| `seq == state.LastSeq`                 | Duplicate → `204 No Content`    |
| `seq == state.LastSeq + 1`             | New message → accepted          |
| `seq > state.LastSeq + 1`             | `ErrProducerSeqGap`             |

This provides **exactly-once delivery within a producer epoch** without requiring the server to coordinate with other instances.

---

## 3. How Data Is Written and Stored

### 3.1 Memory Store Write Path

```
POST /v1/stream/my-topic  →  MemoryStore.Append()
   │
   ├─ Acquire per-producer mutex (if producer headers present)
   ├─ Validate producer state (epoch/seq checks)
   ├─ Append to messages slice (JSON) or data byte slice (binary)
   ├─ Update metadata.CurrentOffset
   ├─ Release mutex
   └─ Notify long-poll waiters (non-blocking channel send)
```

All of this happens in memory. There is **no I/O**. The data is gone the moment the process exits.

### 3.2 File Store Write Path

```
POST /v1/stream/my-topic  →  FileStore.Append()
   │
   ├─ Acquire per-producer mutex
   ├─ Lock metaCacheMu (write lock)
   ├─ Validate producer state from metaCache
   │
   ├─ Resolve segment file path from dirCache
   ├─ Get or open writer from FilePool (LRU-managed)
   ├─ Write length-prefixed message to segment file
   ├─ call file.Sync() / fdatasync()   ← durability guarantee
   │
   ├─ Atomically update metadata in bbolt transaction:
   │    ├─ UpdateOffset(newOffset)
   │    └─ UpdateAppendState(producerState)
   │
   ├─ Update in-memory metaCache
   ├─ Release metaCacheMu
   ├─ Release producer mutex
   └─ Notify long-poll waiters
```

The `Sync()` call after every write ensures the data is on durable storage **before** the HTTP 200 response is returned to the client. This makes the append operation **crash-safe by default** at the cost of write latency (one fsync per message).

### 3.3 Stream Directory Layout on Disk

```
{dataDir}/
├── metadata/
│   └── metadata.db          ← bbolt database (all stream metadata)
└── streams/
    ├── %2Ftopic-a~1700000001~a3f9b2/
    │   └── data.seg
    └── %2Ftopic-b~1700000002~c8e1d4/
        └── data.seg
```

Directory names use the format `{url_encoded_path}~{unix_timestamp}~{random_hex}`. The random suffix allows **safe async deletion**: when a stream is deleted and immediately recreated at the same path, the new stream gets a fresh directory while the old one is still being garbage-collected.

### 3.4 Startup Recovery

When the file store initialises, it:

1. Opens `metadata.db` and loads all stream records into `metaCache` and `dirCache`.
2. For each stream, the segment file is the **source of truth** for actual data. If bbolt and the segment file disagree (e.g., after a crash mid-transaction), the segment file wins.
3. `ScanSegment()` can verify a segment file by reading it sequentially and stopping at the last complete (fully-written) message — partial messages caused by crashes are silently discarded.

---

## 4. What Happens When Storage Gets Full

### 4.1 Per-Message Size Limit

The server enforces a hard **64 MB** limit per message (`MaxMessageSize`). Larger payloads are rejected with `ErrMessageTooLarge` before any data is written.

### 4.2 Disk Full (File Store)

When the underlying disk has no space available:

- The `WriteMessage()` call to the segment file will return an OS-level I/O error.
- This error propagates up and is returned to the client as an HTTP 500 response.
- The stream state is **not corrupted**: the partial write is simply not committed.
- The bbolt metadata update is never attempted, so metadata stays consistent.
- **No automatic eviction of old streams occurs.** The operator must intervene by freeing disk space or deleting old streams (via `DELETE /v1/stream/{path}`).

### 4.3 Memory Full (Memory Store)

The in-memory store has **no built-in capacity limit** on the number of streams or total bytes stored. Growth is bounded only by the amount of free RAM. When the Go runtime runs out of memory the process will be killed by the OS.

Mitigations:
- Set a `Stream-TTL` on every stream so they expire automatically.
- Use the file store for workloads where total data size is unpredictable.
- Monitor RSS/heap size and alert before exhaustion.

### 4.4 File Handle Exhaustion

When more unique stream paths are actively written/read than `max_file_handles` allows, the LRU pool evicts the least recently used handle. This is transparent: the next access simply reopens the file. There is no error.

However, if a single burst touches thousands of unique streams simultaneously, the eviction cost (close + reopen syscalls) can become a bottleneck. Increase `max_file_handles` to match your concurrency profile.

### 4.5 TTL-Based Expiry

Streams support two expiry mechanisms:

| Header                     | Example                          | Behaviour |
|----------------------------|----------------------------------|-----------|
| `Stream-TTL: <seconds>`    | `Stream-TTL: 86400`              | Expires 24 h after creation |
| `Stream-Expires-At: <RFC3339>` | `Stream-Expires-At: 2026-01-01T00:00:00Z` | Expires at an absolute time |

Expired streams return `404 Not Found`. Deletion from disk is lazy (on next access) or performed by a background cleanup goroutine configured with `cleanup_interval`.

---

## 5. Self-Hosting Checklist

### 5.1 Storage Mode Decision

| Scenario                          | Recommended Storage |
|-----------------------------------|---------------------|
| Local development / CI testing    | `memory` (default)  |
| Staging / short-lived data        | `memory` with TTL   |
| Production / durability required  | `file`              |

### 5.2 Minimal Production Caddyfile

```caddyfile
{
    admin off
}

:4437 {
    route /v1/stream/* {
        durable_streams {
            data_dir /var/lib/durable-streams

            # Tune these for your workload:
            max_file_handles 200
            long_poll_timeout 30s
            sse_reconnect_interval 60s
        }
    }
}
```

### 5.3 Configuration Reference

| Parameter              | Default | Description |
|------------------------|---------|-------------|
| `data_dir`             | *(none — uses memory store)* | Path for file-backed storage. **Set this for production.** |
| `max_file_handles`     | `100`   | Max open file descriptors in the LRU pool. Increase if you have many concurrent streams. |
| `long_poll_timeout`    | `30s`   | How long a long-poll request waits for new data before returning an empty 200. |
| `sse_reconnect_interval` | `60s` | How often SSE connections are cycled to clear stale connections. |

### 5.4 Deployment Checklist

- [ ] **Persistent storage**: Set `data_dir` to a path on a durable volume (not a tmpfs or ephemeral container layer).
- [ ] **Disk space**: Estimate total stream volume and ensure the disk has adequate headroom. Streams do not auto-compact unless deleted or expired.
- [ ] **File descriptor limits**: Check the OS limit (`ulimit -n`). Each open file handle in the pool counts against this. Recommended: at least `max_file_handles × 2 + 1024` system limit.
- [ ] **Backups**: `data_dir` contains both `metadata/metadata.db` (bbolt) and `streams/*/data.seg`. Back up the entire `data_dir` directory. Because bbolt uses MVCC copy-on-write, the database file is safe to copy while the server is running; however, for a fully consistent snapshot that guarantees the bbolt metadata and segment files are in sync, take a filesystem-level snapshot or stop the server briefly before copying (see Section 7.3).
- [ ] **Reverse proxy / TLS**: Run Caddy behind TLS (Caddy handles Let's Encrypt automatically) or put a TLS-terminating load balancer in front.
- [ ] **CORS**: The server emits open CORS headers (`Access-Control-Allow-Origin: *`) by default. Restrict this at the reverse proxy layer if your use case requires it.
- [ ] **Authentication**: The protocol has no built-in auth. Protect the endpoint at the network or reverse-proxy layer (e.g., Caddy's `basicauth`, mTLS, or an API gateway).
- [ ] **Stream TTLs**: Always set `Stream-TTL` or `Stream-Expires-At` on streams you do not want to accumulate indefinitely.
- [ ] **Cleanup interval**: Enable background cleanup with `cleanup_interval 1h` (or a suitable interval) so expired streams are removed from disk automatically.
- [ ] **Monitoring**: Watch disk usage, process RSS, file descriptor count, and HTTP error rates.
- [ ] **Graceful shutdown**: Send `SIGTERM` to the Caddy process. The handler's `Cleanup()` method closes all file handles and flushes the bbolt database.

### 5.5 Recommended Directory Structure

```
/var/lib/durable-streams/   ← data_dir
    metadata/
        metadata.db
    streams/
        ...
/etc/caddy/
    Caddyfile
/var/log/caddy/             ← Caddy structured JSON logs
```

---

## 6. In-Memory vs File Store: Persistence Behaviour

### Does data survive a server restart?

| Store        | Restart behaviour |
|--------------|-------------------|
| **Memory store** | **All data is lost immediately.** The in-memory map is discarded. There is no automatic "flush to disk" or write-ahead log. |
| **File store** | Data persists across restarts. On startup the server re-reads `metadata.db` and segment files and resumes from the last successfully synced offset. |

### Is the default in-memory?

**Yes.** If `data_dir` is not set in the Caddyfile (or if you run `durable-streams-server dev`), the server uses the in-memory store. This is intentional: it makes the zero-config experience fast and dependency-free.

**To opt in to persistence**, add `data_dir /your/path` to the `durable_streams` block.

### Why no automatic journaling in memory mode?

The memory store is deliberately kept simple and allocation-efficient. For workloads that need durability, the file store is the right tool. Mixing the two (e.g., buffering in memory and flushing periodically) would introduce partial-durability semantics that complicate recovery reasoning.

---

## 7. Operational Concerns

### 7.1 Concurrency Model

| Mechanism                    | Scope                          | Purpose |
|------------------------------|--------------------------------|---------|
| `producerLocks` (per-producer mutex) | `"{path}:{producerId}"` key | Serialises epoch/seq validation + append so out-of-order HTTP requests don't create false gaps |
| `metaCacheMu` (RWMutex)      | Entire metadata cache           | Allows concurrent metadata reads; exclusive lock only during writes |
| `longPollManager.mu` (Mutex) | Waiter channel map              | Protects channel registration/deregistration |
| bbolt transactions           | Metadata database               | Single-writer; concurrent readers via MVCC |

### 7.2 Crash Recovery Details

| Failure scenario | Recovery outcome |
|------------------|-----------------|
| Crash **during** segment file write | OS will have written partial data. On restart `ScanSegment()` stops at the last complete message. No bbolt update → offset is not advanced. Client will retry the write; the server accepts it as a new message (or as a producer duplicate if `Producer-Seq` was used). |
| Crash **after** segment file write but **before** `Sync()` | The write may not be on disk. No bbolt update → same recovery as above. |
| Crash **after** `Sync()` but **before** bbolt commit | Segment file has the data; bbolt does not reflect the new offset. On the next write, `ScanSegment()` will find the extra data and the offset will be corrected. |
| bbolt file corruption | bbolt uses copy-on-write B-trees and a write-ahead log. It can recover from most partial writes. The segment files remain valid. |
| Segment file partially truncated | `ScanSegment()` skips incomplete trailing message. Data before the truncation point is fully recoverable. |

### 7.3 Backup Strategy

1. **Hot backup (no downtime)**: Copy the entire `data_dir` while the server is running. bbolt uses MVCC, so a mid-write copy will capture a consistent snapshot up to the point of the copy. Segment files are append-only so they are safe to copy at any time.
2. **Cold backup (fully consistent)**: Stop the server (`SIGTERM`), copy `data_dir`, restart. This is the only way to guarantee that the bbolt metadata and segment files are in perfect sync.
3. **Restore**: Replace `data_dir` with the backup copy and restart.

### 7.4 No Built-In Log Rotation or Compaction

Segment files grow indefinitely. There is currently **no built-in log rotation or compaction**. To manage disk usage:

- Set `Stream-TTL` so streams delete themselves.
- Call `DELETE /v1/stream/{path}` explicitly to remove a stream and free its disk space.
- Use the background `cleanup_interval` to auto-remove expired streams.

### 7.5 Scaling Limits

The server is **single-node only**. There is no replication, sharding, or distributed consensus. Practical limits:

| Resource             | Typical bottleneck |
|----------------------|--------------------|
| Write throughput     | One `fsync()` per message; IOPS-bound on spinning disks. Use SSDs for high-frequency writes. |
| Concurrent streams   | `max_file_handles`. Increase and ensure OS `ulimit` is set accordingly. |
| Total stored data    | Available disk space. |
| Long-poll fan-out    | Each waiting client holds a goroutine and a channel. Very large fan-out (10k+ clients on one stream) may require a pub-sub intermediary. |
| bbolt write contention | bbolt allows only one writer at a time. High-concurrency mixed read/write workloads may see queueing at the metadata layer. |

### 7.6 Monitoring Checklist

| Signal to watch               | Why |
|-------------------------------|-----|
| Disk usage in `data_dir`      | Streams never shrink; disk exhaustion blocks **new writes** (HTTP 500) — existing data is not lost, but the service degrades |
| Process RSS                   | Memory store: unbounded growth |
| Open file descriptor count    | Must stay below OS `ulimit -n` |
| HTTP 500 / 507 error rates    | Indicate storage errors |
| Long-poll timeout rate        | High timeouts may mean producers have stopped |
| `metadata.db` file size       | Large bbolt files indicate many streams or large producer-state maps |

### 7.7 Common Operational Questions

**Q: Can I run multiple server instances against the same `data_dir`?**
No. bbolt acquires an exclusive file lock at startup. A second instance will fail to start. Horizontal scaling is not supported.

**Q: How do I delete a single stream?**
```
DELETE /v1/stream/{path}
```
This removes both the segment file and the bbolt record. Disk space is reclaimed immediately.

**Q: What happens if a client re-uses a stream path after deletion?**
A `POST` to a deleted path creates a new stream. The new stream gets a fresh directory (with a different random suffix), so there is no risk of reading old data from the previous stream at the same path.

**Q: How do I inspect the contents of a stream without a client library?**
```bash
curl http://localhost:4437/v1/stream/my-topic
```
This returns all stored messages. For live streaming, add `?live=sse` or `?live=long-poll`.

**Q: Can I change the `Content-Type` of an existing stream?**
No. The content type is locked at creation time and validated on every append. Creating a stream at the same path with a different content type returns an error.

**Q: The producer documentation mentions "epoch" — what is that for?**
When a producer process restarts, it increments its epoch. The server uses this as a fencing token: any in-flight requests from the old process instance (with the lower epoch) are rejected. This prevents zombie producers from writing stale data after a restart.

**Q: Will messages written during a network partition be replayed?**
Clients use their last-seen offset as a resume point. If the client was disconnected and the server kept receiving new messages, the client will receive all missed messages in order when it reconnects and resumes from its saved offset.
