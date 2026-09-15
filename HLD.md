# High-Level Design: Distributed, High-Precision Event-Driven Scheduler

## 1. Requirements Recap

**Functional:**
- Ingest tasks with arbitrary future execution timestamps (+50ms to +1 year), O(log n) or better.
- Non-blocking micro-tick evaluation of ready tasks, O(1) peek.
- Zero-loss crash resilience — rebuild in-memory state from disk sequentially on boot.
- Deterministic Leader/Follower cluster simulation — losing leadership halts ticking instantly; ingestion stays active.
- Thousands of concurrent ingestion requests without races, deadlocks, or starvation.

**Non-functional:**
- Zero external infrastructure (no Docker, no Kafka/Redis/RDBMS actually running) — everything local to `./storage/`.
- Execution jitter (actual − scheduled trigger time) < 15ms.
- Scales to 1,000,000+ resident scheduled events without heap exhaustion or continuous STW GC.
- Append-only, sequential-I/O disk writes only — no random file mutation.

These NFRs directly eliminate two common real-world approaches, and the RFC calls both out explicitly:
- **Relational databases** — row-locking contention under high concurrent writes.
- **Visibility-timeout-based distributed queues** (e.g. SQS-style) — cause duplicate processing during network partitions.

## 2. High-Level Architecture

Named after the real production components each piece plays the role of, even though every one is stubbed locally with zero external infra.

```
                         ┌───────────────┐
   Client / Producer ───▶│ Load Balancer  │  (e.g. Envoy/NGINX — routes to any
                         │ / API Gateway  │   node; ingestion is not leader-restricted)
                         └───────┬───────┘
                                 ▼
                  ┌──────────────────────────────┐
                  │   Scheduler API nodes          │  (stateless, N replicas)
                  │   - validate shape/range only  │
                  │   - no business logic here     │
                  └──────────────┬────────────────┘
                                 ▼  producer.send() — awaits durable broker ack only
                  ┌──────────────────────────────┐
                  │        Kafka (ingestion topic) │  durable, ordered, replayable log
                  │        partitioned by taskId    │  hash → parallel consumers
                  └──────────────┬────────────────┘
                                 ▼  consumer group, at-least-once delivery
                  ┌──────────────────────────────┐
                  │     Indexer Service (consumers) │
                  │  - dedupe via idempotencyKey     │
                  │    (conditional write, see §4)   │
                  │  - dual write:                   │
                  └──────┬────────────────┬─────────┘
                         ▼                ▼
        ┌───────────────────────┐  ┌──────────────────────────┐
        │   Redis (ZSET)          │  │  Cassandra / DynamoDB      │
        │  score = executeAt(ms)  │  │  source of truth for task  │
        │  member = taskId        │  │  metadata + status + audit │
        │  → the "hot" delay index│  │  (QUEUED/DISPATCHED/etc.)  │
        └──────────┬─────────────┘  └──────────────┬────────────┘
                   ▼ ZRANGEBYSCORE(0, now) polled                │
        ┌───────────────────────┐                                │
        │  Ticking Service        │  leader-elected via           │
        │  (only Leader polls)    │◀── ZooKeeper / etcd ──────────┘
        └──────────┬─────────────┘  (consensus for leader election)
                   ▼ dispatch (mark DISPATCHED first — fencing, see §4)
        ┌───────────────────────┐
        │   Worker Pool           │  virtual threads (I/O-bound execution:
        │  (executes task)        │  webhook calls, etc.)
        └──────┬────────┬────────┘
        success│        │failure
               ▼        ▼
      mark SUCCEEDED   Retry Policy (backoff+jitter)
      (Cassandra)      → re-ZADD into Redis at backoff time
                       → exhausted retries → Kafka DLQ topic

        Monitoring: Prometheus (metrics) + Grafana (dashboards) —
        jitter, queue lag, index size, retry/DLQ counts
```

## 3. Component Choices & Trade-offs

### 3.1 Ingestion durability layer — Kafka vs RabbitMQ vs SQS

| | Kafka | RabbitMQ | SQS |
|---|---|---|---|
| Storage model | Append-only log (partitioned) | Per-message store, ack/delete semantics | Managed, opaque |
| Throughput at our scale | Very high, horizontally partitionable | Good, lower ceiling than Kafka | Effectively unlimited but uncontrolled |
| Replay for crash recovery | Native — replay from any offset | Not designed for replay once acked | No replay (Standard), FIFO has limits |
| Native "delay until arbitrary future time" | No — FIFO by production order, not per-message future timestamp | Achievable via TTL+DLX plugin, awkward past short delays | Native delay queue, capped at 15 minutes |
| Matches our append-only/sequential-write NFR | Exact match — Kafka's storage engine *is* this pattern | Partial | N/A (opaque) |
| RFC's named anti-pattern (visibility timeouts → dupes on partition) | N/A | N/A | This is literally SQS's mechanism |

**Choice: Kafka**, for exactly one job — durably buffering *ingestion* so concurrent writers don't hammer the index/DB directly, and so we can replay on crash. **Kafka is not the scheduling mechanism itself** — none of these three natively support "hold this message untouched for up to a year." That's why a custom engine exists at all.

### 3.2 Priority/delay index — Redis (ZSET) vs Memcached vs Hazelcast/Ignite vs Aerospike

| | Redis ZSET | Memcached | Hazelcast/Ignite | Aerospike |
|---|---|---|---|---|
| Sorted-by-score structure | Native (`ZADD`, `ZRANGEBYSCORE`) | None — pure KV | Yes, but with cluster coordination overhead | Weak — optimized for KV, not range queries |
| Insert complexity | O(log n) | N/A | O(log n) + coordination cost | N/A |
| "everything ready to fire" query | `ZRANGEBYSCORE(0, now)` = our `peekReady()` | Not supported | Supported but heavier | Not a natural fit |
| Operational weight | Light, single-threaded | Light | Heavy — full data-grid semantics unneeded here | Heavy — built for massive durable KV |
| Precedent | Sidekiq / Celery's Redis broker use this exact pattern for delayed jobs | — | Multi-primary distributed compute, not our case | Massive durable primary stores, not delay queues |

**Choice: Redis ZSET.** Score = `executeAt` epoch millis, member = `taskId`. Our from-scratch `TaskIndex` (time-partitioned min-heap) is a direct reimplementation of what a single Redis ZSET shard gives for free — the point of this project.

### 3.3 Durable metadata / source of truth — Cassandra/DynamoDB vs Postgres

| | Cassandra / DynamoDB | Postgres |
|---|---|---|
| Writes under heavy concurrent status transitions | No row-lock contention, partition by `taskId` | Row-level locks contend under thousands of concurrent updates — the RFC's named bottleneck |
| Horizontal scale to 1M+ rows | Native | Requires manual sharding at that scale |

**Choice: Cassandra/DynamoDB-style model.** Our stub (append-only WAL + periodic compaction) is a small, honest replica of how these systems implement their storage engines internally (LSM trees) — not just "a fake DB."

### 3.4 Leader election — ZooKeeper/etcd vs our stub

Real systems use ZooKeeper (Zab) or etcd (Raft) for actual distributed consensus with ephemeral session-based liveness. The RFC explicitly asks for **deterministic cluster simulation**, not real consensus — so we intentionally do not implement Raft/Zab (a project of its own). We simulate deterministic role transitions that exercise the same code path (`TickGate` halting instantly on losing leadership) without solving distributed consensus itself. This is a deliberate scope cut.

### 3.5 Worker pool — Java 21 virtual threads vs reactive (Netty/WebFlux) vs fixed platform pool

For thousands of concurrent I/O-bound executions, virtual threads give near-reactive scalability with plain blocking code, no callback-chain complexity. Caveat: virtual threads can be pinned by `synchronized` blocks or blocking native calls — worth watching in the dispatcher implementation.

### 3.6 Monitoring — Prometheus + Grafana

Spring Boot Actuator + Micrometer exposes a Prometheus-scrapeable endpoint essentially for free — standard, low-cost choice for jitter, queue lag, index size, and retry/DLQ counts.

## 4. Failure Modes & Guarantees

### 4.1 Delivery vs execution guarantees — what we actually promise

The RFC's stated goal is "exactly-once execution," but **no real system (Kafka, Temporal, SQS included) actually delivers true exactly-once for external side effects** — it's a known impossibility without receiver-side idempotency. What we build, honestly: **at-least-once delivery and execution, made effectively-once via idempotent writes and fencing.**

**Ordering rule:** consumers must *process, then commit offset* — never the reverse. Committing before processing risks silent message loss on crash (unacceptable, violates zero-loss); processing before committing risks a duplicate redelivery (acceptable, because downstream writes are idempotent).

**Idempotent indexing:** duplicate delivery of the same `taskId` must collapse to a no-op, via conditional writes rather than blind upserts:
- Cassandra/DynamoDB: `INSERT ... IF NOT EXISTS` keyed on `taskId`.
- Redis: `ZADD NX` — only adds if the member doesn't already exist.

**Fencing for execution:** before a worker executes a task, it must win a conditional status transition (e.g. `UPDATE ... SET status='DISPATCHED' WHERE taskId=? IF status='INDEXED'`). Only the winner executes. This is what prevents a double-fire during leader failover, where an old leader and a new leader might otherwise both attempt to dispatch the same task.

**The honest ceiling:** if a worker crashes *after* winning the fencing write but *before* the external call (e.g. webhook) completes, we cannot know whether it fired. Retrying is the only safe default, which means **the receiving system must itself be idempotent** (e.g. accept the `taskId` as a dedup key) to be safe against duplicate firing. This limitation is inherent to distributed execution of external side effects, not a gap in our design.

### 4.2 Sub-15ms jitter — how we hit the timing target

Two distinct problems: finding the ready task cheaply, and waking up precisely enough to fire on time.

**Finding:** `ZRANGEBYSCORE(0, now)` / our heap's `peekReady()` — O(log n) or better, not the bottleneck.

**Waking up precisely — hybrid sleep-then-spin** (the technique used in HFT systems and the LMAX Disruptor):
1. If the next task is more than ~2ms away, sleep coarsely (`Thread.sleep`/`LockSupport.parkNanos`) — cheap, but OS wake-up granularity means it may be a bit late.
2. In the final ~1-2ms before the target time, busy-spin (`System.nanoTime()` polling) instead of sleeping — trades CPU for precision, since sleep/park wake-up latency alone can consume several ms under load.

**Other latency sources that matter more than expected:**
- **GC pauses:** a multi-ms STW pause alone can blow the entire jitter budget — motivates a low-pause collector (e.g. ZGC) over the default G1 for this workload.
- **Allocation in the hot tick path:** allocating on the tick thread risks triggering GC at exactly the worst moment — the tick/dispatch-decision path should be allocation-free where possible.
- **Lock contention:** the tick thread stalling on a lock held by an indexer consumer can eat the whole budget. Time-partitioned sharding limits contention to writers of the same shard.

**Scope clarification:** the <15ms NFR covers *(actual dispatch time − scheduled time)* — the moment we hand off to the worker pool. It does not cover network latency to an external receiver (e.g. webhook round-trip), which is outside our control.

**Deferred upgrade path:** a hierarchical timing wheel (as used by Netty's `HashedWheelTimer` and Kafka's internal "purgatory") gives O(1) insert/expire versus a heap's O(log n). A common production pattern is two-tier: far-future tasks stay in the heap/ZSET; a task migrates into a fine-grained timing wheel only once it's within seconds of firing. Deferred as an LLD decision once we've measured whether O(log n) actually matters at 1M+ tasks.

### 4.3 Redis (hot index) restart or upgrade — cold cache recovery

Redis is deliberately a **derived, disposable cache**, not the source of truth — **Cassandra/DynamoDB is authoritative**, so a Redis restart is a rebuild, never a data-loss event.

**Layer 1 — avoid the rebuild:** run Redis with replication (Sentinel/Cluster) so planned restarts/upgrades are replica promotions, not cold starts. Handles routine maintenance with zero rebuild cost.

**Layer 2 — fallback rebuild (full cluster loss, or first bootstrap):** query Cassandra for all tasks where `status IN (QUEUED, INDEXED)` and bulk `ZADD` them back into Redis. Our time-partitioned design pays off here twice — if Cassandra's clustering key mirrors the same time-bucket scheme as the index shards, the rebuild is a targeted range scan per bucket, not a full table scan across a million rows.

**Tie-back to our own stub:** this is the same pattern as our own crash-resilience FR, one layer up the stack. WAL replay on JVM boot (rebuilding the in-memory heap from the append-only log) is the small-scale version of exactly this — "the durable log is truth, the in-memory structure is disposable and rebuilt from it." Building WAL replay for our stub means we've implemented the mechanism that answers this question at both layers.

## 5. Local Stub Mapping (Plug-and-Play Boundary)

Every real component above is stubbed locally behind a port/interface, so a real backing system can later replace the stub without touching calling code.

| Real component | Local stand-in | Port/interface |
|---|---|---|
| Kafka | Local append-only file as ingestion log; in-process consumer thread pool reads it | `IngestionQueue` |
| Redis ZSET | In-memory time-partitioned min-heap | `TaskIndex` |
| Cassandra/DynamoDB | Local append-only WAL + compaction, replayed on boot | `TaskStore` |
| ZooKeeper/etcd | Deterministic in-process role state machine | `TickGate` / `ClusterCoordinator` |
| Worker pool | Java 21 virtual-thread executor | `ExecutionDispatcher` |
| Prometheus | Micrometer + Actuator endpoint | (Spring-native, no custom port needed) |
