# Day 4 — Message Queues & Event-Driven Architecture

## Day Summary

Days 1–3 built a synchronous stack: a request arrives, every downstream call completes, a response returns. That model breaks the moment any downstream service is slower than your SLA, spikier than your capacity, or simply unavailable. Day 4 is the architectural shift from *request-response* to *event-driven* — where the producer's job ends at publishing a durable event, and consumers process it independently on their own schedule. The mental model shift isn't just about decoupling; it's about accepting that **time is a first-class design dimension**. An event that happened at T=0 can be consumed at T+50ms or T+5min, by one consumer or twenty, and replayed as many times as needed. The real test here is knowing *exactly* what delivery guarantee your system requires (at-most-once, at-least-once, exactly-once) and how to enforce it at the consumer layer — not just at the broker.

---

## Pre-read Checklist

- **Stateless app servers** (Day 1) — Consumers in a consumer group must be stateless for the same reason app servers are; any in-memory processing state is lost on crash, which interacts directly with at-least-once redelivery.
- **Write-behind (write-back) caching** (Day 2) — The write-behind cache pattern is architecturally identical to async message processing: write fast to a buffer, flush to the durable store later. The durability risk is the same; understand why before applying it.
- **Outbox pattern for fan-out workers** (Day 3) — Today formalizes the outbox pattern with at-least-once guarantees and DLQ handling that was left abstract in Day 3. The fan-out worker introduced in Day 3 is a Kafka consumer in today's terms.
- **Async fan-out eventual consistency** (Day 3) — Day 3 accepted that feed updates have eventual consistency. Today establishes the delivery semantics that bound *how* eventual: DLQ retry limits, consumer lag alerts, and backpressure mechanisms.
- **Connection pooling under load** (Day 3) — Consumers opening DB connections for each message processed face the same pool exhaustion problem as app servers. PgBouncer/HikariCP applies equally to consumer workloads.

---

## The Problem, Stated Precisely

**Context:** You're building the async processing backbone for the same social platform from Day 3. Three downstream workflows must be triggered on every new post: (1) fan-out the post to follower feed tables, (2) index the post body and hashtags in the search service, (3) send push notifications to a subset of followers. A fourth workflow triggers on every user follow action: update recommendation models.

### Functional Requirements
- Decouple post creation from all downstream workflows
- Fan-out must process every post; no silent drops
- Push notifications may be delayed but never double-sent for the same event
- Search index must eventually reflect all posts; lag < 5s at p99
- Support replaying historical events to backfill a new consumer (e.g., a new ML pipeline)

### Non-Functional Requirements

| Parameter | Target |
|---|---|
| Write event rate (posts) | 1,160/sec average; 3,500/sec peak |
| Follow event rate | 800/sec average; 2,400/sec peak |
| Fan-out write amplification | Up to 5,000 messages/sec (avg followers × post rate) |
| End-to-end fan-out lag SLA | p99 < 5s |
| Push notification lag SLA | p99 < 10s |
| Search index lag SLA | p99 < 5s |
| Message durability | Zero loss after broker ACK |
| Consumer throughput | Fan-out: 50,000 msg/sec; Search: 5,000 msg/sec; Notify: 5,000 msg/sec |
| Retention (for replay) | 7 days |
| Availability | 99.95% broker uptime |

---

## Capacity Estimation

### Message Volumes

```
Post events:       1,160/sec avg → 100M/day
Follow events:       800/sec avg → 69M/day
Fan-out messages:  ~5,000/sec avg (1,160 posts × avg 4.3 followers)
                   → 432M messages/day to fan-out consumers
Notification msgs: ~1,160/sec (1:1 with posts, filtered down by notification prefs)
Search index msgs: ~1,160/sec (1:1 with posts)

Total broker ingest: ~8,000 msg/sec avg; ~22,000 msg/sec peak
```

### Storage (Kafka log retention at 7 days)

```
Avg message size (post event with metadata): ~2 KB
Daily ingest bytes: 8,000 msg/sec × 2 KB × 86,400 sec = ~1.38 TB/day
7-day retention: 1.38 TB × 7 = ~9.7 TB
With replication factor 3: 9.7 TB × 3 = ~29 TB raw broker storage
```

### Bandwidth

```
Broker ingress:  8,000 msg/sec × 2 KB = 16 MB/sec
Broker egress:   3 consumer groups × 8,000 msg/sec × 2 KB = 48 MB/sec
                 (fan-out consumer reads at ~5× rate due to fan-out expansion)
Peak egress: ~150 MB/sec
```

### Partition Sizing (Kafka)

```
Target: each partition handles ≤ 1,000 msg/sec (safe ceiling for a single consumer thread)
Fan-out topic: 5,000 msg/sec → minimum 5 partitions; use 12 (headroom + rebalance tolerance)
Post topic:    1,160 msg/sec → 4 partitions; use 8
Follow topic:    800 msg/sec → 2 partitions; use 4

Consumer group sizing:
Fan-out: 12 partitions → max 12 consumer instances (scale to match)
```

---

## Core Approaches

### 1. Queue Model (Point-to-Point)

**What it solves:** Task distribution where each unit of work must be done exactly once. Fan-out writes, image resizing, email sending — each message has one processor.

**How it works:** The broker holds messages until a consumer pulls and ACKs them. ACK removes the message from the queue. If the consumer crashes before ACKing, the broker re-delivers after a visibility timeout (SQS: 30s default; RabbitMQ: configurable). This is the source of at-least-once semantics — re-delivery on crash means duplicate processing is possible.

**Internal mechanics (SQS):**
- Message is written to 3 AZs synchronously before the `SendMessage` API returns.
- Consumer calls `ReceiveMessage` (long-poll, up to 20s). Message becomes *invisible* for the visibility timeout duration.
- Consumer calls `DeleteMessage` after successful processing. If visibility timeout expires without deletion, the message reappears.
- `ApproximateNumberOfMessagesNotVisible` metric tells you how many messages are in-flight.

**Failure modes:**
- **Visibility timeout too short:** Consumer takes 35s to process; timeout is 30s → message reappears while original consumer is still working → duplicate processing. Fix: set timeout to 6× your p99 processing time; extend visibility timeout mid-processing via `ChangeMessageVisibility`.
- **Poison message loop:** A malformed message causes the consumer to crash every time. Without a DLQ, it loops forever. Always configure `maxReceiveCount` (e.g., 5) + DLQ.

**When NOT to use:** When multiple independent consumers need the same event (use pub/sub). When ordered processing across all messages is required (use a Kafka partition).

---

### 2. Pub/Sub Model

**What it solves:** One event must trigger N independent workflows. A `post.created` event must reach the fan-out consumer, the search indexer, and the notification service simultaneously — independently of each other's availability or processing speed.

**How it works:** The broker maintains separate subscription cursors per consumer group. Each consumer group reads the topic independently; the broker doesn't delete a message until *all* subscribed groups have consumed it (or retention period expires). This is fundamentally different from a queue — the log is shared but the offset pointers are per-consumer.

**Kafka mechanics:**
```
Topic: post.created  (8 partitions, RF=3)

Consumer Group A: feed-fanout-workers       → offset pointer per partition
Consumer Group B: search-indexer-workers    → independent offset pointer
Consumer Group C: notification-workers      → independent offset pointer

Partition 3, current offsets:
  Group A: offset 84,291  (up to date)
  Group B: offset 84,200  (91 messages behind — consumer lag)
  Group C: offset 83,950  (341 messages behind — alert threshold)
```

**Failure modes:**
- **Consumer lag accumulation:** Group C falls behind → lag grows unboundedly → notifications are delivered hours late. Detection: alert on `consumer_lag > 10,000`. Recovery: scale up consumer instances (add pods; Kafka rebalances partitions automatically).
- **Rebalance storm:** Kafka consumer group rebalances when an instance joins or leaves. During rebalance, *all* partitions stop processing until assignment completes (~5–30s). Frequent pod restarts during deployments cause repeated rebalances. Fix: use static group membership (`group.instance.id`) so a restarting pod resumes its partition without triggering a full rebalance.

---

### 3. At-Least-Once Delivery

**What it solves:** Durability guarantee — no message is silently dropped, even under consumer or broker failure.

**How it works:** The broker delivers a message and expects an explicit ACK. If ACK doesn't arrive within the timeout (consumer crashed, processing failed), the message is redelivered. The consequence: your consumer *will* process duplicate messages. This is not a bug — it's the contract. Your consumer must be **idempotent**.

**Idempotency patterns:**

```python
# Pattern 1: Idempotency key in DB (upsert on conflict)
def process_fan_out(message):
    post_id = message['post_id']
    user_id = message['recipient_user_id']
    # ON CONFLICT DO NOTHING prevents duplicate feed entries
    db.execute("""
        INSERT INTO feed (user_id, post_id, created_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, post_id) DO NOTHING
    """, (user_id, post_id, message['created_at']))
    ack(message)

# Pattern 2: Redis deduplication with TTL (for notification service)
def process_notification(message):
    dedup_key = f"notif:{message['event_id']}"
    if redis.set(dedup_key, 1, nx=True, ex=86400):  # nx = only set if not exists
        send_push_notification(message)
    ack(message)  # Always ACK — even if we skipped as duplicate
```

**Critical rule:** Always ACK the message even when you detect it as a duplicate. If you don't ACK a duplicate and let it time out, the broker will redeliver it again — an infinite retry loop on a duplicate.

**Failure modes:**
- **Non-idempotent side effect:** Sending a push notification is non-idempotent by nature (user receives two "new post" notifications). The Redis dedup key pattern above is the correct fix.
- **Idempotency key collision:** Two different events share the same key due to a bug. One is silently dropped. Use UUIDs, not sequential IDs or compound keys, for event IDs.

---

### 4. Exactly-Once Semantics

**What it solves:** Financial or billing operations where duplicate processing causes real harm (double charging, double crediting). At-least-once + idempotency covers *most* cases; exactly-once is for cases where idempotency is architecturally impossible.

**How it works (Kafka Transactions API):**

```
Producer side:
  - producer.initTransactions()
  - producer.beginTransaction()
  - producer.send(topic, message)
  - producer.sendOffsetsToTransaction(offsets, consumerGroup)
    ↑ atomically commits: message produced + consumer offset advanced
  - producer.commitTransaction()

Broker side:
  - Maintains a transaction log per producer (identified by transactional.id)
  - Other consumers with isolation.level=read_committed skip uncommitted messages
```

The key insight: Kafka's exactly-once is implemented by making the *offset commit* and the *message produce* a single atomic transaction. This prevents the "consumed but not committed" problem (at-least-once) and the "committed but not produced" problem (at-most-once).

**Cost:** ~10–20% throughput reduction vs. at-least-once. Requires `acks=all`, `enable.idempotence=true`, `transactional.id` configured.

**When NOT to use:** When the consumer writes to an external system that doesn't participate in the Kafka transaction (e.g., PostgreSQL). In that case, the Kafka transaction commits but the DB write can still fail — you're back to at-least-once. True exactly-once across heterogeneous systems requires 2PC or the outbox pattern (Day 3).

---

### 5. Dead Letter Queue (DLQ)

**What it solves:** A poison message — one that always fails processing — would otherwise cause a consumer to retry forever, blocking all messages behind it in the partition/queue.

**How it works:**

```
Message lifecycle:
  1. Broker delivers message → consumer attempts processing
  2. Processing fails → consumer NACKs or visibility timeout expires
  3. Broker redelivers → attempt 2
  4. After maxReceiveCount (e.g., 5) failures → broker moves to DLQ
  5. DLQ holds message indefinitely for inspection / manual replay
```

**DLQ design decisions:**
- **Per-topic DLQ vs. shared DLQ:** Per-topic is easier to triage (failures are scoped). Shared DLQ simplifies monitoring (one alert) but harder to debug.
- **DLQ consumer:** Build a DLQ consumer that alerts on new messages, logs payload for debugging, and supports one-click replay back to the original topic after the bug is fixed.
- **Retention on DLQ:** Set longer retention (30 days) than the main topic (7 days) — you need time to investigate and fix the bug before replaying.

**Failure mode:** Forgetting to monitor the DLQ. Messages pile up silently. Users notice missing data weeks later. Fix: `DLQ message count > 0` must be a P1 alert.

---

### 6. Backpressure

**What it solves:** A fast producer overruns a slow consumer, causing the queue to grow unboundedly, eventually exhausting broker storage or causing consumer OOM.

**How it works:**

```
Kafka producer-side (linger + buffer):
  - Producer buffers records in memory (buffer.memory = 32MB default)
  - If buffer is full, producer.send() blocks up to max.block.ms (60s default)
  - After 60s block, throws BufferExhaustedException
  → This IS backpressure: the producer thread blocks instead of dropping

Consumer-side (poll loop throttling):
  - Consumer fetches max.poll.records (500 default) per poll()
  - If processing 500 records takes longer than max.poll.interval.ms (5min),
    Kafka assumes consumer is dead and triggers rebalance
  - Tune: lower max.poll.records OR increase max.poll.interval.ms
```

**Application-level backpressure:**
```python
# Semaphore-based: limit concurrent in-flight messages
semaphore = asyncio.Semaphore(100)  # max 100 concurrent fan-out writes

async def process_message(msg):
    async with semaphore:
        await write_to_cassandra(msg)
        await consumer.commit(msg)
```

**Failure mode:** Ignoring consumer lag until the topic hits retention window. Messages older than 7 days are deleted by the broker regardless of whether they've been consumed — silent data loss. Alert on `consumer_lag_by_time > retention_window * 0.5`.

---

### 7. Event Sourcing

**What it solves:** State that must be reconstructible, auditable, and replayable to new consumers. Instead of storing the current state of a user's follow list, store every `follow` and `unfollow` event. The current state is derived by replaying the log.

**How it works:**

```
Event log (Kafka topic: user.follow.events, retained forever):
  t=0:   { event: FOLLOW,   follower: A, followee: B }
  t=10:  { event: FOLLOW,   follower: A, followee: C }
  t=50:  { event: UNFOLLOW, follower: A, followee: B }
  t=90:  { event: FOLLOW,   follower: A, followee: B }

Materialized view (rebuilt by replaying log):
  A follows: [B, C]   (B added at t=0, removed at t=50, re-added at t=90)
```

**Advantages over mutable state:**
- New consumer (ML pipeline) reads from offset 0 and builds its own view of the data.
- Time-travel debugging: replay to a specific offset to reproduce a bug.
- No `UPDATE`/`DELETE` contention — append-only writes avoid lock conflicts.

**Implementation decision:** Use a compacted Kafka topic (log compaction keeps only the latest event per key) for materialized views, and a non-compacted topic (full history) for auditing and replay.

**When NOT to use:** When state is simple, mutations are rare, and you have no need for audit or replay. Event sourcing adds read complexity (must reconstruct state from log) and tooling overhead. Don't apply it uniformly — apply it where the audit trail or replay capability has direct product value.

---

### 8. Ordering Guarantees

**What it solves:** Operations that must be applied in causal order. Processing an `UNFOLLOW` before the `FOLLOW` it reverses produces incorrect state.

**How it works:**

```
Kafka ordering contract: all messages within a partition are delivered in the
order they were written. Messages across partitions have no ordering guarantee.

Partitioning strategy: hash the key that must be ordered together.
  producer.send(topic, key=user_id, value=follow_event)
  → All events for user_id=A always land in partition hash(A) % N
  → Follow and Unfollow events for A are always ordered within that partition
```

**Trade-off table:**

| Scope | Strategy | Throughput | Ordering |
|---|---|---|---|
| Global | 1 partition | Low (~50K msg/sec max) | Total order |
| Per-entity | Partition by entity key | High (linear scale) | Ordered per entity |
| Best-effort | Random partition | Highest | None |

**Failure mode:** Changing partition count on a live topic breaks key-to-partition mapping — entity A's events start landing on a different partition than its historical events. The new partition has no context of prior state. Fix: never change partition count on a topic with ordering semantics; instead, create a new topic and migrate.

---

## System Architecture Walkthrough

### Write Path (Post Created → Fan-Out)

1. App server processes `POST /posts`, writes the post to PostgreSQL primary, and in the same transaction, inserts a row into the `outbox` table: `{ event_type: 'post.created', payload: {...}, published: false }`.
2. A lightweight **outbox poller** (separate process, runs every 100ms) queries `SELECT * FROM outbox WHERE published=false LIMIT 500`, publishes each row to Kafka topic `post.created`, then marks rows as `published=true` in a batch `UPDATE`.
3. Kafka durably persists the event to all 3 broker replicas before ACKing the producer (`acks=all`, `min.insync.replicas=2`). From this point forward, the event is guaranteed durable.
4. Three consumer groups independently read from `post.created`:
   - **feed-fanout-workers** (12 instances, 12 partitions): query follower list from Cassandra, fan out `N` writes to the `feed` table.
   - **search-indexer-workers** (4 instances, 8 partitions): serialize the post body + hashtags and bulk-index into Elasticsearch.
   - **notification-workers** (4 instances, 8 partitions): look up notification preferences per follower, publish push via APNs/FCM.
5. Each consumer commits its Kafka offset only *after* successfully writing to its downstream store. A consumer crash before commit causes re-delivery (at-least-once); idempotency keys prevent duplicate downstream effects.

### Read Path (Consumer Lag Monitoring)

1. Prometheus scrapes Kafka JMX metrics every 30s: `kafka.consumer.group.lag` per consumer group per partition.
2. Alert fires when `consumer_lag > 10,000` (fan-out) or `consumer_lag_by_time > 60s` (notifications).
3. Kubernetes HPA (Horizontal Pod Autoscaler) scales consumer deployments based on custom metric `consumer_lag_per_instance > 1,000`.

### Failure Handling

- **Kafka broker failure (1 of 3):** With RF=3 and `min.insync.replicas=2`, the cluster continues serving reads and writes. The failed broker's partitions are automatically reassigned to the two surviving brokers. Kafka leader election completes in ~30s.
- **Consumer crash mid-processing:** Message is re-delivered after visibility timeout. Idempotency key prevents double fan-out write (Day 3's `ON CONFLICT DO NOTHING`). Offset is not committed, so Kafka retries from the last committed offset.
- **All instances in a consumer group crash:** Kafka holds messages in the topic (up to retention window). When instances restart, they resume from last committed offset. Zero messages lost; lag accumulates during downtime.
- **Outbox poller crash:** Unpolled events remain in the outbox table, marked `published=false`. When the poller restarts, it picks up from where it left off. This is the outbox pattern's key durability guarantee — the DB is the source of truth, not in-memory producer state.

### Bottleneck Progression

| Event Rate | Bottleneck | Solution |
|---|---|---|
| < 5K msg/sec | None | Single Kafka broker, managed SQS |
| 5K–50K msg/sec | Consumer throughput | Scale consumer instances (Kafka rebalances) |
| 50K–500K msg/sec | Partition count ceiling | Increase partitions; shard producer by key range |
| > 500K msg/sec | Broker I/O | Multi-cluster with Kafka MirrorMaker; dedicated brokers per topic |

---

## Data Model

### Kafka Topic: `post.created`

```json
{
  "event_id":   "uuid-v4",        // idempotency key; unique per event
  "event_type": "post.created",
  "version":    1,                // schema version for consumer compatibility
  "timestamp":  1711234567890,    // epoch ms; used for ordering assertions
  "partition_key": "user_id",     // determines partition assignment
  "payload": {
    "post_id":   "uuid-v4",
    "user_id":   "uuid-v4",
    "body":      "string (≤1KB)",
    "hashtags":  ["string"],
    "created_at": "ISO8601"
  }
}
```

**Storage:** Kafka topic, 8 partitions, RF=3, 7-day retention. Messages are serialized as Avro (with Schema Registry) — not JSON — for compact encoding (~40% smaller) and schema evolution safety.

### Outbox Table (PostgreSQL)

| Field | Type | Notes |
|---|---|---|
| id | UUID (PK) | |
| event_type | VARCHAR(100) | Indexed |
| payload | JSONB | Full event payload |
| published | BOOLEAN | Default false; indexed |
| created_at | TIMESTAMPTZ | Indexed; poller queries `WHERE published=false ORDER BY created_at` |
| published_at | TIMESTAMPTZ | Nullable; set on publish for lag monitoring |

**Storage:** PostgreSQL (same DB as `posts`, same transaction). The outbox table is high-write, high-delete — add a background job to `DELETE FROM outbox WHERE published=true AND published_at < NOW() - INTERVAL '1 hour'` to prevent unbounded growth.

### Consumer Offset Store (Kafka-managed)

Kafka stores committed offsets in the internal `__consumer_offsets` topic (compacted, RF=3). Never manually manage offsets unless implementing exactly-once semantics — let the Kafka client library handle it.

---

## Interview Questions with Model Answers

### Q1 (Mid) — "Why use Kafka instead of a simple database table as a queue?"

**Model answer:** A database-backed queue is a valid starting point at low scale, but it has two fundamental problems at volume. First, `SELECT ... FOR UPDATE SKIP LOCKED` creates lock contention on the queue table that degrades write throughput above ~1K msg/sec. Second, a DB table has no native concept of consumer groups — you'd have to implement your own offset tracking, retry logic, and partition assignment. Kafka separates the log (Kafka) from processing state (consumer offsets), letting you add new consumer groups that read the full history without touching producers or existing consumers. At our target of 8K msg/sec and 3 independent consumer groups, Kafka's separation of concerns and native consumer group management justify the operational overhead.

**Follow-up:** "At what scale would you stick with the database-backed queue?"

**Pitfall:** Defaulting to Kafka for everything without recognizing the operational cost — cluster management, schema registry, Zookeeper/KRaft, monitoring. For a startup with < 1K msg/sec and 1 consumer, SQS or a Postgres-backed queue is correct.

---

### Q2 (Mid) — "How do you guarantee a push notification is sent at most once?"

**Model answer:** At-least-once delivery from Kafka means the notification consumer will process the same event more than once on a retry. I'd generate a deterministic notification ID — `sha256(event_id + recipient_user_id)` — and use a Redis `SET NX EX 86400` call before dispatching to APNs/FCM. If the key already exists, the notification was already sent; skip it and ACK the Kafka message. The 24h TTL bounds Redis memory usage while covering any realistic Kafka retry window. The key insight: I always ACK the Kafka message regardless of the Redis result — a duplicate detection hit is a successful processing, not a failure.

**Follow-up:** "What happens if Redis goes down mid-campaign?"

**Pitfall:** Not ACKing the Kafka message on duplicate detection, causing infinite redelivery. Or relying on APNs/FCM dedup IDs alone — APNs dedup is best-effort, not guaranteed.

---

### Q3 (Senior) — "Your fan-out consumer is 500,000 messages behind after a 20-minute outage. How do you recover without overwhelming Cassandra?"

**Model answer:** Blindly resuming at full speed creates a write spike to Cassandra that could destabilize it — trading one incident for another. I'd use a combination of three controls: (1) Kafka consumer `fetch.max.bytes` and `max.poll.records` to limit the consumer's batch size to 200 records/poll, throttling Cassandra write rate to ~2K/sec; (2) rate limiting in the consumer's processing loop using a token bucket (Guava's `RateLimiter`) capped at Cassandra's write SLA; (3) prioritize fan-out for recent posts (partition by recency) since users only see their current feed — posts from 20 minutes ago are less urgent. Monitor Cassandra's `WriteLatency_p99` and back off if it climbs above 20ms. Total recovery time: ~4 minutes at throttled rate.

**Follow-up:** "The lag is now growing faster than your consumers can process even at full speed. What's wrong?"

**Pitfall:** Just "adding more consumers" without recognizing that consumer count is bounded by partition count. If you have 12 partitions, the 13th consumer instance is idle. The fix is to increase partitions — but that requires a migration plan since you can't change partition count on a live topic without breaking key ordering.

---

### Q4 (Senior) — "How does the outbox pattern guarantee exactly-once delivery from the app server to Kafka?"

**Model answer:** It doesn't — and that distinction is critical. The outbox pattern guarantees *at-least-once* delivery from the DB to Kafka, but it solves the dual-write atomicity problem: without it, you risk writing to Kafka but crashing before the DB commit, or committing to the DB but crashing before Kafka publish. By writing to the outbox table *in the same DB transaction* as the business write, both succeed or both fail atomically. The outbox poller then publishes to Kafka with retries; if it publishes a message but crashes before marking it `published=true`, it will publish again on restart. That's at-least-once. Consumers must still be idempotent. True exactly-once across DB + Kafka requires Kafka's transactional API *and* a Kafka Streams consumer that commits the offset and the downstream DB write atomically — which only works if the downstream store is Kafka itself.

**Follow-up:** "The outbox poller is a single process. What happens if it crashes?"

**Pitfall:** Assuming the outbox pattern gives exactly-once semantics. It gives atomicity on the write side (no lost events) and at-least-once on the publish side (possible duplicate publishes). The two are different problems.

---

### Q5 (Staff/Principal) — "Design a schema evolution strategy for your Kafka events so that adding a new field to `post.created` doesn't break the 3 existing consumers."

**Model answer:** Schema evolution requires a contract between producers and consumers enforced at the broker level — not just documentation. I'd use Apache Avro with a Confluent Schema Registry. Each schema version is registered in the registry; the Avro serializer embeds the schema ID (4 bytes) in every message. The consumer fetches the schema by ID and deserializes accordingly. The evolution rules: *backward-compatible* changes (adding optional fields with defaults) are allowed without bumping the schema version major; consumers on the old schema simply ignore unknown fields. *Forward-compatible* changes (removing a field) require consumers to be updated first. I'd enforce `BACKWARD` compatibility mode in the Schema Registry — it rejects any schema registration that breaks existing consumers. For breaking changes, I use a new topic (`post.created.v2`) and run both topics in parallel during migration, giving consumers a migration window (e.g., 30 days) before decommissioning v1. This avoids coordinated deployments.

**Follow-up:** "A consumer is 6 days into a 7-day retention window and hasn't finished migrating to v2. What do you do?"

**Pitfall:** Using JSON without a schema registry, relying on "just be careful" for evolution. JSON has no enforcement mechanism — a producer deploys a breaking change and consumers fail silently on deserialization errors hours later.

---

## Trade-offs to Articulate

1. **"I chose Kafka over SQS because at 8K msg/sec with 3 independent consumer groups, SQS requires 3 separate queues and 3 separate SNS fan-outs to replicate what Kafka's consumer group model gives natively. The trade-off I'm accepting is operational complexity — running a Kafka cluster requires broker management, partition tuning, and Schema Registry, none of which are necessary with SQS."**

2. **"I chose at-least-once delivery over exactly-once for the fan-out consumer because at 50K fan-out msg/sec, Kafka's transactional API imposes a ~15% throughput reduction and requires all downstream stores (Cassandra) to participate in the transaction — which Cassandra doesn't support. The trade-off I'm accepting is that consumers must implement idempotency, adding complexity to every consumer's processing logic."**

3. **"I chose the outbox pattern over direct Kafka writes from the app server because without it, a crash between the DB commit and the Kafka publish silently drops events — there's no durability guarantee. The trade-off I'm accepting is an extra DB table write per post creation and a polling process that adds ~100ms of additional fan-out latency."**

4. **"I chose partition-by-user-id for ordering over a single global partition because a single partition caps throughput at ~50K msg/sec and makes the entire fan-out single-threaded. The trade-off I'm accepting is that global event ordering across users is not guaranteed — which is acceptable because feed consistency is per-user, not cross-user."**

5. **"I chose Avro + Schema Registry over JSON for event serialization because at 8K msg/sec sustained, Avro's binary encoding is ~40% smaller than JSON, reducing broker storage from 29TB to ~17TB over 7 days, and the Schema Registry enforces backward compatibility at deploy time rather than at runtime. The trade-off I'm accepting is the operational overhead of running a Schema Registry and the developer friction of compiling Avro schemas."**

6. **"I chose consumer-managed offset commits over auto-commit because auto-commit advances the offset on a timer regardless of whether processing succeeded — a consumer crash between auto-commit and a failed downstream write causes silent data loss. The trade-off I'm accepting is that consumer code must explicitly call `consumer.commitSync()` after every successful processing batch, and developers must understand the at-least-once implications of manual offset management."**

---

## Failure Modes and Resilience Patterns

### 1. Kafka Leader Election During Write Spike
- **Symptom:** Producer `send()` calls block for 5–30s; fan-out lag spikes. Error: `NotLeaderOrFollowerException`.
- **Root cause:** A broker carrying partition leaders fails during peak write load. Kafka must elect new leaders for all affected partitions.
- **Detection:** `UnderReplicatedPartitions > 0` alert on Kafka JMX; producer `request-latency-avg` spikes.
- **Mitigation:** `acks=all` with `retries=Integer.MAX_VALUE` and `retry.backoff.ms=100` — producer retries automatically during leader election. App-level: set `max.block.ms=60000` to absorb the election window without surfacing errors to upstream callers.

### 2. Consumer Group Rebalance Storm
- **Symptom:** All consumers in a group stop processing for 5–30s; lag spikes across all partitions simultaneously. Happens repeatedly during rolling deployments.
- **Root cause:** Every pod restart triggers a rebalance. With 12 consumer pods and a 30s deployment rollout window, rebalances can chain continuously.
- **Detection:** `kafka.consumer.rebalance.rate` > 1/min in Prometheus.
- **Mitigation:** Set `group.instance.id` (static membership) — a pod with a known ID rejoins without triggering a rebalance for 5 minutes (`session.timeout.ms=300000`). Use `CooperativeStickyAssignor` instead of the default `RangeAssignor` — cooperative rebalancing only moves partitions that need to move, rather than stopping the world.

### 3. Outbox Table Unbounded Growth
- **Symptom:** PostgreSQL disk usage grows at 100 GB/day; query latency on `posts` table increases (same disk, same autovacuum budget).
- **Root cause:** The cleanup job deleting `published=true` rows stopped running (deployment bug, cron failure).
- **Detection:** Alert on outbox row count > 1M or table size > 10 GB.
- **Mitigation:** Use PostgreSQL table partitioning on `created_at` for the outbox table (daily partitions). Drop the previous day's partition entirely — O(1) vs. row-by-row DELETE. Separate outbox vacuum from the main `posts` table by putting it in a separate schema with its own autovacuum settings.

### 4. Schema Incompatibility Breaking Consumers
- **Symptom:** Consumer pods crash-loop with deserialization errors (`SchemaNotFoundException`, `IncompatibleSchemaException`) after a producer deployment.
- **Root cause:** A producer deployed a new schema version with a removed required field without updating consumers first.
- **Detection:** Consumer pod restart rate > 0 in Kubernetes; DLQ message count spikes.
- **Mitigation:** Schema Registry `BACKWARD` compatibility mode blocks the incompatible schema registration at deploy time, before any producer writes new data. Enforce compatibility check as a CI/CD gate — `mvn schema-registry:test-compatibility` must pass before deployment.

### 5. DLQ Silent Accumulation
- **Symptom:** A subset of users have missing feed entries or undelivered notifications for weeks. No alerts fired.
- **Root cause:** A consumer bug caused specific message shapes to fail processing. They were silently moved to the DLQ. No one was monitoring the DLQ.
- **Detection:** `DLQ message count > 0` is a P1 alert. Add a DLQ consumer that emits a metric for every message received.
- **Mitigation:** Build a DLQ replay tool as day-1 infrastructure, not an afterthought. After fixing the bug, replay with `kafka-consumer-groups.sh --reset-offsets` targeting the DLQ topic. Always set DLQ retention to 30 days.

---

## How This Connects Forward

- **Day 5 (API Design):** The `event_id` in every Kafka message is an idempotency key — the same concept applied at the API layer in Day 5's `POST` idempotency header design. The pattern is identical; the enforcement point differs (broker vs. API gateway).
- **Day 8 (News Feed Design):** Day 8's celebrity fan-out optimization relies entirely on the consumer group model established today. The hybrid architecture (fan-out-on-write for regular users, fan-out-on-read for celebrities) is implemented as two separate consumer groups with different routing logic reading the same `post.created` topic.
- **Day 9 (Search & Analytics):** The search-indexer consumer group introduced today is the write path for the Elasticsearch index covered in Day 9. Day 9 addresses exactly the dual-write consistency problem between Kafka and Elasticsearch.
- **Day 11 (Distributed Transactions):** The outbox pattern today is the informal version of the Saga pattern covered in Day 11. Day 11 formalizes compensating transactions and exactly-once cross-service semantics — the failure modes left open today (outbox poller crash, Cassandra write failure after Kafka ACK) get resolved there.
- **Day 20 (Payment System):** Exactly-once semantics and idempotency keys introduced today are the foundation of Day 20's payment processing design. The Kafka Transactions API pattern for debit+credit atomicity builds directly on today's transactional producer section.

---

## Diagrams to Draw

1. **Queue vs. Pub/Sub Topology:** Two side-by-side diagrams. Left: 3 producer pods → SQS queue → 3 consumer pods (one message consumed by exactly one pod, then deleted). Right: 3 producer pods → Kafka topic (3 partitions) → 3 independent consumer groups (A: fan-out, B: search, C: notify), each with its own offset pointer. Show a single message being read independently by all 3 groups.

2. **At-Least-Once Delivery + Idempotency Flow:** Draw the timeline: producer publishes `event_id=abc` → consumer processes → Cassandra write succeeds → consumer commits offset. Second path: consumer processes → Cassandra write fails → consumer crashes without committing offset → broker redelivers `event_id=abc` → consumer checks dedup key in Redis → skip duplicate → commit offset. Show all branching paths.

3. **Outbox Pattern Transaction Boundary:** Draw the DB transaction box containing: (1) INSERT into `posts`, (2) INSERT into `outbox WHERE published=false`. Outside the transaction: outbox poller reads unpublished rows → publishes to Kafka → marks `published=true`. Show the crash scenarios: crash after DB commit (poller retries), crash after Kafka publish but before marking published (duplicate publish, idempotent consumer handles it).

4. **Kafka Partition + Consumer Group Assignment:** Draw a `post.created` topic with 8 partitions (P0–P7). Show 3 consumer groups, each with a different number of consumer instances (fan-out: 8 instances consuming 1 partition each; search: 4 instances consuming 2 partitions each; notify: 2 instances consuming 4 partitions each). Show one consumer instance failing — draw the rebalance reassignment.

5. **Backpressure + DLQ Full Flow:** Draw a producer → Kafka topic → consumer group. Show the consumer's processing loop: fetch batch → process → write to Cassandra → commit offset. Add the DLQ path: after 5 failures, message routes to DLQ topic. Add the lag monitoring path: Prometheus scrapes consumer lag → alert fires → HPA scales consumer deployment from 4 to 8 pods → Kafka rebalances partitions.
