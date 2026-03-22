# System Design — 21-Day Expert Track
> Intermediate → Advanced. One topic per day, structured as: problem statement, all approaches, and key tools & references.

---

## Week 1 — Foundations & Building Blocks
> Master the core components that appear in every system design interview. These are your tools — know them deeply.

---

### Day 1 — Scalability & Load Balancing

**The Problem**
A single server can't handle millions of requests. How do you scale a system horizontally so that adding more machines increases capacity linearly? And how do you ensure no single server becomes a bottleneck or a point of failure?

**Approaches**

1. **Vertical scaling (scale-up)** — Replace the server with a bigger machine (more CPU, RAM). Simple, requires no code changes, but has a hard ceiling — the largest machine available. Also still a single point of failure.

2. **Horizontal scaling (scale-out)** — Add more machines and distribute traffic across them using a load balancer. Near-infinite scale, but requires your app servers to be stateless (no in-memory session data).

3. **Layer-4 load balancing (transport layer)** — Routes based on IP address and TCP/UDP port. Very fast, low overhead, no content inspection. Use for raw TCP throughput: database connections, game servers, anything non-HTTP or latency-critical.

4. **Layer-7 load balancing (application layer)** — Reads HTTP headers, cookies, URLs, and request bodies. Enables content-based routing (send `/api` to one cluster, `/static` to CDN), sticky sessions via cookies, A/B testing, and WebSocket upgrade handling. Higher CPU cost per connection.

5. **Load balancing algorithms**
   - *Round Robin*: distribute requests evenly in order. Best when all requests have similar cost.
   - *Weighted Round Robin*: assign more traffic to higher-capacity servers.
   - *Least Connections*: send each new request to the server with the fewest active connections. Best for mixed workloads where some requests are much heavier.
   - *IP Hash*: hash the client IP to always route the same user to the same server. Enables soft session affinity without sticky-session cookies.

6. **Health checks** — The LB probes each server on an interval (TCP ping or HTTP `GET /health`). A server that fails N consecutive checks is removed from the pool. It re-enters only after passing M consecutive checks (prevents flapping). Passive health checks also mark servers down after real request failures.

7. **Stateless app servers** — Move all session state out of server memory and into an external store (Redis). Any server can then handle any request. This is the prerequisite for horizontal scaling to work.

8. **SSL/TLS termination** — Offload TLS decryption at the load balancer so app servers only handle plain HTTP, reducing their CPU load. Use end-to-end TLS (re-encrypt from LB to server) when compliance requires encryption in transit throughout.

9. **High-availability load balancer** — Run two LB instances sharing a virtual IP (VIP). A heartbeat daemon (Keepalived) promotes the secondary if the primary fails. Active-passive failover happens in seconds with no DNS change required.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| NGINX | High-performance L7 LB and reverse proxy; widely used in production |
| HAProxy | Battle-tested L4/L7 LB; excellent for high connection counts |
| AWS ALB (Application Load Balancer) | Managed L7 LB; integrates with ECS, EKS, Auto Scaling |
| AWS NLB (Network Load Balancer) | Managed L4 LB; ultra-low latency, static IP support |
| Keepalived | Linux daemon for VRRP-based active-passive LB HA |
| Redis | External session store; makes app servers stateless |
| Consistent Hashing | Algorithm for distributing keys across nodes (Day 6 deep dive) |

---

### Day 2 — Caching Strategies

**The Problem**
Reading from a database on every request is slow (~1–100ms) and expensive. How do you reduce latency to ~1ms for repeated reads while keeping cached data consistent with the source of truth?

**Approaches**

1. **Cache-aside (lazy loading)** — App checks cache first. On a miss, reads from DB, writes the result to cache, then returns it. Cache only contains data that has actually been requested. Downside: first request always hits DB (cold start); potential for stale data if DB is updated and cache isn't invalidated.

2. **Read-through** — Cache sits in front of DB. On a miss, the cache itself fetches from DB and populates itself. App always talks to cache only. Simplifies app code but requires a cache that supports this pattern (e.g. a caching library, not raw Redis).

3. **Write-through** — Every write goes to cache and DB synchronously. Cache is always warm and consistent. Downside: every write pays double latency; cache fills with data that may never be read again.

4. **Write-behind (write-back)** — Write to cache immediately (fast), then flush to DB asynchronously in the background. Very low write latency, but risk of data loss if cache node fails before the flush.

5. **Cache eviction policies**
   - *TTL (Time to Live)*: data expires after N seconds. Simple and predictable. Choose TTL based on how stale the data can be.
   - *LRU (Least Recently Used)*: evict the entry that hasn't been accessed for the longest time.
   - *LFU (Least Frequently Used)*: evict the entry accessed the fewest times. Better for skewed access patterns.

6. **CDN caching** — Geographically distributed cache for static assets (images, CSS, JS, video). Requests are served from the edge node nearest to the user, reducing latency from hundreds of ms to single-digit ms. Cache-Control headers control TTL.

7. **Cache invalidation strategies**
   - *TTL-based*: let it expire naturally.
   - *Event-driven*: on DB write, explicitly delete or update the cache key.
   - *Cache stampede prevention*: when a popular key expires, many requests hit DB simultaneously. Fix with a mutex lock (only one request populates the cache) or probabilistic early expiration (start refreshing slightly before TTL).

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Redis | In-memory key-value store; supports strings, hashes, sorted sets, pub/sub |
| Memcached | Simpler in-memory cache; excellent for pure key-value at very high throughput |
| CloudFront (AWS) | CDN with edge caching; integrates with S3 and ALB |
| Varnish | HTTP accelerator / reverse proxy cache; excellent for complex cache logic |
| Cache-Aside pattern | Martin Fowler's canonical description of the pattern |

---

### Day 3 — Database Design & Scaling

**The Problem**
How do you choose between SQL and NoSQL? How do you scale reads and writes when a single database instance can no longer keep up — whether due to query volume, data size, or write throughput?

**Approaches**

1. **SQL vs NoSQL decision framework**
   - Use SQL (PostgreSQL, MySQL) for: structured relational data, complex queries with JOINs, strong ACID guarantees, financial or transactional data.
   - Use NoSQL (MongoDB, Cassandra, DynamoDB) for: flexible or evolving schema, very high write throughput, horizontal scalability by default, time-series or document data.

2. **Read replicas** — Replicate data asynchronously to one or more read-only nodes. Route all SELECT queries to replicas; route writes only to the primary. Scales read throughput linearly with replica count. Replication lag means replicas may serve slightly stale data.

3. **Sharding (horizontal partitioning)** — Split rows across multiple independent DB instances based on a shard key.
   - *Range sharding*: shard by value range (e.g. userIds 1–1M on shard 1). Simple, but can create hot spots.
   - *Hash sharding*: hash the shard key and mod by shard count. Even distribution, but range queries require hitting all shards.
   - *Directory-based sharding*: a lookup table maps each key to its shard. Most flexible but the lookup table becomes a bottleneck.

4. **Vertical partitioning** — Split columns across tables. Put frequently accessed columns in one table, large/rarely accessed blobs in another. Reduces row size, improves cache efficiency.

5. **Connection pooling** — DB connections are expensive to create. A pool (PgBouncer, HikariCP) maintains a fixed set of open connections and hands them to app threads on demand. Prevents connection exhaustion under high concurrency.

6. **Denormalization** — Duplicate data to avoid expensive JOINs at query time. Trade write complexity and storage for read speed. Common in NoSQL designs.

7. **Multi-master replication** — Multiple nodes accept writes and sync to each other. High write availability but conflict resolution is complex (last-write-wins or application-level merge).

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| PostgreSQL | Full-featured open-source SQL; excellent for complex queries and JSONB |
| MySQL | Widely deployed SQL; strong ecosystem; used at Facebook, Twitter scale |
| MongoDB | Document store; flexible schema; horizontal scaling via sharding |
| Apache Cassandra | Wide-column store; masterless; designed for massive write throughput |
| Amazon DynamoDB | Managed NoSQL; single-digit ms at any scale; global tables |
| PgBouncer | Lightweight PostgreSQL connection pooler |

---

### Day 4 — Message Queues & Event-Driven Architecture

**The Problem**
How do you decouple services so a slow or temporarily-down downstream service doesn't break the upstream caller? How do you handle traffic spikes without losing work, and how do you guarantee no message is dropped?

**Approaches**

1. **Queue model (point-to-point)** — One message is consumed by exactly one consumer. The consumer acknowledges the message and it's deleted from the queue. Use for task distribution: image resizing jobs, email sending, payment processing.

2. **Pub/Sub model** — One message is delivered to all subscribers of a topic. Use for event broadcast: "order placed" event goes to inventory service, email service, and analytics simultaneously.

3. **At-least-once delivery** — The queue guarantees the message will be delivered, but may deliver it more than once (on retry after a crash). Consumers must be idempotent — processing the same message twice produces the same result as processing it once.

4. **Exactly-once semantics** — The message is delivered and processed exactly once. Harder to achieve, requires distributed transactions or idempotency keys + deduplication at the broker level (Kafka transactional API).

5. **Dead Letter Queue (DLQ)** — After N failed processing attempts, the message is moved to a DLQ for manual inspection or replay. Prevents a poison message from blocking the whole queue forever.

6. **Backpressure** — Consumer signals producer to slow down when it's overwhelmed. Prevents queue depth from growing unboundedly and causing OOM crashes. Kafka handles this by making the producer block when the buffer is full.

7. **Event sourcing** — Rather than storing current state, store every event that led to the current state as an immutable log. Reconstruct state by replaying events. Enables audit trails, time-travel debugging, and event replay to new consumers.

8. **Ordering guarantees** — Most queues guarantee FIFO within a partition/queue, not globally. Kafka guarantees ordering within a partition. If global ordering matters, use a single partition (limits throughput) or design your consumer to handle out-of-order events.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Apache Kafka | Distributed log; high throughput; persistent; ordered within partition |
| RabbitMQ | AMQP broker; flexible routing; good for task queues |
| Amazon SQS | Managed queue; at-least-once; scales automatically |
| Google Pub/Sub | Managed pub/sub; global; low latency |
| The Log (Jay Kreps) | Foundational essay on logs as a universal data structure |

---

### Day 5 — API Design: REST, GraphQL & gRPC

**The Problem**
How do you design clean, scalable, maintainable interfaces between services and clients? When do you choose REST vs GraphQL vs gRPC, and how do you handle versioning, pagination, and rate limiting?

**Approaches**

1. **REST (Representational State Transfer)** — Stateless, resource-based URLs, HTTP verbs (GET, POST, PUT, DELETE, PATCH). Self-documenting with OpenAPI/Swagger. Best for: public APIs, simple CRUD, browser clients. Downside: over-fetching (response contains more fields than needed) and under-fetching (multiple round trips needed to get related data).

2. **GraphQL** — Client specifies the exact shape of data it needs in a query. Eliminates over-fetching and under-fetching. One endpoint, strongly typed schema. Best for: complex frontends with diverse data needs, mobile clients on slow connections. Downside: complex caching (queries aren't cacheable by URL), N+1 query problem on the server side.

3. **gRPC** — Binary protocol using Protocol Buffers (Protobuf). Strongly typed contracts. Streaming support (server-side, client-side, bidirectional). Very low latency. Best for: internal service-to-service communication, polyglot microservices, real-time streaming. Downside: not human-readable, harder to debug, not natively supported in browsers.

4. **API versioning**
   - *URL versioning*: `/v1/users`, `/v2/users`. Simple, explicit, easy to route. Most common.
   - *Header versioning*: `Accept: application/vnd.api+json;version=2`. Cleaner URLs but harder to test.
   - *Additive changes*: prefer adding new fields over breaking changes. Mark deprecated fields.

5. **Pagination**
   - *Offset-based*: `?page=3&limit=20`. Simple but breaks when data changes between pages (items inserted/deleted). Performance degrades at large offsets.
   - *Cursor-based*: `?cursor=eyJ1c2VySWQiOiAxMjN9`. Stable, consistent, performant at any depth. Use for any list that may change.

6. **Rate limiting on the API gateway** — Protect backend services from abuse. Apply per-user API key, per-IP, or globally. Return `429 Too Many Requests` with a `Retry-After` header. Pair with a burst allowance so legitimate spiky clients aren't penalized.

7. **Idempotent design** — `GET`, `PUT`, and `DELETE` should be idempotent (calling them multiple times has the same effect as calling once). For `POST`, use an idempotency key header so clients can safely retry.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| OpenAPI / Swagger | REST API specification and documentation standard |
| Protocol Buffers | Binary serialization format used by gRPC |
| GraphQL spec | graphql.org — the official spec and documentation |
| Kong / AWS API Gateway | API gateway with rate limiting, auth, routing |
| Postman / Insomnia | API testing and documentation tools |
| gRPC documentation | grpc.io — official guides and language support matrix |

---

### Day 6 — Consistent Hashing

**The Problem**
In a distributed cache or storage cluster, when a node is added or removed, how do you minimize the number of keys that need to be remapped to a new node? With naive modulo hashing (`key % N`), changing N causes almost every key to remap — unacceptable for a live system.

**Approaches**

1. **Naive modulo hashing** — `server = hash(key) % N`. Adding or removing a node changes N, which remaps approximately `(N-1)/N` of all keys — nearly everything. Not viable for dynamic clusters.

2. **Consistent hashing ring** — Map both keys and nodes to positions on a conceptual ring (0 to 2³²). Each key is assigned to the next clockwise node on the ring. Adding a node only displaces keys between the new node and its predecessor — approximately `K/N` keys (where K = total keys, N = node count). Removing a node only remaps that node's keys to its successor.

3. **Virtual nodes (vnodes)** — Each physical node is represented by multiple points on the ring (e.g. 150 virtual nodes per physical node). This improves load distribution dramatically — without vnodes, random placement of a small number of physical nodes creates uneven arcs and hot spots. Vnodes also allow nodes with different hardware capacities to claim proportionally more ring positions.

4. **Lookup mechanism** — To find which node owns a key: compute `hash(key)`, walk clockwise around the ring until you hit a node. In practice, store sorted node positions in a binary search structure for O(log N) lookup.

5. **Replication with consistent hashing** — Store each key on the next R nodes clockwise (replication factor R). This provides fault tolerance: if one node is down, the next node in the ring serves the key.

6. **Hotspot handling** — If one key is accessed extremely frequently (a "hot key"), the single node owning it becomes a bottleneck regardless of consistent hashing. Solution: replicate the hot key to multiple nodes and randomly distribute reads across those copies.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Apache Cassandra | Uses consistent hashing with vnodes for token-based partitioning |
| Amazon DynamoDB | Consistent hashing underpins its partition-based architecture |
| Chord DHT paper | The foundational academic paper on consistent hashing |
| Rendezvous hashing | Alternative to consistent hashing with simpler implementation |
| Amazon S3 | Uses consistent hashing for object placement across storage nodes |

---

### Day 7 — CAP Theorem, ACID, BASE & Consistency Models

**The Problem**
How do distributed systems make trade-offs between consistency, availability, and partition tolerance? When a network partition splits your cluster, which guarantee do you sacrifice — and what does that mean for your users?

**Approaches**

1. **CAP Theorem** — In the presence of a network partition (P), a distributed system can guarantee either Consistency (C) or Availability (A), but not both simultaneously.
   - *CP*: returns an error or waits until the partition heals rather than returning stale data. Example: HBase, Zookeeper, etcd.
   - *AP*: continues serving requests with potentially stale data rather than returning errors. Example: Cassandra, CouchDB, DynamoDB (by default).

2. **ACID (relational DBs)** — Atomicity (all or nothing), Consistency (data always valid), Isolation (concurrent transactions don't interfere), Durability (committed data survives crashes). Provides strong guarantees at the cost of scalability. Used in PostgreSQL, MySQL.

3. **BASE (NoSQL systems)** — Basically Available (responds even if some nodes are down), Soft state (data may change over time even without input), Eventually consistent (data converges to a consistent state given enough time). Trades consistency for availability and performance.

4. **Consistency models spectrum** (from strongest to weakest)
   - *Linearizability (strong)*: once a write is acknowledged, all subsequent reads see it, globally. Very expensive — requires coordination.
   - *Sequential consistency*: operations appear in some global order consistent with program order. Slightly weaker.
   - *Causal consistency*: if operation A causally precedes B, all nodes see A before B. Weaker, but preserves cause-and-effect.
   - *Read-your-writes*: you always see your own writes immediately.
   - *Monotonic reads*: you never see an older version after seeing a newer one.
   - *Eventual consistency (weakest)*: all replicas will eventually converge, but no timing guarantee.

5. **Quorum reads and writes** — With N replicas, require W nodes to acknowledge a write and R nodes to respond to a read. When `W + R > N`, reads and writes overlap, guaranteeing you read at least one node with the latest write.

6. **Conflict resolution** — When two nodes accept conflicting writes during a partition:
   - *Last-write-wins (LWW)*: use timestamps. Simple but loses data.
   - *Vector clocks*: track causality per node. Detect true conflicts vs. concurrent writes.
   - *CRDTs (Conflict-free Replicated Data Types)*: data structures that merge automatically without conflicts (e.g. counters, sets).

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Designing Data-Intensive Applications (DDIA) | The definitive book; chapters on replication and consistency are essential |
| Amazon Dynamo paper (2007) | Foundational paper on eventual consistency and AP design |
| Google Spanner paper (2012) | CP system achieving global consistency with TrueTime |
| Apache Zookeeper | CP coordination service; used for leader election and config |
| Kyle Kingsbury's Jepsen | Tests distributed system consistency claims under real failures |

---

## Week 2 — Core System Design Problems
> The most commonly asked interview problems. Each teaches a distinct pattern you'll reuse everywhere.

---

### Day 8 — Design a URL Shortener (bit.ly / TinyURL)

**The Problem**
Given a long URL, generate a short 6–8 character alias. Redirects must be sub-50ms. Support 100 million URLs stored and 10 billion redirects per day. The system is extremely read-heavy (~1000:1 read-to-write ratio).

**Approaches**

1. **Encoding strategy — Base62** — Use characters a–z, A–Z, 0–9 (62 characters). A 7-character code gives 62⁷ ≈ 3.5 trillion unique URLs — effectively unlimited. Base62 is URL-safe with no special characters to escape.

2. **Counter-based ID generation** — Use a monotonically incrementing ID from the DB (or a distributed ID service like Snowflake), then Base62-encode it. Simple, no collision risk. Downside: sequential IDs are predictable — users can enumerate all short URLs by incrementing the code.

3. **Random hash approach** — Compute MD5 or SHA-256 of the long URL, take the first 7 characters of the Base62-encoded hash. Check for collisions in the DB (rare but possible). Advantage: same long URL always generates the same short URL (no duplicates).

4. **Bloom filter for existence checks** — Before every insert, check a Bloom filter (probabilistic, memory-efficient) to quickly determine if a short URL already exists, before hitting the DB. Reduces unnecessary DB reads.

5. **Redirect type**
   - *302 (Temporary Redirect)*: browser doesn't cache the redirect; every visit hits your service. Allows analytics tracking and future URL changes.
   - *301 (Permanent Redirect)*: browser caches it; subsequent visits never hit your service. Less infrastructure load but no analytics, no ability to change the destination.

6. **Caching for reads** — The top 20% of short URLs account for ~80% of traffic (Pareto distribution). Cache these hot URLs in Redis with a TTL. Cache hit rate of 80%+ is achievable, reducing DB load dramatically.

7. **Sharding** — Shard the URL database by the short URL hash across multiple DB nodes. Each shard handles a subset of the keyspace. Use consistent hashing for even distribution.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Redis | Cache for hot URL lookups; sub-millisecond reads |
| Apache Cassandra | Distributed storage for URL mappings; handles high write throughput |
| Snowflake ID | Twitter's distributed unique ID generation algorithm |
| Base62 encoding | Standard algorithm for generating URL-safe short codes |
| Bloom filter | Probabilistic structure for fast existence checks |

---

### Day 9 — Design a Rate Limiter

**The Problem**
Protect your API from abuse, ensure fair usage, and prevent a single client from overwhelming your backend. Allow N requests per user per time window. The limiter must work correctly across a distributed fleet of servers.

**Approaches**

1. **Fixed window counter** — Divide time into fixed N-second windows. Count requests per user per window. Simple and memory-efficient, but vulnerable to boundary bursts: a user can make 2× their limit by sending N requests just before and just after a window boundary.

2. **Sliding window log** — Store the timestamp of every request in a sorted log. Count entries in the last N seconds. Perfectly accurate, no boundary burst problem. Downside: memory-intensive — you store one entry per request per user.

3. **Sliding window counter (hybrid)** — Blend the current window count and the previous window count weighted by how far through the current window you are. Approximates a true sliding window with O(1) memory. The practical choice for most systems.

4. **Token bucket** — Each user has a bucket of tokens (capacity = burst limit) that refills at a fixed rate. Each request consumes one token. If the bucket is empty, the request is rejected. Allows controlled bursting up to bucket capacity. Used by AWS API Gateway.

5. **Leaky bucket** — Requests enter a queue (the "bucket") and are processed at a constant outflow rate. Smooths bursty traffic into a steady stream. Good for protecting downstream services from spikes. If the bucket is full, requests are dropped.

6. **Centralized rate limiting with Redis** — Use Redis `INCR` and `EXPIRE` commands. All servers check the same Redis counter, guaranteeing consistency across the fleet. A Lua script makes the check-and-increment atomic. The tradeoff: Redis becomes a dependency — if it's slow, every API call is slow.

7. **Distributed / local rate limiting** — Each server tracks its own counters locally. Fast, no Redis dependency, but requires gossiping state across servers for exact enforcement. Practical for "soft" limits where approximate correctness is acceptable.

8. **Rate limit headers** — Return `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` headers so clients can self-throttle. Return `429 Too Many Requests` with a `Retry-After` header on rejection.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Redis (INCR + Lua) | Atomic counter for centralized rate limiting |
| NGINX rate limiting module | Built-in leaky bucket rate limiting for HTTP servers |
| Stripe's rate limiter blog post | Excellent real-world case study on sliding window implementation |
| AWS API Gateway throttling | Token bucket implementation at cloud scale |
| Cloudflare rate limiting | Edge-level rate limiting with distributed enforcement |

---

### Day 10 — Design a Notification System

**The Problem**
Send push notifications, emails, and SMS to up to 1 billion users. Notifications are triggered by various product events. They must be delivered reliably, must not overwhelm recipient devices, and must support priority tiers (security alerts vs. promotional messages).

**Approaches**

1. **Decoupled notification service** — The notification system is a separate service that consumes events from upstream services via a message queue. Upstream services publish events ("order placed", "friend request"); the notification service translates events into device-appropriate messages. This decoupling prevents notification logic from polluting product code.

2. **Event-driven pipeline** — Upstream service → Kafka topic → notification worker → channel-specific dispatcher (APNs / FCM / SMTP / SMS). Each stage is independently scalable.

3. **Fan-out on write (push model)** — When an event occurs, immediately push a notification record into each recipient's notification queue or inbox. Fast reads, but expensive for high-fan-out events (e.g. a celebrity with 50M followers posting something triggers 50M writes).

4. **Fan-out on read (pull model)** — Store events centrally. Each user's notification inbox is computed at read time by merging events from accounts they follow. Cheap writes, expensive reads. Better for celebrities / high-fan-out accounts.

5. **Hybrid model** — Use push (fan-out on write) for users with < N followers. Use pull (fan-out on read) for celebrities with > N followers. Blend results at read time.

6. **Priority queues** — Separate Kafka topics or SQS queues for different priority tiers: critical (security alerts, 2FA codes) processed immediately, normal (social notifications) processed within seconds, promotional (marketing emails) processed in batch.

7. **Channel dispatchers**
   - *iOS push*: Apple Push Notification Service (APNs)
   - *Android push*: Firebase Cloud Messaging (FCM)
   - *Email*: SendGrid, Mailgun, AWS SES
   - *SMS*: Twilio, AWS SNS

8. **Delivery tracking and retries** — Track delivery status per notification. Retry with exponential backoff on transient failures. After N retries, move to a DLQ for investigation.

9. **User preferences and opt-outs** — Store per-user, per-channel, per-notification-type preferences. Check preferences before dispatching. Respect do-not-disturb hours and notification throttling (don't send 100 notifications in a minute).

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Apache Kafka | Event backbone for decoupled notification pipeline |
| Apple APNs | Push notification delivery for iOS devices |
| Firebase FCM | Push notification delivery for Android and web |
| Amazon SQS + SNS | Managed queue and fan-out for notification pipelines |
| Twilio | SMS and voice notification delivery |
| SendGrid / AWS SES | Transactional and bulk email delivery |

---

### Day 11 — Design a Key-Value Store

**The Problem**
Build a distributed key-value store like Redis or DynamoDB. Support `get(key)`, `put(key, value)`, and `delete(key)`. Handle millions of operations per second, terabytes of data, and node failures without downtime or data loss.

**Approaches**

1. **Consistent hashing for key distribution** — Use a consistent hashing ring to map keys to nodes. Adding or removing a node only remaps a fraction of keys, enabling live resizing of the cluster.

2. **Replication for fault tolerance** — Store each key on the next N nodes clockwise on the ring (replication factor N, typically 3). If one node fails, the other replicas serve reads and accept writes.

3. **Quorum-based consistency** — With N replicas, require W write acknowledgments and R read responses. When `W + R > N`, you're guaranteed to read at least one node with the latest write. Tunable: `W=1, R=N` (fast writes, slow reads) vs `W=N, R=1` (slow writes, fast reads).

4. **Gossip protocol** — Each node periodically selects random peers and exchanges state (which nodes are up, their load, their data versions). No central coordinator needed — the cluster achieves eventual global knowledge through local exchanges.

5. **Vector clocks for conflict detection** — Each write is tagged with a vector clock (a per-node logical timestamp). When two nodes merge conflicting writes, vector clocks reveal causality: if neither version dominates the other, it's a true conflict requiring resolution.

6. **Merkle trees for anti-entropy** — Each node maintains a Merkle tree (hash tree) of its data ranges. Comparing Merkle trees between two nodes quickly identifies which key ranges differ, enabling efficient data sync without transferring all data.

7. **LSM-tree storage engine** — Writes go to an in-memory table (MemTable) and an append-only write-ahead log (WAL). When MemTable is full, it's flushed to disk as an immutable SSTable. Reads check MemTable first, then SSTables. Periodically compacted to reclaim space. Optimized for write-heavy workloads. Used by LevelDB, RocksDB, Cassandra.

8. **Sloppy quorum and hinted handoff** — If a target replica is temporarily down, write to the next available node with a "hint" that it should forward the write when the original node recovers. Improves availability during partial failures.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Amazon Dynamo paper (2007) | The foundational reference for distributed key-value store design |
| Apache Cassandra | Production distributed KV store using these exact mechanisms |
| LevelDB / RocksDB | LSM-tree storage engines widely used in DB internals |
| etcd | Strongly consistent KV store based on Raft consensus |
| Redis Cluster | Sharded, replicated Redis with automatic failover |

---

### Day 12 — Design a Web Crawler

**The Problem**
Build a system that autonomously discovers and downloads web pages at scale. Must handle billions of pages, detect duplicate URLs, respect per-domain crawl politeness rules, and prioritize high-value pages.

**Approaches**

1. **BFS frontier queue** — Use Breadth-First Search to explore the web graph. Start with seed URLs; extract links from each page and add new URLs to the frontier. BFS ensures broad coverage before going deep into any one site.

2. **Priority queue for page ordering** — Score pages by estimated value (PageRank, link count, freshness) and crawl higher-value pages first. Use a min-heap or sorted queue.

3. **URL frontier with politeness** — Maintain separate queues per host domain. A politeness scheduler ensures a minimum delay between consecutive requests to the same host (typically 1–10 seconds), respecting the server and robots.txt crawl-delay directives.

4. **Bloom filter for URL deduplication** — Before adding a URL to the frontier, check a Bloom filter to see if it's already been seen. False positive rate of ~1% is acceptable (some URLs are skipped). Far more memory-efficient than a hash set at billion-URL scale.

5. **Consistent hashing for URL assignment** — Hash each URL's domain to assign it to a specific crawler worker node. All pages from a domain are handled by the same worker, simplifying per-domain politeness enforcement and robots.txt caching.

6. **Robots.txt compliance** — Fetch `https://domain.com/robots.txt` once per domain and cache it. Respect `Disallow` rules and `Crawl-delay`. Refresh the cache periodically.

7. **DNS caching** — DNS lookups are slow and many pages share the same domain. Cache resolved IP addresses per domain (respect TTL). Significantly reduces DNS query volume.

8. **Content deduplication** — After downloading a page, compute a fingerprint (SimHash or exact hash). Compare against seen fingerprints to detect duplicate or near-duplicate content. Skip storing duplicates.

9. **Distributed storage** — Store crawled HTML in distributed object storage (HDFS, S3). Store URL metadata (crawl time, status, fingerprint) in a distributed DB.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Mercator (AltaVista) paper | Classic academic reference for web crawler architecture |
| Apache Kafka | Durable queue for URL frontier and crawl job distribution |
| Apache Hadoop HDFS | Distributed storage for raw crawled content |
| SimHash | Near-duplicate page detection algorithm (used by Google) |
| Bloom filter | Memory-efficient probabilistic URL deduplication |

---

### Day 13 — Design a Chat System (WhatsApp / Slack)

**The Problem**
Build a real-time messaging system supporting 1-on-1 and group chats. Messages must deliver in under 100ms for online users and be queued for offline users. Scale to 1 billion users with hundreds of millions of concurrent connections.

**Approaches**

1. **WebSocket for real-time bidirectional communication** — HTTP is request-response and can't push messages to clients. WebSocket establishes a persistent, full-duplex TCP connection. The server can push messages to the client at any time. Each online user holds one open WebSocket connection to a chat server.

2. **Long polling as a fallback** — In environments where WebSocket is blocked (some corporate firewalls), the client repeatedly sends an HTTP request that the server holds open until a message arrives, then responds. Higher latency and overhead than WebSocket.

3. **Chat server architecture** — A fleet of chat servers, each holding open WebSocket connections for a subset of online users. A connection registry (Redis) maps userId → chatServerId so the system knows which server holds a given user's connection.

4. **Message delivery flow**
   - Sender → their chat server → connection registry lookup → recipient's chat server → WebSocket push to recipient.
   - If recipient is offline: store message in message store + push to notification service.

5. **Offline message delivery** — Store undelivered messages in a durable message queue or DB. On reconnect, the client requests unread messages since last seen ID (cursor-based). Deliver them in order.

6. **Message storage — Cassandra** — Chat data is append-heavy, time-series, and query patterns are always "get last N messages for conversation X." Cassandra's wide-column model with partition key = conversationId and clustering key = timestamp is a natural fit.

7. **Message ordering** — Use a sequence ID per conversation (a Snowflake-style ID that encodes timestamp + node). Clients render messages by sequence ID, not by receipt time.

8. **Group chat fan-out** — When a message is sent to a group of N members, create N delivery records (one per member). Large groups (> ~500 members) use async fan-out via a queue rather than synchronous fan-out.

9. **Read receipts** — Delivered status: chat server confirms receipt via WebSocket ACK. Read status: client sends a read event back to the server, which fans out the "read by X" status to other conversation participants.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| WebSocket protocol (RFC 6455) | The standard for persistent browser-server connections |
| Apache Cassandra | Message storage; optimized for time-series append workloads |
| Redis | Connection registry (userId → chatServerId); pub/sub for server-to-server |
| Apache Kafka | Async fan-out for large group messages and offline delivery |
| HBase | Alternative to Cassandra for message storage (used by Facebook Messenger) |

---

### Day 14 — Design a News Feed (Twitter / Facebook)

**The Problem**
Each user sees a ranked, personalized feed of posts from accounts they follow. The feed must load in under 200ms. Handle celebrities with 100 million followers (high fan-out) alongside normal users without the system collapsing on write.

**Approaches**

1. **Fan-out on write (push model)** — When a user posts, immediately write the post ID to every follower's feed cache. Feed reads are cheap — just retrieve the pre-computed list. Write amplification is the cost: a post by a celebrity triggers 100M writes.

2. **Fan-out on read (pull model)** — When a user opens their feed, fetch recent posts from all accounts they follow and merge them. No write amplification, but reads are expensive — N followees means N DB queries at read time. Unacceptable at scale for users who follow thousands of accounts.

3. **Hybrid model (the practical answer)** — Fan-out on write for regular users (< ~10K followers). Fan-out on read for celebrities (> ~10K followers). At feed-read time, merge the pre-computed feed with live-fetched celebrity posts. This keeps write amplification bounded while keeping reads fast.

4. **Feed cache — Redis sorted set** — Store each user's feed as a Redis sorted set where score = post timestamp (or relevance score). `ZRANGE` retrieves the top N posts in O(log N + N). Trim to the most recent ~1000 posts to bound memory usage.

5. **Cursor-based pagination** — The client passes the ID of the last-seen post to get the next page. Stable regardless of new posts being inserted above it. Never use offset pagination for feeds — it breaks as new posts shift offsets.

6. **Media storage** — Images and videos are stored in distributed object storage (S3) and served via CDN. The post record contains only the asset URL. Video thumbnails are generated asynchronously after upload.

7. **Ranking / relevance** — The raw chronological feed is re-scored by an ML model at read time based on engagement signals, relationship strength, and content type. This re-ranking is applied on the cached feed to avoid a full DB scan.

8. **Write path** — Post creation → save to post DB → publish to Kafka → feed workers consume and fan out to follower feed caches.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Redis sorted sets | Feed cache with efficient range queries by score |
| Apache Kafka | Fan-out pipeline from post creation to feed workers |
| Apache Cassandra | Post storage and user graph (who follows whom) |
| Elasticsearch | Full-text search over posts |
| How Twitter built its timelines | Twitter engineering blog posts on the hybrid fan-out model |

---

## Week 3 — Advanced & Ambiguous Problems
> Complex, open-ended problems that test trade-off reasoning and principal-engineer thinking.

---

### Day 15 — Design a Video Streaming Platform (YouTube / Netflix)

**The Problem**
Upload, transcode, store, and stream video to 2 billion users at up to 4K resolution. 500 hours of video are uploaded every minute. Serve 1 billion hours of watched video daily. Minimize rebuffering; adapt to varying network conditions.

**Approaches**

1. **Chunked upload to blob storage** — Client splits the video file into chunks (e.g. 5MB each) and uploads them via a resumable upload API directly to object storage (S3). If the upload is interrupted, only the missing chunks need to be re-sent. The API returns a pre-signed S3 URL so the client uploads directly, bypassing your app servers.

2. **Transcoding pipeline** — After upload, a message is published to a Kafka topic. A fleet of transcoding workers consumes jobs and uses FFmpeg to re-encode the video into multiple resolutions (360p, 720p, 1080p, 4K) and multiple formats (H.264, VP9, AV1). Workers run in parallel — one resolution per worker per video. The output is a set of short video segments (2–10 seconds each).

3. **Adaptive Bitrate Streaming (ABR)** — Instead of one large video file, the client downloads a sequence of short segments. The player maintains a quality-selection algorithm (e.g. Netflix's BOLA) that picks the resolution based on current download speed and buffer state. Protocols: HLS (Apple) and MPEG-DASH (standard).

4. **CDN for video delivery** — Video segments are stored in object storage and cached at CDN edge nodes globally. The client's player fetches segments from the nearest edge node. For long-tail content, the CDN fetches from origin on first request and caches for subsequent viewers.

5. **Metadata service** — Video metadata (title, description, tags, view count, uploader) is stored in a relational DB (MySQL). Separate from the video binary. Queried at page load time.

6. **Thumbnail generation** — Async job triggered after transcoding. Extract N frames, store in S3, update metadata DB with thumbnail URLs.

7. **View count — approximate at scale** — Exact per-view DB writes would overwhelm the DB. Use HyperLogLog in Redis for approximate unique viewer counts. Reconcile against exact counts in a batch job every hour.

8. **Search and recommendations** — Videos are indexed in Elasticsearch for full-text search. A recommendation engine (typically a two-tower neural network) serves personalized video suggestions based on watch history and engagement signals.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| FFmpeg | Open-source video transcoding library |
| HLS / MPEG-DASH | Adaptive bitrate streaming protocols |
| Amazon S3 | Object storage for video segments and thumbnails |
| Amazon CloudFront | CDN for global video delivery |
| Apache Kafka | Transcoding job queue and event pipeline |
| HyperLogLog | Probabilistic cardinality estimation for view counts |

---

### Day 16 — Design a Search Typeahead

**The Problem**
Return the top 5 search suggestions as the user types, in under 100ms, for billions of queries per day. Suggestions must reflect trending queries and be personalized to the user's history.

**Approaches**

1. **Trie data structure** — A prefix tree where each node represents a character. Traversal from root to a node spells out a prefix. At each node, store the top-K completions for that prefix (pre-computed). Lookup is O(L) where L = prefix length. The canonical data structure for typeahead.

2. **Trie sharding** — A full trie for all queries is too large for one machine (billions of entries). Shard the trie by prefix hash: queries starting with "a"–"f" go to shard 1, etc. Each shard fits in memory.

3. **Pre-computed top-K per node** — Rather than traversing the entire subtree at query time to find the top-K suggestions, pre-compute and store the top-K completions at every node in the trie during the offline trie rebuild. Read is then O(1) after traversal.

4. **Query log aggregation pipeline** — Collect all search queries → aggregate counts per query string in a Spark batch job (daily or hourly) → rebuild the trie with updated frequencies → hot-swap the trie in the serving layer with zero downtime.

5. **Redis cache for top prefixes** — The most common prefixes (e.g. single characters, two-character combinations) are queried billions of times. Cache the top-K suggestions for these prefixes in Redis for sub-millisecond response.

6. **Personalization layer** — After fetching global top-K suggestions, blend with the user's own search history (weighted by recency). This re-ranks or replaces suggestions based on personal relevance.

7. **Client-side optimizations** — Debounce: wait 150–300ms after the user stops typing before sending a request (avoids a request per keystroke). Client-side cache: cache results for prefixes already typed in the current session.

8. **Elasticsearch as an alternative** — For many teams, Elasticsearch's built-in `prefix` and `completion` suggester queries are fast enough and eliminate the need to build a custom trie. Trades raw performance for operational simplicity.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Trie (prefix tree) | Core data structure for typeahead; O(L) prefix lookup |
| Redis | Cache for top-prefix suggestions |
| Apache Spark | Batch aggregation of query logs to compute top-K per prefix |
| Elasticsearch completion suggester | Managed alternative to a custom trie |
| Apache Kafka | Streaming query log ingestion pipeline |

---

### Day 17 — Design a Ride-Sharing Service (Uber / Lyft)

**The Problem**
Match riders to nearby available drivers in under 2 minutes. Handle real-time GPS location updates from millions of active drivers. Compute surge pricing in high-demand areas. Coordinate the full trip lifecycle from request to payment.

**Approaches**

1. **Geospatial indexing — Geohash** — Divide the Earth's surface into a hierarchical grid. Each cell is identified by a short alphanumeric string (a Geohash). Two locations with similar Geohashes are geographically close. Encode driver locations as Geohashes and index them for fast spatial queries.

2. **S2 geometry library (alternative)** — Google's S2 library divides the sphere into hierarchical cells using a space-filling curve. More uniform cell sizes than Geohash. Used by Uber internally. More accurate for edge cases near cell boundaries.

3. **Driver location service with Redis** — Drivers publish a GPS ping every 4 seconds to a location service. The service stores `GEOADD key longitude latitude driverId` in Redis. When a rider requests a trip, `GEORADIUS` returns all drivers within a given radius in order of distance. Scales to millions of drivers.

4. **Trip matching flow** — Rider requests trip → query Redis for nearby available drivers within R km → rank by distance + ETA + acceptance rate → offer trip to top driver → if declined, expand radius and retry → assign when driver accepts.

5. **Consistent hashing for location servers** — Shard the map into geographic regions and assign each region to a location server using consistent hashing. All drivers in a region connect to the same server, enabling efficient proximity queries.

6. **Surge pricing** — A pricing service monitors supply (available drivers) and demand (open trip requests) per Geohash cell in real time. When demand/supply ratio exceeds a threshold in a cell, the multiplier increases. Computed as a batch job every 60 seconds and cached.

7. **Trip state machine** — Each trip transitions through states: `REQUESTED → MATCHED → DRIVER_EN_ROUTE → DRIVER_ARRIVED → IN_PROGRESS → COMPLETED → PAID`. Store state in Cassandra with event timestamps.

8. **ETA computation** — Use a routing engine (OSRM, Google Maps API) to compute ETA from driver's current location to rider. Pre-warm a road graph cache for the most common routes in each city.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Geohash | Hierarchical geospatial encoding for proximity queries |
| Google S2 library | Sphere-based hierarchical cell geometry (more uniform than Geohash) |
| Redis GEO commands | GEOADD / GEORADIUS for real-time driver location storage |
| Apache Kafka | GPS event stream from driver apps |
| Apache Cassandra | Trip lifecycle event storage |
| OSRM | Open-source routing engine for ETA computation |

---

### Day 18 — Design Cloud File Storage (Dropbox / Google Drive)

**The Problem**
Store, sync, and share files across devices. Handle files up to 50GB. Minimize bandwidth by syncing only changed portions of files. Support versioning, conflict resolution, and collaborative access.

**Approaches**

1. **Block-based storage** — Split each file into fixed-size blocks (e.g. 4MB). Store blocks by content hash (content-addressable storage). A file is represented as an ordered list of block hashes. Identical blocks across different files are stored only once (deduplication).

2. **Delta sync (chunked upload)** — On file change, the client computes block hashes locally and sends only the list to the server. The server responds with which block hashes it's missing. The client uploads only those blocks. For a small edit to a large file, this can reduce upload size from gigabytes to kilobytes.

3. **Client-side deduplication** — Before uploading any block, the client checks if the server already has it (by hash). If so, the upload is skipped entirely. This means "uploading" a file someone else already uploaded takes ~0 bytes of bandwidth.

4. **Metadata service** — A relational DB (MySQL) stores the file tree: file name, owner, permissions, version history, and the ordered list of block hashes for each file version. Separating metadata from blocks allows fast directory listing without touching object storage.

5. **Conflict resolution**
   - *Last-write-wins*: simplest, but silently discards one version.
   - *Conflict copy (Dropbox approach)*: if two devices edit the same file while offline, the second sync creates a "conflicted copy" alongside the original. The user resolves manually.
   - *Operational transform / CRDT*: for collaborative real-time editing (Google Docs style), more complex but enables merge without user intervention.

6. **Change notification** — Long polling or WebSocket connection from each client to a metadata service. When a file changes (on another device or a collaborator's edit), the server pushes a notification. The client fetches the updated block list and downloads only changed blocks.

7. **Object storage for blocks** — Blocks are stored in S3-compatible object storage, keyed by their content hash. Immutable — blocks are never updated, only new blocks are added. Old blocks from deleted versions are garbage-collected asynchronously.

8. **CDN for frequently accessed files** — Hot files (recently shared publicly or accessed by many users) are cached at CDN edge nodes to reduce origin storage costs and improve download speed.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Amazon S3 | Object storage for content-addressable block storage |
| Content-addressable storage | The pattern of keying data by its hash |
| rsync algorithm | The classic delta-sync algorithm (conceptual foundation) |
| MySQL | Metadata service for file trees, versioning, permissions |
| Redis / WebSocket | Change notification channel for real-time sync |

---

### Day 19 — Design a Distributed Cache

**The Problem**
Build a production-grade distributed cache (like Redis Cluster or Memcached). Handle hot keys, cache stampedes, thundering herd, and node failures without data loss or availability gaps.

**Approaches**

1. **Consistent hashing with vnodes** — Distribute keys across cache nodes using a consistent hashing ring. Use virtual nodes (100–200 per physical node) for even distribution. Adding/removing a node remaps only ~K/N keys.

2. **Hot key problem** — A single extremely popular key (a trending post, a product launch page) routes all its traffic to one cache node, creating a hotspot. Solutions: (a) detect hot keys and replicate them across multiple nodes, routing reads randomly; (b) client-side in-process caching for a short TTL (milliseconds); (c) local shard within the cache node using multiple data structures.

3. **Cache stampede (thundering herd on expiry)** — When a popular key expires, many concurrent requests miss the cache and simultaneously hit the DB. Solutions: (a) mutex lock — the first miss acquires a lock and populates the cache; others wait. (b) Probabilistic early expiration — each read has a small chance of proactively refreshing before TTL expires, spreading the refresh load over time.

4. **Thundering herd on node restart** — When a cache node restarts empty, all its traffic falls through to the DB simultaneously. Solution: stagger TTLs using random jitter (TTL = base ± random_offset) so keys don't all expire at the same moment.

5. **Write strategies**
   - *Write-through*: write to cache and DB synchronously. Cache always warm, but double latency per write.
   - *Write-behind*: write to cache immediately, flush to DB asynchronously. Fast writes, small risk of data loss on crash.
   - *Write-around*: write to DB only; cache is populated on the next read miss. Simple, good for write-once data.

6. **Memory management — slab allocator (Memcached)** — Pre-allocate memory in fixed-size classes (slabs). Each class handles objects of a specific size range. Avoids heap fragmentation. Objects that don't fit the assigned slab size waste some memory but never fragment.

7. **Replication and failover** — Each cache shard has a primary and one or more replicas. Replicas receive async writes from the primary. On primary failure, Redis Sentinel or Cluster automatically promotes a replica and updates routing tables. Clients reconnect transparently.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Redis Cluster | Sharded + replicated Redis with automatic failover |
| Redis Sentinel | High-availability monitoring and automatic failover for non-cluster Redis |
| Memcached | High-throughput multi-threaded cache; simpler than Redis |
| Consistent hashing | Key distribution algorithm across cache nodes |
| XFetch algorithm | Probabilistic early expiration algorithm for cache stampede prevention |

---

### Day 20 — Design a Payment System / Stock Exchange

**The Problem**
Process financial transactions or match buy/sell orders where correctness is non-negotiable. Money cannot be lost, double-spent, or created from nowhere. The system must be auditable and recoverable from any failure state.

**Approaches**

1. **Idempotency keys** — Every payment request includes a unique client-generated idempotency key. The server stores (key → result) permanently. If the same key is submitted again (due to a network retry), return the stored result instead of processing twice. Prevents double charges from client retries.

2. **Two-phase commit (2PC)** — For operations that span multiple services (debit account A, credit account B), 2PC coordinates an atomic outcome: Phase 1 (Prepare) — all participants lock resources and vote yes/no; Phase 2 (Commit) — if all voted yes, commit; else rollback. Strongly consistent but blocking — a failed coordinator leaves participants locked indefinitely.

3. **Saga pattern** — Break a distributed transaction into a sequence of local transactions, each publishing an event. If a step fails, execute compensating transactions to undo previous steps (e.g. refund a charge if fulfillment fails). Eventual consistency with explicit rollback logic. Better availability than 2PC.

4. **Event sourcing** — Never update or delete records. Store every state transition as an immutable event (e.g. `PAYMENT_INITIATED`, `PAYMENT_AUTHORIZED`, `PAYMENT_CAPTURED`). Reconstruct current state by replaying the event log. Provides a complete, auditable history and enables replay to debug or recover from errors.

5. **CQRS (Command Query Responsibility Segregation)** — Separate the write model (commands that change state) from the read model (queries for reporting/balance display). The write model is event-sourced and strongly consistent; the read model is an eventually consistent materialized view optimized for queries.

6. **Order book (for exchange design)** — Two sorted data structures: a max-heap of buy orders (bids) sorted by price descending, and a min-heap of sell orders (asks) sorted by price ascending. When the best bid ≥ best ask, a match is made. The matching engine runs single-threaded to avoid race conditions on the order book state.

7. **Sequencer service** — Assign a monotonically increasing, gap-free sequence number to every order or transaction. Ensures a total ordering of events. Implemented as a single-writer service (the bottleneck is managed by making it fast, not distributed).

8. **Serializable isolation** — Financial transactions require the highest isolation level — serializable — to prevent anomalies like phantom reads or write skew. PostgreSQL's SSI (Serializable Snapshot Isolation) provides this without locking.

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Stripe's idempotency keys documentation | Canonical real-world implementation of idempotency |
| Apache Kafka | Immutable event log for event sourcing |
| Saga pattern (Chris Richardson) | Microservices.io — definitive guide to sagas |
| CQRS pattern (Martin Fowler) | Command-query separation for read/write scalability |
| PostgreSQL SSI | Serializable snapshot isolation without locking |
| LMAX Disruptor | High-performance single-threaded order matching via ring buffer |

---

### Day 21 — Mock Interview + Trade-off Mastery

**The Problem**
Consolidate all 20 days of knowledge into a coherent interview framework. Practice starting from a blank whiteboard, asking the right questions, making trade-offs explicit, and communicating design decisions under time pressure.

**Approaches**

1. **The interview framework (45 minutes)**
   - *0–5 min*: Requirements — ask clarifying questions. What are the functional requirements (what it must do)? What are the non-functional requirements (scale, latency SLA, consistency needs, availability target)?
   - *5–10 min*: Capacity estimation — back-of-envelope math. QPS, storage per year, bandwidth. Ballpark in 2 minutes; don't overfit.
   - *10–15 min*: API design — define the core endpoints or interfaces. The API surfaces the functional requirements.
   - *15–30 min*: High-level design — draw the main components and data flow. Cover the happy path end to end.
   - *30–42 min*: Deep dive — go deep on 2–3 components. This is where you prove expertise. Cover the data model, bottlenecks, failure modes.
   - *42–45 min*: Trade-offs — explicitly call out what you chose and what you sacrificed. This is what separates senior from mid-level.

2. **How to frame trade-offs** — For every major decision, name the alternatives and explain why you chose what you did given the constraints. Use the format: "I chose X over Y because at this scale, [the bottleneck / constraint] means [Y's cost] outweighs [Y's benefit]."

3. **Back-of-envelope estimation patterns**
   - 1M DAU × 10 actions/day = 10M events/day ≈ 115 QPS
   - 1KB per event × 10M events = 10GB/day, 3.6TB/year
   - Peak QPS ≈ 2–3× average QPS
   - Read:write ratio varies enormously — always ask or estimate from use case

4. **Know when NOT to use a technology** — Kafka for a queue with 100 messages/day is overkill. A single Redis node for session storage handling 10 QPS doesn't need clustering. Over-engineering is a red flag. Match tool complexity to scale.

5. **Common failure modes to address proactively** — Single point of failure (always ask: what happens if this component dies?), cascading failures (circuit breakers, bulkheads), thundering herd (cache stampede, retry jitter), data skew (hot partitions in sharded systems).

6. **Trade-off vocabulary to internalize**
   - Latency vs consistency (CAP, quorum)
   - Read performance vs write performance (indexes, denormalization)
   - Cost vs scalability (managed services vs self-hosted)
   - Simplicity vs resilience (single DB vs distributed system)
   - Strong consistency vs availability (CP vs AP)

7. **Practice problems for Day 21 mock sessions**
   - Design a hotel booking system (focus: concurrent seat/room reservation without double booking)
   - Design a distributed job scheduler (focus: exactly-once execution, failure recovery)
   - Design a leaderboard for a mobile game (focus: real-time ranking, score updates at scale)
   - Design a content moderation pipeline (focus: async ML inference, human review queue)

**Key Tools & References**

| Tool / Reference | What it's for |
|---|---|
| Grokking the System Design Interview | Most widely used structured system design course |
| ByteByteGo (Alex Xu) | System Design Interview Vol. 1 & 2; excellent diagrams and frameworks |
| Designing Data-Intensive Applications (DDIA) | The deepest technical reference for storage, replication, consistency |
| Exponent mock interviews | Practice with real interviewers from top companies |
| High Scalability blog | Real-world architecture deep dives from companies at scale |
| The Morning Paper | Academic paper summaries (Dynamo, Spanner, Kafka, etc.) |

---

## Quick Reference — Master Trade-off Table

| Decision | Option A | Option B | When to pick A | When to pick B |
|---|---|---|---|---|
| Scaling | Vertical | Horizontal | Small scale, quick fix | Sustained load, fault tolerance needed |
| Load balancing | L4 | L7 | Non-HTTP, latency-critical | HTTP, content routing needed |
| Consistency | Strong (CP) | Eventual (AP) | Financial data, user identity | Social features, analytics, feeds |
| Storage | SQL | NoSQL | Relational, ACID needed | High write throughput, flexible schema |
| Read scaling | Cache | Read replicas | Repeat reads of same data | Diverse query patterns |
| Messaging | Queue | Pub/Sub | One consumer per task | Multiple consumers per event |
| Session storage | In-memory | External (Redis) | Single server (dev) | Distributed, horizontal scale |
| Fan-out | On write | On read | Regular users, fast reads | Celebrities, high fan-out |
| Sync protocol | WebSocket | Long polling | Real-time, bidirectional | Constrained environments |
| Pagination | Cursor | Offset | Mutable feeds, large datasets | Simple, stable datasets |

---

*Generated for the 21-Day System Design Expert Track. Study one day at a time. Draw every diagram on paper before reading the answer.*
