# Day 1 — Scalability & Load Balancing

---

## 1. Day Summary

Day 1 is about the single most fundamental mental model shift in system design: **you cannot scale a system by making a single machine bigger forever**. The ceiling exists, and it's low. This day forces you to internalize horizontal scaling as the default posture — and to understand that horizontal scaling is not free. It demands statelessness, intelligent traffic distribution, and health-aware routing. Every concept from Day 2 onward (caching, sharding, message queues, replication) assumes you already think in terms of a fleet of machines behind a load balancer, not a single monolith. If you can't articulate the difference between L4 and L7 balancing, explain why app servers must be stateless, or design a high-availability LB pair from memory, you are not ready for any subsequent day.

---

## 2. Pre-read Checklist

This is Day 1 — there are no prior days. However, you must be fluent in the following foundational concepts:

- **TCP/IP networking basics** — how a TCP connection is established (3-way handshake), what IP addresses and ports represent, the difference between transport-layer (L4) and application-layer (L7) in the OSI model.
- **HTTP request/response lifecycle** — methods (GET, POST, PUT, DELETE), status codes (200, 301, 302, 429, 500, 503), headers (Host, Connection, Cookie).
- **TLS/SSL fundamentals** — what happens during a TLS handshake, why it's CPU-intensive, what a certificate chain is.
- **Basic hashing** — what a hash function does, properties of a good hash (uniform distribution, deterministic), concept of modulo.
- **Client-server architecture** — the model where clients initiate requests and servers respond.

---

## 3. The Problem, Stated Precisely

### Functional Requirements

| Requirement | Detail |
|---|---|
| Serve HTTP requests from web/mobile clients | GET, POST, PUT, DELETE against a REST API |
| Route incoming traffic to a pool of backend servers | No single server handles all traffic |
| Detect unhealthy servers and stop routing to them | Health checks, automatic removal and re-admission |
| Support session continuity for stateful clients | Even though servers are stateless, users should not lose session context |

### Non-Functional Requirements

| Metric | Target |
|---|---|
| Total QPS (queries per second) | 100,000 sustained, 300,000 peak |
| p99 latency (end-to-end) | < 200 ms |
| Availability | 99.99% (≤ 52.6 min downtime/year) |
| Server fleet size | 50–200 app servers (auto-scaled) |
| Failover time (LB) | < 3 seconds |
| Health check interval | Every 5 seconds |
| Session store latency | < 2 ms (Redis round-trip) |

---

## 4. Capacity Estimation

### Traffic

| Metric | Calculation | Result |
|---|---|---|
| Average QPS | Given | 100,000 QPS |
| Peak QPS | 3× average | 300,000 QPS |
| Requests per day | 100K × 86,400 | ~8.6 billion/day |
| Per-server QPS (50 servers) | 100K ÷ 50 | 2,000 QPS/server |
| Per-server QPS (200 servers at peak) | 300K ÷ 200 | 1,500 QPS/server |

### Bandwidth

| Metric | Calculation | Result |
|---|---|---|
| Average request size | ~2 KB (headers + small body) | — |
| Average response size | ~10 KB (JSON payload) | — |
| Ingress (requests) | 100K × 2 KB = 200 MB/s | ~1.6 Gbps |
| Egress (responses) | 100K × 10 KB = 1 GB/s | ~8 Gbps |
| Peak egress | 3× → 3 GB/s | ~24 Gbps |

### Session Storage (Redis)

| Metric | Calculation | Result |
|---|---|---|
| Active sessions (concurrent) | ~5 million users online | 5M |
| Session size | ~1 KB (user ID, token, preferences) | — |
| Total session memory | 5M × 1 KB | ~5 GB |
| Redis cluster | 2 nodes × 8 GB each (with replication) | 16 GB provisioned |

### LB Connection Table

| Metric | Calculation | Result |
|---|---|---|
| Concurrent connections | ~500,000 (keep-alive) | — |
| Memory per connection (L4) | ~256 bytes | ~128 MB |
| Memory per connection (L7) | ~2 KB (buffered headers) | ~1 GB |

---

## 5. Core Approaches

### 5.1 Vertical Scaling (Scale-Up)

**Problem it solves:** The simplest path — take your one server and give it more CPU, RAM, and faster disks.

**How it works:** Upgrade the hardware. 4 cores → 64 cores, 16 GB RAM → 1 TB RAM, HDD → NVMe SSD. No code changes required. The application runs exactly as before, just faster.

**Failure modes:**
- Hard ceiling: the largest EC2 instance (`u-24tb1.metal`, 448 vCPUs, 24 TB RAM) is the limit. Beyond that, you're stuck.
- Still a single point of failure — one machine dies, everything dies.
- Diminishing returns: going from 4 to 8 cores doubles throughput for parallelizable workloads. Going from 64 to 128 does not.

**When NOT to use:** When you need fault tolerance or when you foresee traffic exceeding the capacity of the largest single machine. By Day 3 (Database Design), you'll see the same ceiling in databases.

---

### 5.2 Horizontal Scaling (Scale-Out)

**Problem it solves:** Removes the ceiling on capacity by adding more machines (nodes) to a fleet.

**How it works:** Deploy N identical app server instances. Place a load balancer in front that distributes traffic. Each server processes only a fraction of total traffic. Adding more servers increases total capacity near-linearly.

**Prerequisite:** App servers **must be stateless**. If Server A stores user sessions in local memory, Server B cannot serve that user's next request. Therefore all shared state (sessions, carts, tokens) must move to an external store (Redis, Memcached). This is covered in approach 5.7 below.

**Failure modes:**
- If the load balancer itself fails and has no HA pair, it becomes the single point of failure.
- Uneven load distribution (without proper algorithm selection) leads to hot servers and cold servers.
- Deployment complexity increases — rolling deploys, health checks, service discovery all become necessary.

**When NOT to use:** For a prototype or low-traffic service (< 1,000 QPS) where a single sufficiently large server is cheaper and simpler to operate. Over-engineering a horizontally scaled fleet for 100 users is a red flag.

---

### 5.3 Layer-4 Load Balancing (Transport Layer)

**Problem it solves:** High-throughput traffic distribution with minimal latency overhead.

**How it works:** The LB inspects only the TCP/UDP packet headers — source/destination IP and port. It makes a routing decision based on these four fields alone and forwards the packet. No connection termination, no payload inspection, no HTTP parsing.

**Technically:** Operates via NAT (Network Address Translation). The LB rewrites the destination IP of incoming packets to the selected backend server. Return traffic is either routed back through the LB (full proxy mode) or directly from server to client (DSR — Direct Server Return, used for high-egress workloads like video streaming).

**Complexity:** O(1) per packet — hash of the 4-tuple.

**Failure modes:**
- Cannot route by URL path, HTTP header, or cookie — no content awareness.
- Cannot do SSL termination (payload is opaque).
- No retries on failure — if the backend drops the connection mid-stream, the client sees a reset.

**When NOT to use:** When you need content-based routing (e.g., `/api` → cluster A, `/static` → CDN). When you need sticky sessions by cookie. Use L7 instead.

---

### 5.4 Layer-7 Load Balancing (Application Layer)

**Problem it solves:** Intelligent, content-aware traffic routing.

**How it works:** The LB terminates the TCP connection from the client, reads the full HTTP request (method, path, headers, cookies, body), applies routing rules, then opens a new connection to the selected backend. This is a **full reverse proxy**.

**Capabilities:**
- Path-based routing: `/api/*` → API servers, `/static/*` → file servers or CDN
- Header-based routing: `Accept-Language: fr` → French servers
- Cookie-based sticky sessions: route requests with `JSESSIONID=abc` to the same server
- A/B testing: route 5% of traffic to a canary deployment
- WebSocket upgrade handling: detect `Upgrade: websocket` and route to WebSocket-capable servers
- Request/response manipulation: add headers, rewrite URLs, strip cookies

**Complexity:** Higher CPU cost per connection — must fully parse HTTP. For HTTPS, must terminate TLS (see 5.8).

**Failure modes:**
- Higher latency per request vs L4 (parse overhead, double connection setup).
- More complex configuration — routing rules, health check paths, TLS certificates.
- If the LB performs body inspection (e.g., WAF), CPU cost increases dramatically.

**When NOT to use:** For non-HTTP protocols (raw TCP game servers, database connections). For latency-critical paths where every microsecond matters.

---

### 5.5 Load Balancing Algorithms

| Algorithm | Mechanism | Best For | Weakness |
|---|---|---|---|
| **Round Robin** | Rotate through servers 1 → 2 → 3 → 1 | Homogeneous servers, uniform request cost | Ignores server load and capacity |
| **Weighted Round Robin** | Server with weight 3 gets 3× traffic vs weight 1 | Mixed-capacity fleet (16-core and 64-core servers) | Doesn't react to real-time load |
| **Least Connections** | Route to server with fewest active connections | Mixed workloads: some requests take 10ms, others 5s | Requires tracking active connections per server |
| **IP Hash** | `hash(client_ip) % N` → always same server | Soft session affinity without cookies | Skewed if a NAT gateway masks many users under one IP |

**Implementation detail (Least Connections):** The LB maintains a min-heap of `(active_connections, server_id)`. On each new request: pop the min, increment its count, route. When a response completes: decrement the count, re-heapify. Time complexity: O(log N) per request.

---

### 5.6 Health Checks

**Problem it solves:** Automatically removes degraded servers from the pool and re-admits them when healthy.

**How it works:**
- **Active health check:** The LB sends a probe (TCP SYN or HTTP `GET /health`) to each server every T seconds (typically 3–10s).
  - If a server fails **K consecutive checks** (e.g., K=3), it's marked DOWN and removed from the pool.
  - To re-enter, the server must pass **M consecutive checks** (e.g., M=5), preventing flapping.
- **Passive health check:** The LB monitors real request responses. If a server returns 5 consecutive 5xx errors or times out, it's proactively marked DOWN without waiting for the next scheduled probe.

**Health endpoint contract:** `/health` should return `200 OK` only when the server can actually serve traffic — it should check DB connectivity, Redis connectivity, disk space, and thread pool availability. A server that returns `200` but can't actually process requests is a "zombie" — the worst failure mode.

```json
// GET /health → 200 OK
{
  "status": "healthy",
  "db": "connected",
  "redis": "connected",
  "uptime_seconds": 84021
}
```

---

### 5.7 Stateless App Servers

**Problem it solves:** Enables horizontal scaling. Any server can handle any request because no request-specific state lives on the server.

**How it works:** All session data (user auth tokens, shopping cart, CSRF tokens) is stored in an **external session store** — typically Redis or Memcached.

```
Client → LB → Any server → Redis.GET(session_id) → process request
```

**Data structure in Redis:**
```
Key:    session:<uuid>
Value:  { "userId": 42, "role": "admin", "cart": [...], "expiry": 1711200000 }
TTL:    1800 seconds (30 min)
```

**Why not local memory?** If Server A stores the session and the next request goes to Server B, the user is logged out. If Server A crashes, all sessions on it are lost. External Redis decouples session lifetime from server lifetime.

**Interaction with Day 2 (Caching):** Redis as a session store is a specialized form of cache-aside. If the session key is missing (cache miss), the app treats it as a new session and redirects to login.

---

### 5.8 SSL/TLS Termination

**Problem it solves:** TLS handshakes are CPU-intensive (RSA decryption or ECDHE key exchange). Offloading this to the LB frees app server CPU for business logic.

**How it works:**
- The LB terminates the TLS connection: client ↔ LB is HTTPS, LB ↔ backend is plain HTTP.
- The LB holds the TLS certificate and private key.
- Backend servers never see encrypted traffic and don't need certificates.

**End-to-end encryption (re-encryption):** When compliance (PCI-DSS, HIPAA) requires encryption in transit across the internal network:
- LB ↔ backend also uses TLS (with internal/self-signed certs).
- Double TLS overhead, but meets the requirement.

---

### 5.9 High-Availability Load Balancer

**Problem it solves:** The LB is a single point of failure. If it dies, the entire fleet is unreachable.

**How it works:**
- Deploy two LB instances: **primary** (active) and **secondary** (passive).
- Both share a **Virtual IP (VIP)** via the VRRP protocol (implemented by Keepalived on Linux).
- The primary holds the VIP and serves all traffic. The secondary monitors the primary via heartbeat packets.
- If the primary fails (heartbeat timeout, typically 3s), the secondary claims the VIP via a gratuitous ARP broadcast. Clients continue sending traffic to the same IP — they notice nothing.

```
                     ┌─────────────────────┐
                     │    Virtual IP (VIP)   │
                     │    203.0.113.10       │
                     └────────┬────────────┘
                              │
                 ┌────────────┴────────────┐
                 │                          │
          ┌──────┴──────┐          ┌───────┴──────┐
          │  LB Primary  │◄────────►│  LB Secondary │
          │  (Active)    │ heartbeat │  (Passive)    │
          └──────┬──────┘          └───────┬──────┘
                 │                          │
        ┌────────┼────────┐                │
        ▼        ▼        ▼     (takes over on failure)
     Server1  Server2  Server3
```

**Failover time:** < 3 seconds with Keepalived defaults. Zero DNS propagation because the VIP doesn't change.

**Active-active alternative:** Both LBs serve traffic simultaneously (DNS returns both IPs, or an upstream ECMP router splits traffic). Higher throughput but more complex state synchronization.

---

## 6. System Architecture Walkthrough

### The Write Path (e.g., POST /api/orders)

1. **Client sends HTTPS request** to the VIP (`203.0.113.10:443`).
2. **Active LB receives the packet.** Terminates TLS (decrypts the request). Parses HTTP headers to apply L7 routing rules.
3. **LB selects a backend** using least-connections algorithm. Validates the server is healthy (passed last health check).
4. **LB forwards the plain HTTP request** to the selected app server (`10.0.1.15:8080`), injecting `X-Forwarded-For` and `X-Request-Id` headers.
5. **App server receives the request.** Reads the session token from the `Authorization` header. Calls Redis to validate the session (`GET session:<token>`).
6. **App server processes business logic** (validate order, check inventory, write to DB).
7. **App server returns the response** (e.g., `201 Created`). The LB re-encrypts the response (if end-to-end TLS) and forwards to the client.
8. **LB decrements the active-connections counter** for that server.

### The Read Path (e.g., GET /api/products/42)

1. Same steps 1–4 as the write path.
2. **App server checks the cache** (Redis) for `product:42`. On cache hit → return immediately (< 2ms).
3. **On cache miss** → query the database, populate the cache with a TTL, return the response.
4. LB forwards the response to the client.

### Failure Handling

- **App server crash mid-request:** The LB detects the connection reset. Passive health check marks the server as suspect. The client receives a `502 Bad Gateway`. Client-side retry logic (with idempotency key for writes) resubmits to a different server.
- **LB primary crash:** Keepalived detects missed heartbeat within 3s. Secondary claims the VIP. In-flight TCP connections on the primary are dropped — clients must reconnect. New connections are unaffected.
- **Redis (session store) down:** App servers cannot validate sessions. Two options: (a) fail open — treat sessions as expired, force re-login. (b) fail closed — return `503 Service Unavailable` until Redis is back. Redis Sentinel auto-promotes a replica within seconds.

### Bottlenecks at Scale

| Bottleneck | Scale Trigger | Mitigation |
|---|---|---|
| Single LB throughput | > 500K concurrent connections | Active-active LB pair, or DNS-based multi-LB |
| Session store (Redis) | > 10M active sessions | Redis Cluster (sharded across 6+ nodes) |
| TLS termination CPU | High HTTPS QPS | Hardware TLS offload cards, or distribute across multiple LBs |
| Single AZ failure | Availability zone outage | Multi-AZ deployment; LBs + servers + Redis in ≥ 2 AZs |

---

## 7. Data Model

### Session Entity (Redis)

| Field | Type | Purpose |
|---|---|---|
| `session:<uuid>` | Key (String) | Primary key; UUID generated at login |
| `userId` | Integer | Identifies the authenticated user |
| `role` | String | Authorization level (user, admin) |
| `csrfToken` | String | Cross-site request forgery token |
| `createdAt` | Epoch (Integer) | Login timestamp |
| TTL | 1800s | Auto-expiry after 30 min of inactivity |

**Why Redis?** Sub-millisecond reads, supports TTL natively, replication via Sentinel/Cluster for HA. The access pattern is simple key-value retrieval — no joins, no range queries. Redis handles 100K+ GET/SET ops/sec on a single node.

**Scaling:** Redis Cluster partitions keys by slot (CRC16 hash of key, 16384 slots). Adding nodes redistributes slots. At 10M sessions × 1KB = 10GB, a 3-node cluster with replication handles this comfortably.

### Health Check Metadata (LB internal)

| Field | Type | Purpose |
|---|---|---|
| `server_id` | String | Internal identifier for the backend |
| `ip:port` | String | Network address of the backend |
| `status` | Enum (UP/DOWN/DRAINING) | Current health status |
| `consecutive_failures` | Integer | Counter for sequential check failures |
| `consecutive_successes` | Integer | Counter for sequential check passes |
| `last_check_time` | Timestamp | When the last probe was sent |
| `active_connections` | Integer | Current in-flight requests to this server |

**Storage:** In-memory on the LB itself. This is not persisted — on LB restart, all servers start as DOWN and must pass M health checks to enter the pool (safe default).

---

## 8. Interview Questions with Model Answers

### Q1 (Mid-Level): What is the difference between vertical and horizontal scaling?

**Model Answer:** Vertical scaling means upgrading a single server's hardware — more CPU, RAM, faster disks. It requires no code changes but has a hard ceiling (the largest machine available) and is still a single point of failure. Horizontal scaling means adding more identical servers and using a load balancer to distribute traffic. It offers near-unlimited scalability and fault tolerance but requires the application to be stateless — all shared state must be externalized to a store like Redis.

**Follow-up:** "If your app servers are stateful, what specifically do you need to change to enable horizontal scaling?"

**Trap:** Candidates describe horizontal scaling but forget the statelessness prerequisite, or say "just add more servers" without explaining how session state is handled.

---

### Q2 (Mid-Level): Explain the difference between Layer-4 and Layer-7 load balancing.

**Model Answer:** Layer-4 routes based on the TCP/UDP 4-tuple (source/destination IP and port) without inspecting the payload. It's fast, low-overhead, and works for any TCP protocol. Layer-7 terminates the client connection, reads the full HTTP request (path, headers, cookies), and makes content-aware routing decisions — like sending `/api` requests to one cluster and `/static` to a CDN. The tradeoff is higher CPU cost per request at L7 due to TLS termination and HTTP parsing.

**Follow-up:** "When would you use L4 even for HTTP traffic?"

**Trap:** Candidates say L7 is always better. In reality, L4 is preferred when you need maximum throughput and don't need content-based routing — e.g., distributing traffic across a set of L7 load balancers.

---

### Q3 (Senior): How do you prevent the load balancer itself from becoming a single point of failure?

**Model Answer:** Deploy an active-passive pair of LBs sharing a Virtual IP (VIP) using the VRRP protocol (Keepalived). The primary LB holds the VIP and serves all traffic. The secondary monitors via heartbeat. On primary failure, the secondary claims the VIP via gratuitous ARP within 3 seconds — no DNS change needed, clients are unaware. For higher throughput, use active-active with DNS round-robin or ECMP routing at the network layer to split traffic across both LBs. In cloud environments, AWS ALB/NLB handles this transparently with multi-AZ redundancy built in.

**Follow-up:** "What happens to in-flight TCP connections when the primary fails over?"

**Trap:** Candidates mention DNS failover, which takes minutes (TTL propagation). VIP-based failover is the correct approach for sub-second recovery.

---

### Q4 (Senior): You're running Least Connections across 50 servers, but one server is significantly slower due to a degraded disk. What happens?

**Model Answer:** Least Connections routes to the server with the fewest active connections. A slow server completes requests slowly, so its active connection count stays high — it naturally receives fewer new requests. This is actually a strength of Least Connections: it self-adjusts. However, if the server is *barely* slow (25% slower), it still accepts significant traffic and returns degraded responses. The real fix is passive health checking — if the server's p99 latency exceeds a threshold or its error rate increases, the LB should mark it as DOWN proactively, not wait for it to fail a TCP health check. NGINX supports this via `max_fails` and `fail_timeout` on the upstream.

**Follow-up:** "How do you handle a server that's returning 200 OK but with incorrect data?"

**Trap:** Candidates assume Least Connections fully solves degraded-server problems. It mitigates but doesn't eliminate the issue — active and passive health checks are complementary.

---

### Q5 (Staff/Principal): Design an auto-scaling strategy that works with your LB. How do new servers enter the pool, and how do you handle scale-down without dropping requests?

**Model Answer:** Pair the LB with an auto-scaling group (ASG) that monitors aggregate CPU utilization and request queue depth. When the average CPU across the fleet exceeds 60% for 2 minutes, the ASG launches new instances. New instances run a startup script, begin accepting health checks, and enter the LB pool only after passing M consecutive checks — this prevents routing traffic to a server that's still warming its JIT, loading caches, or establishing DB connection pools. For scale-down, the LB marks the server as DRAINING: it stops sending new requests but allows in-flight requests to complete (connection draining window, typically 30–60 seconds). Only after all active connections are closed does the ASG terminate the instance. The scale-down threshold should be significantly lower than scale-up (e.g., scale-up at 60% CPU, scale-down at 30%) to prevent oscillation.

**Follow-up:** "What metric would you use instead of CPU if your workload is I/O-bound?"

**Trap:** Candidates scale on CPU alone for all workloads. For I/O-bound or memory-bound services, active connection count or request latency p99 is a better signal. Another trap: scaling down too aggressively and dropping active requests because connection draining wasn't configured.

---

## 9. Trade-offs to Articulate

1. **"I chose horizontal scaling over vertical scaling because at 100K QPS, no single machine can serve the load, which means vertical scaling's hard ceiling makes it non-viable. The tradeoff I'm accepting is the operational complexity of managing a fleet of stateless servers and an external session store."**

2. **"I chose Layer-7 load balancing over Layer-4 because at this scale, we need content-based routing (separating API and static traffic), and the ability to do sticky sessions and A/B testing. The tradeoff I'm accepting is higher per-request CPU cost on the LB due to HTTP parsing and TLS termination."**

3. **"I chose Least Connections over Round Robin because our requests have highly variable processing times (some are cached sub-ms, others hit the DB for 200ms). The tradeoff I'm accepting is the overhead of tracking active connections per server, which adds O(log N) complexity per routing decision."**

4. **"I chose Redis as an external session store over in-memory sessions because horizontal scaling requires stateless servers, which means any server must serve any user. The tradeoff I'm accepting is an additional network hop per request (~1ms to Redis) and a new dependency whose failure would invalidate all sessions."**

5. **"I chose active-passive LB failover over active-active because at 100K QPS, a single LB can handle the load, which means the operational complexity of synchronizing state across two active LBs outweighs the benefit. The tradeoff I'm accepting is wasted capacity — the standby LB sits idle during normal operation."**

6. **"I chose TLS termination at the LB over end-to-end TLS because our internal network is within a private VPC, which means the risk of in-transit interception is low. The tradeoff I'm accepting is that traffic between the LB and app servers is unencrypted, which would not satisfy strict PCI-DSS compliance requirements."**

---

## 10. Failure Modes and Resilience Patterns

### 1. Load Balancer Primary Failure

| Aspect | Detail |
|---|---|
| **Symptoms** | All incoming requests fail; clients see connection refused or timeout |
| **Root cause** | Hardware failure, kernel panic, NIC failure, OOM kill of LB process |
| **Detection** | Secondary LB detects missed VRRP heartbeats (3 missed within 9s) |
| **Mitigation** | Secondary claims the VIP via gratuitous ARP; failover completes in < 3s. Pager alert fires for ops team to investigate and replace the failed primary |

### 2. App Server Crash Under Load

| Aspect | Detail |
|---|---|
| **Symptoms** | 502/504 errors for a subset of requests; increased latency percentiles |
| **Root cause** | OOM kill, unhandled exception, thread pool exhaustion, runaway GC |
| **Detection** | Active health check failure (3 consecutive misses); passive detection via 5xx error spike |
| **Mitigation** | LB removes server from pool. ASG launches a replacement. Connection draining allows in-flight requests on other servers to complete normally |

### 3. Redis (Session Store) Outage

| Aspect | Detail |
|---|---|
| **Symptoms** | All users are logged out simultaneously; login pages show higher traffic than normal |
| **Root cause** | Redis primary crash, network partition isolating Redis from app servers |
| **Detection** | App servers log Redis connection errors; alert on `redis_connection_failures` metric exceeding threshold |
| **Mitigation** | Redis Sentinel promotes a replica within 10–30s. App servers reconnect automatically via Sentinel-aware client. Degraded mode: serve read-only/cacheable responses without session validation for a brief window |

### 4. Thundering Herd After Deploy

| Aspect | Detail |
|---|---|
| **Symptoms** | Latency spikes to 5–10× normal immediately after a rolling deploy; some servers return 503 |
| **Root cause** | Freshly deployed servers have cold JIT compiler, empty connection pools, and cold local caches. LB routes full traffic immediately |
| **Detection** | Alert on p99 latency exceeding 2× baseline within 5 min of a deploy |
| **Mitigation** | Warm-up period: configure the LB to send ramp-up traffic (NGINX `slow_start=30s`). The server gradually receives full traffic over 30 seconds after passing health checks |

### 5. Hot Server Due to IP Hash Skew

| Aspect | Detail |
|---|---|
| **Symptoms** | One server at 95% CPU while others are at 20%; alerts on per-server CPU imbalance |
| **Root cause** | A large corporate NAT gateway sends all traffic from 10,000 employees under a single IP. IP hash routes all of it to one server |
| **Detection** | Per-server CPU and connection count monitoring; alert when any server exceeds 2× the fleet average |
| **Mitigation** | Switch from IP Hash to Least Connections for that traffic class. Or use L7 cookie-based sticky sessions which distribute traffic at the user level, not the IP level |

---

## 11. How This Connects Forward

| Concept from Day 1 | Required In | Dependency |
|---|---|---|
| **Stateless app servers + Redis session store** | Day 2 (Caching Strategies) | Redis patterns reappear as a caching layer; cache-aside assumes stateless servers |
| **Horizontal scaling model** | Day 3 (Database Scaling) | DB read replicas and sharding extend the scale-out principle to the data layer |
| **Health checks + failure detection** | Day 4 (Message Queues) | Dead Letter Queues and retry logic build on the same failure-detection mindset |
| **L7 routing and rate limit headers** | Day 5 (API Design) | API gateways (Kong, AWS API Gateway) are specialized L7 LBs with rate limiting |
| **IP Hash algorithm and its distribution problem** | Day 6 (Consistent Hashing) | Consistent hashing solves exactly the hot-spot and remapping problem that naive `hash % N` creates |
| **VIP-based HA and failover** | Day 7 (CAP Theorem) | The availability guarantee of the LB pair maps to the "A" in CAP |
| **LB + fleet model** | Day 8–14 (All system design problems) | Every system designed from Day 8 onward assumes a load-balanced, horizontally scaled compute layer |

---

## 12. Diagrams to Draw (Descriptions Only)

### Diagram 1: Basic Horizontally Scaled Architecture
Draw a client at the top, a single LB box below it, and 3 app server boxes below the LB connected by arrows. Below the app servers, draw a Redis box (labeled "Session Store") and a Database box. Show the client → LB path as HTTPS (label the arrow "TLS"), and LB → servers as HTTP (label "plain HTTP"). Draw a dashed arrow from each app server to Redis and to the Database.

### Diagram 2: High-Availability Load Balancer with VIP
Draw two LB boxes side by side: one labeled "Primary (Active)", the other "Secondary (Passive)". Above both, draw a single box labeled "VIP: 203.0.113.10" with arrows down to both LBs. Draw a double-headed arrow between the two LBs labeled "VRRP heartbeat". Below both LBs, draw 3 app servers. Show normal traffic flowing through the Primary only. Add a red "X" on the Primary and a dashed arrow showing the VIP shifting to the Secondary.

### Diagram 3: Health Check State Machine
Draw a state machine with three states: **HEALTHY**, **SUSPECT**, and **DOWN**. Arrow from HEALTHY → SUSPECT on "1 failed check". Arrow from SUSPECT → DOWN on "K total consecutive failures". Arrow from DOWN → SUSPECT on "1 passed check". Arrow from SUSPECT → HEALTHY on "M consecutive passes". Arrow from SUSPECT → HEALTHY on "successful real request (passive check reset)".

### Diagram 4: Request Flow Through L7 LB
Draw a sequential flow diagram from left to right: **Client** → [TLS handshake] → **LB** → [parse HTTP: read path, headers, cookies] → [select algorithm: Least Connections] → [check health table] → **App Server** → [Redis session lookup] → [DB query] → **Response** back through LB → [re-encrypt if E2E TLS] → **Client**. Label each arrow with the protocol/data being passed. Label latency at each hop.

### Diagram 5: Auto-Scaling with Connection Draining
Draw a timeline diagram. Start with 3 servers in the LB pool (show the pool as a box). Mark a time point where "CPU > 60% for 2 min". Show the ASG launching Server 4. Show Server 4 entering a "Health Check Window" (M consecutive checks). Show it entering the pool. Then mark "CPU < 30% for 10 min". Show Server 3 entering DRAINING state (no new requests, existing connections finish). After a draining window, show Server 3 removed from the pool and terminated.

---
