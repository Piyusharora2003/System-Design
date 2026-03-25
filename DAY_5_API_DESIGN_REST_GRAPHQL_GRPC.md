# Day 5 — API Design: REST, GraphQL & gRPC

---

## 1. Day Summary

Days 1–4 built the infrastructure layer: scaling compute, caching reads, scaling storage, and decoupling services with queues. Day 5 shifts to the **contract layer** — the interfaces between those services and between your backend and the outside world. API design is not about picking a protocol; it's about understanding what each protocol optimizes for and matching that to your access pattern. REST optimizes for discoverability and cacheability. GraphQL optimizes for flexible client queries. gRPC optimizes for low-latency service-to-service calls. The mental model shift today: **the API is the product**. A poorly designed API forces every client to work around your mistakes, introduces coupling that prevents backend refactoring, and creates performance bottlenecks that no amount of caching or scaling can fix. If you can't articulate why cursor-based pagination exists, when rate limiting should be token-bucket vs sliding-window, or why idempotency keys are non-negotiable for POST endpoints, you'll design brittle systems that break under real-world conditions.

---

## 2. Pre-read Checklist

| Concept | Day Covered | Why It's Required |
|---|---|---|
| Horizontal scaling & stateless servers | Day 1 | REST's statelessness constraint maps directly to the stateless server architecture you designed |
| L7 load balancing & content-based routing | Day 1 | API gateways are specialized L7 LBs — path routing, header inspection, TLS termination |
| Rate limit headers (429, Retry-After) | Day 1 | Day 1 introduced rate limiting at the LB level; today you design the API-level rate limiting strategies |
| Cache-Control headers & CDN caching | Day 2 | REST API responses leverage Cache-Control headers; understanding cacheability is critical for API performance |
| Cache-aside pattern | Day 2 | API responses are often cache-aside over the data layer; understanding TTL and invalidation informs API response headers |
| Database access patterns (SQL/NoSQL) | Day 3 | API design choices (pagination, filtering) directly impact the queries the database must execute |
| Message queues & async processing | Day 4 | Many API endpoints (file upload, report generation) return immediately and process work via queues — the API must communicate this pattern |
| Idempotency & at-least-once delivery | Day 4 | Idempotent API endpoints connect directly to the retry semantics from Day 4 |

---

## 3. The Problem, Stated Precisely

### Functional Requirements

| Requirement | Detail |
|---|---|
| Design external-facing APIs for web/mobile clients | RESTful endpoints with JSON responses |
| Design internal service-to-service APIs | High-throughput, low-latency, strongly typed |
| Support flexible data fetching for diverse clients | Mobile (bandwidth-constrained) vs web (rich UI) |
| Handle API versioning without breaking existing clients | Backward-compatible evolution |
| Paginate large result sets efficiently | Stable pagination under concurrent writes |
| Protect backend from abuse | Per-user rate limiting with clear client feedback |

### Non-Functional Requirements

| Metric | Target |
|---|---|
| API response latency (p99) | < 100 ms for REST, < 10 ms for gRPC (internal) |
| API availability | 99.99% |
| Total API QPS | 200,000 QPS across all endpoints |
| Rate limit per user | 1,000 requests/min (default tier) |
| Payload size limit | 1 MB for REST/GraphQL, 4 MB for gRPC streaming |
| Backward compatibility | No breaking changes within a major version for ≥ 12 months |
| Documentation coverage | 100% of public endpoints documented in OpenAPI spec |

---

## 4. Capacity Estimation

### Traffic

| Metric | Calculation | Result |
|---|---|---|
| Total API QPS | Given | 200,000 QPS |
| Peak QPS | 3× average | 600,000 QPS |
| Read-heavy endpoints (GET) | 80% of traffic | 160,000 QPS |
| Write endpoints (POST/PUT/DELETE) | 20% of traffic | 40,000 QPS |
| Internal gRPC calls (service-to-service) | 5× external (fanout) | 1,000,000 QPS |

### Bandwidth

| Metric | Calculation | Result |
|---|---|---|
| Average REST response size | 5 KB (JSON) | — |
| Average gRPC response size | 1 KB (Protobuf binary) | — |
| REST egress | 160K × 5 KB = 800 MB/s | ~6.4 Gbps |
| gRPC egress (internal) | 1M × 1 KB = 1 GB/s | ~8 Gbps |
| REST ingress (requests) | 200K × 1 KB = 200 MB/s | ~1.6 Gbps |
| Peak REST egress | 3× → 2.4 GB/s | ~19.2 Gbps |

### Rate Limiting Storage (Redis)

| Metric | Calculation | Result |
|---|---|---|
| Unique API clients (concurrently active) | 2 million | — |
| Rate limit record size | ~64 bytes (counter + timestamp + bucket metadata) | — |
| Total memory | 2M × 64 B | ~128 MB |
| Redis node | 1 node with 256 MB is sufficient | Replicated for HA |

### API Gateway

| Metric | Calculation | Result |
|---|---|---|
| Connections per gateway instance | ~50,000 concurrent | — |
| Gateway instances needed (peak) | 600K / 50K | 12 instances |
| TLS termination CPU overhead | ~20% of gateway CPU | — |

---

## 5. Core Approaches

### 5.1 REST (Representational State Transfer)

**Problem it solves:** Provides a standardized, human-readable, cacheable interface for CRUD operations over HTTP.

**How it works:**

REST maps resources to URLs and operations to HTTP methods:

```
GET    /api/v1/users/42          → Read user 42
POST   /api/v1/users             → Create a new user
PUT    /api/v1/users/42          → Replace user 42 entirely
PATCH  /api/v1/users/42          → Partially update user 42
DELETE /api/v1/users/42          → Delete user 42
```

**Architectural constraints (Roy Fielding's dissertation):**
1. **Stateless:** Each request contains all information needed to process it. No server-side session (Day 1 — stateless servers).
2. **Client-server separation:** Client and server evolve independently.
3. **Cacheable:** Responses must declare themselves cacheable or non-cacheable (Day 2 — Cache-Control headers).
4. **Uniform interface:** Resources are identified by URIs; representations (JSON, XML) are separate from the resource itself.
5. **Layered system:** The client doesn't know if it's talking to the origin server, a CDN, or a gateway (Day 1 — LB transparency).

**Response format:**
```json
// GET /api/v1/users/42 → 200 OK
{
  "id": 42,
  "name": "Alice",
  "email": "alice@example.com",
  "created_at": "2026-01-15T10:30:00Z",
  "_links": {
    "self": "/api/v1/users/42",
    "orders": "/api/v1/users/42/orders"
  }
}
```

**HTTP status codes — use them correctly:**

| Code | Meaning | When to use |
|---|---|---|
| 200 | OK | Successful GET, PUT, PATCH |
| 201 | Created | Successful POST (resource created) |
| 204 | No Content | Successful DELETE |
| 400 | Bad Request | Invalid input / malformed JSON |
| 401 | Unauthorized | Missing or invalid auth token |
| 403 | Forbidden | Valid auth but insufficient permissions |
| 404 | Not Found | Resource doesn't exist |
| 409 | Conflict | Duplicate resource / version conflict |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Server Error | Unhandled server exception |
| 503 | Service Unavailable | Server overloaded or in maintenance |

**Failure modes:**
- **Over-fetching:** `/api/v1/users/42` returns 50 fields when the client needs 3. Wastes bandwidth, especially on mobile.
- **Under-fetching:** To build a user profile page, the client calls `/users/42`, then `/users/42/orders`, then `/users/42/addresses` — three round trips. On high-latency mobile networks, this is 600ms+ total.
- **N+1 problem (client-side):** Fetching a list of 20 users, then fetching each user's avatar URL individually = 21 requests.

**When NOT to use:** For internal service-to-service communication at scale — JSON parsing overhead and HTTP/1.1 connection limitations make REST 5–10× slower than gRPC. For clients with highly variable data needs — GraphQL eliminates over/under-fetching.

---

### 5.2 GraphQL

**Problem it solves:** Lets the client request exactly the data it needs in one query — no over-fetching, no under-fetching, no multiple round trips.

**How it works:**

A single endpoint (`POST /graphql`). The client sends a query describing the shape of the response:

```graphql
# Client query
query {
  user(id: 42) {
    name
    email
    orders(last: 5) {
      id
      total
      status
    }
  }
}
```

```json
// Server response — exact shape matches the query
{
  "data": {
    "user": {
      "name": "Alice",
      "email": "alice@example.com",
      "orders": [
        { "id": 901, "total": 59.99, "status": "delivered" },
        { "id": 902, "total": 124.50, "status": "shipped" }
      ]
    }
  }
}
```

**Schema definition (strongly typed):**
```graphql
type User {
  id: ID!
  name: String!
  email: String!
  orders(last: Int): [Order!]!
}

type Order {
  id: ID!
  total: Float!
  status: OrderStatus!
}

enum OrderStatus {
  PENDING
  SHIPPED
  DELIVERED
  CANCELLED
}
```

**Technical internals:**
- **Resolvers:** Each field in the schema has a resolver function that fetches data. `user.name` resolves from the User table; `user.orders` resolves from the Order table. Resolvers are composed — the framework calls them recursively as needed.
- **DataLoader pattern:** Solves the N+1 problem on the server side. Instead of resolving each `order` individually (N DB queries), DataLoader batches all order IDs from the current execution context and makes one `SELECT * FROM orders WHERE id IN (...)` query.
- **Introspection:** The schema is queryable — clients can discover all types, fields, and relationships at runtime. Enables tools like GraphiQL and Apollo Studio.

**Failure modes:**
- **N+1 query problem (server-side):** Without DataLoader, a query for 20 users with their orders triggers 1 user query + 20 order queries = 21 DB queries. DataLoader reduces this to 2 queries.
- **Deeply nested queries (complexity attack):** A malicious query: `{ user { friends { friends { friends { ... } } } } }` can explode exponentially. Mitigate with query depth limiting (max depth = 7), query cost analysis (assign cost per field, reject queries exceeding a budget), and timeouts.
- **Caching difficulty:** Unlike REST (where each URL is a natural cache key), GraphQL queries are arbitrary combinations of fields. HTTP-level caching (CDN, browser) doesn't work. Solutions: persisted queries (hash the query → use hash as cache key), response-level caching per query hash, or field-level caching in the resolver.

**When NOT to use:**
- Simple CRUD APIs with predictable, uniform access patterns — REST is simpler.
- Internal microservice communication — GraphQL's flexibility is wasted; gRPC's strict contracts and binary encoding are more appropriate.
- When your team doesn't have the expertise to operate a GraphQL server correctly (schema design, N+1 prevention, cost analysis).

---

### 5.3 gRPC

**Problem it solves:** Ultra-low-latency, strongly typed, binary-encoded communication between services.

**How it works:**

1. **Define the contract** in a `.proto` file (Protocol Buffers):
```protobuf
syntax = "proto3";

service UserService {
  rpc GetUser (GetUserRequest) returns (UserResponse);
  rpc ListUsers (ListUsersRequest) returns (stream UserResponse); // server streaming
  rpc CreateUser (CreateUserRequest) returns (UserResponse);
}

message GetUserRequest {
  int64 user_id = 1;
}

message UserResponse {
  int64 id = 1;
  string name = 2;
  string email = 3;
  int64 created_at = 4; // epoch seconds
}
```

2. **Generate code:** `protoc` generates client stubs and server interfaces in your language (Go, Java, Python, C++).

3. **Communicate:** Client calls `userService.GetUser(request)` — the library handles serialization, transport, and deserialization.

**Protocol details:**
- **Transport:** HTTP/2 — multiplexed streams over a single TCP connection. No head-of-line blocking (unlike HTTP/1.1). Connection reuse dramatically reduces latency for service-to-service calls.
- **Serialization:** Protocol Buffers — binary encoding. 3–10× smaller and 20–100× faster to serialize/deserialize than JSON.
- **Streaming modes:**
  - *Unary:* One request, one response (like REST).
  - *Server streaming:* One request, stream of responses (e.g., live price feed).
  - *Client streaming:* Stream of requests, one response (e.g., upload chunks).
  - *Bidirectional streaming:* Both sides stream (e.g., chat, real-time collaboration).

**Failure modes:**
- **Not browser-native:** Browsers can't make raw gRPC calls (no HTTP/2 Trailers support). Use gRPC-Web (a proxy that translates) or expose a REST/GraphQL gateway for browser clients.
- **Hard to debug:** Binary payloads aren't human-readable. Use `grpcurl` (CLI tool) or Postman's gRPC support. In production, structured logging of request/response metadata is essential.
- **Schema evolution pitfalls:** Renaming or removing fields breaks backward compatibility. Never reuse field numbers. Use `reserved` to prevent accidental reuse.

```protobuf
message UserResponse {
  reserved 3;           // field 3 was removed, prevent reuse
  reserved "address";   // field name "address" is retired
  int64 id = 1;
  string name = 2;
  // field 3 was "address" — removed in v2
  string email = 4;
}
```

**When NOT to use:** For public APIs — REST's human readability and browser support are essential. For teams without polyglot build tooling — Protobuf code generation adds build complexity.

**Builds on Day 1:** gRPC runs behind an L4 load balancer (Day 1). L7 isn't needed because gRPC handles its own routing via service names. However, L7 gRPC-aware LBs (Envoy, Linkerd) can do per-RPC load balancing instead of per-connection, which is critical for long-lived HTTP/2 connections.

---

### 5.4 API Versioning

| Strategy | Mechanism | Example | Pros | Cons |
|---|---|---|---|---|
| **URL versioning** | Version in the URL path | `/v1/users`, `/v2/users` | Explicit, easy to route at LB, easy to test | URL pollution; harder to deprecate |
| **Header versioning** | Version in a custom header | `Accept: application/vnd.api+json;version=2` | Clean URLs | Hidden from browsers; harder to test with curl |
| **Query parameter** | Version as a query param | `/users?version=2` | Simple | Pollutes query string; easy to forget |
| **Additive changes (preferred)** | Never break; add new fields | Add `phone` field to response | No versioning needed; backward compatible | Doesn't work for breaking changes |

**Best practice:** Use URL versioning for major breaking changes (v1 → v2). For non-breaking changes, prefer additive field additions. Mark deprecated fields in documentation with a sunset date. Run both versions simultaneously for 6–12 months during migration.

**gRPC versioning:** Add new fields to Protobuf messages (they're ignored by old clients). For breaking changes, create a new service package (`v2.UserService`). Never reuse field numbers.

---

### 5.5 Pagination

**Offset-based:**
```
GET /api/v1/users?page=3&limit=20
```

```sql
SELECT * FROM users ORDER BY id LIMIT 20 OFFSET 40;
```

| Aspect | Detail |
|---|---|
| **Pros** | Simple to implement; client can jump to any page |
| **Cons** | Performance degrades at large offsets (DB scans and discards OFFSET rows). Inconsistent results: if a row is inserted on page 1 after page 2 was fetched, page 3 will include a duplicate |
| **Complexity** | O(OFFSET + LIMIT) on the DB — at page 10,000 with limit 20, the DB scans 200,000 rows to return 20 |

**Cursor-based:**
```
GET /api/v1/users?cursor=eyJ1c2VySWQiOjEyM30=&limit=20
```

The cursor is an opaque, base64-encoded pointer to the last item on the previous page:
```json
// Decoded cursor
{ "userId": 123 }
```

```sql
SELECT * FROM users WHERE id > 123 ORDER BY id LIMIT 20;
```

| Aspect | Detail |
|---|---|
| **Pros** | O(LIMIT) regardless of page depth — uses an index seek, not a scan. Stable: inserts/deletes don't cause duplicates or skips |
| **Cons** | Can't jump to arbitrary pages (no "page 50" link). The cursor is opaque — clients can't construct cursors manually |
| **Complexity** | O(LIMIT) with a B-tree index on the sort key |

**Response format for cursor pagination:**
```json
{
  "data": [ ... ],
  "pagination": {
    "next_cursor": "eyJ1c2VySWQiOjE0M30=",
    "has_next": true,
    "limit": 20
  }
}
```

**Rule of thumb:** Use cursor-based pagination for any list that is mutable (feeds, orders, messages). Use offset only for static, small datasets (admin tables < 10,000 rows) or when "jump to page N" is a genuine product requirement.

---

### 5.6 Rate Limiting on the API Gateway

**Problem it solves:** Prevent a single client from monopolizing resources and protect the backend from traffic spikes and abuse.

**Algorithms (detailed in Day 9 — here is the API-layer perspective):**

| Algorithm | API Behavior | Best For |
|---|---|---|
| **Token bucket** | Client has a burst capacity (bucket) that refills at a constant rate. Burst-friendly | Public APIs with bursty legitimate traffic (mobile apps) |
| **Sliding window counter** | Smooth request counting over a rolling time window. No boundary burst | Internal APIs where strict enforcement matters |
| **Fixed window** | Count per time window, reset at boundaries. Simple but allows 2× burst at boundaries | Low-stakes rate limiting (analytics endpoints) |

**Implementation with Redis:**
```lua
-- Atomic Lua script for sliding window counter
local key = KEYS[1]
local window = tonumber(ARGV[1])  -- window size in seconds
local limit = tonumber(ARGV[2])   -- max requests per window
local now = tonumber(ARGV[3])     -- current timestamp in ms

-- Remove entries outside the window
redis.call('ZREMRANGEBYSCORE', key, 0, now - window * 1000)

-- Count requests in the current window
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, now .. ':' .. math.random())
    redis.call('EXPIRE', key, window)
    return 0  -- allowed
else
    return 1  -- rejected
end
```

**Response headers (RFC 6585 + draft-ietf-httpapi-ratelimit-headers):**
```http
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1711214400
Retry-After: 45
Content-Type: application/json

{
  "error": "rate_limit_exceeded",
  "message": "You have exceeded 1000 requests per minute. Retry after 45 seconds.",
  "retry_after_seconds": 45
}
```

**Builds on Day 1:** The API gateway is a specialized L7 load balancer. Rate limiting runs as middleware in the gateway before the request reaches the app server — the same architectural position as health checks.

**Builds on Day 4:** Rate limiting counters in Redis are structurally similar to at-least-once delivery tracking. If the Redis node fails, the system should fail-open (allow requests) rather than fail-closed (reject all), unless the API serves financial transactions.

---

### 5.7 Idempotent API Design

**Problem it solves:** Network failures, client retries, and duplicate submissions must not create duplicate side effects.

**Naturally idempotent methods:**
- `GET` — reading the same resource twice returns the same result.
- `PUT` — replacing a resource with the same data twice yields the same state.
- `DELETE` — deleting an already-deleted resource is a no-op (return 404 or 204).

**Non-idempotent by default:**
- `POST` — creating a resource twice creates two resources.

**Solution — Idempotency key:**
```http
POST /api/v1/payments
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
Content-Type: application/json

{
  "amount": 99.99,
  "currency": "USD",
  "recipient": "user_42"
}
```

**Server-side implementation:**
```python
def create_payment(request):
    idempotency_key = request.headers["Idempotency-Key"]
    
    # Check if this key was already processed
    existing = redis.get(f"idempotency:{idempotency_key}")
    if existing:
        return json.loads(existing)  # return stored result
    
    # Process the payment
    result = payment_service.charge(request.body)
    
    # Store the result (TTL = 24 hours)
    redis.set(f"idempotency:{idempotency_key}", json.dumps(result), ex=86400)
    
    return result
```

**Key insight:** The idempotency key is generated by the **client**, not the server. This allows safe retries — the client sends the same key on retry and gets the same response. The server stores `(key → response)` with a TTL.

**Builds on Day 4:** Idempotency is the API-layer manifestation of the same principle behind idempotent consumers in message queues (Day 4). In both cases, the goal is: processing the same input twice produces the same output.

---

## 6. System Architecture Walkthrough

### The Read Path (REST API — GET /api/v1/products?cursor=abc&limit=20)

1. **Client sends HTTPS request** to the API gateway (e.g., Kong, AWS API Gateway).
2. **Gateway authenticates:** Validates the API key or JWT token. Rejects with `401` if invalid.
3. **Gateway checks rate limit:** Runs the sliding-window counter script in Redis. If exceeded, returns `429` with `Retry-After` header. **Request never reaches the app server.**
4. **Gateway routes to the correct service:** L7 path-based routing sends `/products/*` to the Product Service cluster (via the LB from Day 1).
5. **App server receives the request.** Decodes the cursor (`{ "productId": 500 }`).
6. **Cache check (Day 2):** `GET product_list:cursor_abc` in Redis. On cache hit → return immediately.
7. **Cache miss → DB query:** `SELECT * FROM products WHERE id > 500 ORDER BY id LIMIT 21` (fetch one extra to determine `has_next`).
8. **Build response:** Serialize to JSON. Set `Cache-Control: private, max-age=60`. Encode the next cursor.
9. **Return through gateway.** Gateway adds `X-Request-Id` for tracing.

### The Write Path (REST API — POST /api/v1/orders)

1. **Client sends POST** with `Idempotency-Key` header.
2. **Gateway authenticates and rate-limits** (same as read path).
3. **App server checks idempotency key** in Redis. If key exists → return stored response (no DB write).
4. **Validate request body:** Check required fields, data types, business rules. Return `400` with field-level errors if invalid.
5. **Write to database** (within a transaction).
6. **Publish event to Kafka** (Day 4): `order.created` event for downstream consumers (notification service, inventory service).
7. **Store the response against the idempotency key** in Redis (TTL = 24h).
8. **Return `201 Created`** with `Location: /api/v1/orders/12345` header.

### Internal Service-to-Service Call (gRPC)

1. **Order Service needs to validate inventory.** Calls `inventoryService.CheckStock(productId=42, quantity=3)` via gRPC.
2. **Service discovery (Consul / Kubernetes DNS)** resolves `inventory-service` to a set of IPs.
3. **gRPC client-side load balancing** (built into the gRPC library) picks an instance using round-robin across the HTTP/2 connection pool.
4. **Request serialized** as Protobuf binary (~50 bytes) and sent over the existing HTTP/2 connection.
5. **Inventory Service processes** the check (Redis cache → PostgreSQL if miss) and responds.
6. **Total latency:** ~2–5ms (vs ~20–30ms for the same call over REST+JSON).

### Failure Handling

| Failure | API Behavior | Client-Side Response |
|---|---|---|
| **Backend server crash** | Gateway receives `502 Bad Gateway` from LB; retries on a different server (for idempotent GET/PUT/DELETE) | Client retries automatically (exponential backoff) |
| **Rate limit exceeded** | Gateway returns `429` with `Retry-After` | Client waits and retries after the specified delay |
| **Malformed request** | App returns `400` with a descriptive error body | Client fixes the request; no retry needed |
| **Auth token expired** | Gateway returns `401` | Client refreshes the token and retries |
| **DB timeout on write** | App returns `503 Service Unavailable` | Client retries with the same idempotency key — safe |
| **gRPC deadline exceeded** | gRPC returns `DEADLINE_EXCEEDED` status code | Caller retries or falls back to degraded behavior |

---

## 7. Data Model

### API Key / Client Registration (PostgreSQL)

| Field | Type | Purpose |
|---|---|---|
| `id` | UUID (PK) | Internal identifier |
| `api_key` | VARCHAR(64), indexed | The key clients pass in `Authorization` header |
| `client_name` | VARCHAR(255) | Human-readable client identifier |
| `rate_limit_tier` | ENUM (free, standard, premium) | Determines rate limit: 100 / 1,000 / 10,000 req/min |
| `created_at` | TIMESTAMP | For auditing |
| `revoked_at` | TIMESTAMP, nullable | Soft-delete; null = active |

**Why PostgreSQL?** API keys are relational (they reference clients, tiers, permissions), they require ACID guarantees (revoking a key must take effect immediately), and the dataset is small (thousands to millions of keys, not billions). A relational DB with an index on `api_key` gives O(log N) lookup.

**Access pattern:** `SELECT * FROM api_keys WHERE api_key = ? AND revoked_at IS NULL` — executed on every authenticated API call. This result is cached in Redis (Day 2) with a 60s TTL to avoid hitting the DB on every request.

### Rate Limit Record (Redis)

| Field | Type | Purpose |
|---|---|---|
| Key | `ratelimit:<client_id>:<window>` | Per-client, per-window counter |
| Value | Sorted Set (ZSET) | Timestamps of requests (for sliding window) |
| TTL | Equal to the window size (60s) | Auto-cleanup |

**Why Redis?** Atomic operations (`ZADD`, `ZCARD`, `ZREMRANGEBYSCORE`) in a Lua script make the rate-limit check and increment a single O(log N) operation. Sub-millisecond latency ensures the rate limiter doesn't become the bottleneck.

### Idempotency Record (Redis)

| Field | Type | Purpose |
|---|---|---|
| Key | `idempotency:<idempotency_key>` | Client-provided UUID |
| Value | Serialized JSON response | The full response to return on retry |
| TTL | 86400 seconds (24 hours) | Keys older than 24h are unlikely to be retried |

**Why Redis (not the DB)?** Idempotency records are ephemeral, high-throughput, and accessed by exact key. They don't need durability beyond 24 hours. If Redis loses them, the worst case is a duplicate side effect on retry — which is rare and handled by DB-level constraints (unique indexes).

### Protobuf Schema Registry (for gRPC)

| Component | Location | Purpose |
|---|---|---|
| `.proto` files | Git repository (schema registry) | Source of truth for service contracts |
| Generated stubs | Build artifact per language | Client/server code generated by `protoc` |
| Schema compatibility checks | CI pipeline | Validate that `.proto` changes are backward compatible before merge |

**Why a schema registry?** gRPC with Protobuf is a contract-first protocol. The `.proto` file IS the API documentation. A schema registry (Buf.build or a Git repo with CI checks) prevents breaking changes from being deployed.

---

## 8. Interview Questions with Model Answers

### Q1 (Mid-Level): When would you choose REST over GraphQL?

**Model Answer:** I'd choose REST when the API is public-facing and needs to be easily discoverable and cacheable. REST's URL-per-resource model maps naturally to HTTP caching — each URL is a cache key, and CDNs can cache responses based on the URL and `Cache-Control` headers. GraphQL queries are POST requests with arbitrary bodies, which makes HTTP caching effectively impossible without workarounds like persisted queries. I'd also choose REST when the access patterns are predictable and uniform — if every client needs the same fields, GraphQL's flexibility is overhead without benefit.

**Follow-up:** "What if you have both a web app and a mobile app with very different data needs for the same screen?"

**Trap:** Candidates say REST is always simpler. In the mobile-vs-web scenario, REST leads to either over-fetching (mobile gets fields it doesn't need, wasting bandwidth) or multiple endpoint variants (maintaining `/users/42?fields=name,email` or separate mobile/web endpoints). This is where GraphQL's single-query flexibility genuinely shines.

---

### Q2 (Mid-Level): What is the difference between offset-based and cursor-based pagination?

**Model Answer:** Offset pagination uses `LIMIT/OFFSET` in the DB query: `SELECT * FROM posts OFFSET 1000 LIMIT 20`. It's simple but has two problems: the DB scans and discards 1000 rows before returning 20 (O(OFFSET) performance), and concurrent inserts cause inconsistent pages — a new post on page 1 pushes everything down, so page 2 returns a duplicate. Cursor pagination uses a pointer to the last-seen item: `SELECT * FROM posts WHERE id > 1000 ORDER BY id LIMIT 20`. It's O(LIMIT) via index seek, stable under concurrent writes, and works efficiently at any depth. I'd use cursor for any mutable feed, and offset only for small, static datasets where "jump to page N" is needed.

**Follow-up:** "How do you implement cursor-based pagination with a compound sort key, like `(created_at, id)` for chronological ordering?"

**Trap:** Candidates use `id` as the cursor for a chronologically-sorted feed. If `id` doesn't correlate with insertion time (e.g., UUIDs), the cursor breaks. The cursor must be on the sort key: `WHERE (created_at, id) > (cursor_timestamp, cursor_id) ORDER BY created_at, id`.

---

### Q3 (Senior): How do you handle API versioning for a public API with 10,000 active integrations?

**Model Answer:** I'd use URL versioning (`/v1/`, `/v2/`) for major breaking changes and additive field additions for non-breaking evolution within a version. When a breaking change is necessary, I'd release `/v2/` alongside `/v1/`, give a 12-month deprecation notice, and track v1 usage per client. The API gateway routes `/v1/*` and `/v2/*` to separate service deployments so the old version continues running unchanged. I'd also publish a changelog and migration guide, and proactively reach out to the top 100 clients by volume. Critical: never delete or rename fields in a live version — only add new ones. Mark deprecated fields in the OpenAPI spec with `deprecated: true`.

**Follow-up:** "What if a client is stuck on v1 and refuses to migrate, but v1 has a security vulnerability?"

**Trap:** Candidates treat versioning as purely a routing problem. The reality is organizational — versioning strategy must include deprecation policy, client communication, usage monitoring, and a sunset date enforced by the gateway (return `410 Gone` after the sunset date).

---

### Q4 (Senior): Design the rate limiting strategy for an API that serves both free-tier and enterprise-tier clients.

**Model Answer:** I'd configure the API gateway with tiered rate limits stored in Redis: free-tier gets 100 req/min with a burst of 20, enterprise gets 10,000 req/min with a burst of 2,000. I'd use the token bucket algorithm because it allows controlled bursting — enterprise clients often have bursty traffic patterns at the start of batch jobs. The API key determines the tier; on each request, the gateway looks up the tier (cached from PostgreSQL) and applies the corresponding bucket parameters. All clients receive `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` headers so they can self-throttle. For enterprise clients, I'd also support a per-endpoint rate limit (e.g., 1,000 req/min for writes, 5,000 for reads) to protect write-heavy endpoints from accidental overload.

**Follow-up:** "What happens if Redis is down? Do you fail-open (allow all requests) or fail-closed (reject all)?"

**Trap:** Candidates give a one-size-fits-all answer. The correct approach is nuanced: fail-open for read endpoints (serving data is more important than perfect rate limiting), fail-closed for write endpoints in financial systems (allowing unlimited writes could cause double-charges), and local in-memory fallback counters as a temporary approximation.

---

### Q5 (Staff/Principal): You're building an API platform that serves 500 microservices internally via gRPC and 10,000 external clients via REST. How do you design the API gateway architecture?

**Model Answer:** I'd use a two-tier gateway architecture. The **external gateway** (Kong or AWS API Gateway) handles REST traffic: TLS termination, authentication (JWT/API key validation), rate limiting, request transformation (REST → internal gRPC), and response serialization (Protobuf → JSON). The **internal mesh** (Envoy as a sidecar proxy, managed by Istio) handles gRPC service-to-service traffic: per-RPC load balancing (critical for long-lived HTTP/2 connections — per-connection load balancing from Day 1 doesn't work here), mTLS (mutual TLS between services), circuit breaking, retry with deadline propagation, and distributed tracing. I'd enforce that all inter-service communication uses gRPC with Protobuf — no internal REST — and maintain a shared Protobuf schema registry with CI checks for backward compatibility. The external gateway translates REST ↔ gRPC using a gRPC-JSON transcoder (built into Envoy). This way, internal services have one implementation (gRPC) but support both internal consumers (native gRPC) and external consumers (REST via the transcoder) without duplicating code.

**Follow-up:** "How do you propagate deadlines across a gRPC call chain of 5 services?"

**Trap:** Candidates treat the gateway as a single black box. At 500 microservices, the gateway becomes a bottleneck if all traffic funnels through one point. The sidecar/mesh approach distributes gateway logic to every pod. Another trap: not mentioning deadline propagation — a 200ms client timeout must be propagated so that service 5 in the chain doesn't start work it can't finish.

---

## 9. Trade-offs to Articulate

1. **"I chose REST over GraphQL for the public API because our clients are diverse third-party integrators who need stable, cacheable, well-documented endpoints, which means GraphQL's per-query flexibility and caching difficulties would create friction. The tradeoff I'm accepting is that mobile clients will over-fetch some data — mitigated by sparse fieldsets (`?fields=name,email`) as a REST extension."**

2. **"I chose gRPC over REST for internal service-to-service communication because at 1M internal QPS, the 5× payload reduction from Protobuf and HTTP/2 multiplexing save ~4 Gbps of internal bandwidth, which means we avoid deploying additional network capacity. The tradeoff I'm accepting is increased debugging difficulty (binary payloads) and build complexity (code generation)."**

3. **"I chose cursor-based pagination over offset because our feed is append-heavy with 10K writes/minute, which means offset pagination would produce inconsistent pages with duplicates. The tradeoff I'm accepting is that clients can't jump to arbitrary pages — they can only paginate forward sequentially."**

4. **"I chose URL versioning over header versioning because our 10,000 API integrators include small teams without sophisticated HTTP client libraries, which means header-based versioning would increase integration friction. The tradeoff I'm accepting is URL pollution (`/v1/`, `/v2/`) and the operational cost of running multiple versions simultaneously."**

5. **"I chose a token bucket algorithm over a sliding window for rate limiting because our enterprise clients have legitimately bursty traffic patterns (batch jobs), which means a strict per-second limit would reject valid requests during spikes. The tradeoff I'm accepting is that a client could exhaust their entire minute's quota in a 2-second burst, potentially overloading a specific backend server."**

6. **"I chose to fail-open on rate limiter Redis outages for read endpoints because blocking all reads during a cache failure would create a system-wide outage for a non-critical guardrail, which means accepting temporary unthrottled traffic is less damaging. The tradeoff I'm accepting is that during a Redis outage, abusive clients go unthrottled — mitigated by L4 connection limits at the LB as a secondary defense."**

---

## 10. Failure Modes and Resilience Patterns

### 1. API Gateway Overload

| Aspect | Detail |
|---|---|
| **Symptoms** | Gateway returns `503` to all clients; p99 latency spikes above 5s; connection queue grows |
| **Root cause** | Traffic spike exceeds the gateway's connection handling capacity; TLS handshake CPU exhaustion |
| **Detection** | Gateway's active connection count exceeds configured max; CPU > 90%; alert on 503 rate |
| **Mitigation** | Auto-scale the gateway fleet (it's a stateless service behind a NLB). Offload TLS to a hardware accelerator. Enable connection queuing with a bounded queue (reject immediately if queue is full, don't make clients wait indefinitely) |

### 2. Rate Limiter Redis Failure

| Aspect | Detail |
|---|---|
| **Symptoms** | Rate limiting stops working; high-traffic clients are no longer throttled; backend services may become overloaded |
| **Root cause** | Redis instance OOM, network partition between gateway and Redis |
| **Detection** | Gateway logs Redis connection errors; alert on `rate_limiter_redis_connection_failures` metric |
| **Mitigation** | Fail-open for most endpoints (allow traffic, log the bypass). Activate local in-memory rate counters on each gateway instance as a best-effort fallback. The local counters won't be globally coordinated but prevent a single gateway from flooding a backend. Restore Redis from Sentinel-promoted replica within 30s |

### 3. N+1 Query Explosion in GraphQL

| Aspect | Detail |
|---|---|
| **Symptoms** | DB CPU spikes; query count per GraphQL request reaches 50–100; p99 latency exceeds 2s |
| **Root cause** | A new GraphQL resolver was deployed without DataLoader batching; nested queries trigger individual DB calls |
| **Detection** | Application Performance Monitoring (APM) shows high query count per request; alert on DB connection pool exhaustion |
| **Mitigation** | Implement DataLoader for every resolver that accesses a data source. Add query cost analysis middleware that rejects queries exceeding a cost budget. Set a per-request timeout of 5s at the gateway level |

### 4. gRPC Client-Side Load Balancing Failure

| Aspect | Detail |
|---|---|
| **Symptoms** | All gRPC traffic from Service A routes to a single instance of Service B; that instance is overloaded while others are idle |
| **Root cause** | HTTP/2 multiplexes all RPCs over a single TCP connection. The L4 LB sees one connection and routes all traffic to one backend. gRPC client-side LB (which picks a different connection per RPC) was not configured |
| **Detection** | Per-instance QPS monitoring for Service B shows extreme skew; one instance at 90% CPU, others at 5% |
| **Mitigation** | Enable gRPC client-side load balancing (e.g., `round_robin` policy in the gRPC channel). Alternatively, use Envoy sidecar proxy which does per-RPC L7 load balancing for gRPC natively. Never rely on L4/TCP load balancing for long-lived gRPC connections |

### 5. Breaking API Change Deployed to Production

| Aspect | Detail |
|---|---|
| **Symptoms** | External clients return parsing errors, 4xx spike, support tickets flood in |
| **Root cause** | A field was renamed or its type changed in the API response without versioning, breaking backward compatibility |
| **Detection** | Spike in 4xx errors from a subset of clients; integration test failures from partner CI pipelines; support ticket volume |
| **Mitigation** | Immediate rollback via canary deployment. Add the old field back alongside the new one (additive fix). Post-incident: enforce backward compatibility checks in CI (schema-diff tooling). For Protobuf: use `buf breaking` to detect breaking changes before merge |

---

## 11. How This Connects Forward

| Concept from Day 5 | Required In | Dependency |
|---|---|---|
| **REST API design patterns** | Day 8 (URL Shortener) | The URL shortener's `POST /shorten` and `GET /:code` endpoints are classic REST design |
| **Rate limiting algorithms** | Day 9 (Design a Rate Limiter) | Day 9 deep-dives the algorithms introduced here (token bucket, sliding window, leaky bucket) |
| **Cursor-based pagination** | Day 14 (News Feed) | The feed API uses cursor-based pagination — offset is unacceptable for a mutable, real-time feed |
| **gRPC and Protobuf** | Day 13 (Chat System) | Chat message serialization between services uses Protobuf for minimal payload size |
| **Idempotency keys** | Day 20 (Payment System) | Idempotency keys are the foundation of safe payment processing — the exact pattern from today |
| **API gateway as L7 LB** | Day 12 (Web Crawler) | Robots.txt and crawl-rate limiting are rate-limiting problems from the server's perspective |
| **Streaming (gRPC server-streaming)** | Day 15 (Video Streaming) | ABR video delivery is conceptually a streaming response — progressive delivery of chunks |

---

## 12. Diagrams to Draw (Descriptions Only)

### Diagram 1: Two-Tier API Gateway Architecture
Draw external clients (browser, mobile, third-party) on the left. They connect to an **External API Gateway** (labeled: TLS termination, auth, rate limiting, REST→gRPC transcoding). The gateway connects to an **internal network** containing 4 microservice boxes (User Service, Order Service, Inventory Service, Payment Service). Between each microservice, draw bidirectional arrows labeled "gRPC + mTLS." Each microservice has a small Envoy sidecar box attached. Label the internal communication "HTTP/2 + Protobuf." Show a Protobuf Schema Registry box at the bottom connected to all services.

### Diagram 2: REST vs GraphQL Data Fetching Comparison
Draw two parallel sequences. **REST (left):** Client makes 3 sequential requests: `GET /users/42` (200ms) → `GET /users/42/orders` (200ms) → `GET /users/42/addresses` (200ms). Total: 600ms, 3 round trips. **GraphQL (right):** Client makes 1 request: `POST /graphql` with a query containing `user`, `orders`, and `addresses`. Server resolves all three in parallel from the DB. Total: 250ms, 1 round trip. Label the tradeoff: "REST: cacheable, 3 round trips. GraphQL: not cacheable, 1 round trip."

### Diagram 3: Cursor-Based Pagination Flow
Draw a timeline showing a database with rows `[id: 98, 99, 100, 101, 102, 103, 104, 105]`. **Page 1 request:** `LIMIT 3` → returns `[98, 99, 100]`, cursor = `100`. **Page 2 request:** `WHERE id > 100 LIMIT 3` → returns `[101, 102, 103]`, cursor = `103`. Show a new row `(id: 99.5)` inserted between requests — label it "new insert." Show that Page 2 is unaffected (returns 101–103, no duplicate). Then draw the same scenario with offset: Page 2 with `OFFSET 3` now returns `[100, 101, 102]` — `100` is a duplicate because the insert shifted everything.

### Diagram 4: Rate Limiting with Token Bucket
Draw a bucket (rectangle) labeled "capacity = 10 tokens." Show tokens being added at a rate of "2 tokens/sec." Show 3 requests arriving simultaneously, each consuming 1 token. Show the bucket level decreasing from 10 → 7. Then show a burst of 8 requests — bucket drops to 0, the 8th request is rejected with `429`. Show the bucket refilling 2 tokens/sec while the client waits.

### Diagram 5: Idempotency Key Flow
Draw two sequences — **first request** and **retry (same key).** **First request:** Client → POST with `Idempotency-Key: abc` → Gateway → App Server → check Redis (miss) → process payment → write to DB → store response in Redis (`idempotency:abc`) → return `201`. **Retry:** Client → POST with same `Idempotency-Key: abc` → Gateway → App Server → check Redis (hit) → return stored response → return `201`. Label: "No duplicate charge. Same response both times."

---
