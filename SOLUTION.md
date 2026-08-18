# Solution: Webhook Ingest Defect Fixes

## 1. What Was Broken and Why

### Defect 1: Dedup Is Check-Then-Act, Not Atomic

**Bug**: `Ingest()` performed a `SELECT` (`EventExists`) followed by a separate `INSERT` (`InsertEvent`) with no transaction and no unique constraint on `events.event_id`. Two concurrent redeliveries of the same `event_id` could both pass the `SELECT` before either `INSERT` landed, creating duplicate rows in `events`, `calls`, and double-counted `account_stats`.

**Fix**: Added a `UNIQUE` constraint on `events.event_id` (`002_unique_event_id.sql`), changed `InsertEvent` to use `INSERT ... ON CONFLICT (event_id) DO NOTHING`, and wrapped the entire insert→upsert-call→increment-stats sequence in a single Postgres transaction via `store.InTx`. The `SELECT`-based pre-check was removed entirely; the `INSERT` itself is now the dedup gate. If `RowsAffected() == 0`, the event is a duplicate and the transaction short-circuits.

### Defect 2: Recording Goroutine Uses Dying Request Context

**Bug**: The goroutine spawned for recording processing captured `ctx`, which was `r.Context()`. Once the HTTP handler returned (immediately after spawning the goroutine), `net/http` cancelled that context. `MarkRecordingProcessed` ran against a cancelled context and failed. The error was silently discarded by a `// TODO: handle` comment.

**Fix**: Replaced `ctx` with `context.WithTimeout(context.Background(), 30*time.Second)` so the goroutine is not tied to the HTTP request lifecycle. Errors are now logged via `slog` instead of being swallowed.

### Defect 3: In-Flight Recording Goroutines Not Tracked by Shutdown

**Bug**: `srv.Shutdown()` in `main.go` only drained in-flight HTTP requests. Background recording goroutines weren't tracked in any `WaitGroup`, so `SIGTERM` abandoned them mid-flight.

**Fix**: Added a `sync.WaitGroup` field (`wg`) to `Service`. `Ingest` calls `s.wg.Add(1)` before launching the goroutine and `defer s.wg.Done()` inside it. A new `Service.Shutdown(ctx)` method blocks on `wg.Wait()` or returns early if the context deadline fires. `main.go` now calls `svc.Shutdown(shutdownCtx)` after `srv.Shutdown(shutdownCtx)`.

### Defect 4: `Cache.Record` Doesn't Take the Lock

**Bug**: `Record` read and wrote `c.m` with no lock at all, while `Get` held `RLock`. Concurrent `Record` calls raced on the map (which can panic in Go) and on the counters (which silently lose increments).

**Fix**: Added `c.mu.Lock()` / `defer c.mu.Unlock()` at the top of `Record`.

---

## 2. Why `ON CONFLICT DO NOTHING` on a Postgres Unique Constraint over Redis `SETNX`

| Criterion | Postgres `UNIQUE` + `ON CONFLICT` | Redis `SETNX` |
|---|---|---|
| **Atomicity** | The unique constraint makes the INSERT itself the atomic dedup gate. Combined with a transaction, the event-insert + call-upsert + stats-increment are all-or-nothing. | `SETNX` only guards the check. You still need a separate DB write, creating a window between the two where crashes can leave the system in an inconsistent state (key set, row not inserted — or vice-versa). |
| **Durability** | Postgres `fsync`s WAL to disk. If the transaction commits, the dedup is permanent. | Redis is in-memory by default. A crash or restart loses the dedup set, allowing reprocessing of already-handled events. Even with AOF, Redis offers weaker durability guarantees. |
| **Single source of truth** | The database that stores the event *is* the dedup authority. No possibility of divergence. | Introduces a second stateful system that must stay in sync with Postgres. Two sources of truth = eventual inconsistency. |
| **TTL / expiry** | The constraint is permanent — duplicates are rejected forever (correct for immutable event IDs). | `SETNX` keys typically need a TTL to avoid unbounded memory growth, which means old event IDs can be replayed after expiry. |
| **Operational overhead** | Zero additional infrastructure. Postgres is already in the stack. | Requires Redis to be highly available, monitored, and backed up — purely for dedup. |
| **Performance** | Under moderate load, the B-tree unique index lookup is sub-millisecond. Postgres handles this at tens of thousands of TPS with connection pooling. | Redis is faster for pure key lookups (~0.1 ms), but the difference is irrelevant when you're about to do a Postgres write anyway. |

**Bottom line**: For this workload, `ON CONFLICT DO NOTHING` is simpler, more durable, and eliminates an entire class of distributed-consistency bugs. Redis `SETNX` makes sense when dedup is a best-effort optimisation in front of an idempotent pipeline, not when it's the only safeguard.

---

## 3. What to Change for 10k Webhooks/sec

### Database Layer

- **Connection pooling**: Introduce PgBouncer (transaction mode) in front of Postgres to multiplex thousands of application connections over a smaller pool of server connections. The current `pgxpool` max of 20 is too low; PgBouncer can manage this more efficiently.
- **Partitioning**: Range-partition the `events` table by `received_at` (e.g., daily). This keeps indexes small and makes retention/archival trivial (`DROP PARTITION` vs. mass `DELETE`).
- **Batch inserts**: Instead of one transaction per webhook, buffer events into micro-batches (e.g., 50–100 per batch) and issue multi-row `INSERT ... ON CONFLICT` statements. This amortises round-trip and WAL-write overhead.
- **Read replicas**: Offload the stats endpoint (`SELECT` on `account_stats`) to a read replica so it doesn't compete with the write path.

### Application Layer

- **Horizontal scaling**: Run multiple stateless app instances behind a load balancer. The dedup is enforced by Postgres, so any instance can handle any webhook.
- **Async recording pipeline**: Replace the goroutine-per-recording model with a proper work queue (e.g., a Postgres-backed job table, or a message broker like NATS/Kafka). This decouples ingestion throughput from recording processing latency, provides retries with backoff, and allows separate scaling of recording workers.
- **In-memory cache sharding**: The `stats.Cache` mutex becomes a bottleneck under high concurrency. Use a sharded map (e.g., `sync.Map`, or a fixed array of lock-guarded buckets keyed by `hash(accountID) % N`) to reduce contention.

### Infrastructure

- **Rate limiting**: Apply per-account and global rate limits at the load balancer or API gateway to protect the system from bursts and abuse.
- **Backpressure**: If the database falls behind, the app should return `429 Too Many Requests` or `503 Service Unavailable` rather than queuing unboundedly in memory.
- **Observability**: Add metrics (Prometheus) for ingestion rate, dedup hit rate, transaction latency, recording queue depth, and goroutine count. Set alerts on saturation signals.
- **Autoscaling**: Use Kubernetes HPA (or equivalent) keyed on CPU/memory and custom metrics (queue depth) to automatically scale app pods.
