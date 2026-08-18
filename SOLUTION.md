# Solution

## 1. What was broken

Dedup was implemented as check-then-act: a SELECT to see if the event existed, then a separate INSERT, with no transaction and no unique constraint on `event_id` — so two concurrent redeliveries could both pass the check before either insert landed, creating duplicate rows and double-counted stats. The recording goroutine captured the HTTP request's context, which `net/http` cancels the moment the handler returns; `MarkRecordingProcessed` therefore ran against a cancelled context and failed, and the error was silently discarded. Those same goroutines weren't tracked by any wait group, so graceful shutdown via SIGTERM abandoned in-flight recording work without waiting for it to finish. Finally, the stats cache's `Record` method read and wrote the map without taking its own mutex, while `Get` did hold a read lock — so concurrent writes raced on both the map and the counters.

## 2. Why Postgres UNIQUE + ON CONFLICT over Redis SETNX

Postgres is already the durable source of truth for the event, so making the INSERT itself the dedup gate — via a UNIQUE constraint and ON CONFLICT DO NOTHING — keeps the check and the write atomic inside one transaction. A Redis SETNX introduces a second stateful system that can drift from Postgres (especially across crashes or restarts) and requires its own TTL policy to avoid unbounded memory growth, which in turn reopens a replay window after keys expire. Keeping dedup in Postgres eliminates that entire class of consistency problems with zero additional infrastructure.

## 3. What I'd change for 10k webhooks/sec

**Batch writes.** Instead of one transaction per webhook, buffer events into micro-batches and issue multi-row INSERTs, amortising the per-event round-trip and WAL-write overhead against Postgres.

**Queue-based recording processing.** Replace the bare goroutine-per-recording model with a persistent work queue so that ingestion throughput is fully decoupled from recording-processing latency, and work survives process restarts.

**Horizontal scaling of app instances.** Because dedup is enforced by Postgres, any instance can safely handle any webhook, so adding instances behind a load balancer scales the HTTP and batching layer linearly.
