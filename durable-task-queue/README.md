# Durable Task Queue

A runnable portfolio implementation of a **persistent, at-least-once work queue** using Python and SQLite. It illustrates concepts used by cloud platforms: atomic claims, per-task idempotency keys, lease timeouts, fencing tokens, bounded exponential backoff, dead-letter handling, operational metrics, and crash/restart recovery.

> **Scope:** Local SQLite demo, not a managed AWS product and not a production-grade distributed system. Task handlers must implement idempotent side effects, since at-least-once processing can execute them more than once.

## Quick start

Requires Python 3.10+ and no third-party dependencies.

```bash
python -m unittest discover -s tests -v
python -m durablequeue.demo --db demo_queue.db
```

The demo enqueues two distinct tasks plus a duplicate key, intentionally triggers transient and persistent failures, then prints the resulting task statuses. Run with a fresh DB to reproduce the initial result.

## Design

1. `enqueue(idempotency_key, payload)` atomically inserts an item or returns its existing ID if the key already exists. A duplicate request does **not** overwrite the original payload.
2. `claim()` opens a `BEGIN IMMEDIATE` SQLite transaction, selects one ready task, increments the attempt count, and assigns a random lease-fencing token.
3. `complete(task)` requires the *current* unexpired lease token. A stale worker cannot acknowledge a task reclaimed by another worker.
4. `fail(task)` schedules a retry after `min(2^(attempt-1), max_backoff)` seconds or moves an exhausted task to `dead`.
5. An expired lease makes the task eligible for reclamation. A process restart reads the same SQLite file.

## Guarantees and tradeoffs

- **Durable task state:** SQLite WAL mode persists queue records between processes.
- **At-least-once attempts:** lease recovery permits retries after a worker crashes; it does not prevent duplicated external side effects.
- **Lease fencing:** stale worker acknowledgements are rejected by token and expiry checks. Handlers must still fence or deduplicate external mutations.
- **Concurrency:** SQLite `BEGIN IMMEDIATE` serializes claims among writers on the same database. This is not designed for multi-region deployment or high-throughput workloads.
- **Dead letters:** terminally failed tasks remain queryable for inspection rather than disappearing.

## Tests / CI

The standard-library unit tests cover idempotent enqueues, retry timing, backoff, dead-letter transitions, stale leases, expiry, persistence, claim ordering, and invalid input. GitHub Actions reruns the suite on pushes and PRs.

## Future work

Add a worker service with heartbeats, structured logging, transaction-aware handler checkpoints, and configurable queue priorities. For real cloud applications, compare the design with Amazon SQS and DynamoDB idempotency tables.
