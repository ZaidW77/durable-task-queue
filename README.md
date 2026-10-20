# Durable Task Queue

A local Python and SQLite implementation of a **persistent work queue with at-least-once processing attempts**. It demonstrates atomic task claims, idempotent enqueue requests, lease recovery, bounded retries, dead-letter handling, and tests for competing workers.

The project makes queue mechanics visible in a small codebase suitable for explaining in an interview. It uses the Python standard library and runs without AWS credentials or a cloud service. It is an educational local queue; handlers still need idempotent side effects because a task can execute more than once.

## Features

- Persist JSON task payloads and statuses in a file-backed SQLite database using WAL mode.
- Deduplicate enqueue requests with a unique idempotency key while preserving the first payload.
- Serialize claims with `BEGIN IMMEDIATE` so competing workers cannot claim the same active task.
- Issue a fresh lease token per claim and reject expired or stale acknowledgements.
- Retry explicit failures with bounded exponential backoff.
- Reclaim expired leases within the attempt limit and retain exhausted tasks in a `dead` state.
- Expose status counts through `stats()` and task metadata through `inspect()`.

## Architecture and technologies

```text
Producer -> enqueue(key, payload) -> SQLite tasks table
                                         |
                                  claim() transaction
                                         |
                                         v
                                   Worker / handler
                                   /             \
                           complete()           fail()
                               |                /    \
                           completed       pending   dead
                                            retry    exhausted
```

A crashed worker leaves a leased task behind. A later `claim()` either reclaims its expired lease or moves it to `dead` if its attempt budget is exhausted.

| Component | Responsibility |
| --- | --- |
| `durablequeue/queue.py` | Queue API, schema, transactions, leases, and retry policy |
| `durablequeue/__init__.py` | Public `DurableQueue`, `Task`, and `LeaseLost` exports |
| `durablequeue/demo.py` | Deterministic simulation of transient and persistent failures |
| `tests/` | Standard-library `unittest` suite using temporary databases |
| `pyproject.toml` | Python package metadata and optional installation |
| `.github/workflows/tests.yml` | Automated tests, installation, and demo checks |

**Stack:** Python 3.10+, SQLite via `sqlite3`, JSON serialization, UUID lease tokens, `argparse`, and `unittest`. No third-party runtime packages are required.

## Setup and installation

```bash
git clone https://github.com/ZaidW77/durable-task-queue.git
cd durable-task-queue
python3 -m venv .venv
source .venv/bin/activate
```

On Windows, use `py -3 -m venv .venv` and `.venv\Scripts\Activate.ps1` in PowerShell. The examples below use the activated environment's `python`.

Run directly from the repository root, or optionally install the package to use it from another directory:

```bash
python -m pip install .
```

Installation downloads the `setuptools` build dependency if needed. Running from the checkout needs no package installation. Python must include the standard `sqlite3` module. The database's parent directory must already exist and be writable. Use a file path; `:memory:` and empty paths are rejected because operations use separate connections.

## Run the demo

Choose a fresh database path for the initial result:

```bash
python -m durablequeue.demo --db demo_queue.db
```

The demo enqueues a notification, repeats its key with a different payload, and enqueues an operation that keeps failing. It advances an explicit simulation clock without sleeping. The notification succeeds on attempt two; the other task reaches its two-attempt limit.

Expected output:

```json
{
  "stats": {
    "pending": 0,
    "leased": 0,
    "completed": 1,
    "dead": 1
  },
  "tasks": [
    {
      "id": 1,
      "idem_key": "notification-001",
      "status": "completed",
      "attempts": 2,
      "max_attempts": 3,
      "error": null
    },
    {
      "id": 3,
      "idem_key": "notification-002",
      "status": "dead",
      "attempts": 2,
      "max_attempts": 2,
      "error": "Simulated upstream timeout"
    }
  ]
}
```

The duplicate insert can consume an SQLite AUTOINCREMENT value, so task IDs need not be consecutive. Rerunning against the same database preserves its existing tasks and statuses. To replay the initial simulation, choose another unused database filename. Local databases and their WAL/journal files are ignored by Git.

## Use the Python API

```python
from durablequeue import DurableQueue

queue = DurableQueue("jobs.db")
queue.enqueue("email-001", {"operation": "send_email"}, max_attempts=3)
task = queue.claim(lease_seconds=30)
if task is not None:
    # Perform idempotent work here, then acknowledge before the lease expires.
    queue.complete(task)
print(queue.stats())
```

For a failed handler, call `queue.fail(task, "failure reason")` while the lease is valid. `complete()` and `fail()` raise `LeaseLost` after expiry or reassignment. Each operation commits or rolls back its transaction and closes its connection; there is no queue-level `close()` requirement.

## Task lifecycle and guarantees

| Operation / event | Result |
| --- | --- |
| `enqueue()` with a new key | Insert `pending` task |
| `enqueue()` with an existing key | Return original ID without replacing its payload, including completed/dead tasks |
| `claim()` | Select a ready task ordered by `available_at`, then ID; increment attempts and mark `leased` |
| `complete()` with current unexpired token | Mark `completed` |
| `fail()` with attempts remaining | Return to `pending` after backoff |
| `fail()` at the attempt limit | Mark `dead` |
| Expired lease with attempts remaining | Eligible for a new claim and token |
| Expired final lease | Next `claim()` moves it to `dead` |

The default retry delays are 1, 2, 4, ... seconds, capped by `max_backoff` (default 60 seconds); the exponent is additionally capped at 20. The default `max_attempts` is 3 and includes the initial claim. Enqueue payloads must be dictionaries containing JSON-serializable values, with no NaN/infinity; keys must be nonblank strings, and attempt limits must be positive integers. Explicit times and durations must be finite numbers, and lease/backoff durations must be positive. Omit `now` in normal use to use the system clock; explicit times support deterministic tests and the demo.

## Testing

Run the complete suite from the repository root:

```bash
python -m unittest discover -s tests -v
```

Tests cover duplicate enqueue behavior, retry timing, exponential backoff, dead letters after explicit failures and crashed final attempts, stale lease rejection, persistence, claim ordering, concurrent claims, lease expiry during lock contention, input validation, connection cleanup, and transaction rollback. Tests use temporary databases and controlled clocks rather than sleeping.

GitHub Actions is configured to run the suite, install the package, and run the installed demo on Python 3.10, 3.12, and 3.14 for pushes and pull requests.

## Design decisions and limitations

- **At-least-once attempts:** lease recovery allows repeated execution, up to the attempt budget. A crash after an external side effect but before acknowledgement can duplicate that effect.
- **Local lease protection:** random lease tokens protect queue acknowledgements. They do not fence external services or guarantee exactly-once side effects.
- **SQLite concurrency:** write transactions serialize competing claims on one local database. This is not a multi-region queue or a high-throughput service.
- **Pull-based recovery:** no background sweeper or heartbeat exists. `stats()` and `inspect()` report stored statuses; expired final leases become dead during the next `claim()`.
- **Local data:** payloads are stored as JSON without application-level encryption or authentication. Use synthetic data for the demo and protect the database using filesystem permissions.
- **Interview discussion:** explain the crash window, why an idempotency key differs from handler idempotency, how transactions prevent double claims, and how the design differs from a managed service such as Amazon SQS.
