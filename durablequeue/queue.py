"""Durable, claim-once-at-a-time task queue with lease-fenced acknowledgments.

This queue provides at-least-once attempts, not exactly-once side effects. An
expired worker may still execute a side effect, so real handlers must be
idempotent. Designed as a local portfolio/learning system, not a replacement
for SQS or a production orchestration service.
"""
from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


def _finite_number(value: float, name: str) -> float:
    """Keep invalid numeric values out of SQLite's time comparisons."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f'{name} must be a finite number')
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError(f'{name} must be a finite number') from exc
    if not math.isfinite(number):
        raise ValueError(f'{name} must be a finite number')
    return number


class LeaseLost(RuntimeError):
    """Raised when another worker owns a task or a lease has expired."""


@dataclass(frozen=True)
class Task:
    id: int
    key: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    lease_token: str


class DurableQueue:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        if not self.db_path or self.db_path == ':memory:':
            raise ValueError('db_path must name a file-backed SQLite database')
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        """Commit or roll back the operation, then always close its connection."""
        db = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
        try:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA busy_timeout=10000')
            with db:
                yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idem_key TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending', 'leased', 'completed', 'dead')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    available_at REAL NOT NULL,
                    leased_until REAL,
                    lease_token TEXT,
                    error TEXT
                )
            ''')
            db.execute('CREATE INDEX IF NOT EXISTS tasks_claim ON tasks(status, available_at, leased_until)')

    def enqueue(self, key: str, payload: dict[str, Any], *,
                max_attempts: int = 3, now: float | None = None) -> int:
        """Enqueue a task; reusing a key returns the original task id."""
        if (not isinstance(key, str) or not key.strip()
                or isinstance(max_attempts, bool) or not isinstance(max_attempts, int)
                or max_attempts < 1 or not isinstance(payload, dict)):
            raise ValueError('Requires nonblank string key, dict payload, and positive integer max_attempts')
        serialized = json.dumps(payload, sort_keys=True, allow_nan=False)
        moment = None if now is None else _finite_number(now, 'now')
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            moment = _finite_number(time.time(), 'now') if moment is None else moment
            db.execute('''INSERT OR IGNORE INTO tasks
                (idem_key, payload_json, max_attempts, available_at)
                VALUES (?, ?, ?, ?)''', (key, serialized, max_attempts, moment))
            row = db.execute('SELECT id FROM tasks WHERE idem_key=?', (key,)).fetchone()
            return int(row['id'])

    def claim(self, *, lease_seconds: float = 30,
              now: float | None = None) -> Task | None:
        """Atomically claim the oldest ready or expired task.

        Each new claim gets a random fencing token so stale workers cannot
        acknowledge a task that has been re-leased. An expired final attempt
        moves to dead-letter state instead of exceeding the retry budget.
        """
        lease_seconds = _finite_number(lease_seconds, 'lease_seconds')
        if lease_seconds <= 0:
            raise ValueError('lease_seconds must be positive')
        moment = None if now is None else _finite_number(now, 'now')
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            moment = _finite_number(time.time(), 'now') if moment is None else moment
            leased_until = _finite_number(moment + lease_seconds, 'lease expiry')
            db.execute('''UPDATE tasks SET status='dead', available_at=?,
                leased_until=NULL, lease_token=NULL, error='Lease expired after final attempt'
                WHERE status='leased' AND leased_until <= ? AND attempts >= max_attempts''',
                (moment, moment))
            row = db.execute('''SELECT * FROM tasks
                WHERE attempts < max_attempts
                  AND ((status='pending' AND available_at <= ?)
                    OR (status='leased' AND leased_until <= ?))
                ORDER BY available_at, id LIMIT 1''', (moment, moment)).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            attempt = row['attempts'] + 1
            db.execute('''UPDATE tasks SET status='leased', attempts=?,
                leased_until=?, lease_token=? WHERE id=?''',
                (attempt, leased_until, token, row['id']))
            return Task(row['id'], row['idem_key'], json.loads(row['payload_json']),
                        attempt, row['max_attempts'], token)

    def complete(self, task: Task, *, now: float | None = None) -> None:
        moment = None if now is None else _finite_number(now, 'now')
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            moment = _finite_number(time.time(), 'now') if moment is None else moment
            cursor = db.execute('''UPDATE tasks
                SET status='completed', leased_until=NULL, lease_token=NULL, error=NULL
                WHERE id=? AND status='leased' AND lease_token=? AND leased_until > ?''',
                (task.id, task.lease_token, moment))
            if cursor.rowcount != 1:
                raise LeaseLost(f'Task {task.id} lease is no longer valid')

    def fail(self, task: Task, reason: str, *, now: float | None = None,
             max_backoff: float = 60) -> str:
        """Reschedule with exponential backoff or move to dead-letter state."""
        max_backoff = _finite_number(max_backoff, 'max_backoff')
        if max_backoff <= 0:
            raise ValueError('max_backoff must be positive')
        moment = None if now is None else _finite_number(now, 'now')
        dead = task.attempt >= task.max_attempts
        next_state = 'dead' if dead else 'pending'
        delay = min(float(2 ** min(task.attempt - 1, 20)), max_backoff)
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            moment = _finite_number(time.time(), 'now') if moment is None else moment
            available_at = _finite_number(moment if dead else moment + delay, 'retry time')
            cursor = db.execute('''UPDATE tasks SET status=?, available_at=?,
                leased_until=NULL, lease_token=NULL, error=?
                WHERE id=? AND status='leased' AND lease_token=? AND leased_until > ?''',
                (next_state, available_at,
                 reason[:500], task.id, task.lease_token, moment))
            if cursor.rowcount != 1:
                raise LeaseLost(f'Task {task.id} lease is no longer valid')
        return next_state

    def stats(self) -> dict[str, int]:
        result = {'pending': 0, 'leased': 0, 'completed': 0, 'dead': 0}
        with self._connect() as db:
            for row in db.execute('SELECT status, COUNT(*) AS n FROM tasks GROUP BY status'):
                result[row['status']] = row['n']
        return result

    def inspect(self) -> list[dict]:
        with self._connect() as db:
            return [dict(row) for row in db.execute(
                'SELECT id, idem_key, status, attempts, max_attempts, error '
                'FROM tasks ORDER BY id'
            )]
