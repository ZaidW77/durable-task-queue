import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from durablequeue import DurableQueue, LeaseLost


class DurableQueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'queue.db'
        self.q = DurableQueue(self.path)

    def test_enqueue_and_complete(self):
        self.q.enqueue('a', {'value': 1}, now=10)
        task = self.q.claim(now=10)
        self.assertEqual(task.payload, {'value': 1})
        self.q.complete(task, now=11)
        self.assertEqual(self.q.stats()['completed'], 1)

    def test_idempotency_key_does_not_replace_payload(self):
        a = self.q.enqueue('x', {'v': 'original'}, now=10)
        b = self.q.enqueue('x', {'v': 'replacement'}, now=10)
        self.assertEqual(a, b)
        self.assertEqual(self.q.claim(now=10).payload, {'v': 'original'})

    def test_retry_waits_for_backoff(self):
        self.q.enqueue('x', {}, now=10)
        task = self.q.claim(now=10)
        self.assertEqual(self.q.fail(task, 'timeout', now=10.5), 'pending')
        self.assertIsNone(self.q.claim(now=11.4))
        self.assertEqual(self.q.claim(now=11.5).attempt, 2)

    def test_exponential_backoff(self):
        self.q.enqueue('x', {}, now=0)
        one = self.q.claim(now=0)
        self.q.fail(one, 'one', now=0)
        two = self.q.claim(now=1)
        self.q.fail(two, 'two', now=1)
        self.assertIsNone(self.q.claim(now=2.9))
        self.assertEqual(self.q.claim(now=3).attempt, 3)

    def test_dead_letter_after_max_attempts(self):
        self.q.enqueue('x', {}, max_attempts=2, now=0)
        one = self.q.claim(now=0)
        self.q.fail(one, 'error', now=0)
        two = self.q.claim(now=1)
        self.assertEqual(self.q.fail(two, 'still down', now=1), 'dead')
        self.assertIsNone(self.q.claim(now=10))
        self.assertEqual(self.q.stats()['dead'], 1)

    def test_expired_lease_can_be_reclaimed(self):
        self.q.enqueue('x', {}, now=0)
        old = self.q.claim(now=0, lease_seconds=2)
        self.assertIsNone(self.q.claim(now=1))
        new = self.q.claim(now=2)
        self.assertNotEqual(old.lease_token, new.lease_token)
        with self.assertRaises(LeaseLost):
            self.q.complete(old, now=2.5)
        self.q.complete(new, now=3)

    def test_complete_after_expiry_is_rejected(self):
        self.q.enqueue('x', {}, now=0)
        task = self.q.claim(now=0, lease_seconds=1)
        with self.assertRaises(LeaseLost):
            self.q.complete(task, now=2)

    def test_persists_across_instances(self):
        self.q.enqueue('x', {'durable': True}, now=0)
        q2 = DurableQueue(self.path)
        self.assertTrue(q2.claim(now=0).payload['durable'])

    def test_claim_oldest_ready(self):
        self.q.enqueue('first', {}, now=0)
        self.q.enqueue('later', {}, now=5)
        self.assertEqual(self.q.claim(now=5).key, 'first')

    def test_empty_claim(self):
        self.assertIsNone(self.q.claim(now=0))

    def test_invalid_arguments(self):
        with self.assertRaises(ValueError):
            self.q.enqueue('', {}, now=0)
        with self.assertRaises(ValueError):
            self.q.enqueue('x', {}, max_attempts=0, now=0)
        with self.assertRaises(ValueError):
            self.q.claim(lease_seconds=0, now=0)

    def test_expired_final_attempt_is_dead_lettered(self):
        self.q.enqueue('crashing', {}, max_attempts=2, now=0)
        first = self.q.claim(now=0, lease_seconds=1)
        final = self.q.claim(now=1, lease_seconds=1)
        self.assertEqual((first.attempt, final.attempt), (1, 2))
        self.assertIsNone(self.q.claim(now=1.5))
        self.assertIsNone(self.q.claim(now=2))
        self.assertIsNone(self.q.claim(now=100))
        self.assertEqual(self.q.stats()['dead'], 1)
        self.assertEqual(self.q.inspect()[0]['attempts'], 2)
        self.assertIn('Lease expired', self.q.inspect()[0]['error'])
        with self.assertRaises(LeaseLost):
            self.q.complete(final, now=2)
        with self.assertRaises(LeaseLost):
            self.q.fail(final, 'late error', now=2)

    def test_expired_exhausted_task_does_not_block_ready_task(self):
        self.q.enqueue('crashing', {}, max_attempts=1, now=0)
        self.q.claim(now=0, lease_seconds=1)
        self.q.enqueue('ready', {}, now=0)
        self.assertEqual(self.q.claim(now=1).key, 'ready')
        self.assertEqual(self.q.stats()['dead'], 1)

    def test_stale_worker_cannot_fail_reclaimed_task(self):
        self.q.enqueue('x', {}, now=0)
        old = self.q.claim(now=0, lease_seconds=1)
        current = self.q.claim(now=1)
        with self.assertRaises(LeaseLost):
            self.q.fail(old, 'stale failure', now=1)
        self.q.complete(current, now=2)
        self.assertEqual(self.q.stats()['completed'], 1)

    def test_rejects_invalid_keys_and_attempt_limits(self):
        for key in (None, 1, [], '', '   '):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.q.enqueue(key, {}, now=0)
        for attempts in (0, -1, 1.5, True, None, '2', float('nan'), float('inf')):
            with self.subTest(attempts=attempts), self.assertRaises(ValueError):
                self.q.enqueue('x', {}, max_attempts=attempts, now=0)
        self.assertEqual(sum(self.q.stats().values()), 0)

    def test_rejects_nonfinite_times_without_changing_task(self):
        self.q.enqueue('valid', {}, now=0)
        task = self.q.claim(now=0)
        for moment in (float('nan'), float('inf'), -float('inf')):
            operations = {
                'enqueue': lambda: self.q.enqueue('invalid', {}, now=moment),
                'claim': lambda: self.q.claim(now=moment),
                'complete': lambda: self.q.complete(task, now=moment),
                'fail': lambda: self.q.fail(task, 'error', now=moment),
            }
            for name, operation in operations.items():
                with self.subTest(method=name, now=moment), self.assertRaises(ValueError):
                    operation()
        self.assertEqual(self.q.stats(), {'pending': 0, 'leased': 1, 'completed': 0, 'dead': 0})

    def test_rejects_invalid_durations_without_stranding_task(self):
        self.q.enqueue('valid', {}, now=0)
        invalid = (float('nan'), float('inf'), -float('inf'), 0, -1)
        for duration in invalid:
            with self.subTest(lease_seconds=duration), self.assertRaises(ValueError):
                self.q.claim(now=0, lease_seconds=duration)
        task = self.q.claim(now=0)
        for duration in invalid:
            with self.subTest(max_backoff=duration), self.assertRaises(ValueError):
                self.q.fail(task, 'error', now=0, max_backoff=duration)
        self.q.complete(task, now=1)
        self.assertEqual(self.q.stats()['completed'], 1)

    def test_rejects_non_file_databases(self):
        for path in (':memory:', ''):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, 'file-backed'):
                DurableQueue(path)

    def test_concurrent_instances_do_not_claim_same_task(self):
        other = DurableQueue(self.path)
        self.q.enqueue('one-task', {}, now=0)
        start = threading.Barrier(2)

        def claim(queue):
            start.wait(timeout=5)
            return queue.claim(now=0)

        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(claim, (self.q, other)))
        claimed = [task for task in results if task is not None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0].key, 'one-task')
        self.assertEqual(self.q.inspect()[0]['attempts'], 1)

    def test_failed_claim_rolls_back_expiry_cleanup(self):
        self.q.enqueue('crashing', {}, max_attempts=1, now=0)
        self.q.claim(now=0, lease_seconds=1)
        self.q.enqueue('ready', {}, now=0)
        with patch('durablequeue.queue.uuid.uuid4', side_effect=RuntimeError('token failure')):
            with self.assertRaisesRegex(RuntimeError, 'token failure'):
                self.q.claim(now=1)
        self.assertEqual([row['status'] for row in self.q.inspect()], ['leased', 'pending'])
        self.assertEqual(self.q.claim(now=1).key, 'ready')
        self.assertEqual(self.q.stats()['dead'], 1)

    def test_connections_close_after_success_and_failure(self):
        opened = []
        connect = sqlite3.connect

        def tracked_connect(*args, **kwargs):
            db = connect(*args, **kwargs)
            opened.append(db)
            return db

        with patch('durablequeue.queue.sqlite3.connect', side_effect=tracked_connect):
            queue = DurableQueue(self.path)
            queue.enqueue('x', {}, now=0)
            with patch('durablequeue.queue.uuid.uuid4', side_effect=RuntimeError('token failure')):
                with self.assertRaises(RuntimeError):
                    queue.claim(now=0)
            task = queue.claim(now=0)
            queue.fail(task, 'retry', now=0)
            queue.complete(queue.claim(now=1), now=1)
            queue.stats()
            queue.inspect()
        for db in opened:
            with self.assertRaises(sqlite3.ProgrammingError):
                db.execute('SELECT 1')

    def _run_after_lock_wait(self, operation):
        """Advance a fake clock while an operation waits for a real write lock."""
        waiting = threading.Event()
        clock = [0.0]
        connect = sqlite3.connect
        blocker = connect(self.path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')

        class NotifyingConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                if sql == 'BEGIN IMMEDIATE':
                    waiting.set()
                return super().execute(sql, parameters)

        def notifying_connect(*args, **kwargs):
            return connect(*args, factory=NotifyingConnection, **kwargs)

        try:
            with patch('durablequeue.queue.sqlite3.connect', side_effect=notifying_connect), \
                    patch('durablequeue.queue.time.time', side_effect=lambda: clock[0]), \
                    ThreadPoolExecutor(max_workers=1) as workers:
                result = workers.submit(operation)
                try:
                    self.assertTrue(waiting.wait(timeout=5), 'operation did not request write lock')
                    clock[0] = 2.0
                finally:
                    blocker.commit()
                return result.result(timeout=5)
        finally:
            blocker.close()

    def test_default_claim_clock_is_read_after_lock_wait(self):
        self.q.enqueue('ready-after-wait', {}, now=1)
        task = self._run_after_lock_wait(lambda: self.q.claim(lease_seconds=1))
        self.assertIsNotNone(task)
        self.q.complete(task, now=2.5)
        self.assertEqual(self.q.stats()['completed'], 1)

    def test_default_enqueue_clock_is_read_after_lock_wait(self):
        task_id = self._run_after_lock_wait(lambda: self.q.enqueue('new', {}))
        self.assertIsNone(self.q.claim(now=1))
        self.assertEqual(self.q.claim(now=2).id, task_id)

    def test_complete_rejects_lease_that_expires_during_lock_wait(self):
        self.q.enqueue('x', {}, now=0)
        task = self.q.claim(now=0, lease_seconds=1)
        with self.assertRaises(LeaseLost):
            self._run_after_lock_wait(lambda: self.q.complete(task))
        self.assertEqual(self.q.stats()['leased'], 1)
        self.assertEqual(self.q.claim(now=2).attempt, 2)

    def test_fail_rejects_lease_that_expires_during_lock_wait(self):
        self.q.enqueue('x', {}, now=0)
        task = self.q.claim(now=0, lease_seconds=1)
        with self.assertRaises(LeaseLost):
            self._run_after_lock_wait(lambda: self.q.fail(task, 'late failure'))
        self.assertEqual(self.q.stats()['leased'], 1)
        self.assertEqual(self.q.claim(now=2).attempt, 2)

    def test_default_retry_time_is_calculated_after_lock_wait(self):
        self.q.enqueue('x', {}, now=0)
        task = self.q.claim(now=0, lease_seconds=10)
        state = self._run_after_lock_wait(lambda: self.q.fail(task, 'retry'))
        self.assertEqual(state, 'pending')
        self.assertIsNone(self.q.claim(now=2.9))
        self.assertEqual(self.q.claim(now=3).attempt, 2)

    def test_overflowing_lease_expiry_leaves_task_ready(self):
        self.q.enqueue('x', {}, now=0)
        with self.assertRaisesRegex(ValueError, 'lease expiry'):
            self.q.claim(now=1e308, lease_seconds=1e308)
        self.assertEqual(self.q.inspect()[0]['attempts'], 0)
        self.assertEqual(self.q.claim(now=0).attempt, 1)


if __name__ == '__main__':
    unittest.main()
