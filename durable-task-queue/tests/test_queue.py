import tempfile
import unittest
from pathlib import Path
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


if __name__ == '__main__':
    unittest.main()
