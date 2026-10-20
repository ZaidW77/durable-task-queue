"""Demonstration: a transient failure is retried, a permanent failure dies."""
import argparse
import json
from pathlib import Path

from .queue import DurableQueue


def main() -> None:
    parser = argparse.ArgumentParser(description='Run reproducible durable queue demo')
    parser.add_argument('--db', type=Path, default=Path('demo_queue.db'))
    args = parser.parse_args()
    q = DurableQueue(args.db)
    q.enqueue('notification-001', {'operation': 'notify', 'message': 'Hello'}, now=100)
    q.enqueue('notification-001', {'operation': 'notify', 'message': 'Duplicate'}, now=100)
    q.enqueue('notification-002', {'operation': 'unavailable'}, max_attempts=2, now=100)
    for moment in (100, 101, 102, 103, 104, 105):
        while (task := q.claim(now=moment)) is not None:
            if task.payload['operation'] == 'notify' and task.attempt >= 2:
                q.complete(task, now=moment)
            else:
                q.fail(task, 'Simulated upstream timeout', now=moment)
    print(json.dumps({'stats': q.stats(), 'tasks': q.inspect()}, indent=2))


if __name__ == '__main__':
    main()
