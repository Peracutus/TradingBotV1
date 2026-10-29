"""Run liveness is separate from virtual account and experiment state."""
from __future__ import annotations
import json
import os
import time
import uuid
from pathlib import Path
from .models import canonical

TERMINAL = {'FAILED', 'STOPPED', 'COMPLETED'}
HEARTBEAT_TTL_MS = 20_000
MAX_HEARTBEAT_GAP_MS = 30_000


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f'.{os.getpid()}.tmp')
    with temporary.open('w') as stream:
        stream.write(canonical(value) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def liveness(document, now_ms):
    state = document.get('process_state', 'STALE')
    if state in TERMINAL:
        return state
    heartbeat = document.get('last_update')
    return 'RUNNING' if heartbeat is not None and 0 <= now_ms - heartbeat <= HEARTBEAT_TTL_MS else 'STALE'


class PaperRun:
    def __init__(self, path, *, run_id=None, wall_ms=None, monotonic=None, required_ms=86_400_000):
        self.path = Path(path)
        now = int(time.time() * 1000) if wall_ms is None else wall_ms
        self.start_monotonic = time.monotonic() if monotonic is None else monotonic
        self.document = dict(run_id=run_id or uuid.uuid4().hex, pid=os.getpid(),
            started_at=now, last_update=now, observation_started_at=None,
            observed_uptime_ms=0, required_ms=required_ms, status='RUNNING_NOT_PASSED',
            process_state='RUNNING', experiment_state='STARTING', connections=0,
            heartbeat_gaps=[], protection_interruptions=0)
        self.observation_monotonic = None
        self.persist()

    def begin_observation(self, now_ms, monotonic):
        self.document.update(observation_started_at=now_ms, last_update=now_ms, experiment_state='PAPER')
        self.observation_monotonic = monotonic
        self.persist()

    def tick(self, now_ms, monotonic, *, connections=0, processed=0, unhealthy=False):
        previous = self.document['last_update']
        if self.observation_monotonic is not None:
            if now_ms - previous > MAX_HEARTBEAT_GAP_MS or now_ms < previous:
                self.document['heartbeat_gaps'].append(dict(previous=previous, current=now_ms))
            self.document['observed_uptime_ms'] = max(0, int((monotonic - self.observation_monotonic) * 1000))
        if unhealthy:
            self.document['protection_interruptions'] += 1
        self.document.update(last_update=now_ms, connections=connections, processed_batches=processed)
        self.persist()

    def finish(self, state, now_ms, *, error=None):
        assert state in TERMINAL
        self.document.update(process_state=state, ended_at=now_ms, last_update=now_ms,
            status={'FAILED': 'FAILED', 'STOPPED': 'INCOMPLETE_OR_REVIEW_REQUIRED', 'COMPLETED': 'PASSED'}[state])
        if error is not None:
            self.document['error'] = str(error)
        self.persist()

    def view(self, now_ms=None):
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        return dict(self.document, process_state=liveness(self.document, now_ms))

    def persist(self):
        atomic_json(self.path, self.document)


def record_process_exit(acceptance_path, snapshot_path, returncode, now_ms):
    """External witness handles SIGKILL and exceptions before child cleanup."""
    acceptance_path = Path(acceptance_path)
    document = json.loads(acceptance_path.read_text()) if acceptance_path.exists() else {}
    state = document.get('process_state')
    if returncode != 0 or state not in TERMINAL:
        document.update(process_state='FAILED', status='FAILED', ended_at=now_ms, last_update=now_ms,
            error=document.get('error', f'process exited without terminal status (returncode={returncode})'))
        atomic_json(acceptance_path, document)
    snapshot_path = Path(snapshot_path)
    snapshot = json.loads(snapshot_path.read_text()) if snapshot_path.exists() else {}
    snapshot.update(status=document.get('process_state', 'FAILED'), run=document,
        process_exit_code=returncode, process_exit_confirmed_at=now_ms)
    atomic_json(snapshot_path, snapshot)
    return document
