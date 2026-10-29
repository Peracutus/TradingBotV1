"""External observer. Starts exactly one new paper run; never auto-restarts it."""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from .lifecycle import atomic_json, liveness, record_process_exit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['run', 'status'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--name', default='paper-bugfix-v1-0-1')
    parser.add_argument('--hours', type=float, default=24)
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    acceptance = Path(args.db).with_suffix('.acceptance.json')
    process_file = Path(args.db).with_suffix('.process.json')
    if args.mode == 'status':
        document = json.loads(acceptance.read_text())
        document['process_state'] = liveness(document, int(time.time() * 1000))
        print(json.dumps(document, ensure_ascii=False, indent=2))
        return
    if Path(args.db).exists() or acceptance.exists():
        raise ValueError('new acceptance requires a new database and run_id')
    run_id = uuid.uuid4().hex
    child = subprocess.Popen([sys.executable, '-m', 'trading_v1', 'paper', '--db', args.db,
        '--output', args.output, '--name', args.name, '--hours', str(args.hours),
        '--port', str(args.port), '--run-id', run_id])
    atomic_json(process_file, dict(run_id=run_id, pid=child.pid, supervisor_pid=os.getpid(), process_state='RUNNING'))
    returncode = child.wait()
    now = int(time.time() * 1000)
    document = record_process_exit(acceptance, args.output, returncode, now)
    atomic_json(process_file, dict(run_id=run_id, pid=child.pid, returncode=returncode,
        process_state=document['process_state'], ended_at=now))
    raise SystemExit(returncode)


if __name__ == '__main__':
    main()
