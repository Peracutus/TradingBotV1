"""Regression coverage for the failed 2026-10-19 run and its liveness reporting."""
import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from trading_v1.models import *
from trading_v1.engine import Engine
from trading_v1.store import Store, JournalConflict, MarketDataConflict
from trading_v1.data import BybitSource
from trading_v1.lifecycle import PaperRun, liveness, record_process_exit
from test_trading_v1 import events, meta, decision, T


class BugfixRegression(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'run.sqlite'
        self.config = ExperimentConfig()
        self.store = Store(self.path, self.config)
        self.engine = Engine(self.config, self.store, [meta()])

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_active_stale_exit_pending_active_same_votes_new_cause(self):
        self.engine.process(events(T, '100.00'))
        self.engine.heartbeat(H4 + 91_000)
        self.engine.heartbeat(H4 + 301_000)
        self.assertEqual(self.engine.accounts['aggregate'].state, 'EXIT_PENDING')
        before = self.store.db.execute("SELECT count(*) FROM portfolio_decisions").fetchone()[0]
        # Recovered history caused the original ACTIVE -> EXIT_PENDING -> ACTIVE collision.
        batch = [replace(e, received_at=H4 + 302_000, recovered=True) for e in events(T + MINUTE, '101')]
        self.assertTrue(self.engine.process(batch))
        self.assertEqual(self.engine.accounts['aggregate'].state, 'ACTIVE')
        self.assertGreater(self.store.db.execute("SELECT count(*) FROM portfolio_decisions").fetchone()[0], before)
        rows = [json.loads(r[0]) for r in self.store.db.execute('SELECT payload FROM portfolio_decisions')]
        self.assertTrue(all(r['cause_event'] for r in rows))
        expected = self.engine.snapshot()
        self.assertFalse(self.engine.process(batch))
        self.assertEqual(expected, self.engine.snapshot())

    def test_identical_bar_twice_never_repeats_signal_or_fill(self):
        batch = events()
        self.engine.process(batch, [decision(batch[0], meta())])
        for i in (1, 2):
            frame = events(T + i * MINUTE)
            self.engine.process(frame)
            self.assertFalse(self.engine.process(frame))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM signals").fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM fills WHERE account='s'").fetchone()[0], 1)

    def test_repeated_active_target_with_changed_real_exposure_has_new_decision(self):
        first=events()
        self.engine.process(first,[decision(first[0],meta())])
        self.engine.process(events(T+MINUTE));self.engine.process(events(T+2*MINUTE))
        self.assertGreater(self.engine.accounts['aggregate'].positions['BTC'].quantity,0)
        self.engine.heartbeat(self.engine.now+91_000)
        self.assertEqual(self.engine.accounts['aggregate'].state,'DATA_STALE')
        self.engine.process(events(T+3*MINUTE,'101'))
        self.assertEqual(self.engine.accounts['aggregate'].state,'ACTIVE')
        self.assertEqual(self.store.recover(),self.engine.snapshot())

    def test_rest_ws_equivalence_normalizes_numbers_only(self):
        rest = [replace(e, source='Bybit_public', recovered=True, transport='REST') for e in events(T, '100.00')]
        ws = [replace(e, received_at=e.close_ms+250, source='Bybit_public', transport='WebSocket', volume=D('0.000')) for e in events(T, '100')]
        self.engine.process(rest)
        state = self.engine.snapshot()
        self.assertEqual(rest[0].event_id, ws[0].event_id)
        self.assertEqual(rest[0].data_hash, ws[0].data_hash)
        self.assertFalse(self.engine.process(ws))
        self.assertEqual(state, self.engine.snapshot())
        source = BybitSource(Path(self.tmp.name) / 'raw')
        source.observer = self.engine.observe_market
        source.observe(ws[0])
        self.assertFalse(self.store.conflict_path.exists())

    def test_real_correction_preserves_history_and_blocks_restart_writes(self):
        batch = events()
        self.engine.process(batch, [decision(batch[0], meta())])
        before = self.engine.snapshot()
        count = self.store.db.execute('SELECT count(*) FROM events').fetchone()[0]
        corrected = replace(batch[0], close=D('100.5'))
        with self.assertRaises(MarketDataConflict):
            self.engine.process([corrected, batch[1]])
        self.assertEqual(self.engine.snapshot(), before)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM events').fetchone()[0], count)
        diagnostic = json.loads(self.store.conflict_path.read_text().splitlines()[0])
        self.assertEqual(diagnostic['policy'], 'KEEP_ACCEPTED_VERSION_HALT_REQUIRE_REVIEW')
        self.assertNotEqual(diagnostic['old_market_hash'], diagnostic['new_market_hash'])
        self.assertEqual(diagnostic['event_id'], corrected.event_id)
        with self.assertRaisesRegex(ValueError, 'financial processing blocked'):
            self.engine.process(events(T + MINUTE))
        self.store.close()
        self.store = Store(self.path, self.config)
        restored = Engine(self.config, self.store, [meta()])
        self.assertEqual(restored.snapshot(), before)
        with self.assertRaisesRegex(ValueError, 'financial processing blocked'):
            restored.heartbeat(H4 + 1000)

    def test_inflight_source_conflict_not_hidden_as_connection_retry(self):
        source = BybitSource(Path(self.tmp.name) / 'raw')
        source.reject_correction = self.engine.market_conflict
        original = replace(events()[0], source='Bybit_public', transport='REST')
        source.observe(original)
        with self.assertRaises(MarketDataConflict):
            source.observe(replace(original, close=D('100.2'), transport='WebSocket'))
        self.assertTrue(self.store.integrity_error)

    def test_internal_fill_conflict_rolls_back_and_latches_financial_halt(self):
        batch = events()
        self.engine.process(batch, [decision(batch[0], meta())])
        self.engine.process(events(T + MINUTE))
        pending = next(o for o in self.engine.orders.values() if o.account_id == 's')
        frame = events(T + 2 * MINUTE)
        fill_id = key(pending.order_id, frame[0].event_id, 0)
        # A conflicting immutable event exists; it must never be replaced by a fill.
        with self.store.transaction():
            self.store.append(fill_id, 'FILL', frame[0].open_ms, {'corrupt_fixture': True})
        before = self.engine.snapshot()
        with self.assertRaises(JournalConflict):
            self.engine.process(frame)
        self.assertEqual(before, self.engine.snapshot())
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM fills').fetchone()[0], 0)
        diagnostic = json.loads(self.store.conflict_path.read_text().splitlines()[0])
        self.assertEqual(diagnostic['event_id'], fill_id)
        self.assertTrue(diagnostic['diff'])
        with self.assertRaisesRegex(ValueError, 'financial processing blocked'):
            self.engine.process(frame)

    def test_repeated_timer_is_noop_and_restart_has_no_duplicate_execution(self):
        b = events()
        self.engine.process(b, [decision(b[0], meta())])
        self.engine.heartbeat(H4 + 1000)
        expected = self.engine.snapshot()
        self.engine.heartbeat(H4 + 1000)
        self.assertEqual(expected, self.engine.snapshot())
        for i in (1, 2):
            self.engine.process(events(T+i*MINUTE))
        expected = self.engine.snapshot()
        self.store.close()
        self.store = Store(self.path, self.config)
        restored = Engine(self.config, self.store, [meta()])
        self.assertEqual(expected, restored.snapshot())
        self.assertFalse(restored.process(events(T+2*MINUTE)))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM fills WHERE account='s'").fetchone()[0], 1)

    def test_same_internal_id_different_payload_and_time_still_rejected(self):
        self.store.append('decision', 'PORTFOLIO_DECISION', 1, {'v': '0.00'})
        self.assertEqual(self.store.append('decision', 'PORTFOLIO_DECISION', 1, {'v': '0.00'}), 1)
        with self.assertRaises(JournalConflict):
            self.store.append('decision', 'PORTFOLIO_DECISION', 1, {'v': '0'})


class LifecycleRegression(unittest.TestCase):
    def test_fresh_running_stale_and_terminal_statuses(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = PaperRun(Path(tmp)/'acceptance.json', wall_ms=1000, monotonic=0)
            self.assertEqual(run.view(2000)['process_state'], 'RUNNING')
            self.assertEqual(run.view(22_000)['process_state'], 'STALE')
            run.finish('FAILED', 2000, error='injected fatal')
            self.assertEqual(run.view(99_000)['process_state'], 'FAILED')
            self.assertEqual(json.loads(run.path.read_text())['status'], 'FAILED')

    def test_final_counter_and_gap_detection_do_not_fake_24h(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = PaperRun(Path(tmp)/'a.json', wall_ms=1000, monotonic=0)
            run.begin_observation(1000, 0)
            run.tick(6000, 5)
            run.tick(86_401_000, 86400)
            self.assertEqual(run.document['observed_uptime_ms'], 86_400_000)
            self.assertTrue(run.document['heartbeat_gaps'])

    def test_external_witness_reports_abrupt_exit_and_updates_stale_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'a.json';snapshot=Path(tmp)/'s.json'
            run=PaperRun(path, wall_ms=1000, monotonic=0)
            snapshot.write_text('{"status":"PAPER_RUNNING"}')
            child=subprocess.run([sys.executable,'-c','import os; os._exit(7)'])
            record_process_exit(path,snapshot,child.returncode,2000)
            self.assertEqual(json.loads(path.read_text())['process_state'], 'FAILED')
            self.assertEqual(json.loads(snapshot.read_text())['status'], 'FAILED')
            self.assertEqual(json.loads(snapshot.read_text())['process_exit_code'],7)

    def test_new_run_does_not_accumulate_previous_uptime(self):
        with tempfile.TemporaryDirectory() as tmp:
            a=PaperRun(Path(tmp)/'a.json',wall_ms=0,monotonic=0)
            a.begin_observation(0,0);a.tick(3_600_000,3600);a.finish('STOPPED',3_600_000)
            b=PaperRun(Path(tmp)/'b.json',wall_ms=4_000_000,monotonic=4000)
            self.assertNotEqual(a.document['run_id'],b.document['run_id'])
            self.assertEqual(b.document['observed_uptime_ms'],0)

    def test_panel_fetch_failure_is_stale_not_running(self):
        from trading_v1.web import PAGE
        self.assertIn('STALE — heartbeat недоступен',PAGE)


class FatalPaperIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_unhandled_startup_failure_writes_failed_snapshot(self):
        from unittest.mock import patch,AsyncMock
        from types import SimpleNamespace
        from trading_v1.cli import paper
        with tempfile.TemporaryDirectory() as tmp:
            db=Path(tmp)/'run.sqlite';out=Path(tmp)/'status.json'
            args=SimpleNamespace(db=str(db),output=str(out),hours=24,run_id='fatal-fixture')
            with patch('trading_v1.data.BybitSource.bootstrap',new=AsyncMock(side_effect=RuntimeError('injected fatal'))):
                with self.assertRaisesRegex(RuntimeError,'injected fatal'):
                    await paper(args)
            self.assertEqual(json.loads(out.read_text())['status'],'FAILED')
            self.assertEqual(json.loads(db.with_suffix('.acceptance.json').read_text())['process_state'],'FAILED')


if __name__ == '__main__':unittest.main()
