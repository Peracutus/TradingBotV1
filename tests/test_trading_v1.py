import asyncio,copy,json,sqlite3,tempfile,time,unittest
from pathlib import Path
from dataclasses import replace
from trading_v1.models import *
from trading_v1.store import Store
from trading_v1.engine import Engine
from trading_v1.risk import RiskManager
from trading_v1.orchestrator import Orchestrator
from trading_v1.registry import records

T=H4-MINUTE

def events(t=T,price='100',low=None,source='test'):
    price=D(price)
    return [MarketEvent(n,t,price,max(price,D('101')),D(low) if low else price,price,source=source) for n in ('BTC','ETH')]
def meta(n='BTC',name='s',canonical_id=None):
    return dict(agent_id=name+':'+n,implementation_id=name+':v1',canonical_id=canonical_id or name,instrument=n,account_id=name,family='trend',policy='base',aggregate=True,admission='R1')
def decision(e,m,action=Action.ENTRY,target=ONE,available=None,valid=None):
    now=e.close_ms if available is None else available
    return StrategyDecision(m['agent_id'],m['implementation_id'],m['canonical_id'],e.instrument,'trend',action,target,now,valid or now+H4+90_000,e.event_id,D('1'),'test')

class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'e.sqlite';self.c=ExperimentConfig();self.store=Store(self.path,self.c);self.m=meta();self.e=Engine(self.c,self.store,[self.m])
    def tearDown(self):self.store.close();self.tmp.cleanup()
    def enter(self):
        batch=events();self.e.process(batch,[decision(batch[0],self.m)])
        self.e.process(events(T+MINUTE));self.e.process(events(T+2*MINUTE))
    def fills(self):return [json.loads(r[0]) for r in self.store.db.execute("SELECT payload FROM events WHERE type='FILL' ORDER BY seq")]
    def test_latency_and_same_kernel(self):
        batch=events();self.e.process(batch,[decision(batch[0],self.m)]);self.assertFalse(self.fills())
        self.e.process(events(T+MINUTE));self.assertFalse(self.fills())
        self.e.process(events(T+2*MINUTE));self.assertEqual(len(self.fills()),2)
        self.assertEqual(self.fills()[0]['time_ms'],T+2*MINUTE)
    def test_gap_stop_and_recovery_does_not_save_trade(self):
        self.enter();self.e.process(events(T+3*MINUTE,'95',low='90'))
        sells=[f for f in self.fills() if f['side']=='SELL'];self.assertTrue(sells)
        self.assertTrue(all(D(f['reference_price'])==95 for f in sells));self.assertEqual(self.e.accounts['s'].positions['BTC'].protection,'STOP_LOCKED')
    def test_touch_then_recovery_stops_at_adverse_price(self):
        self.enter();stop=self.e.accounts['s'].positions['BTC'].stop
        self.e.process(events(T+3*MINUTE,'100',low='97'))
        f=next(f for f in self.fills() if f['account_id']=='s' and f['side']=='SELL')
        self.assertEqual(D(f['reference_price']),stop);self.assertTrue(f['ambiguous'])
    def test_duplicate_batch_and_conflicting_data(self):
        self.enter();count=len(self.fills());self.assertFalse(self.e.process(events(T+2*MINUTE)));self.assertEqual(len(self.fills()),count)
        with self.assertRaisesRegex(ValueError,'conflicting'):self.e.process(events(T+2*MINUTE,'99'))
    def test_atomic_rollback_after_fill(self):
        b=events();self.e.process(b,[decision(b[0],self.m)]);self.e.process(events(T+MINUTE))
        before=self.e.snapshot();self.store.failpoint='after_fill'
        with self.assertRaisesRegex(RuntimeError,'after_fill'):self.e.process(events(T+2*MINUTE))
        self.assertEqual(before,self.e.snapshot());self.assertEqual(self.fills(),[])
        self.store.failpoint=None;self.e.process(events(T+2*MINUTE));self.assertEqual(len(self.fills()),2)
    def test_atomic_rollback_before_commit(self):
        before=self.e.snapshot();self.store.failpoint='before_commit'
        with self.assertRaisesRegex(RuntimeError,'before_commit'):self.e.process(events())
        self.assertEqual(before,self.e.snapshot());self.store.failpoint=None;self.assertTrue(self.e.process(events()))
    def test_recovery_reconstructs_cash_cost_and_quantity(self):
        self.enter();self.e.process(events(T+3*MINUTE,'95',low='90'));expected=self.e.snapshot();self.store.close()
        self.store=Store(self.path,self.c);other=Engine(self.c,self.store,[self.m]);self.assertEqual(expected,other.snapshot())
    def test_mutable_projection_tampering_fails_recovery(self):
        self.enter();self.store.db.execute("UPDATE portfolio_state SET payload='{}' WHERE id='s'")
        with self.assertRaisesRegex(ValueError,'RECOVERY_ERROR'):self.store.recover()
    def test_immutable_events(self):
        self.enter()
        with self.assertRaises(sqlite3.IntegrityError):self.store.db.execute("UPDATE events SET type='bad'")
        with self.assertRaises(sqlite3.IntegrityError):self.store.db.execute('DELETE FROM events')
    def test_experiment_config_immutable(self):
        with self.assertRaisesRegex(ValueError,'configuration changed'):Store(self.path,replace(self.c,fee=D('.002')))
    def test_future_signal_rejected_atomically(self):
        b=events();before=self.e.snapshot()
        with self.assertRaisesRegex(ValueError,'unavailable'):self.e.process(b,[decision(b[0],self.m,available=H4+1)])
        self.assertEqual(self.e.snapshot(),before)
    def test_stale_then_emergency_exit_and_old_buy_not_restored(self):
        self.enter();last=self.e.now;self.e.heartbeat(last+91_000)
        self.assertTrue(all(a.state=='DATA_STALE' for a in self.e.accounts.values()))
        self.e.heartbeat(last+301_000);self.assertTrue(all(a.state=='EXIT_PENDING' for a in self.e.accounts.values()))
        sells={k:o.ready_at for k,o in self.e.orders.items() if o.side=='SELL'};self.e.heartbeat(last+306_000)
        self.assertEqual(sells,{k:o.ready_at for k,o in self.e.orders.items() if o.side=='SELL'})
    def test_growth_does_not_trigger_risk_budget_sale(self):
        self.enter();self.e.process(events(T+3*MINUTE,'105'))
        self.assertTrue(self.e.accounts['s'].positions['BTC'].quantity>0)
        self.assertFalse(any(o.reason=='risk_or_capital_limit' and o.side=='SELL' for o in self.e.orders.values()))
    def test_expired_common_signal_does_not_exit_individual(self):
        self.enter();a=self.e.accounts['s'];before=a.positions['BTC'].quantity
        self.e.heartbeat(H4*3)
        self.assertEqual(a.positions['BTC'].quantity,before)
        # Data emergency is a separate cause, not strategy expiry.
    def test_protective_stop_blocks_hold_and_old_entry(self):
        self.enter();self.e.process(events(T+3*MINUTE,'95',low='90'));b=events(T+4*MINUTE,'100')
        self.e.process(b,[decision(b[0],self.m,Action.HOLD)])
        self.assertFalse(any(o.side=='BUY' and o.status=='PENDING' for o in self.e.orders.values()))
    def test_decision_order_does_not_change_financial_result(self):
        m2=meta('ETH');engine2=Engine(self.c,self.store,[self.m,m2])
        # Separate databases, identical versions, reverse analysis completion order.
        with tempfile.TemporaryDirectory() as tmp:
            s2=Store(Path(tmp)/'second.sqlite',self.c);e2=Engine(self.c,s2,[self.m,m2])
            b=events();d=[decision(b[0],self.m),decision(b[1],m2)]
            engine2.process(b,d);e2.process(b,list(reversed(d)))
            for t in [T+MINUTE,T+2*MINUTE]:engine2.process(events(t));e2.process(events(t))
            self.assertEqual(engine2.snapshot(),e2.snapshot());s2.close()
    def test_delayed_stop_update_effective_next_eligible_bar(self):
        self.enter();a=self.e.accounts['s'];p=a.positions['BTC'];old=p.stop
        o=VirtualOrder('u','u','s','BTC','UPDATE_STOP',ZERO,T+4*MINUTE,'lock3',D('99'))
        self.e.orders['u']=o
        self.e.process(events(T+3*MINUTE));self.assertEqual(p.stop,old)
        self.e.process(events(T+4*MINUTE));self.assertEqual(p.stop,D('99'))
    def test_cash_plus_positions_equals_equity(self):
        self.enter()
        for a in self.e.accounts.values():self.assertEqual(a.equity,a.cash+sum((p.quantity*self.e.marks[n] for n,p in a.positions.items()),ZERO))
    def test_short_not_created_by_negative_target(self):
        b=events();self.e.process(b,[decision(b[0],self.m,target=-ONE)])
        self.assertFalse(self.e.orders)
    def test_R0_signal_not_admitted(self):
        self.e.agent_meta[self.m['agent_id']]['admission']='R0';b=events();self.e.process(b,[decision(b[0],self.m)])
        self.assertFalse(self.e.orders)

class RiskTests(unittest.TestCase):
    def setUp(self):self.c=ExperimentConfig();self.r=RiskManager(self.c)
    def test_joint_risk_limits_both_positions(self):
        a=PortfolioState('x',D('900'),D('1000'),positions={'BTC':PositionState(D('1'),D('100'),D('100'),D('98'))})
        r=self.r.admission(a,{'BTC':D('100'),'ETH':D('100')},'ETH',D('2'),D('98'))
        self.assertLess(r.quantity,D('2'));self.assertLessEqual(r.loss_to_stops+r.new_costs,self.c.risk_limit*r.post_equity+D('1e-6'))
    def test_entry_costs_are_not_paid_twice(self):
        a=PortfolioState('x',D('1000'),D('1000'));r=self.r.admission(a,{'BTC':D('100')},'BTC',ONE,D('98'))
        self.assertEqual(r.new_costs,D('100')*((ONE+self.c.fee)*(ONE+self.c.slippage)-ONE))
        a.cash=D('899.84995');a.positions['BTC']=PositionState(ONE,D('100.15005'),D('100.05'),D('98'))
        old=self.r.admission(a,{'BTC':D('100')},'BTC',ZERO,D('98'))
        self.assertEqual(old.new_costs,ZERO)
    def test_invalid_stop_and_no_borrowing(self):
        a=PortfolioState('x',D('1'),D('1000'));r=self.r.admission(a,{'BTC':D('100')},'BTC',ONE,D('101'));self.assertEqual(r.quantity,ZERO)
    def test_growth_risk_prevents_add_only(self):
        a=PortfolioState('x',D('900'),D('1010'),positions={'BTC':PositionState(ONE,D('100'),D('100'),D('98'))})
        self.assertEqual(self.r.admission(a,{'BTC':D('110')},'BTC',ONE,D('98')).quantity,ZERO)

class RegistryTests(unittest.TestCase):
    def test_namespaces_and_no_automatic_legacy_R1(self):
        rows=records();self.assertEqual(len([r for r in rows if r['hypothesis_id'].startswith('legacy50:')]),50);self.assertTrue(all(r['level']=='R0' for r in rows))
        self.assertEqual(len({r['implementation_id'] for r in rows}),len(rows))
    def test_apex_no_external_import_permission(self):
        apex=next(r for r in records() if r['hypothesis_id']=='external:apex-usdjpy');self.assertEqual(apex['license'],'UNVERIFIED');self.assertNotIn('aggregate',apex['modes'])

class OrchestrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_64_agents_and_failure_isolation(self):
        class A:
            def __init__(self,i):self.metadata=dict(agent_id=str(i),instrument='BTC',interval_ms=H4)
            def analyze(self,h,e,now):
                if self.metadata['agent_id']=='0':raise ValueError('bad agent')
                return StrategyDecision(self.metadata['agent_id'],'v','c','BTC','trend',Action.HOLD,ZERO,now,now+1000,e.event_id,D('1'),'synthetic load only')
        o=Orchestrator([A(i) for i in range(64)])
        try:
            out,errors=await o.analyze({},events(),H4);self.assertEqual(len(out),63);self.assertEqual(len(errors),1)
        finally:o.close()
    async def test_timeout_quarantined_not_resubmitted(self):
        class A:
            metadata=dict(agent_id='slow',instrument='BTC',interval_ms=H4)
            def analyze(self,*args):time.sleep(.1);return None
        o=Orchestrator([A()],timeout=.01)
        try:
            await o.analyze({},events(),H4);self.assertIn('slow',o.quarantined)
            _,err=await o.analyze({},events(),H4);self.assertEqual(err[0]['reason'],'QUARANTINED')
        finally:o.close()


class ExtendedAcceptance(unittest.TestCase):
    def make(self,folder,name='x',mode='replay'):
        config=ExperimentConfig(name=name,mode=mode);store=Store(Path(folder)/(name+'.sqlite'),config);return Engine(config,store,[meta()]),store
    def test_recorded_replay_and_paper_share_financial_kernel_including_timers(self):
        with tempfile.TemporaryDirectory() as tmp:
            a,sa=self.make(tmp,'paper','paper');b,sb=self.make(tmp,'replay')
            try:
                frames=[events(T+i*MINUTE) for i in range(7)]
                for i,frame in enumerate(frames):
                    ds=[decision(frame[0],meta())] if i==0 else []
                    a.process(frame,ds);b.process(frame,ds)
                    a.heartbeat(frame[0].close_ms+1000);b.heartbeat(frame[0].close_ms+1000)
                self.assertEqual(a.summary()['accounts'],b.summary()['accounts'])
                from trading_v1.data import RecordedSource
                async def collect():return [x async for x in RecordedSource(sa.path)]
                sequence=asyncio.run(collect());self.assertEqual(sum(isinstance(x,int) for x in sequence),7)
                self.assertEqual(sum(isinstance(x,list) for x in sequence),7)
            finally:sa.close();sb.close()
    def test_expiry_reduces_only_common_target_while_market_stays_fresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                batch=events();d=decision(batch[0],meta(),valid=H4+MINUTE*4)
                e.process(batch,[d])
                for i in range(1,8):e.process(events(T+i*MINUTE))
                self.assertGreater(e.accounts['s'].positions['BTC'].quantity,ZERO)
                self.assertEqual(e.accounts['aggregate'].positions['BTC'].quantity,ZERO)
            finally:s.close()
    def test_stop_lock_requires_new_entry_after_unlock(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                p=e.accounts['aggregate'].positions.setdefault('BTC',PositionState(protection='STOP_LOCKED',unlock_after=H4));e.marks={'BTC':D('100'),'ETH':D('100')};e.last_market={'BTC':H4,'ETH':H4}
                frame=events(H4);d=decision(frame[0],meta(),available=H4+MINUTE)
                e.process(frame,[d]);self.assertEqual(e.accounts['aggregate'].positions['BTC'].protection,'ACTIVE')
            finally:s.close()
    def test_individual_stop_does_not_suppress_other_strategy_votes(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                first=events();e.process(first,[decision(first[0],meta())]);e.process(events(T+MINUTE));e.process(events(T+2*MINUTE))
                e.accounts['s'].positions['BTC'].stop=D('99.5')
                e.process(events(T+3*MINUTE,low='99'))
                self.assertFalse(e.contrib[meta()['agent_id']]['suppressed'])
                self.assertGreater(e.accounts['aggregate'].positions['BTC'].quantity,ZERO)
            finally:s.close()
    def test_partial_exit_allocates_purchase_fees_and_cost_proportionally(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                first=events();e.process(first,[decision(first[0],meta())]);e.process(events(T+MINUTE));e.process(events(T+2*MINUTE))
                p=e.accounts['s'].positions['BTC'];cost=p.cost;q=p.quantity;half=qty(q/2)
                with s.transaction():e._queue(e.accounts['s'],'BTC','SELL',half,H4+MINUTE*2,'partial','half')
                e.process(events(T+3*MINUTE));e.process(events(T+4*MINUTE))
                self.assertEqual(p.cost,cost-cost*half/q)
                self.assertEqual(p.quantity,q-half)
            finally:s.close()
    def test_new_position_stop_is_set_at_actual_entry_not_old_decision_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                b=events();e.process(b,[decision(b[0],meta())]);e.process(events(T+MINUTE,'102'));e.process(events(T+2*MINUTE,'102'))
                p=e.accounts['s'].positions['BTC'];self.assertEqual(p.initial_distance,D('2'));self.assertEqual(p.stop,D('102')*D('1.0005')-2)
            finally:s.close()
    def test_duplicate_signal_not_cancelled_or_executed_twice(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                b=events();d=decision(b[0],meta());e.process(b,[d,d]);e.process(events(T+MINUTE));e.process(events(T+2*MINUTE))
                self.assertEqual(s.db.execute("SELECT count(*) FROM fills WHERE account='s'").fetchone()[0],1)
            finally:s.close()
    def test_signal_cannot_impersonate_another_instrument(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                b=events();d=replace(decision(b[0],meta()),instrument='ETH')
                with self.assertRaisesRegex(ValueError,'metadata'):e.process(b,[d])
            finally:s.close()
    def test_no_redistribution_of_missing_family(self):
        with tempfile.TemporaryDirectory() as tmp:
            e,s=self.make(tmp)
            try:
                b=events();e.process(b,[decision(b[0],meta())])
                row=s.db.execute("SELECT payload FROM portfolio_decisions WHERE account='aggregate' AND json_extract(payload,'$.instrument')='BTC' ORDER BY rowid DESC LIMIT 1").fetchone()
                self.assertEqual(D(json.loads(row[0])['target_exposure']),D('.09375'))
            finally:s.close()

class PendingCancellationTests(unittest.TestCase):
    def test_exit_before_fill_cancels_purchase(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=ExperimentConfig();s=Store(Path(tmp)/'x.sqlite',c);e=Engine(c,s,[meta()])
            try:
                b=events();e.process(b,[decision(b[0],meta())])
                b=events(T+MINUTE);e.process(b,[decision(b[0],meta(),Action.EXIT)])
                e.process(events(T+2*MINUTE))
                self.assertEqual(s.db.execute('SELECT count(*) FROM fills').fetchone()[0],0)
                self.assertTrue(all(a.reserved_cash==0 for a in e.accounts.values()))
            finally:s.close()

class WebAcceptance(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_routes_read_only_and_refresh(self):
        from aiohttp.test_utils import TestClient,TestServer
        from aiohttp import web
        from trading_v1.web import PAGE
        app=web.Application();app.router.add_get('/',lambda r:web.Response(text=PAGE,content_type='text/html'))
        async with TestClient(TestServer(app)) as client:
            response=await client.get('/');self.assertEqual(response.status,200);self.assertIn('setInterval(update,5000)',await response.text())
            self.assertEqual((await client.post('/')).status,405)

class RestartAcceptance(unittest.TestCase):
    def test_restarted_batch_is_not_applied_twice(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'x.sqlite';c=ExperimentConfig();s=Store(path,c);e=Engine(c,s,[meta()]);b=events()
            e.process(b,[decision(b[0],meta())]);e.process(events(T+MINUTE));e.process(events(T+2*MINUTE));expected=e.snapshot();s.close()
            s=Store(path,c)
            try:
                e=Engine(c,s,[meta()]);self.assertFalse(e.process(events(T+2*MINUTE)));self.assertEqual(e.snapshot(),expected)
            finally:s.close()

class PendingRiskReservationTests(unittest.TestCase):
    def test_pending_eth_and_btc_cannot_each_reserve_half_percent(self):
        c=ExperimentConfig();r=RiskManager(c);a=PortfolioState('x',D('1000'),D('1000'));marks={'BTC':D('100'),'ETH':D('100')}
        first=r.admission(a,marks,'BTC',D('5'),D('98'))
        order=VirtualOrder('b','b','x','BTC','BUY',first.quantity,1000,'test',D('98'))
        a.reserved_cash=first.quantity*D('100')*(ONE+c.slippage)*(ONE+c.fee)
        second=r.admission(a,marks,'ETH',D('5'),D('98'),[order])
        self.assertEqual(second.quantity,ZERO)


class FinalInvariantTests(unittest.TestCase):
    def test_full_H01_H20_registry_keeps_unimplemented_models_visible(self):
        ids={r['hypothesis_id'] for r in records()}
        self.assertTrue({'research20:H%02d'%i for i in range(1,21)}<=ids)
    def test_corrupted_consumer_offset_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=ExperimentConfig();s=Store(Path(tmp)/'x.sqlite',c);e=Engine(c,s,[meta()])
            try:
                e.process(events());s.db.execute('UPDATE consumer_offsets SET event_seq=0')
                with self.assertRaisesRegex(ValueError,'consumer offset'):s.recover()
            finally:s.close()
    def test_orphan_fill_projection_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=ExperimentConfig();s=Store(Path(tmp)/'x.sqlite',c);e=Engine(c,s,[meta()])
            try:
                e.process(events());s.db.execute("INSERT INTO fills VALUES('fake','fake','s','{}')")
                with self.assertRaisesRegex(ValueError,'fill projection'):s.recover()
            finally:s.close()
    def test_receipt_delay_does_not_unlock_current_stopped_bar(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=ExperimentConfig();s=Store(Path(tmp)/'x.sqlite',c);e=Engine(c,s,[meta()])
            try:
                p=e.accounts['aggregate'].positions.setdefault('BTC',PositionState(protection='STOP_LOCKED',unlock_after=H4))
                b=[replace(x,received_at=H4+1000) for x in events()]
                e.process(b,[decision(b[0],meta(),available=H4+1000)])
                self.assertEqual(p.protection,'STOP_LOCKED')
            finally:s.close()
    def test_negative_target_closes_long_instead_of_creating_short(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=ExperimentConfig();s=Store(Path(tmp)/'x.sqlite',c);e=Engine(c,s,[meta()])
            try:
                b=events();e.process(b,[decision(b[0],meta())]);e.process(events(T+MINUTE));e.process(events(T+2*MINUTE))
                b=events(T+3*MINUTE);e.process(b,[decision(b[0],meta(),target=-ONE)])
                e.process(events(T+4*MINUTE));e.process(events(T+5*MINUTE))
                self.assertEqual(e.accounts['s'].positions['BTC'].quantity,ZERO)
            finally:s.close()

if __name__=='__main__':unittest.main()
