"""Single Decimal accounting/execution kernel for archive and recorded/live paper bars."""
from __future__ import annotations
import copy,json
from dataclasses import asdict
from .models import *
from .risk import RiskManager
from .collector import SignalCollector
from .store import MarketDataConflict

class Engine:
    def __init__(self,config,store,agents=()):
        self.c=config;self.store=store;self.risk=RiskManager(config)
        self.accounts={'aggregate':PortfolioState('aggregate',config.initial_capital,config.initial_capital,equity=config.initial_capital)}
        self.agent_meta={a['agent_id']:a for a in agents}
        for a in agents:
            if a.get('admission')=='R1':
                account=a['account_id']
                self.accounts.setdefault(account,PortfolioState(account,config.initial_capital,config.initial_capital,equity=config.initial_capital,policy=a.get('policy','base')))
        self.marks={};self.last_market={};self.contrib={};self.orders={};self.targets={};self.atr={};self.now=0;self.last_batch='';self.cause_event=''
        state=store.recover()
        if state:self.restore(state)
    def snapshot(self):
        return primitive(dict(initial_capital=self.c.initial_capital,accounts=self.accounts,marks=self.marks,last_market=self.last_market,contributions=self.contrib,orders=self.orders,targets=self.targets,atr=self.atr,now=self.now,last_batch=self.last_batch))
    def restore(self,s):
        self.accounts={}
        for name,a in s['accounts'].items():
            p={n:PositionState(**{k:(int(v) if k in ('unlock_after','entry_at') else v if k=='protection' else D(v)) for k,v in row.items()}) for n,row in a['positions'].items()}
            args={k:(v if k in ('account_id','state','policy') else D(v)) for k,v in a.items() if k!='positions'}
            self.accounts[name]=PortfolioState(**args,positions=p)
        self.marks={n:D(v) for n,v in s['marks'].items()};self.last_market=s['last_market'];self.contrib=s['contributions'];self.targets=s['targets'];self.atr={n:D(v) for n,v in s['atr'].items()};self.now=s['now'];self.last_batch=s['last_batch']
        self.orders={n:VirtualOrder(**{k:(D(v) if k in ('quantity','reserved_cash') or k in ('stop','initial_distance') and v is not None else v) for k,v in o.items()}) for n,o in s['orders'].items()}
    def diagnostic(self,kind,time,payload):self.store.append(key(self.c.name,kind,time,payload),kind,time,payload)
    def market_conflict(self,old,e,old_seq=None):
        diagnostic=self.store.record_conflict(e.event_id,'MARKET_CORRECTION_PENDING',e.ready_at,primitive(old),primitive(e),old_seq=old_seq,source=e.source,instrument=e.instrument,open_ms=e.open_ms,policy='KEEP_ACCEPTED_VERSION_HALT_REQUIRE_REVIEW',old_market_hash=old.data_hash,new_market_hash=e.data_hash)
        raise MarketDataConflict(diagnostic)
    def observe_market(self,e):
        """First accepted closed bar is frozen. Corrections require explicit review."""
        row=self.store.db.execute('SELECT seq,payload FROM events WHERE event_key=?',(key(self.c.name,'market',e.event_id),)).fetchone()
        if not row:return False
        old=json.loads(row['payload'])['event']
        restored=MarketEvent(**{k:D(v) if k in ('open','high','low','close','volume') else v for k,v in old.items()})
        if restored.data_hash!=e.data_hash:
            self.market_conflict(restored,e,row['seq'])
        return True
    def process(self,events,decisions=(),diagnostics=()):
        self.store.ensure_writable()
        events=sorted(events,key=lambda e:e.instrument)
        if not events:return False
        if len({e.open_ms for e in events})!=1 or len({e.instrument for e in events})!=len(events):raise ValueError('unaligned batch')
        batch=key([e.event_id for e in events])
        known=[self.observe_market(e) for e in events]
        if self.store.seen(batch):
            if not all(known):raise ValueError('RECOVERY_ERROR: incomplete duplicate batch')
            return False
        if any(known):raise ValueError('RECOVERY_ERROR: partial duplicate batch')
        before=self.snapshot()
        try:
            with self.store.transaction():
                for e in events:
                    last=self.last_market.get(e.instrument)
                    if last is not None and e.open_ms<int(last):raise ValueError('out-of-order market event')
                    if last is not None and e.open_ms!=int(last):raise ValueError('missing market bars; recover before proceeding')
                    self.store.append(key(self.c.name,'market',e.event_id),'MARKET',e.ready_at,dict(event=primitive(e),data_hash=e.data_hash))
                validated=SignalCollector(self.agent_meta).collect(decisions,events,max(e.ready_at for e in events))
                self.cause_event=key(self.c.name,'market_batch',batch)
                self._batch(events,validated)
                for item in diagnostics:self.diagnostic('AGENT_DIAGNOSTIC',self.now,item)
                self.last_batch=batch
                self.store.checkpoint(batch,self.now,self.snapshot())
            return True
        except BaseException:
            self.restore(before);raise
    def _cancel_buys(self,account,symbol=None):
        for o in self.orders.values():
            if o.account_id==account.account_id and o.status=='PENDING' and o.side=='BUY' and (symbol is None or o.instrument==symbol):
                o.status='CANCELLED';account.reserved_cash-=o.reserved_cash;o.reserved_cash=ZERO;self._save_order(o)
    def _save_order(self,o):
        body=canonical(o);old=self.store.db.execute('SELECT payload FROM orders WHERE id=?',(o.order_id,)).fetchone()
        if not old or old[0]!=body:self.store.append(key(o.order_id,'state',body),'ORDER_STATE',self.now,o)
        self.store.db.execute('INSERT OR REPLACE INTO orders VALUES(?,?,?,?)',(o.order_id,o.account_id,o.status,body))
    def _queue(self,a,symbol,side,q,time,reason,decision,stop=None,initial_distance=None):
        q=qty(q)
        if q<=0:return
        self._cancel_buys(a,symbol)
        # Superseded pending sells are replaced by the latest target; protective orders dominate.
        for old in self.orders.values():
            if old.account_id==a.account_id and old.instrument==symbol and old.status=='PENDING':
                if old.reason in ('drawdown','data_emergency','stop') and reason not in ('drawdown','data_emergency','stop'):return
                old.status='CANCELLED';self._save_order(old)
        oid=key(decision,side)
        if oid in self.orders or self.store.db.execute('SELECT 1 FROM orders WHERE id=?',(oid,)).fetchone():return
        reserve=q*self.marks[symbol]*(ONE+self.c.slippage)*(ONE+self.c.fee) if side=='BUY' else ZERO
        reserve=min(reserve,max(ZERO,a.cash-a.reserved_cash));a.reserved_cash+=reserve
        o=VirtualOrder(oid,decision,a.account_id,symbol,side,q,time+self.c.latency_ms,reason,stop,reserved_cash=reserve,initial_distance=initial_distance)
        self.orders[oid]=o;self._save_order(o);self.store.append(key(oid,'created'),'ORDER',time,o)
    def _fill(self,a,o,e,reference,reason=None,ambiguous=False):
        if o.status!='PENDING':return
        p=a.positions.setdefault(e.instrument,PositionState());q=o.quantity
        if o.side=='UPDATE_STOP':
            if p.quantity:p.stop=max(p.stop,o.stop or p.stop)
            o.status='APPLIED';self._save_order(o);return
        a.reserved_cash-=o.reserved_cash;o.reserved_cash=ZERO
        if o.side=='BUY':
            if a.state!='ACTIVE' or p.protection=='STOP_LOCKED' or e.recovered:
                o.status='CANCELLED';self._save_order(o);return
            if not p.quantity and o.initial_distance is not None:o.stop=reference*(ONE+self.c.slippage)-o.initial_distance
            pending=[p for p in self.orders.values() if p.account_id==a.account_id and p.status=='PENDING' and p.side=='BUY' and p.order_id!=o.order_id]
            rr=self.risk.admission(a,self.marks,e.instrument,q,o.stop or ZERO,pending);q=rr.quantity
            self.diagnostic('RISK_CHECK',e.open_ms,dict(order=o.order_id,result=rr,phase='execution'))
        else:q=min(q,p.quantity)
        q=qty(q)
        if not q:
            o.status='REJECTED';self._save_order(o);return
        px=reference*(ONE+self.c.slippage if o.side=='BUY' else ONE-self.c.slippage);fee=q*px*self.c.fee
        released=ZERO;realized=ZERO
        if o.side=='BUY':
            amount=q*px+fee;a.cash-=amount
            if not p.quantity:
                p.stop=o.stop or ZERO;p.initial_distance=px-p.stop;p.entry_at=e.open_ms
            p.average_price=(p.average_price*p.quantity+px*q)/(p.quantity+q)
            p.quantity+=q;p.cost+=amount
        else:
            released=p.cost*q/p.quantity
            amount=q*px-fee;realized=amount-released;a.cash+=amount;a.realized_pnl+=realized
            p.quantity-=q;p.cost-=released
            if not p.quantity:p.cost=ZERO;p.average_price=ZERO
        f=VirtualFill(key(o.order_id,e.event_id,0),o.order_id,a.account_id,e.instrument,o.side,q,reference,px,fee,q*abs(px-reference),released,realized,e.event_id,e.open_ms,reason or o.reason,ambiguous)
        self.store.append(f.fill_id,'FILL',e.open_ms,f)
        self.store.db.execute('INSERT INTO fills VALUES(?,?,?,?)',(f.fill_id,o.order_id,a.account_id,canonical(f)))
        if self.store.failpoint=='after_fill':raise RuntimeError('injected after_fill')
        o.status='FILLED';o.quantity=q;self._save_order(o)
    def _immediate_exit(self,a,e,reference,reason,ambiguous=False):
        self._cancel_buys(a,e.instrument);p=a.positions[e.instrument]
        for old in self.orders.values():
            if old.account_id==a.account_id and old.instrument==e.instrument and old.status=='PENDING':old.status='CANCELLED';self._save_order(old)
        oid=key(self.c.name,a.account_id,e.instrument,e.event_id,reason)
        o=VirtualOrder(oid,oid,a.account_id,e.instrument,'SELL',p.quantity,e.open_ms,reason)
        self.orders[oid]=o;self._save_order(o);self._fill(a,o,e,reference,reason,ambiguous)
        if reason=='stop':
            p.protection='STOP_LOCKED';p.unlock_after=(e.open_ms//H4+1)*H4
            # Erase entry episode; historical HOLDs cannot resurrect it.
            if a.account_id=='aggregate':
                for v in self.contrib.values():
                    if v['instrument']==e.instrument:v['target']='0';v['suppressed']=True
    def _batch(self,events,decisions):
        open_time=events[0].open_ms
        for e in events:self.marks[e.instrument]=e.open
        # Pending orders use only open information. Existing stop at a gap beats a strategic order.
        for a in self.accounts.values():
            for e in events:
                p=a.positions.get(e.instrument)
                if p and p.quantity and e.open<=p.stop:self._immediate_exit(a,e,e.open,'stop')
            for o in sorted(list(self.orders.values()),key=lambda o:(o.side=='BUY',o.ready_at,o.instrument,o.account_id,o.reason,o.order_id)):
                if o.account_id==a.account_id and o.status=='PENDING' and o.ready_at<=open_time:
                    e=next((x for x in events if x.instrument==o.instrument),None)
                    if e:self._fill(a,o,e,e.open)
        for a in self.accounts.values():
            for e in events:
                p=a.positions.get(e.instrument)
                if p and p.quantity and e.low<=p.stop:self._immediate_exit(a,e,min(e.open,p.stop),'stop',e.high>p.stop and e.low<p.stop)
        for e in events:self.marks[e.instrument]=e.close;self.last_market[e.instrument]=e.close_ms
        self.now=max(e.ready_at for e in events)
        for a in self.accounts.values():
            if a.state=='DATA_STALE':a.state='ACTIVE'
        for a in self.accounts.values():
            a.equity=self.risk.equity(a,self.marks);a.peak=max(a.peak,a.equity)
            a.max_dd=min(a.max_dd,a.equity/a.peak-ONE)
            if a.account_id=='aggregate' and a.equity<=a.peak*(ONE-self.c.max_drawdown):
                a.state='HALTED_DRAWDOWN';self._protect(a,self.now,'drawdown')
            if a.state=='EXIT_PENDING' and not any(p.quantity for p in a.positions.values()):a.state='ACTIVE'
        for d in decisions:
            if d.available_at>self.now or d.input_event not in [e.event_id for e in events]:raise ValueError('unavailable signal input')
            if d.agent_id not in self.agent_meta or self.agent_meta[d.agent_id].get('admission')!='R1':
                self.diagnostic('BLOCKED',self.now,dict(agent=d.agent_id,reason='not_R1'));continue
            self.store.append(key(self.c.name,'signal',d.decision_id),'SIGNAL',d.available_at,d)
            self.store.db.execute('INSERT OR IGNORE INTO signals VALUES(?,?,?,?)',(d.decision_id,d.agent_id,d.available_at,canonical(d)))
            observed_close=next(e.close_ms for e in events if e.event_id==d.input_event)
            old=self.contrib.get(d.agent_id);target=D(old['target']) if old else ZERO
            if d.action==Action.EXIT:target=ZERO
            elif d.action==Action.ENTRY:target=d.target
            suppressed=old.get('suppressed',False) if old else False
            if d.action==Action.ENTRY:suppressed=False
            self.contrib[d.agent_id]=dict(primitive(d),target=str(target),suppressed=suppressed,observed_close=observed_close,decision_id=d.decision_id)
            if d.interval_ms==H4 and d.atr>0:self.atr[d.instrument]=d.atr
            meta=self.agent_meta[d.agent_id];a=self.accounts[meta['account_id']]
            p=a.positions.setdefault(d.instrument,PositionState())
            if p.protection=='STOP_LOCKED' and d.action==Action.ENTRY and observed_close>p.unlock_after:p.protection='ACTIVE'
            if d.action==Action.EXIT or d.action==Action.ENTRY and d.target<=0:self._target(a,d.instrument,ZERO,d.available_at,'strategy_exit',d.decision_id,d.atr)
            elif d.action==Action.ENTRY and not p.quantity and p.protection=='ACTIVE':
                self._target(a,d.instrument,self.c.instrument_cap*max(ZERO,target),d.available_at,'strategy_entry',d.decision_id,d.atr)
            if a.policy=='lock3' and p.quantity and d.interval_ms==H4 and self.marks[d.instrument]>=p.average_price+3*p.initial_distance:
                # Apply through delayed UPDATE_STOP, never retrospectively in the just-closed bar.
                new_stop=max(p.stop,p.average_price+p.initial_distance)
                oid=key(d.decision_id,a.account_id,'update_stop')
                if oid not in self.orders:
                    o=VirtualOrder(oid,d.decision_id,a.account_id,d.instrument,'UPDATE_STOP',ZERO,d.available_at+self.c.latency_ms,'lock3',new_stop)
                    self.orders[oid]=o;self._save_order(o)
        self._aggregate(self.now)
        for a in self.accounts.values():
            self._exposure_override(a,self.now)
            a.equity=self.risk.equity(a,self.marks)
        self.orders={k:o for k,o in self.orders.items() if o.status=='PENDING'}
    def _target(self,a,symbol,exposure,time,reason,identity,atr,inputs=None,contributions=None):
        if symbol not in self.marks:return
        if exposure<=0:self._cancel_buys(a,symbol)
        p=a.positions.setdefault(symbol,PositionState());eq=self.risk.equity(a,self.marks)
        target=qty(max(ZERO,exposure)*eq/self.marks[symbol]);delta=target-p.quantity
        dec=PortfolioDecision(key(self.c.name,'portfolio-v2',self.cause_event,identity,a.account_id,symbol,reason),a.account_id,symbol,p.quantity*self.marks[symbol]/eq if eq else ZERO,exposure,target,reason,tuple(inputs) if inputs is not None else (identity,),contributions or {},self.cause_event)
        self.store.append(key(dec.decision_id,'portfolio'),'PORTFOLIO_DECISION',time,dec)
        self.store.db.execute('INSERT OR IGNORE INTO portfolio_decisions VALUES(?,?,?,?)',(dec.decision_id,a.account_id,time,canonical(dec)))
        if delta<0:
            self._queue(a,symbol,'SELL',-delta,time,reason,dec.decision_id);return
        if not delta or a.state!='ACTIVE' or p.protection=='STOP_LOCKED' or atr<=0:return
        stop=p.stop if p.quantity else self.marks[symbol]*(ONE+self.c.slippage)-2*atr
        pending=[o for o in self.orders.values() if o.account_id==a.account_id and o.status=='PENDING' and o.side=='BUY' and o.instrument!=symbol]
        rr=self.risk.admission(a,self.marks,symbol,delta,stop,pending)
        self.diagnostic('RISK_CHECK',time,dict(decision=dec.decision_id,result=rr,phase='decision'))
        if rr.quantity:self._queue(a,symbol,'BUY',rr.quantity,time,reason,dec.decision_id,stop,2*atr if not p.quantity else None)
    def _aggregate(self,time):
        a=self.accounts['aggregate']
        for symbol in self.c.instruments:
            if symbol not in self.marks:continue
            # Fixed canonical membership: missing/expired members are not renormalized.
            family_members={f:set() for f in self.c.family_weights}
            for m in self.agent_meta.values():
                if m.get('aggregate') and m.get('admission')=='R1' and m['instrument']==symbol:family_members[m['family']].add(m['canonical_id'])
            selected={}
            for agent,v in sorted(self.contrib.items()):
                m=self.agent_meta[agent]
                if not m.get('aggregate') or v['instrument']!=symbol or v['suppressed'] or int(v['valid_until'])<=time:continue
                selected.setdefault(v['canonical_id'],v)
            z=ZERO;breakdown={}
            for family,weight in self.c.family_weights.items():
                members=family_members[family]
                if members:
                    for member in sorted(members):
                        active=member in selected;score=D(selected[member]['target']) if active else ZERO
                        w=weight/D(len(members));z+=w*score
                        breakdown[member]=dict(weight=w,target=score,contribution=w*score,active=active,agent=selected[member]['agent_id'] if active else None)
            exposure=self.c.instrument_cap*min(ONE,max(ZERO,z))
            p=a.positions.setdefault(symbol,PositionState())
            if p.protection=='STOP_LOCKED':
                fresh=any(v['action']=='ENTRY' and int(v.get('observed_close',v['available_at']))>p.unlock_after and not v['suppressed'] for v in selected.values())
                if fresh:p.protection='ACTIVE'
            identity=key(symbol,exposure,p.protection,a.state)
            inputs=tuple(sorted(v.get('decision_id',key(v['agent_id'],v['input_event'])) for v in selected.values()))
            revision=key(identity,inputs)
            changed=self.targets.get(symbol)!=identity
            if changed:
                self.targets[symbol]=identity
                self._target(a,symbol,exposure,time,'aggregate',identity,self.atr.get(symbol,ZERO),inputs,breakdown)
            elif self.targets.get(symbol+':inputs')!=revision:
                eq=self.risk.equity(a,self.marks)
                dec=PortfolioDecision(key(self.c.name,'portfolio-v2',self.cause_event,a.account_id,symbol,'unchanged',revision),a.account_id,symbol,p.quantity*self.marks[symbol]/eq if eq else ZERO,exposure,p.quantity,'aggregate_no_target_change',inputs,breakdown,self.cause_event)
                self.store.append(key(dec.decision_id,'portfolio'),'PORTFOLIO_DECISION',time,dec)
                self.store.db.execute('INSERT OR IGNORE INTO portfolio_decisions VALUES(?,?,?,?)',(dec.decision_id,a.account_id,time,canonical(dec)))
            self.targets[symbol+':inputs']=revision
    def _protect(self,a,time,reason):
        self._cancel_buys(a)
        for symbol,p in a.positions.items():
            if p.quantity and not any(o.status=='PENDING' and o.account_id==a.account_id and o.instrument==symbol and o.side=='SELL' and o.reason in ('drawdown','data_emergency') for o in self.orders.values()):self._queue(a,symbol,'SELL',p.quantity,time,reason,key(a.account_id,symbol,reason,time))
        if reason=='data_emergency':
            for v in self.contrib.values():v['suppressed']=True
    def _exposure_override(self,a,time):
        exposures,eq=self.risk.exposures(a,self.marks)
        capped={n:min(self.c.instrument_cap,e) for n,e in exposures.items()};gross=sum(capped.values(),ZERO)
        factor=min(ONE,self.c.gross_cap/gross) if gross else ONE
        for n,e in exposures.items():
            allowed=capped[n]*factor
            if e>allowed and not any(o.status=='PENDING' and o.account_id==a.account_id and o.instrument==n and o.side=='SELL' for o in self.orders.values()):
                self._target(a,n,allowed,time,'exposure_limit',key(a.account_id,n,'cap',time),self.atr.get(n,ZERO))
    def heartbeat(self,time):
        self.store.ensure_writable()
        if self.store.db.execute('SELECT 1 FROM events WHERE event_key=?',(key(self.c.name,'timer',time),)).fetchone():return
        if time<self.now:return
        before=self.snapshot()
        try:
            with self.store.transaction():
                self.cause_event=key(self.c.name,'timer',time)
                for a in self.accounts.values():
                    if a.state in ('HALTED_DRAWDOWN','RECOVERY_ERROR'):continue
                    age=max((time-int(self.last_market[n]) for n in self.c.instruments if n in self.last_market),default=0)
                    if age>self.c.emergency_ms:
                        a.state='EXIT_PENDING';self._protect(a,time,'data_emergency')
                    elif age>self.c.stale_ms:
                        a.state='DATA_STALE';self._cancel_buys(a)
                self.store.append(key(self.c.name,'timer',time),'HEARTBEAT',time,dict(now=time))
                self.now=time;self._aggregate(time)
                self.store.checkpoint(key('heartbeat',time),time,self.snapshot())
        except BaseException:self.restore(before);raise
    def summary(self):
        return primitive(dict(now=self.now,accounts=self.accounts,pending_orders=sum(o.status=='PENDING' for o in self.orders.values()),signals=len(self.contrib),marks=self.marks))
