from __future__ import annotations
import asyncio,time,json
from pathlib import Path
from .models import *
from .adapters import build_agents
from .registry import persist_registry
from .orchestrator import Orchestrator
from .engine import Engine
from .store import Store
class Runtime:
    def __init__(self,config,path,history,scope='all'):
        self.config=config;self.history=history
        if config.agent_scope!=scope:raise ValueError('agent scope differs from immutable experiment')
        self.agents,self.registry,self.blocked=build_agents(history.bars,scope)
        from dataclasses import replace
        admitted=key([a.metadata for a in self.agents])
        if config.admission_hash and config.admission_hash!=admitted:raise ValueError('admitted membership changed; use original seed/config')
        config=replace(config,admission_hash=admitted);self.config=config
        self.store=Store(path,config)
        persist_registry(self.store,self.registry,[dict(instrument=n,timeframes=[MINUTE,H4,DAY],provider='Bybit spot',history_hashes=history.hashes) for n in ('BTC','ETH')])
        self.engine=Engine(config,self.store,[a.metadata for a in self.agents])
        # Reconstruct rolling indicators/partial4h buckets from recorded inputs on restart.
        for row in self.store.db.execute("SELECT payload FROM events WHERE type='MARKET' ORDER BY seq"):
            e=json.loads(row[0])['event'];event=MarketEvent(**{k:D(v) if k in ('open','high','low','close','volume') else v for k,v in e.items()})
            self.history.push([event])
        self.orchestrator=Orchestrator(self.agents,config.agent_timeout_s)
        self.processed=0;self.recovered_batches=0;self.fresh_batches=0;self.started=int(time.time()*1000);self.status='READY';self.errors=[];self.run=None
    async def step(self,events):
        self.store.ensure_writable()
        batch=key([e.event_id for e in sorted(events,key=lambda e:e.instrument)])
        if self.store.seen(batch):return self.engine.process(events)
        # Rejected corrections must not enter indicator history.
        for event in events:self.engine.observe_market(event)
        self.history.push(events)
        now=max(e.ready_at for e in events)
        if self.config.mode=='paper':now=max(now,int(time.time()*1000))
        if any(e.recovered for e in events):decisions=[];diagnostics=[]
        else:decisions,diagnostics=await self.orchestrator.analyze(self.history.bars,events,now)
        # On an existing isolated episode, exit wins over an overlapping entry.
        from dataclasses import replace
        adjusted=[]
        for d in decisions:
            a=self.engine.accounts[self.engine.agent_meta[d.agent_id]['account_id']];p=a.positions.get(d.instrument)
            if p and p.quantity and d.indicators.get('exit'):d=replace(d,action=Action.EXIT,target=ZERO)
            adjusted.append(d)
        if self.config.mode=='paper':
            completed=max(now,int(time.time()*1000))
            events=[replace(e,available_at=completed) for e in events]
            adjusted=[replace(d,available_at=completed) for d in adjusted]
        result=self.engine.process(events,adjusted,diagnostics)
        if result:
            self.processed+=1
            if any(e.recovered for e in events):self.recovered_batches+=1
            else:self.fresh_batches+=1
        return result
    async def replay(self,source):
        self.status='REPLAY'
        async for batch in source:
            if isinstance(batch,int):self.engine.heartbeat(batch)
            else:await self.step(batch)
        self.status='REPLAY_COMPLETE'
    def snapshot(self):
        from .metrics import account_metrics
        run=self.run.view() if self.run else None
        return dict(account_metrics=primitive(account_metrics(self.store,self.engine)),status=run['process_state'] if run else self.status,experiment_status=self.status,run=run,config_hash=self.config.config_hash,started=self.started,processed_batches=self.processed,recovered_batches=self.recovered_batches,fresh_batches=self.fresh_batches,agents=self.orchestrator.metrics,registry=self.registry,blocked=self.blocked,engine=self.engine.summary(),errors=self.errors)
    def close(self):self.orchestrator.close();self.store.close()
