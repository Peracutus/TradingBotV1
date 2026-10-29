"""Causal prefix-only adapters. No backtest main(), no private exchange calls."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from math import log,sqrt
import statistics
from .models import *
from .registry import records,ROOT
from research_v0.core import Candle,EmaCrossStrategy
from research_v0.resample import complete_utc_days
from research_v0 import strategies as hmod
import research_framework as legacy

@dataclass
class Agent:
    metadata:dict
    model:object=None
    legacy_id:int|None=None
    def analyze(self,history,event,now):
        m=self.metadata;n=m['instrument'];interval=m['interval_ms'];data=history[interval]
        rows=data[n]
        if len(rows)<201:raise ValueError('BLOCKED: insufficient_warmup')
        if any(x.time+interval>now for x in rows):raise ValueError('future input')
        if rows[-1].time+interval!=event.close_ms:raise ValueError('incomplete/unaligned strategy bar')
        if self.legacy_id is not None:
            # Cross-asset legacy rules must see the same completed calendar prefix.
            if [x.time for x in data['BTC']]!=[x.time for x in data['ETH']]:raise ValueError('cross_asset_timeline_mismatch')
            dictionaries,feats=history['_legacy'][interval];i=len(rows)-1
            entry,exit=legacy.signal(self.legacy_id,n,i,dictionaries,feats);atr=feats[n]['atr'][i];scale=1.
            if self.legacy_id==27:scale=min(1.,legacy.mean(feats[n]['atr'][i-50:i])/atr) if atr else 1.
            if self.legacy_id==28:
                recent=[log(feats[n]['close'][j]/feats[n]['close'][j-1]) for j in range(i-29,i+1)]
                vol=statistics.pstdev(recent)*sqrt(365);scale=min(1.,.2/vol) if vol else 1.
        else:
            cache=history.setdefault('_prepared',{})
            identity=key(type(self.model).__name__,self.model,n,len(rows),rows[-1].time)
            if identity not in cache:cache[identity]=self.model.prepare(rows)
            prepared=cache[identity];i=len(rows)-1
            entry=self.model.evaluate(n,rows,prepared,i) is not None
            exit=self.model.exit_signal(n,rows,prepared,i);atr=prepared.atr[i];scale=1.
        # Original engines exit before entries. An exit condition on an empty account
        # must not hide a simultaneous permitted entry; runner normalizes against episode state.
        action=Action.ENTRY if entry else Action.EXIT if exit else Action.HOLD
        return StrategyDecision(m['agent_id'],m['implementation_id'],m['canonical_id'],n,m['family'],action,D(str(scale)) if entry else ZERO,now,rows[-1].time+2*interval+90_000,event.event_id,D(str(atr)),'entry' if entry else 'exit' if exit else 'no_new_decision',interval,dict(atr=str(atr),implementation_hash=m['implementation_hash'],entry=bool(entry),exit=bool(exit)))

def analysis_context(history):
    ctx={interval:{n:tuple(rows) for n,rows in market.items()} for interval,market in history.items()};ctx['_legacy']={};ctx['_prepared']={}
    data=history[DAY]
    dictionaries={a:[dict(open=x.open,high=x.high,low=x.low,close=x.close,volume=x.volume) for x in rows] for a,rows in data.items()}
    if all(dictionaries.values()):ctx['_legacy'][DAY]=(dictionaries,{a:legacy.features(rows) for a,rows in dictionaries.items()})
    return ctx

def build_agents(history,scope='all'):
    history=analysis_context(history)
    registry=records();agents=[];diagnostics=[]
    models={name:getattr(hmod,name)() for name in ('H01EmaTrend','H02ConfirmedMa','H03NormalizedSlope','H04Donchian','H05DailyMomentum','H06DailyMomentumAcceleration','H08BollingerRange','H09RsiRange','H10VolatilityCompression')}
    for rec in registry:
        hid=rec['hypothesis_id'];model=None;legacy_id=None
        if rec['reason']:
            diagnostics.append(dict(implementation=rec['implementation_id'],reason=rec['reason']));continue
        if hid.startswith('legacy50:'):
            if scope=='baseline':continue
            legacy_id=int(hid.split(':')[1]);interval=DAY
        else:
            name=hid.split(':')[1]
            if name.startswith('baseline'):model=EmaCrossStrategy();interval=H4
            else:
                if scope=='baseline':continue
                model=next(m for k,m in models.items() if k.startswith(name) and (len(name)>2 or k[3:4].isalpha())) if name not in ('H01','H02','H03','H04','H05','H06','H08','H09','H10') else next(m for k,m in models.items() if k.startswith(name))
                interval=DAY if name in ('H05','H06') else H4
        allowed=[]
        for instrument in ('BTC','ETH'):
            if legacy_id in (42,43) and instrument=='BTC':continue
            rows=history[interval][instrument]
            if len(rows)<201:diagnostics.append(dict(implementation=rec['implementation_id'],instrument=instrument,reason='insufficient_warmup'));continue
            canonical_id='legacy50:06' if legacy_id in (27,28) else 'research20:baseline_causal' if hid=='research20:baseline_lock3' else hid
            account=hid # one shared account for multi-instrument strategy
            meta=dict(agent_id=hid+':'+instrument,implementation_id=rec['implementation_id'],implementation_hash=rec['implementation_hash'],canonical_id=canonical_id,instrument=instrument,interval_ms=interval,account_id=account,family=rec['family'],policy='lock3' if hid.endswith('baseline_lock3') else 'base',aggregate=hid!='research20:baseline_lock3' and legacy_id not in (27,28),admission='R0')
            a=Agent(meta,model,legacy_id)
            # Two deterministic prefix calls + truncation ensure adapter only uses admitted data.
            fake=MarketEvent(instrument,rows[-1].time,D(str(rows[-1].open)),D(str(rows[-1].high)),D(str(rows[-1].low)),D(str(rows[-1].close)),interval_ms=interval)
            try:
                x=a.analyze(history,fake,fake.close_ms);y=a.analyze(history,fake,fake.close_ms)
                if canonical(x)!=canonical(y) or x.available_at<fake.close_ms or x.atr<=0:raise ValueError('adapter validation')
            except Exception as exc:
                diagnostics.append(dict(agent=meta['agent_id'],reason=str(exc)));continue
            meta['admission']='R1';agents.append(a);allowed.append(instrument)
        if allowed:
            rec.update(level='R1',modes=['replay','isolated_paper','aggregate'] if hid!='research20:baseline_lock3' else ['replay','isolated_paper'],validated_instruments=allowed,technical_evidence='deterministic closed-prefix adapter check; no OOS claim')
    return agents,registry,diagnostics
