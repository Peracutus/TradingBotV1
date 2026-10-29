from __future__ import annotations
import csv,gzip,json,hashlib,asyncio,ssl,time
from pathlib import Path
from contextlib import ExitStack
from collections import defaultdict
from .models import *
from .registry import ROOT
from research_v0.core import Candle,load_candles
from research_v0.resample import complete_utc_days
from .store import MarketDataConflict

class History:
    def __init__(self,seed,start):
        self.bars={H4:{},DAY:{}};self.minute_groups={};self.hashes={}
        for n in ('BTC','ETH'):
            p=(ROOT/'baselines/2026-10-07_ema30_100_200_stop2'/f'{n}USDT_240.csv' if seed=='frozen' else ROOT/f'five_year_validation/data/{n}USDT/{n}USDT_240.csv')
            rows,manifest=load_candles(p,n);self.hashes[str(p)]=manifest.raw_file_hash
            self.bars[H4][n]=[r for r in rows if r.time+H4<=start]
            self.bars[DAY][n]=complete_utc_days(self.bars[H4][n])
    def save(self,path):
        payload=dict(hashes=self.hashes,bars={n:[dict(time=r.time,open=r.open,high=r.high,low=r.low,close=r.close,volume=r.volume) for r in rows] for n,rows in self.bars[H4].items()})
        Path(path).write_text(canonical(payload)+'\n')
    @classmethod
    def from_bundle(cls,path):
        payload=json.loads(Path(path).read_text());obj=cls.__new__(cls);obj.minute_groups={};obj.hashes=payload['hashes'];obj.bars={H4:{},DAY:{}}
        for n,rows in payload['bars'].items():
            obj.bars[H4][n]=[Candle(int(r['time']),*(float(r[k]) for k in ('open','high','low','close','volume'))) for r in rows]
            obj.bars[DAY][n]=complete_utc_days(obj.bars[H4][n])
        return obj
    def push(self,events):
        for e in events:
            group_time=e.open_ms//H4*H4;k=(e.instrument,group_time)
            group=self.minute_groups.setdefault(k,[]);group.append(e)
            for old in list(self.minute_groups):
                if old[0]==e.instrument and old[1]<group_time:del self.minute_groups[old]
            if e.close_ms%H4:continue
            if [x.open_ms for x in group]!=list(range(group_time,group_time+H4,MINUTE)):continue
            bar=Candle(group_time,float(group[0].open),max(float(x.high) for x in group),min(float(x.low) for x in group),float(group[-1].close),sum(float(x.volume) for x in group))
            n=e.instrument;rows=self.bars[H4][n]
            if not rows or bar.time>rows[-1].time:
                if rows and bar.time-rows[-1].time!=H4:raise ValueError('history gap; bootstrap incomplete')
                rows.append(bar)
                if e.close_ms%DAY==0:
                    six=rows[-6:]
                    if len(six)==6 and [r.time for r in six]==list(range(e.close_ms-DAY,e.close_ms,H4)):
                        day=Candle(six[0].time,six[0].open,max(x.high for x in six),min(x.low for x in six),six[-1].close,sum(x.volume for x in six))
                        self.bars[DAY][n].append(day)

class ReplaySource:
    def __init__(self,start,end):
        if start%H4 or end%MINUTE or end<=start:raise ValueError('replay needs a4h-aligned UTC start and later minute-aligned end')
        for n in ('BTC','ETH'):
            m=json.loads((ROOT/f'external_benchmarks/observed_1m/{n}USDT/manifest.json').read_text())
            if start<m['coverage_start_ms'] or end>m['coverage_end_ms']+MINUTE:raise ValueError('requested replay window outside observed minute coverage')
        self.start=start;self.end=end
    async def __aiter__(self):
        with ExitStack() as stack:
            streams={n:csv.DictReader(stack.enter_context(gzip.open(ROOT/f'external_benchmarks/observed_1m/{n}USDT/{n}USDT_1m.csv.gz','rt'))) for n in ('BTC','ETH')}
            for pair in zip(*streams.values(),strict=True):
                if len({r['time'] for r in pair})!=1:raise ValueError('minute alignment')
                t=int(pair[0]['time'])
                if t<self.start:continue
                if t>=self.end:break
                yield [MarketEvent(n,t,*(D(r[k]) for k in ('open','high','low','close','volume')),source='Bybit_saved_1m') for n,r in zip(streams,pair)]

class RecordedSource:
    def __init__(self,path):self.path=path
    async def __aiter__(self):
        import sqlite3
        db=sqlite3.connect(f'file:{Path(self.path).resolve()}?mode=ro',uri=True)
        try:
            batch=[];t=None
            for kind,payload in db.execute("SELECT type,payload FROM events WHERE type IN ('MARKET','HEARTBEAT') ORDER BY seq"):
                if kind=='HEARTBEAT':
                    if batch:yield batch;batch=[];t=None
                    yield int(json.loads(payload)['now']);continue
                r=json.loads(payload)['event'];e=MarketEvent(**{k:D(v) if k in ('open','high','low','close','volume') else v for k,v in r.items()})
                if t is not None and t!=e.open_ms:yield batch;batch=[]
                batch.append(e);t=e.open_ms
            if batch:yield batch
        finally:db.close()

class BybitSource:
    """Only public endpoints; one HTTP session and one WS connection per runtime."""
    def __init__(self,archive):
        self.archive=Path(archive);self.archive.mkdir(parents=True,exist_ok=True);self.requests=0;self.connections=0;self.errors=[]
        self.observer=None;self.reject_correction=None;self.observed={}
        cafile='/etc/ssl/cert.pem';self.ssl=ssl.create_default_context(cafile=cafile if Path(cafile).exists() else None);self.limit=asyncio.Lock();self.expected=None;self.stopping=False
    def archive_raw(self,kind,payload):
        body=canonical(payload);digest=hashlib.sha256(body.encode()).hexdigest();p=self.archive/f'{kind}_{digest}.json'
        if not p.exists():p.write_text(body+'\n')
        return str(p)
    def observe(self,event):
        if self.observer:self.observer(event)
        old=self.observed.get(event.event_id)
        if old and old.data_hash!=event.data_hash:
            if self.reject_correction:self.reject_correction(old,event)
            diagnostic=dict(event_id=event.event_id,type='MARKET_CORRECTION_PENDING',old=primitive(old),new=primitive(event),policy='HALT_REQUIRE_REVIEW')
            self.archive_raw('correction',diagnostic)
            raise MarketDataConflict(diagnostic)
        self.observed[event.event_id]=event
        if len(self.observed)>6000:self.observed.pop(next(iter(self.observed)))
    async def rest(self,session,symbol,interval,start,end):
        params=dict(category='spot',symbol=symbol+'USDT',interval=str(interval),start=str(start),end=str(end-1),limit='1000')
        for attempt in range(5):
            async with self.limit:
                self.requests+=1
                async with session.get('https://api.bybit.com/v5/market/kline',params=params,ssl=self.ssl) as response:
                    if response.status==429:
                        delay=float(response.headers.get('Retry-After','2'));payload=None
                    else:response.raise_for_status();payload=await response.json();delay=0
            if payload is not None and payload.get('retCode')==0:
                if payload.get('result',{}).get('category')!='spot' or payload['result'].get('symbol')!=symbol+'USDT':raise ValueError('Bybit response venue/instrument mismatch')
                self.archive_raw('rest',dict(received_at=int(time.time()*1000),request=params,response=payload));return payload
            if attempt==4:raise RuntimeError(f'Bybit REST unavailable: {payload}')
            await asyncio.sleep(min(30,delay or 2**attempt))
    async def bootstrap(self,history,session,end):
        for n in ('BTC','ETH'):
            rows=history.bars[H4][n];cursor=rows[-1].time+H4
            while cursor+H4<=end:
                p=await self.rest(session,n,240,cursor,end)
                items=sorted(p['result']['list'],key=lambda r:int(r[0]));added=0
                for r in items:
                    t=int(r[0])
                    if t<cursor or t+H4>min(end,int(p['time'])):continue
                    if t!=rows[-1].time+H4:raise ValueError('bootstrap4h gap')
                    rows.append(Candle(t,*(float(r[k]) for k in range(1,6))));added+=1
                if not added:break
                cursor=rows[-1].time+H4
            history.bars[DAY][n]=complete_utc_days(rows)
        history.hashes['bootstrap_archive']=key(sorted(x.name for x in self.archive.glob('rest*')))
    async def recover(self,session,start,end):
        collected={}
        for n in ('BTC','ETH'):
            cursor=start;rows={}
            while cursor<end:
                p=await self.rest(session,n,1,cursor,min(end,cursor+1000*MINUTE))
                for r in p['result']['list']:
                    t=int(r[0])
                    if start<=t<end and t+MINUTE<=int(p['time']):rows[t]=r
                nxt=min(end,cursor+1000*MINUTE)
                if any(t not in rows for t in range(cursor,nxt,MINUTE)):raise ValueError('missing REST recovery minutes')
                cursor=nxt
            collected[n]=rows
        now=int(time.time()*1000)
        batches=[[MarketEvent(n,t,*(D(collected[n][t][k]) for k in range(1,6)),received_at=now,source='Bybit_public',recovered=True,transport='REST') for n in ('BTC','ETH')] for t in range(start,end,MINUTE)]
        for batch in batches:
            for event in batch:self.observe(event)
        return batches
    async def stream(self,session,start):
        self.expected=start;pending=defaultdict(dict)
        while not self.stopping:
            try:
                end=int(time.time()*1000)//MINUTE*MINUTE
                if self.expected<end:
                    for batch in await self.recover(session,self.expected,end):
                        self.expected=batch[0].close_ms;yield batch
                async with session.ws_connect('wss://stream.bybit.com/v5/public/spot',ssl=self.ssl,heartbeat=20,receive_timeout=40) as ws:
                    self.connections+=1
                    await ws.send_json(dict(op='subscribe',args=[f'kline.1.{n}USDT' for n in ('BTC','ETH')]))
                    async for message in ws:
                        import aiohttp
                        if message.type!=aiohttp.WSMsgType.TEXT:continue
                        payload=json.loads(message.data)
                        if not payload.get('topic','').startswith('kline.'):continue
                        n=payload['topic'].split('.')[-1].removesuffix('USDT')
                        if n not in ('BTC','ETH'):raise ValueError('unexpected public instrument')
                        now=int(time.time()*1000)
                        for r in payload.get('data',[]):
                            if r.get('confirm') is not True:continue
                            if str(r.get('interval'))!='1':raise ValueError('unexpected kline interval')
                            t=int(r['start'])
                            self.archive_raw('ws',dict(received_at=now,response=payload))
                            event=MarketEvent(n,t,*(D(r[k]) for k in ('open','high','low','close','volume')),received_at=now,source='Bybit_public',transport='WebSocket')
                            self.observe(event)
                            if t<self.expected:continue
                            if n in pending[t] and pending[t][n].data_hash!=event.data_hash:raise ValueError('conflicting WS closed candle')
                            pending[t][n]=event
                            if t>self.expected:
                                for batch in await self.recover(session,self.expected,t):self.expected=batch[0].close_ms;yield batch
                            if len(pending.get(self.expected,{}))==2:
                                batch=[pending[self.expected][a] for a in ('BTC','ETH')];del pending[self.expected];self.expected+=MINUTE;yield batch
                        if self.stopping:return
            except asyncio.CancelledError:raise
            except MarketDataConflict:raise
            except Exception as exc:
                self.errors.append(dict(at=int(time.time()*1000),error=str(exc)));self.errors=self.errors[-30:]
                await asyncio.sleep(2)
