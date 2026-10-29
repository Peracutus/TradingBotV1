from __future__ import annotations
import argparse,asyncio,json,time,signal,hashlib
from pathlib import Path
from dataclasses import replace
from datetime import datetime,timezone
from .models import *
from .data import History,ReplaySource,RecordedSource,BybitSource
from .runtime import Runtime
from .registry import ROOT,records

def timestamp(s):
    value=datetime.fromisoformat(s.replace('Z','+00:00'))
    if value.tzinfo is None:value=value.replace(tzinfo=timezone.utc)
    return int(value.timestamp()*1000)
def verify_baseline():
    folder=ROOT/'baselines/2026-10-07_ema30_100_200_stop2';manifest=json.loads((folder/'BASELINE_MANIFEST.json').read_text())
    out={n:hashlib.sha256((folder/n).read_bytes()).hexdigest()==v for n,v in manifest['files_sha256'].items()}
    if not all(out.values()):raise ValueError('Frozen baseline changed')
    return out

def make_config(args,history):
    versions={r['implementation_id']:r.get('implementation_hash','') for r in records()}
    archive=ROOT/'outputs/v1/source_versions';archive.mkdir(parents=True,exist_ok=True)
    for p in (ROOT/'trading_v1').glob('*.py'):
        digest=hashlib.sha256(p.read_bytes()).hexdigest();versions['runtime:'+p.name]=digest
        saved=archive/(digest+'.py')
        if not saved.exists():saved.write_bytes(p.read_bytes())
    if args.command=='replay':
        for n in ('BTC','ETH'):
            p=ROOT/f'external_benchmarks/observed_1m/{n}USDT/{n}USDT_1m.csv.gz';history.hashes[str(p)]=hashlib.sha256(p.read_bytes()).hexdigest()
            manifest=json.loads(p.with_name('manifest.json').read_text())
            if history.hashes[str(p)]!=manifest['observed_csv_gz_sha256']:raise ValueError('Minute archive hash mismatch')
    config=ExperimentConfig(name=args.name,agent_scope=args.scope,mode='paper' if args.command=='paper' else 'replay',implementation_versions=versions,input_hashes=history.hashes)
    if args.config:
        values=json.loads(Path(args.config).read_text())
        decimals={'initial_capital','fee','slippage','risk_limit','instrument_cap','gross_cap','max_drawdown'}
        allowed=decimals|{'latency_ms','stale_ms','emergency_ms','agent_timeout_s','family_weights'}
        if set(values)-allowed:raise ValueError('unsupported configuration fields: '+str(sorted(set(values)-allowed)))
        for k in decimals & values.keys():values[k]=D(str(values[k]))
        if 'family_weights' in values:values['family_weights']={k:D(str(v)) for k,v in values['family_weights'].items()}
        config=replace(config,**values)
    return replace(config,fee=config.fee*args.cost_multiple,slippage=config.slippage*args.cost_multiple)

async def replay(args):
    start=timestamp(args.start);end=timestamp(args.end);history=History(args.seed,start)
    config=make_config(args,history);runtime=Runtime(config,args.db,history,args.scope)
    try:
        if args.port:
            from .web import serve
            web=await serve(runtime,args.port)
        else:web=None
        await runtime.replay(ReplaySource(start,end));snapshot=runtime.snapshot();snapshot['frozen_integrity']=verify_baseline()
        out=Path(args.output);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(canonical(snapshot)+'\n')
        print(canonical(dict(status=runtime.status,agents=len(runtime.agents),batches=runtime.processed,accounts={n:{k:primitive(getattr(a,k)) for k in ('cash','equity','realized_pnl','max_dd')} for n,a in runtime.engine.accounts.items()},output=str(out))))
        if web:await web.cleanup()
    finally:runtime.close()

async def paper(args):
    import aiohttp
    from .web import serve
    from .lifecycle import PaperRun,atomic_json
    if args.hours<=0:raise ValueError('positive paper duration required')
    # A resumed financial journal is not a continuous acceptance run.
    if Path(args.db).exists():raise ValueError('paper acceptance requires a new database; preserve old run')
    acceptance_file=Path(args.db).with_suffix('.acceptance.json')
    if acceptance_file.exists():raise ValueError('paper run already exists')
    run=PaperRun(acceptance_file,run_id=args.run_id)
    runtime=None;web=None;producer=None
    out=Path(args.output)
    atomic_json(out,dict(status='RUNNING',run=run.view(),experiment_status='STARTING'))
    try:
        now=int(time.time()*1000);start=now//H4*H4;history=History('long',start)
        source=BybitSource(Path(args.db).parent/('public_data_'+run.document['run_id']))
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=35)) as session:
            await source.bootstrap(history,session,start)
            seedfile=Path(args.db).with_suffix('.seed.json');history.save(seedfile)
            history.hashes[str(seedfile)]=hashlib.sha256(seedfile.read_bytes()).hexdigest()
            config=make_config(args,history)
            runtime=Runtime(config,args.db,history,args.scope);runtime.run=run
            atomic_json(Path(args.db).with_suffix('.config.json'),runtime.config)
            source.observer=runtime.engine.observe_market;source.reject_correction=runtime.engine.market_conflict
            web=await serve(runtime,args.port);runtime.status='PAPER'
            stop=asyncio.Event();loop=asyncio.get_running_loop()
            for sig in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(sig,stop.set)
            queue=asyncio.Queue(maxsize=100)
            cursor=min(runtime.engine.last_market.values()) if runtime.engine.last_market else start
            async def produce():
                async for batch in source.stream(session,cursor):await queue.put(batch)
            producer=asyncio.create_task(produce())
            observed_start=time.monotonic();deadline=observed_start+args.hours*3600
            run.begin_observation(int(time.time()*1000),observed_start)
            last_heartbeat=0
            print(canonical(dict(status='RUNNING',run_id=run.document['run_id'],url=f'http://127.0.0.1:{args.port}',agents=len(runtime.agents),db=str(args.db))),flush=True)
            while not stop.is_set() and time.monotonic()<deadline:
                if producer.done():
                    producer.result()
                    raise RuntimeError('public source terminated')
                try:batch=await asyncio.wait_for(queue.get(),min(5.,max(.001,deadline-time.monotonic())))
                except asyncio.TimeoutError:batch=[]
                if batch:await runtime.step(batch);queue.task_done()
                now=int(time.time()*1000)
                if now-last_heartbeat>=5000:
                    runtime.engine.heartbeat(now);last_heartbeat=now
                    runtime.errors=source.errors[-30:]
                    unhealthy=runtime.fresh_batches>0 and any(a.state!='ACTIVE' for a in runtime.engine.accounts.values())
                    run.tick(now,time.monotonic(),connections=source.connections,processed=runtime.processed,unhealthy=unhealthy)
                    atomic_json(out,runtime.snapshot())
            # Final elapsed time is measured here, not rounded down to the preceding heartbeat.
            now=int(time.time()*1000)
            run.tick(now,time.monotonic(),connections=source.connections,processed=runtime.processed)
            verified=runtime.store.recover()==runtime.engine.snapshot()
            passed=(not stop.is_set() and run.document['observed_uptime_ms']>=run.document['required_ms']
                and runtime.fresh_batches>0 and verified and not run.document['heartbeat_gaps']
                and not run.document['protection_interruptions'] and not runtime.store.integrity_error
                and all(a.state=='ACTIVE' for a in runtime.engine.accounts.values()))
            run.document.update(final_reconciliation=verified,fresh_batches=runtime.fresh_batches,
                recovered_batches=runtime.recovered_batches,frozen_integrity=verify_baseline())
            run.finish('COMPLETED' if passed else 'STOPPED',now)
            runtime.status='PAPER_STOPPED';atomic_json(out,runtime.snapshot())
    except BaseException as exc:
        run.finish('FAILED',int(time.time()*1000),error=exc)
        if runtime:
            runtime.status='FAILED';atomic_json(out,runtime.snapshot())
        else:atomic_json(out,dict(status='FAILED',run=run.view()))
        raise
    finally:
        if producer:
            producer.cancel();await asyncio.gather(producer,return_exceptions=True)
        if web:await web.cleanup()
        if runtime:runtime.close()

async def recorded(args):
    # Reuse initial experiment config and seed with a new experiment identity only.
    import sqlite3
    db=sqlite3.connect(f'file:{Path(args.source_db).resolve()}?mode=ro',uri=True)
    cfg=json.loads(db.execute('SELECT config FROM experiments').fetchone()[0]);first=json.loads(db.execute("SELECT payload FROM events WHERE type='MARKET' ORDER BY seq LIMIT 1").fetchone()[0])['event']['open_ms'];db.close()
    for field in ('initial_capital','fee','slippage','risk_limit','instrument_cap','gross_cap','max_drawdown'):cfg[field]=D(cfg[field])
    cfg['agent_timeout_s']=float(cfg['agent_timeout_s'])
    cfg['family_weights']={k:D(v) for k,v in cfg['family_weights'].items()};cfg['instruments']=tuple(cfg['instruments']);cfg['name']=args.name;cfg['mode']='replay'
    bundle=Path(args.source_db).with_suffix('.seed.json')
    history=History.from_bundle(bundle) if bundle.exists() else History(args.seed,first)
    runtime=Runtime(ExperimentConfig(**cfg),args.db,history,args.scope)
    try:
        await runtime.replay(RecordedSource(args.source_db));Path(args.output).parent.mkdir(parents=True,exist_ok=True);Path(args.output).write_text(canonical(runtime.snapshot())+'\n');print(canonical(dict(status=runtime.status,output=args.output)))
    finally:runtime.close()

def controls(args):
    # Independent original engines remain independent, including their explicit limitations.
    from five_year_validation.causal_open_audit import replay as old_replay
    from research_v0.core import load_candles,backtest
    folder=ROOT/'baselines/2026-10-07_ema30_100_200_stop2'
    data={n:load_candles(folder/f'{n}USDT_240.csv',n)[0] for n in ('BTC','ETH')};times=[r.time for r in data['BTC']];lo=times.index(timestamp('2025-10-05T04:00:00Z'))
    original=backtest(data,lo,len(times));causal,_=old_replay(data,lo,len(times),use_current_open=True)
    apex_source=json.loads((ROOT/'cross_market/apex_usdjpy_v1_independent_replay_2026-10-13.json').read_text())
    from cross_market.replay_apex_usdjpy_v1 import load,atr_wilder,signal_and_time_exit
    from audit_apex_leader import run
    rows=load();atr=atr_wilder(rows);signals,exits=signal_and_time_exit(rows,atr);fx=run(rows,atr,signals,exits,risk=.5,cap=1.,cost=.016,causal=True)
    out=dict(frozen_integrity=verify_baseline(),frozen_return_pct=original.return_pct,causal_return_pct=causal.return_pct,apex_independent_reference=dict(return_pct=fx['return_pct'],trades=fx['trade_count'],source_commit=apex_source['source_commit'],new_engine_admission='BLOCKED: license and FX fill adapter unvalidated'),note='Reference engines, not substituted into new accounting or promoted to paper')
    Path(args.output).parent.mkdir(parents=True,exist_ok=True);Path(args.output).write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))

def main():
    p=argparse.ArgumentParser(description='Research only: no real order endpoints');sub=p.add_subparsers(dest='command',required=True)
    for name in ('replay','paper','recorded'):
        s=sub.add_parser(name);s.add_argument('--db',required=True);s.add_argument('--name',default='research-v1');s.add_argument('--scope',choices=['all','baseline'],default='all');s.add_argument('--output',default='outputs/v1/status.json')
        if name!='recorded':
            s.add_argument('--cost-multiple',type=int,choices=[1,2],default=1);s.add_argument('--config')
        if name=='replay':
            s.add_argument('--start',default='2026-09-01T00:00:00Z');s.add_argument('--end',default='2026-09-02T00:00:00Z');s.add_argument('--seed',choices=['long','frozen'],default='long');s.add_argument('--port',type=int,default=0)
        if name=='paper':s.add_argument('--hours',type=float,default=24.);s.add_argument('--port',type=int,default=8765);s.add_argument('--run-id')
        if name=='recorded':s.add_argument('--source-db',required=True);s.add_argument('--seed',choices=['long','frozen'],default='long')
    s=sub.add_parser('controls');s.add_argument('--output',default='outputs/v1/controls.json')
    args=p.parse_args()
    if args.command=='controls':controls(args)
    else:asyncio.run(globals()[args.command](args))
if __name__=='__main__':main()
