from __future__ import annotations
import asyncio,time
from concurrent.futures import ThreadPoolExecutor
from .models import *
class Orchestrator:
    def __init__(self,agents,timeout=10.,workers=4):
        self.agents=agents;self.timeout=timeout;self.pool=ThreadPoolExecutor(max_workers=workers,thread_name_prefix='analysis');self.limit=asyncio.Semaphore(workers)
        self.context_cache=None;self.context_identity=None
        self.metrics={a.metadata['agent_id']:dict(status='READY',calls=0,errors=0,latency_ms=0) for a in agents};self.quarantined=set()
    async def analyze(self,history,events,now):
        # Immutable prefix snapshot shared read-only during the whole timestamp batch.
        jobs=[];eventmap={e.instrument:e for e in events}
        from .adapters import analysis_context
        if history:
            identity=tuple((interval,n,len(rows),rows[-1].time if rows else -1) for interval,data in history.items() for n,rows in data.items())
            if identity!=self.context_identity:
                self.context_cache=await asyncio.get_running_loop().run_in_executor(self.pool,analysis_context,history)
                self.context_identity=identity
            history=self.context_cache
        async def one(a):
            ident=a.metadata['agent_id'];m=self.metrics[ident]
            if ident in self.quarantined:return None,dict(agent=ident,reason='QUARANTINED')
            interval=a.metadata['interval_ms'];n=a.metadata['instrument']
            if n not in eventmap or eventmap[n].close_ms%interval!=0:return None,None
            async with self.limit:
                start=time.monotonic();m['status']='RUNNING'
                future=asyncio.get_running_loop().run_in_executor(self.pool,a.analyze,history,eventmap[n],now)
                try:
                    result=await asyncio.wait_for(asyncio.shield(future),self.timeout)
                    m.update(status='READY',calls=m['calls']+1,latency_ms=(time.monotonic()-start)*1000)
                    return result,None
                except asyncio.TimeoutError:
                    self.quarantined.add(ident);m.update(status='QUARANTINED',errors=m['errors']+1)
                    future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
                    return None,dict(agent=ident,reason='analysis_timeout_thread_not_killed')
                except Exception as exc:
                    m.update(status='ERROR',errors=m['errors']+1)
                    if m['errors']>=3:self.quarantined.add(ident);m['status']='QUARANTINED'
                    return None,dict(agent=ident,reason=str(exc))
        for a in self.agents:jobs.append(asyncio.create_task(one(a),name=a.metadata['agent_id']))
        results=await asyncio.gather(*jobs)
        return sorted([r for r,d in results if r],key=lambda r:r.agent_id),[d for r,d in results if d]
    def close(self):self.pool.shutdown(wait=False,cancel_futures=True)
