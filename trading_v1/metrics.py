"""Descriptive metrics; no significance claims from short paper samples."""
from decimal import Decimal as D
from collections import defaultdict
import json,math,statistics
from .models import ZERO

def account_metrics(store,engine):
    totals=defaultdict(lambda:dict(fees=ZERO,turnover=ZERO,slippage=ZERO,fill_count=0))
    for row in store.db.execute('SELECT payload FROM fills'):
        f=json.loads(row[0]);t=totals[f['account_id']];t['fees']+=D(f['fee']);t['turnover']+=D(f['quantity'])*D(f['price']);t['slippage']+=D(f['slippage_cost']);t['fill_count']+=1
    for name,a in engine.accounts.items():
        t=totals[name];t['unrealized_pnl']=sum((p.quantity*engine.marks[n]-p.cost for n,p in a.positions.items()),ZERO)
        t['return_pct']=(a.equity/engine.c.initial_capital-1)*100;t['turnover_multiple']=t['turnover']/engine.c.initial_capital
    return totals

def daily_statistics(store):
    # Group by market interval, never by network receipt time.
    coverage=defaultdict(set)
    for (payload,) in store.db.execute("SELECT payload FROM events WHERE type='MARKET'"):
        e=json.loads(payload)['event']
        if e['instrument']=='BTC':coverage[int(e['open_ms'])//86400000].add(int(e['open_ms']))
    ends={}
    for state in store.iter_states():
        close=int(state['last_market'].get('BTC',0));day=(close-1)//86400000
        if close and close%86400000==0 and len(coverage[day])==1440:
            ends[day]={name:D(a['equity']) for name,a in state['accounts'].items()}
    equity=defaultdict(list)
    for day,accounts in sorted(ends.items()):
        for name,e in accounts.items():equity[name].append((day,e))
    output={}
    for name,series in equity.items():
        returns=[float(b[1]/a[1]-1) for a,b in zip(series,series[1:]) if b[0]-a[0]==1]
        n=len(returns);sd=statistics.stdev(returns) if n>=2 else 0;down=math.sqrt(sum(min(x,0)**2 for x in returns)/n) if n else 0
        output[name]=dict(complete_days=len(series),return_observations=n,sharpe=statistics.mean(returns)/sd*math.sqrt(365) if n>=30 and sd else None,sortino=statistics.mean(returns)/down*math.sqrt(365) if n>=30 and down else None,status='descriptive_only' if n>=30 else 'insufficient_30_daily_returns',daily_equity=[dict(day=d,equity=str(e)) for d,e in series])
    return output
