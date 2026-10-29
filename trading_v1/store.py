"""One-writer SQLite journal; fills and state projections commit atomically."""
from __future__ import annotations
import json,sqlite3,threading,os
from contextlib import contextmanager
from pathlib import Path
from .models import canonical,key

class JournalConflict(ValueError):
    def __init__(self,diagnostic):
        self.diagnostic=diagnostic
        super().__init__('conflicting duplicate event: '+diagnostic['type']+' '+diagnostic['event_id'])

class MarketDataConflict(JournalConflict):pass

def payload_diff(old,new,path=''):
    if isinstance(old,dict) and isinstance(new,dict):
        return [d for k in sorted(old.keys()|new.keys()) for d in payload_diff(old.get(k),new.get(k),path+'/'+k)]
    return [] if old==new else [dict(path=path,old=old,new=new)]
SCHEMA='''
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS experiments(id TEXT PRIMARY KEY,config_hash TEXT NOT NULL,config TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS hypotheses(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS data_capabilities(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_key TEXT UNIQUE NOT NULL,type TEXT NOT NULL,time_ms INTEGER NOT NULL,payload TEXT NOT NULL,previous_hash TEXT NOT NULL,event_hash TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS immutable_events_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'immutable journal'); END;
CREATE TRIGGER IF NOT EXISTS immutable_events_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'immutable journal'); END;
CREATE TRIGGER IF NOT EXISTS immutable_experiments_update BEFORE UPDATE ON experiments BEGIN SELECT RAISE(ABORT,'immutable experiment'); END;
CREATE TRIGGER IF NOT EXISTS immutable_experiments_delete BEFORE DELETE ON experiments BEGIN SELECT RAISE(ABORT,'immutable experiment'); END;
CREATE TABLE IF NOT EXISTS signals(id TEXT PRIMARY KEY,agent TEXT NOT NULL,time_ms INTEGER NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_decisions(id TEXT PRIMARY KEY,account TEXT NOT NULL,time_ms INTEGER NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY,account TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fills(id TEXT PRIMARY KEY,order_id TEXT NOT NULL,account TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_state(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS positions(account TEXT NOT NULL,instrument TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,instrument));
CREATE TABLE IF NOT EXISTS consumer_offsets(consumer TEXT PRIMARY KEY,event_seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY,event_seq INTEGER NOT NULL,state_hash TEXT NOT NULL,payload TEXT NOT NULL);
'''

def changes(before,after,path=()):
    result=[]
    if isinstance(before,dict) and isinstance(after,dict):
        for name in sorted(before.keys()-after.keys()):result.append([list(path+(name,)),None,True])
        for name in sorted(after):
            if name not in before:result.append([list(path+(name,)),after[name],False])
            else:result.extend(changes(before[name],after[name],path+(name,)))
    elif before!=after:result.append([list(path),after,False])
    return result

def apply_changes(state,delta):
    for path,value,remove in delta:
        target=state
        for name in path[:-1]:target=target[name]
        if remove:del target[path[-1]]
        else:target[path[-1]]=value
    return state

class Store:
    def __init__(self,path,config):
        Path(path).parent.mkdir(parents=True,exist_ok=True);self.path=str(path);self.owner=threading.get_ident()
        self.db=sqlite3.connect(path,isolation_level=None);self.db.row_factory=sqlite3.Row;self.db.executescript(SCHEMA)
        old=self.db.execute('SELECT config_hash FROM experiments WHERE id=?',(config.name,)).fetchone()
        if old and old[0]!=config.config_hash:
            self.db.close();raise ValueError('experiment configuration changed; create a new experiment')
        if not old and self.db.execute('SELECT count(*) FROM experiments').fetchone()[0]:
            self.db.close();raise ValueError('one experiment per database')
        if not old:self.db.execute('INSERT INTO experiments VALUES(?,?,?)',(config.name,config.config_hash,canonical(config)))
        self.experiment=config.name;self.failpoint=None
        self.conflict_path=Path(str(path)+'.conflicts.jsonl')
        self.integrity_error=self.conflict_path.exists() and self.conflict_path.stat().st_size>0
    def ensure_writable(self):
        if self.integrity_error:raise ValueError('RECOVERY_ERROR: unresolved journal/data conflict; financial processing blocked')
    def record_conflict(self,event_key,kind,time,old,new,**context):
        diagnostic=dict(event_id=event_key,type=kind,time_ms=time,old=old,new=new,
                        old_payload_hash=key(old),new_payload_hash=key(new),diff=payload_diff(old,new),**context)
        self.integrity_error=True
        # Separate durable diagnostic survives rollback of the financial transaction.
        with self.conflict_path.open('a') as out:
            out.write(canonical(diagnostic)+'\n');out.flush();os.fsync(out.fileno())
        return diagnostic
    @contextmanager
    def transaction(self):
        if threading.get_ident()!=self.owner:raise RuntimeError('single writer violated')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            if self.failpoint=='before_commit':raise RuntimeError('injected before_commit')
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK');raise
    def append(self,event_key,kind,time,payload):
        self.ensure_writable()
        body=canonical(payload);old=self.db.execute('SELECT seq,type,time_ms,payload FROM events WHERE event_key=?',(event_key,)).fetchone()
        if old:
            if old['type']!=kind or old['payload']!=body or old['time_ms']!=time:
                diagnostic=self.record_conflict(event_key,kind,time,json.loads(old['payload']),json.loads(body),old_seq=old['seq'],old_time_ms=old['time_ms'],old_type=old['type'],source='internal' if kind!='MARKET' else 'market',policy='HALT_REQUIRE_REVIEW')
                raise JournalConflict(diagnostic)
            return old['seq']
        previous=self.db.execute('SELECT event_hash FROM events ORDER BY seq DESC LIMIT 1').fetchone();previous=previous[0] if previous else ''
        digest=key(previous,event_key,kind,time,body)
        cur=self.db.execute('INSERT INTO events(event_key,type,time_ms,payload,previous_hash,event_hash) VALUES(?,?,?,?,?,?)',(event_key,kind,time,body,previous,digest))
        return cur.lastrowid
    def seen(self,batch):return self.db.execute('SELECT 1 FROM events WHERE event_key=?',(key(self.experiment,'batch',batch),)).fetchone() is not None
    def checkpoint(self,batch,time,state):
        body=canonical(state)
        previous=self.db.execute('SELECT payload,state_hash FROM checkpoints WHERE id=?',(self.experiment,)).fetchone()
        if previous:
            payload=dict(base_hash=previous['state_hash'],result_hash=key(state),changes=changes(json.loads(previous['payload']),json.loads(body)))
            seq=self.append(key(self.experiment,'batch',batch),'ENGINE_STATE_DELTA',time,payload)
        else:seq=self.append(key(self.experiment,'batch',batch),'ENGINE_STATE',time,state)
        self.db.execute('INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?)',(self.experiment,seq,key(state),body))
        self.db.execute('INSERT OR REPLACE INTO consumer_offsets VALUES(?,?)',(self.experiment,seq))
        for name,a in state['accounts'].items():
            self.db.execute('INSERT OR REPLACE INTO portfolio_state VALUES(?,?)',(name,canonical(a)))
            for symbol,p in a['positions'].items():self.db.execute('INSERT OR REPLACE INTO positions VALUES(?,?,?)',(name,symbol,canonical(p)))
    def recover(self):
        row=self.db.execute('SELECT * FROM checkpoints WHERE id=?',(self.experiment,)).fetchone()
        if row is None:return None
        state=json.loads(row['payload']);event=self.db.execute('SELECT * FROM events WHERE seq=?',(row['event_seq'],)).fetchone()
        if key(state)!=row['state_hash'] or not event:raise ValueError('RECOVERY_ERROR: checkpoint mismatch')
        if event['type']=='ENGINE_STATE' and event['payload']!=row['payload']:raise ValueError('RECOVERY_ERROR: checkpoint mismatch')
        if event['type']=='ENGINE_STATE_DELTA' and json.loads(event['payload'])['result_hash']!=row['state_hash']:raise ValueError('RECOVERY_ERROR: delta checkpoint mismatch')
        latest=self.db.execute("SELECT max(seq) FROM events WHERE type IN ('ENGINE_STATE','ENGINE_STATE_DELTA')").fetchone()[0]
        if latest!=row['event_seq']:raise ValueError('RECOVERY_ERROR: checkpoint offset')
        offset=self.db.execute('SELECT event_seq FROM consumer_offsets WHERE consumer=?',(self.experiment,)).fetchone()
        if not offset or offset[0]!=row['event_seq']:raise ValueError('RECOVERY_ERROR: consumer offset')
        previous='';rebuilt=None;state_hash=None
        for e in self.db.execute('SELECT * FROM events ORDER BY seq'):
            expected=key(previous,e['event_key'],e['type'],e['time_ms'],e['payload'])
            if previous!=e['previous_hash'] or expected!=e['event_hash']:raise ValueError('RECOVERY_ERROR: journal hash chain')
            previous=e['event_hash']
            if e['type']=='ENGINE_STATE':rebuilt=json.loads(e['payload']);state_hash=key(rebuilt)
            elif e['type']=='ENGINE_STATE_DELTA':
                delta=json.loads(e['payload'])
                if state_hash!=delta['base_hash']:raise ValueError('RECOVERY_ERROR: delta chain')
                rebuilt=apply_changes(rebuilt,delta['changes']);state_hash=delta['result_hash']
        if rebuilt is None or key(rebuilt)!=row['state_hash']:raise ValueError('RECOVERY_ERROR: journal state reconstruction')
        for name,a in state['accounts'].items():
            projection=self.db.execute('SELECT payload FROM portfolio_state WHERE id=?',(name,)).fetchone()
            if not projection or projection[0]!=canonical(a):raise ValueError('RECOVERY_ERROR: financial projection mismatch')
            for symbol,p in a['positions'].items():
                projected=self.db.execute('SELECT payload FROM positions WHERE account=? AND instrument=?',(name,symbol)).fetchone()
                if not projected or projected[0]!=canonical(p):raise ValueError('RECOVERY_ERROR: position projection mismatch')
        for order_id,o in state['orders'].items():
            projected=self.db.execute('SELECT payload FROM orders WHERE id=?',(order_id,)).fetchone()
            if not projected or projected[0]!=canonical(o):raise ValueError('RECOVERY_ERROR: pending order projection')
        mismatch=self.db.execute("SELECT count(*) FROM events e LEFT JOIN fills f ON f.id=e.event_key WHERE e.type='FILL' AND (f.id IS NULL OR f.payload!=e.payload)").fetchone()[0]
        fill_count=self.db.execute('SELECT count(*) FROM fills').fetchone()[0]
        event_fill_count=self.db.execute("SELECT count(*) FROM events WHERE type='FILL'").fetchone()[0]
        if mismatch or fill_count!=event_fill_count:raise ValueError('RECOVERY_ERROR: fill projection')
        # Rebuild accounting from immutable fill events, never trust mutable fill projections alone.
        from decimal import Decimal as D
        balances={n:D(state['initial_capital']) for n in state['accounts']};quantities={};basis={};realized={n:D(0) for n in balances}
        for e in self.db.execute("SELECT payload FROM events WHERE type='FILL' ORDER BY seq"):
            f=json.loads(e[0]);n=f['account_id'];k=(n,f['instrument']);q=D(f['quantity']);p=D(f['price']);fee=D(f['fee']);b=D(f['cost_basis'])
            sign=1 if f['side']=='BUY' else -1
            balances[n]+=(-q*p-fee if sign==1 else q*p-fee)
            quantities[k]=quantities.get(k,D(0))+sign*q;basis[k]=basis.get(k,D(0))+(q*p+fee if sign==1 else -b)
            realized[n]+=D(f['realized_pnl'])
        for n,a in state['accounts'].items():
            eq=D(a['cash'])+sum((D(p['quantity'])*D(state['marks'][symbol]) for symbol,p in a['positions'].items() if D(p['quantity'])),D(0))
            if abs(eq-D(a['equity']))>D('1e-18'):raise ValueError('RECOVERY_ERROR: equity reconstruction')
            if abs(balances[n]-D(a['cash']))>D('1e-18') or abs(realized[n]-D(a['realized_pnl']))>D('1e-18'):raise ValueError('RECOVERY_ERROR: cash reconstruction')
            for s,p in a['positions'].items():
                if quantities.get((n,s),D(0))!=D(p['quantity']) or abs(basis.get((n,s),D(0))-D(p['cost']))>D('1e-18'):raise ValueError('RECOVERY_ERROR: position reconstruction')
        return state
    def iter_states(self):
        state=None
        for kind,payload in self.db.execute("SELECT type,payload FROM events WHERE type IN ('ENGINE_STATE','ENGINE_STATE_DELTA') ORDER BY seq"):
            if kind=='ENGINE_STATE':state=json.loads(payload)
            else:state=apply_changes(state,json.loads(payload)['changes'])
            yield state
    def close(self):self.db.close()
