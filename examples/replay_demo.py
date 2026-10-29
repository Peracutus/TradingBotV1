"""Synthetic offline accounting example. No market data or live order API."""
import sys, tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from trading_v1.models import *
from trading_v1.store import Store
from trading_v1.engine import Engine

def main():
    config=ExperimentConfig(name="synthetic-demo")
    agent=dict(agent_id="demo:BTC",implementation_id="demo:v1",canonical_id="demo",instrument="BTC",account_id="demo",family="trend",policy="base",aggregate=True,admission="R1")
    with tempfile.TemporaryDirectory() as folder:
        store=Store(Path(folder)/"demo.sqlite",config)
        try:
            engine=Engine(config,store,[agent])
            for i in range(5):
                t=H4-MINUTE+i*MINUTE
                batch=[MarketEvent(n,t,D("100"),D("101"),D("99"),D("100"),source="synthetic") for n in config.instruments]
                signals=[]
                if i==0:
                    signals=[StrategyDecision(agent["agent_id"],agent["implementation_id"],agent["canonical_id"],"BTC","trend",Action.ENTRY,ONE,batch[0].close_ms,batch[0].close_ms+H4,batch[0].event_id,ONE,"synthetic example")]
                engine.process(batch,signals)
            print("SYNTHETIC DEMONSTRATION — not a real-market performance result")
            print(canonical(engine.summary()))
        finally:store.close()

if __name__=="__main__":main()
