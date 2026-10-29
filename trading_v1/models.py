from __future__ import annotations
import hashlib,json
from dataclasses import dataclass,field,asdict,is_dataclass
from decimal import Decimal,ROUND_DOWN
from enum import StrEnum
from typing import Protocol,AsyncIterator

D=Decimal
ZERO=D('0');ONE=D('1');MINUTE=60_000;H4=14_400_000;DAY=86_400_000

def primitive(value):
    if is_dataclass(value):return primitive(asdict(value))
    if isinstance(value,Decimal):return str(value)
    if isinstance(value,dict):return {str(k):primitive(v) for k,v in sorted(value.items())}
    if isinstance(value,(list,tuple)):return [primitive(v) for v in value]
    if isinstance(value,float):return str(value)
    return value

def canonical(value):return json.dumps(primitive(value),sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def key(*parts):return hashlib.sha256(canonical(parts).encode()).hexdigest()
def qty(value):return max(ZERO,D(value)).quantize(D('.00000001'),rounding=ROUND_DOWN)

def decimal_text(value):
    """Exact numeric normalization, without Decimal context rounding."""
    if value == 0:return '0'
    text=format(value,'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text

class Action(StrEnum):ENTRY='ENTRY';EXIT='EXIT';HOLD='HOLD'
class Protection(StrEnum):
    ACTIVE='ACTIVE';DATA_STALE='DATA_STALE';EXIT_PENDING='EXIT_PENDING';STOP_LOCKED='STOP_LOCKED';HALTED_DRAWDOWN='HALTED_DRAWDOWN';RECOVERY_ERROR='RECOVERY_ERROR'

@dataclass(frozen=True)
class ExperimentConfig:
    name:str='research-v1'
    initial_capital:Decimal=D('1000')
    fee:Decimal=D('.001')
    slippage:Decimal=D('.0005')
    latency_ms:int=1000
    risk_limit:Decimal=D('.005')
    instrument_cap:Decimal=D('.375')
    gross_cap:Decimal=D('.75')
    max_drawdown:Decimal=D('.10')
    stale_ms:int=90_000
    emergency_ms:int=300_000
    agent_timeout_s:float=10.
    instruments:tuple[str,...]=('BTC','ETH')
    family_weights:dict[str,Decimal]=field(default_factory=lambda:{n:D('.25') for n in ('trend','breakout','mean_reversion','volatility')})
    implementation_versions:dict[str,str]=field(default_factory=dict)
    input_hashes:dict[str,str]=field(default_factory=dict)
    mode:str='replay'
    agent_scope:str='all'
    admission_hash:str=''
    @property
    def config_hash(self):return key(self)
    def __post_init__(self):
        if self.mode not in ('replay','paper'):raise ValueError('no live order mode')
        for n in ('initial_capital','fee','slippage','risk_limit','instrument_cap','gross_cap','max_drawdown'):
            v=getattr(self,n)
            if not isinstance(v,Decimal) or not v.is_finite():raise ValueError(n)
        if self.initial_capital<=0 or not ZERO<self.risk_limit<=ONE or not ZERO<self.instrument_cap<=self.gross_cap<=ONE:raise ValueError('invalid risk limits')
        if not ZERO<=self.fee<ONE or not ZERO<=self.slippage<ONE or not ZERO<self.max_drawdown<ONE:raise ValueError('invalid costs/drawdown')
        if set(self.family_weights)!= {'trend','breakout','mean_reversion','volatility'}:raise ValueError('unknown/missing strategy family')
        if any(not isinstance(v,Decimal) or not v.is_finite() for v in self.family_weights.values()):raise ValueError('finite Decimal weights required')
        if sum(self.family_weights.values())!=ONE or any(v<0 for v in self.family_weights.values()):raise ValueError('family weights')
        if self.latency_ms<0 or not 0<self.stale_ms<self.emergency_ms:raise ValueError('timing')

@dataclass(frozen=True)
class MarketEvent:
    instrument:str
    open_ms:int
    open:Decimal
    high:Decimal
    low:Decimal
    close:Decimal
    volume:Decimal=ZERO
    interval_ms:int=MINUTE
    received_at:int|None=None
    available_at:int|None=None
    source:str='archive'
    recovered:bool=False
    transport:str='archive'
    @property
    def close_ms(self):return self.open_ms+self.interval_ms
    @property
    def event_id(self):return key(self.source,self.instrument,self.interval_ms,self.open_ms)
    @property
    def data_hash(self):return key(self.instrument,self.open_ms,self.interval_ms,*map(decimal_text,(self.open,self.high,self.low,self.close,self.volume)))
    @property
    def ready_at(self):return max(self.close_ms,self.received_at or self.close_ms,self.available_at or self.close_ms)
    def __post_init__(self):
        if self.interval_ms<=0 or self.open_ms%self.interval_ms:raise ValueError('unaligned bar')
        if any(not x.is_finite() for x in (self.open,self.high,self.low,self.close,self.volume)):raise ValueError('nonfinite market data')
        if min(self.open,self.high,self.low,self.close)<=0 or self.volume<0 or self.low>min(self.open,self.close) or self.high<max(self.open,self.close):raise ValueError('invalid OHLCV')

@dataclass(frozen=True)
class StrategyDecision:
    agent_id:str
    implementation_id:str
    canonical_id:str
    instrument:str
    family:str
    action:Action
    target:Decimal
    available_at:int
    valid_until:int
    input_event:str
    atr:Decimal
    reason:str
    interval_ms:int=H4
    indicators:dict=field(default_factory=dict)
    version:str='1'
    @property
    def decision_id(self):return key(self.agent_id,self.version,self.input_event)
    def __post_init__(self):
        if not self.target.is_finite() or not -ONE<=self.target<=ONE or self.valid_until<=self.available_at:raise ValueError('invalid signal')
        if self.atr<0 or not self.atr.is_finite():raise ValueError('invalid ATR')

@dataclass(frozen=True)
class PortfolioDecision:
    decision_id:str;account_id:str;instrument:str;current_exposure:Decimal;target_exposure:Decimal;target_quantity:Decimal;reason:str;inputs:tuple[str,...];contributions:dict=field(default_factory=dict);cause_event:str=''
@dataclass
class VirtualOrder:
    order_id:str;decision_id:str;account_id:str;instrument:str;side:str;quantity:Decimal;ready_at:int;reason:str;stop:Decimal|None=None;status:str='PENDING';reserved_cash:Decimal=ZERO;initial_distance:Decimal|None=None
@dataclass(frozen=True)
class VirtualFill:
    fill_id:str;order_id:str;account_id:str;instrument:str;side:str;quantity:Decimal;reference_price:Decimal;price:Decimal;fee:Decimal;slippage_cost:Decimal;cost_basis:Decimal;realized_pnl:Decimal;event_id:str;time_ms:int;reason:str;ambiguous:bool=False
@dataclass
class PositionState:
    quantity:Decimal=ZERO;cost:Decimal=ZERO;average_price:Decimal=ZERO;stop:Decimal=ZERO;initial_distance:Decimal=ZERO;protection:str='ACTIVE';unlock_after:int=0;entry_at:int=0
@dataclass
class PortfolioState:
    account_id:str;cash:Decimal;peak:Decimal;realized_pnl:Decimal=ZERO;equity:Decimal=ZERO;reserved_cash:Decimal=ZERO;state:str='ACTIVE';max_dd:Decimal=ZERO;policy:str='base';positions:dict[str,PositionState]=field(default_factory=dict)

class Clock(Protocol):
    def now_ms(self)->int:...
class MarketSource(Protocol):
    def __aiter__(self)->AsyncIterator[list[MarketEvent]]:...
class ReplayClock:
    def __init__(self):self.value=0
    def now_ms(self):return self.value
    def advance(self,value):
        if value<self.value:raise ValueError('clock regression')
        self.value=value
