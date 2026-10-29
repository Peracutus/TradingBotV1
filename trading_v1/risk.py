"""Decimal post-change admission budget; no correlation discount."""
from dataclasses import dataclass
from decimal import Decimal
from .models import D,ZERO,ONE,qty,ExperimentConfig,PortfolioState
@dataclass(frozen=True)
class RiskResult:
    quantity:Decimal;loss_to_stops:Decimal;new_costs:Decimal;post_equity:Decimal;reason:str
class RiskManager:
    def __init__(self,config:ExperimentConfig):self.c=config
    def equity(self,account,marks):return account.cash+sum((p.quantity*marks[n] for n,p in account.positions.items() if p.quantity),ZERO)
    def admission(self,account:PortfolioState,marks,symbol,quantity,stop,pending=()):
        c=self.c;price=marks[symbol];equity=self.equity(account,marks)
        current=account.positions.get(symbol);old_qty=current.quantity if current else ZERO
        if stop<=0 or stop>=price:return RiskResult(ZERO,ZERO,ZERO,equity,'invalid_stop')
        existing=ZERO
        for n,p in account.positions.items():
            if p.quantity:
                existing+=p.quantity*max(ZERO,marks[n]-p.stop*(ONE-c.slippage)*(ONE-c.fee))
        reserved_costs=ZERO;reserved_notional=ZERO;reserved_symbol=ZERO
        for o in pending:
            mark=marks[o.instrument];pending_stop=(mark*(ONE+c.slippage)-o.initial_distance if o.initial_distance is not None else o.stop)
            if pending_stop is None or pending_stop<=0 or pending_stop>=mark:return RiskResult(ZERO,existing,ZERO,equity,'invalid_pending_stop')
            existing+=o.quantity*max(ZERO,mark-pending_stop*(ONE-c.slippage)*(ONE-c.fee))
            reserved_costs+=o.quantity*mark*((ONE+c.slippage)*(ONE+c.fee)-ONE)
            reserved_notional+=o.quantity*mark
            if o.instrument==symbol:reserved_symbol+=o.quantity
        def check(q):
            costs=reserved_costs+q*price*((ONE+c.slippage)*(ONE+c.fee)-ONE)
            post=equity-costs
            loss=existing+q*max(ZERO,price-stop*(ONE-c.slippage)*(ONE-c.fee))
            gross=sum((p.quantity*marks[n] for n,p in account.positions.items() if p.quantity),ZERO)+reserved_notional+q*price
            ok=(q*price*(ONE+c.slippage)*(ONE+c.fee)<=account.cash-account.reserved_cash and post>0 and loss+costs<=c.risk_limit*post and (old_qty+reserved_symbol+q)*price<=c.instrument_cap*post and gross<=c.gross_cap*post)
            return ok,loss,costs,post
        hi=qty(quantity);lo=ZERO
        if check(hi)[0]:lo=hi
        else:
            for _ in range(90):
                mid=(lo+hi)/2
                if check(mid)[0]:lo=mid
                else:hi=mid
        q=qty(lo);ok,loss,costs,post=check(q)
        if not ok or q*price<D('5'):q=ZERO
        return RiskResult(q,loss,costs,post,'approved' if q else 'risk_or_capital_limit')
    def exposures(self,account,marks):
        eq=self.equity(account,marks)
        return ({n:p.quantity*marks[n]/eq for n,p in account.positions.items()} if eq>0 else {}),eq
