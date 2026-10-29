"""Validate and deduplicate a timestamp batch before financial decisions."""
from .models import canonical
class SignalCollector:
    def __init__(self,agents):self.agents=agents
    def collect(self,decisions,events,now):
        by_id={};inputs={e.event_id:e for e in events}
        for d in decisions:
            if d.agent_id not in self.agents:raise ValueError('unknown signal agent')
            a=self.agents[d.agent_id]
            for name in ('implementation_id','canonical_id','instrument','family'):
                if getattr(d,name)!=a[name]:raise ValueError('signal metadata mismatch: '+name)
            if d.input_event not in inputs or d.available_at>now or d.available_at<inputs[d.input_event].ready_at:raise ValueError('unavailable signal input')
            if d.instrument!=inputs[d.input_event].instrument:raise ValueError('signal instrument mismatch')
            if d.valid_until<=now:continue
            old=by_id.get(d.decision_id)
            if old is not None and canonical(old)!=canonical(d):raise ValueError('conflicting duplicate signal')
            by_id[d.decision_id]=d
        return sorted(by_id.values(),key=lambda d:d.agent_id)
