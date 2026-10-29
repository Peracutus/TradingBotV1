"""Namespaced hypotheses with evidence-based per-mode admission."""
from pathlib import Path
import hashlib,json,re
from .models import key,canonical
ROOT=Path(__file__).resolve().parents[1]
FAMILIES={**{n:'mean_reversion' for n in (11,12,13,14,15,16,18,20,26,31)},**{n:'breakout' for n in (1,2,9,10,24,29,32)},**{n:'volatility' for n in (21,22,23,25,27,28)}}

def source_hash(path):return hashlib.sha256((ROOT/path).read_bytes()).hexdigest()
def records():
    output=[]
    for h in json.loads((ROOT/'hypotheses_50.json').read_text()):
        ident=f"legacy50:{h['id']:02}"
        output.append(dict(hypothesis_id=ident,name=h['title'],implementation_id=ident+':v1',implementation_hash=source_hash('research_framework.py'),source='local research_framework.py',license='local research; no external code imported',required_datasets=h['required_data'],development_status='ready_for_validation' if h['status']=='runnable' else 'blocked',level='R0',modes=[],reason=None if h['status']=='runnable' else h['status'],family=FAMILIES.get(h['id'],'trend')))
    for name in ('baseline_causal','baseline_lock3','H01','H02','H03','H04','H05','H06','H08','H09','H10'):
        output.append(dict(hypothesis_id='research20:'+name,name=name,implementation_id='research20:'+name+':v1',implementation_hash=source_hash('research_v0/core.py') if name.startswith('baseline') else source_hash('research_v0/strategies.py'),source='local research_v0',license='local research',required_datasets=['BTC/ETH spot OHLCV'],development_status='ready_for_validation',level='R0',modes=[],reason=None,family='mean_reversion' if name in ('H08','H09') else 'volatility' if name=='H10' else 'breakout' if name=='H04' else 'trend'))
    present={r['hypothesis_id'] for r in output}
    for line in (ROOT/'RESEARCH_ARCHITECTURE_v1.md').read_text().splitlines():
        match=re.match(r'^\| (H\d{2}) \|',line)
        if not match:continue
        ident='research20:'+match.group(1)
        if ident in present:continue
        cells=[x.strip() for x in line.strip('|').split('|')]
        need=cells[4] if len(cells)>4 else 'unvalidated dataset'
        output.append(dict(hypothesis_id=ident,name=cells[1],implementation_id=ident+':unimplemented',implementation_hash=source_hash('RESEARCH_ARCHITECTURE_v1.md'),source='local design RESEARCH_ARCHITECTURE_v1.md',license='design only; no imported implementation',required_datasets=[need],development_status='data_required' if match.group(1) in ('H12','H13','H16','H20') else 'implementing',level='R0',modes=[],reason='missing_validated_adapter_or_point_in_time_data',family='breakout' if match.group(1) in ('H15','H18','H19') else 'trend'))
        present.add(ident)
    output.append(dict(hypothesis_id='external:apex-usdjpy',name='Apex independent control',implementation_id='external:apex-usdjpy:3dab20e',source='https://github.com/sbrakni/MQL5-trading-bot-claude-experiment',commit='3dab20e20a846edae9ac6fcf56d6b090dbba9f98',implementation_hash=source_hash('cross_market/replay_apex_usdjpy_v1.py'),license='UNVERIFIED',required_datasets=['USDJPY H1 BID; execution contract for FX'],development_status='blocked',level='R0',modes=['reference_replay'],reason='license_and_FX_execution_adapter_not_validated',family='breakout'))
    return output

def persist_registry(store,records,capabilities):
    with store.transaction():
        for r in records:store.db.execute('INSERT OR REPLACE INTO hypotheses VALUES(?,?)',(r['implementation_id'],canonical(r)))
        for c in capabilities:store.db.execute('INSERT OR REPLACE INTO data_capabilities VALUES(?,?)',(key(c),canonical(c)))
