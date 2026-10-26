"""Synthetic IPC authority for isolated gate/CLI tests only."""
import json,os,sys,time,pathlib
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'ops/production'))
from authorization import canonical,digest
from test_authorization import sample

def packet(pid,policy,executable):
 m=sample();m['policy_digest']=digest(policy);m['account']=json.loads(policy)['account']
 mono=time.monotonic_ns()
 return dict(format='gsx-execution-authority-v1',pid=pid,manifest=m,manifest_digest=digest(canonical(m)),artifact_id=1,check_run_id=11,executable=executable,clock_wall_ms=int(time.time()*1000),clock_mono_ns=str(mono),final_start_ns=str(mono),deadline_mono_ns=str(mono+10_000_000_000))
