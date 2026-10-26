"""Offline Production-controller regressions; all credentials are synthetic.

Exercise real authorization/deploy functions. No CLI, network or provider call.
"""
import contextlib,copy,datetime as dt,hashlib,importlib.util,io,json,os,pathlib,sys,time,unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'ops/production'),str(ROOT/'tests/production')]
import authorization as a
import deploy as d
from test_authorization import sample,Fake
import test_deploy as fixture
from test_deploy import Invoked

MISSING=object()
BAD=[True,False,1.0,'1',None,0,-1,{},[],MISSING]

def substitute(record,key,value):
 if value is MISSING:record.pop(key,None)
 else:record[key]=value

class SnapshotAPI(Fake):
 """Each GET is an independent API snapshot, not a shared mutable object."""
 def __init__(self,m,final_change=None):
  super().__init__(m);self.final_change=final_change;self.run_reads=0;self.calls=[]
 def get(self,path,deadline=None):
  self.calls.append((path,deadline))
  if path.endswith('/actions/runs/8'):
   self.run_reads+=1;result=copy.deepcopy(self.run)
   if self.run_reads==2 and self.final_change:self.final_change(result)
   return result
  return copy.deepcopy(super().get(path,deadline))

class ControllerIdentity(unittest.TestCase):
 def test_valid_two_run_reads_same_deadline(self):
  m=sample();api=SnapshotAPI(m);deadline=time.monotonic()+7
  self.assertEqual(a.fresh(api,m,11,a.Clock(),deadline),deadline)
  self.assertEqual(api.run_reads,2)
  self.assertTrue(all(x[1]==deadline for x in api.calls))
  self.assertTrue(api.calls[-1][0].endswith('/actions/runs/8'))

 def test_run_numeric_types_and_missing(self):
  m=sample()
  for route in [('id',),('run_attempt',),('workflow_id',),('repository','id'),('actor','id'),('triggering_actor','id')]:
   for value in BAD+[8.0,'8',1085006449.0,125151458.0]:
    with self.subTest(route=route,value=repr(value)):
     run=copy.deepcopy(Fake(m).run);record=run
     for key in route[:-1]:record=record[key]
     substitute(record,route[-1],value)
     with self.assertRaises(a.Rejected):a.run_identity(run,m)

 def test_manifest_numeric_types(self):
  for key in ['repository_id','owner_id','workflow_id','run_id','attempt','environment_id']:
   for value in BAD:
    with self.subTest(key=key,value=repr(value)):
     m=sample();substitute(m,key,value)
     with self.assertRaises((a.Rejected,KeyError)):a.manifest(m)

 def test_missing_or_malformed_run_records(self):
  m=sample()
  for key in list(Fake(m).run):
   with self.subTest(key=key):
    run=copy.deepcopy(Fake(m).run);run.pop(key)
    with self.assertRaises(a.Rejected):a.run_identity(run,m)
  for value in [None,[],True,'run']:
   with self.assertRaises(a.Rejected):a.run_identity(value,m)

 def test_final_identity_drift(self):
  edits=[('id',99),('run_attempt',2),('event','push'),('head_sha','0'*40),('head_branch','other'),
         ('path','other.yml'),('workflow_id',99),('created_at','2026-01-01T00:00:00Z'),
         ('status','completed'),('status','queued'),('conclusion','cancelled'),
         ('repository',{'id':a.REPO_ID,'full_name':'other/repo'}),
         ('repository',{'id':7,'full_name':a.REPO}),('actor',{'id':a.OWNER_ID,'login':'other'}),
         ('triggering_actor',{'id':a.OWNER_ID,'login':'other'}),('conclusion',MISSING)]
  for key,value in edits:
   with self.subTest(key=key,value=repr(value)):
    m=sample();api=SnapshotAPI(m,lambda r:substitute(r,key,value))
    with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())
    self.assertEqual(api.run_reads,2)

 def test_final_numeric_substitution(self):
  for key in ['id','run_attempt','workflow_id']:
   for value in BAD:
    with self.subTest(key=key,value=repr(value)):
     m=sample();api=SnapshotAPI(m,lambda r:substitute(r,key,value))
     with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())

 def test_job_numeric_types_and_required_fields(self):
  for index in [0,1]:
   for key in ['id','run_id','run_attempt']:
    for value in BAD:
     with self.subTest(index=index,key=key,value=repr(value)):
      m=sample();api=Fake(m);substitute(api.jobs['jobs'][index],key,value)
      with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())
   for key in ['head_sha','status','conclusion']:
    with self.subTest(index=index,key=key):
     m=sample();api=Fake(m);api.jobs['jobs'][index].pop(key)
     with self.assertRaises((a.Rejected,KeyError)):a.fresh(api,m,11,a.Clock())

 def test_duplicate_missing_and_contradictory_jobs(self):
  edits=[lambda x:x['jobs'].pop(),
         lambda x:x['jobs'][1].update(id=x['jobs'][0]['id']),
         lambda x:x['jobs'][1].update(name='Deploy Production'),
         lambda x:x['jobs'][0].update(head_sha='0'*40),
         lambda x:x['jobs'][0].update(check_run_url='wrong'),
         lambda x:x['jobs'][0].update(status='completed',conclusion='cancelled'),
         lambda x:x['jobs'][1].update(status='in_progress',conclusion=None),
         lambda x:x['jobs'][1].update(conclusion='failure')]
  for i,edit in enumerate(edits):
   with self.subTest(i=i):
    m=sample();api=Fake(m);edit(api.jobs);api.jobs['total_count']=len(api.jobs['jobs'])
    with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())

 def test_approval_numeric_and_malformed_evidence(self):
  for key in ['user','environment']:
   for value in BAD:
    with self.subTest(key=key,value=repr(value)):
     m=sample();api=Fake(m);r=api.reviews[0]
     substitute(r['user'] if key=='user' else r['environments'][0],'id',value)
     with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())
  edits=[lambda r:r.update(environments=[None]),lambda r:r['environments'].append(copy.deepcopy(r['environments'][0])),
         lambda r:r.update(state='rejected'),lambda r:r.update(comment='wrong'),
         lambda r:r['environments'][0].update(name='other'),lambda r:r['user'].update(login='other')]
  for i,edit in enumerate(edits):
   with self.subTest(i=i):
    m=sample();api=Fake(m);edit(api.reviews[0])
    with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())

 def test_boolean_environment_id_cannot_equal_one(self):
  m=sample();m['environment_id']=1;api=Fake(m);api.reviews[0]['environments'][0]['id']=True
  with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())

 def test_check_run_and_runner_numeric_inputs(self):
  m=sample()
  for value in BAD:
   with self.subTest(value=repr(value)):
    with self.assertRaises(a.Rejected):a.fresh(Fake(m),m,value,a.Clock())
  for value in [True,1,1.0,None,'','0','01','+1',' 1','1\n','1.0','1e1','１２']:
   with self.assertRaises(a.Rejected):a.runner_integer(value,'ID')
  self.assertEqual(a.runner_integer('123','ID'),123)

 def test_response_loss_at_every_read_no_retry(self):
  for failed_call in range(1,7):
   with self.subTest(failed_call=failed_call):
    m=sample();api=SnapshotAPI(m);read=api.get;count=0
    def lose(path,deadline=None):
     nonlocal count;count+=1
     if count==failed_call:raise OSError('synthetic response loss')
     return read(path,deadline)
    api.get=lose
    with self.assertRaises(OSError):a.fresh(api,m,11,a.Clock())
    self.assertEqual(count,failed_call)

 def test_final_read_consumes_existing_deadline(self):
  m=sample();api=SnapshotAPI(m);clock=SimpleNamespace(check=lambda expires:None)
  # start=0; final post-read clock=10: strict deadline, no reset.
  with patch.object(a.time,'monotonic',side_effect=[0,10]):
   with self.assertRaises(a.Rejected):a.fresh(api,m,11,clock,10)
  self.assertEqual(api.run_reads,2)

 def test_replay_expired_run_and_rerun(self):
  m=sample();m['created_at']='2026-01-01T00:00:00Z';m['expires_at']='2026-01-01T00:15:00Z'
  with self.assertRaises(a.Rejected):a.fresh(Fake(m),m,11,a.Clock())
  m=sample();api=Fake(m);api.run.update(run_attempt=2)
  with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())

 def test_python_canonical_numeric_unicode(self):
  cases=[({'n':1.0},b'{"n":1.0}'),({'n':-0.0},b'{"n":-0.0}'),({'n':1e-7},b'{"n":1e-07}'),
         ({'n':1e20},b'{"n":1e+20}'),({'é':'ذهب\n🙂'},'{"é":"ذهب\\n🙂"}'.encode()),
         ({'😀':1,'\ue000':2},'{"\ue000":2,"😀":1}'.encode())]
  for value,expected in cases:
   with self.subTest(value=value):
    self.assertEqual(a.canonical(value),expected);self.assertEqual(a.digest(expected),hashlib.sha256(expected).hexdigest())
  for n in [float('nan'),float('inf'),-float('inf')]:
   with self.assertRaises(ValueError):a.canonical({'n':n})
  self.assertNotEqual(a.canonical({'é':1}),a.canonical({'e\u0301':1}))

 def test_api_parser_rejects_float_string_and_boolean_attempt(self):
  m=sample()
  for value in [True,1.0,'1',None]:
   with self.subTest(value=value):
    run=copy.deepcopy(Fake(m).run);run['run_attempt']=value
    with self.assertRaises(a.Rejected):a.run_identity(a.parse(json.dumps(run)),m)

 def test_offline_probe_refuses_credentials_without_reading_value(self):
  spec=importlib.util.spec_from_file_location('offline_probe',ROOT/'ops/qualification/deployment_readiness.py')
  probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
  sentinel='SYNTHETIC-SECRET-NEVER-LOG'
  out=io.StringIO()
  with patch.dict(os.environ,{'GSX_PRODUCTION_CF_API_TOKEN':sentinel},clear=True),contextlib.redirect_stdout(out):
   self.assertEqual(probe.main(),1)
  self.assertNotIn(sentinel,out.getvalue());self.assertIn('credential-bearing',out.getvalue())

 def test_offline_probe_redacts_unexpected_test_output(self):
  spec=importlib.util.spec_from_file_location('offline_probe_redaction',ROOT/'ops/qualification/deployment_readiness.py')
  probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
  sentinel='SYNTHETIC-RAW-ERROR-NEVER-LOG'
  class Failure(unittest.TestCase):
   def runTest(self):
    print(sentinel);raise OSError(sentinel)
  out=io.StringIO()
  with patch.dict(os.environ,{},clear=True),patch.object(probe.unittest.defaultTestLoader,'loadTestsFromName',side_effect=lambda name:unittest.TestSuite([Failure()])),contextlib.redirect_stdout(out):
   self.assertEqual(probe.main(),1)
  self.assertNotIn(sentinel,out.getvalue());self.assertIn('OFFLINE PROBE FAIL',out.getvalue())

 def test_same_run_replay_with_contradictory_state_refused(self):
  m=sample();api=SnapshotAPI(m)
  a.fresh(api,m,11,a.Clock())
  api.run.update(status='completed',conclusion='success')
  with self.assertRaises(a.Rejected):a.fresh(api,m,11,a.Clock())

 def test_deadlines_exact_boundaries(self):
  m=sample();m['credential_mode']='migration';m['migration_t0']=m['created_at'];m['isolation_qualified']=False
  clock=SimpleNamespace(check=lambda expires:None)
  cutoff=a.stamp(m['migration_t0'])+2400
  with patch.object(a.time,'time',return_value=cutoff):
   with self.assertRaises(a.Rejected):a.window(m,clock)
  with patch.object(a.time,'time',return_value=cutoff-0.001):a.window(m,clock)
  clock=a.Clock();expiry=a.stamp(m['expires_at'])
  with patch.object(a.time,'time',return_value=expiry),patch.object(a.time,'monotonic',return_value=clock.mono+expiry-clock.wall):
   with self.assertRaisesRegex(a.Rejected,'authorization expired'):clock.check(m['expires_at'])

class ControllerBoundary(unittest.TestCase):
 """Reuse accepted real deploy fixture without duplicating its original tests."""
 setUp=fixture.DeploymentVerifier.setUp
 save=fixture.DeploymentVerifier.save
 call=fixture.DeploymentVerifier.call
 def test_artifact_identity_rejected_before_cloudflare_or_exec(self):
  for key in ['id','expired']:
   values=BAD+[2] if key=='id' else [0,1,'false',None,MISSING,True]
   for value in values:
    with self.subTest(key=key,value=repr(value)):
     original=copy.deepcopy(self.artifact);substitute(self.artifact,key,value)
     exe=self.call()
     with self.assertRaises(a.Rejected):d.deploy(self.build)
     exe.assert_not_called();d.cfget.assert_not_called();d.baseline.assert_not_called()
     self.artifact=original

 def test_artifact_run_numeric_and_source_binding(self):
  for value in BAD:
   with self.subTest(value=repr(value)):
    art=copy.deepcopy(self.artifact);substitute(art['workflow_run'],'id',value)
    with self.assertRaises(a.Rejected):d.artifact_identity(art,1,self.m)
  for key,value in [('head_sha','0'*40)]:
   art=copy.deepcopy(self.artifact);art['workflow_run'][key]=value
   with self.assertRaises(a.Rejected):d.artifact_identity(art,1,self.m)

 def test_actual_authorization_failures_block_exec(self):
  changes=[lambda r:r.update(run_attempt=True),lambda r:r.update(status='completed',conclusion='cancelled'),
           lambda r:r.update(actor={'id':a.OWNER_ID,'login':'other'}),lambda r:r.pop('conclusion')]
  for i,change in enumerate(changes):
   with self.subTest(i=i):
    exe=self.call();api=SnapshotAPI(self.m,change)
    with patch.object(d,'fresh',a.fresh),patch.object(d,'API',return_value=SimpleNamespace(get=lambda p:copy.deepcopy(self.artifact))):
     # deploy uses its API for artifact and then the real fresh validator.
     real_fresh=lambda unused,m,c,clock,deadline:a.fresh(api,m,c,clock,deadline)
     with patch.object(d,'fresh',side_effect=real_fresh):
      with self.assertRaises(a.Rejected):d.deploy(self.build)
    exe.assert_not_called();self.assertEqual(api.run_reads,2)

 def test_success_uses_actual_final_validator(self):
  exe=self.call();api=SnapshotAPI(self.m)
  with patch.object(d,'fresh',side_effect=lambda unused,m,c,clock,deadline:a.fresh(api,m,c,clock,deadline)):
   with self.assertRaises(Invoked):d.deploy(self.build)
  self.assertEqual(api.run_reads,2);self.assertEqual(exe.call_count,1)

 def test_startup_latency_is_outside_python_deadline(self):
  exe=self.call();virtual={'elapsed':0,'first_mutation_ms':None}
  def blocked_startup(*unused):
   virtual['elapsed']+=20
   # Model the first possible mutation after CLI discovery; send nothing.
   virtual['first_mutation_ms']=virtual['elapsed']*1000
   raise Invoked()
  exe.side_effect=blocked_startup
  with self.assertRaises(Invoked):d.deploy(self.build)
  self.assertEqual(virtual['first_mutation_ms'],20000);self.assertEqual(exe.call_count,1)

if __name__=='__main__':unittest.main()
