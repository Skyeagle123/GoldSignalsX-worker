import copy,datetime as dt,importlib.util,json,pathlib,sys,time,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'ops/production'))
from authorization import *
V='12345678-1234-1234-1234-123456789abc'
def sample():
 created=dt.datetime.fromtimestamp(time.time()-60,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ');expires=dt.datetime.fromtimestamp(stamp(created)+900,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
 return dict(format='gsx-production-authorization-v1',repository=REPO,repository_id=REPO_ID,owner=OWNER,owner_id=OWNER_ID,workflow_path=WORKFLOW,workflow_id=3,workflow_sha='a'*40,workflow_sha256='b'*64,source_sha='a'*40,source_tree='c'*40,run_id=8,event='workflow_dispatch',attempt=1,environment=ENVIRONMENT,environment_id=9,account='d'*32,worker='goldsignalsx-worker',source_digest='e'*64,config_digest='f'*64,toolchain_digest='1'*64,artifact_digest='2'*64,policy_digest='3'*64,baseline={'deployments':'5'*64,'settings':'6'*64,'subdomain':'7'*64,'schedules':'8'*64,'migration_tag':'v1-gold-feed'},rollback={'version_id':V,'version_digest':'4'*64},created_at=created,expires_at=expires,credential_mode='steady',migration_t0=None,isolation_qualified=True)
class Fake:
 def __init__(self,m):
  self.run=dict(id=8,run_attempt=1,event='workflow_dispatch',head_sha='a'*40,head_branch='main',repository={'id':REPO_ID,'full_name':REPO},actor={'id':OWNER_ID,'login':OWNER},triggering_actor={'id':OWNER_ID,'login':OWNER},workflow_id=3,path=WORKFLOW,created_at=m['created_at'],status='in_progress',conclusion=None)
  self.reviews=[dict(environments=[dict(id=9,name=ENVIRONMENT)],state='approved',user=dict(id=OWNER_ID,login=OWNER),comment='GSX-PRODUCTION-APPROVE v1 sha256:'+manifest(m))]
  self.jobs={'total_count':2,'jobs':[dict(id=10,name='Deploy Production',run_id=8,run_attempt=1,head_sha='a'*40,status='in_progress',conclusion=None,check_run_url=f'https://api.github.com/repos/{REPO}/check-runs/11'),dict(id=12,name='Prepare Production',run_id=8,run_attempt=1,head_sha='a'*40,status='completed',conclusion='success')]}
  self.main='a'*40;self.tree='c'*40
 def get(self,path,deadline=None):
  if path.endswith('/approvals'):return self.reviews
  if '/attempts/1/jobs?'in path:return self.jobs
  if path.endswith('/heads/main'):return {'object':{'sha':self.main}}
  if '/git/commits/'in path:return {'tree':{'sha':self.tree}}
  return self.run
class Authorization(unittest.TestCase):
 def test_valid(self):
  m=sample();self.assertGreater(fresh(Fake(m),m,11,Clock()),time.monotonic())
 def test_canonical(self):self.assertEqual(canonical({'b':1,'a':2}),b'{"a":2,"b":1}')
 def test_duplicate_json(self):
  with self.assertRaises(Rejected):parse('{"a":1,"a":2}')
 def test_float_json(self):
  with self.assertRaises(Rejected):parse('{"a":1.2}')
 def test_expiry(self):
  m=sample();clock=Clock();clock.wall=stamp(m['expires_at'])+1
  with patch('authorization.time.time',return_value=clock.wall),patch('authorization.time.monotonic',return_value=clock.mono):
   with self.assertRaisesRegex(Rejected,'authorization expired'):clock.check(m['expires_at'])
 def test_backward_clock(self):
  clock=Clock()
  with patch('authorization.time.time',return_value=clock.wall-10):
   with self.assertRaises(Rejected):clock.check(sample()['expires_at'])
 def test_small_backward_clock(self):
  clock=Clock()
  with patch('authorization.time.time',return_value=clock.wall-0.01):
   with self.assertRaisesRegex(Rejected,'clock discontinuity'):clock.check(sample()['expires_at'])
 def test_clock_before_authenticated_run(self):
  expiry=dt.datetime.fromtimestamp(time.time()+1800,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
  with self.assertRaisesRegex(Rejected,'clock before authenticated run'):Clock().check(expiry)
 def test_ten_second_limit(self):
  from types import SimpleNamespace
  m=sample();api=Fake(m);clock=SimpleNamespace(check=lambda expires:None)
  with patch('authorization.time.monotonic',side_effect=[0,11]):
   with self.assertRaisesRegex(Rejected,'ten seconds'):fresh(api,m,11,clock)
 def test_unqualified_steady_mode(self):
  m=sample();m['isolation_qualified']=False
  with self.assertRaisesRegex(Rejected,'isolation not qualified'):manifest(m)
 def test_migration_valid(self):
  m=sample();m.update(credential_mode='migration',migration_t0=m['created_at'],isolation_qualified=False);manifest(m);window(m,Clock())
 def test_migration_after_minute_40(self):
  m=sample();m['created_at']=dt.datetime.fromtimestamp(time.time()-2600,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ');m['expires_at']=dt.datetime.fromtimestamp(stamp(m['created_at'])+900,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ');m.update(credential_mode='migration',migration_t0=m['created_at'],isolation_qualified=False)
  from types import SimpleNamespace
  with self.assertRaisesRegex(Rejected,'migration start cutoff'):window(m,SimpleNamespace(check=lambda expires:None))
 def test_migration_dispatch_after_cutoff(self):
  m=sample();m.update(credential_mode='migration',migration_t0=dt.datetime.fromtimestamp(stamp(m['created_at'])-2401,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),isolation_qualified=False)
  with self.assertRaisesRegex(Rejected,'migration dispatch window'):manifest(m)
 def test_migration_future_t0(self):
  m=sample();m.update(credential_mode='migration',migration_t0=m['expires_at'],isolation_qualified=False)
  with self.assertRaisesRegex(Rejected,'migration dispatch window'):manifest(m)
 def test_no_cached_api_fallback(self):
  m=sample();api=Fake(m);api.get=lambda *a:(_ for _ in ()).throw(OSError('unavailable'))
  with self.assertRaises(OSError):fresh(api,m,11,Clock())

def manifest_case(key,value):
 def test(self):
  m=sample();m[key]=value
  with self.assertRaises((Rejected,ValueError,TypeError)):manifest(m)
 return test
for k,v in {'repository':'other/repo','repository_id':True,'owner':'other','owner_id':1,'workflow_path':'other.yml','workflow_sha':'z'*40,'source_sha':'d'*40,'source_tree':'x','attempt':2,'event':'push','environment':'other','environment_id':'9','account':'x','worker':'other','artifact_digest':'x','created_at':'bad','expires_at':'bad','format':'other','rollback':'unbound-version'}.items():setattr(Authorization,'test_manifest_'+k,manifest_case(k,v))
def evidence_case(change):
 def test(self):
  m=sample();api=Fake(m);change(api)
  with self.assertRaises((Rejected,KeyError,TypeError)):fresh(api,m,11,Clock())
 return test
changes={
 'missing_approval':lambda a:a.reviews.clear(),
 'duplicate_approval':lambda a:a.reviews.append(copy.deepcopy(a.reviews[0])),
 'rejection':lambda a:a.reviews[0].update(state='rejected'),
 'wrong_approval_owner':lambda a:a.reviews[0]['user'].update(id=1),
 'wrong_environment':lambda a:a.reviews[0]['environments'][0].update(id=1),
 'wrong_comment':lambda a:a.reviews[0].update(comment='approved'),
 'incomplete_jobs':lambda a:a.jobs.update(total_count=3),
 'duplicate_job':lambda a:(a.jobs['jobs'].append(copy.deepcopy(a.jobs['jobs'][0])),a.jobs.update(total_count=3)),
 'wrong_check_run':lambda a:a.jobs['jobs'][0].update(check_run_url='wrong'),
 'job_cancelled':lambda a:a.jobs['jobs'][0].update(status='completed',conclusion='cancelled'),
 'wrong_job_run':lambda a:a.jobs['jobs'][0].update(run_id=3),
 'prepare_failed':lambda a:a.jobs['jobs'][1].update(conclusion='failure'),
 'run_cancelled':lambda a:a.run.update(status='completed',conclusion='cancelled'),
 'run_rerun':lambda a:a.run.update(run_attempt=2),
 'run_branch':lambda a:a.run.update(head_branch='tag'),
 'run_owner':lambda a:a.run['actor'].update(id=1),
 'run_source':lambda a:a.run.update(head_sha='d'*40),
 'main_advanced':lambda a:setattr(a,'main','d'*40),
 'tree_changed':lambda a:setattr(a,'tree','d'*40)}
for name,fn in changes.items():setattr(Authorization,'test_'+name,evidence_case(fn))
