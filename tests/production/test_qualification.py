import importlib.util,pathlib,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('qualification',ROOT/'ops/qualification/read_only.py');q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)
class QualificationIsolation(unittest.TestCase):
 def test_mutation_paths_refused(self):
  for host,path in [('api.github.com','/installation/token'),('api.github.com','/app/installations/1/access_tokens'),('api.cloudflare.com','/client/v4/accounts/x/workers/scripts/y/versions')]:
   with self.subTest(path=path),patch.object(q.urllib.request,'build_opener') as transport:
    with self.assertRaises(q.Refused):q.read(host,path,'synthetic',set())
    transport.assert_not_called()
 def test_redirect_refused(self):
  with self.assertRaises(q.Refused):q.NoRedirect().redirect_request(None,None,None,None,None,None)
 def test_wrong_mode_refused_before_transport(self):
  with patch.object(q,'read') as transport:
   with self.assertRaises(q.Refused):q.run('deploy')
   transport.assert_not_called()
 def test_untrusted_context_refused(self):
  with patch.dict(q.os.environ,{},clear=True),patch.object(q,'read') as transport:
   with self.assertRaises(q.Refused):q.run('publication')
   transport.assert_not_called()
 def context(self):
  return {'GITHUB_REPOSITORY':q.REPO,'GITHUB_REPOSITORY_ID':'1085006449','GITHUB_ACTOR_ID':'125151458','GITHUB_TRIGGERING_ACTOR':'Skyeagle123','GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_REF':'refs/heads/main','GITHUB_RUN_ATTEMPT':'1','GITHUB_RUN_ID':'12','GITHUB_SHA':'a'*40,'GH_READ_TOKEN':'synthetic','GSX_QUALIFICATION_READ_POLICY_VERIFIED':'true','GSX_QUALIFICATION_CF_READ_TOKEN':'synthetic-read'}
 def fake(self,mode,state='in_progress'):
  def read(host,path,*args):
   if host=='api.cloudflare.com':return {'success':True,'errors':[]}
   if path.endswith('/approvals'):return [{'state':'approved','user':{'id':125151458},'environments':[{'id':25273664884 if mode=='production' else 25273747086,'name':'goldsignalsx-'+mode}]}]
   if path.endswith('/main'):return {'object':{'sha':'a'*40}}
   return {'head_sha':'a'*40,'run_attempt':1,'event':'workflow_dispatch','status':state,'actor':{'id':125151458},'triggering_actor':{'id':125151458},'repository':{'id':1085006449},'path':'.github/workflows/qualification-only.yml'}
  return read
 def test_positive_read_only_composition(self):
  for mode in ('production','publication'):
   with self.subTest(mode=mode),patch.dict(q.os.environ,self.context(),clear=True),patch.object(q,'source'),patch.object(q,'read',side_effect=self.fake(mode)) as reads:
    q.run(mode);self.assertEqual(reads.call_count,6 if mode=='production' else 3)
 def test_cancelled_run_refused(self):
  with patch.dict(q.os.environ,self.context(),clear=True),patch.object(q,'source'),patch.object(q,'read',side_effect=self.fake('publication','cancelled')):
   with self.assertRaises(q.Refused):q.run('publication')
 def test_production_write_credential_refused(self):
  env=self.context();env['GSX_PRODUCTION_CF_API_TOKEN']='synthetic-write'
  with patch.dict(q.os.environ,env,clear=True),patch.object(q,'read') as reads:
   with self.assertRaises(q.Refused):q.run('production')
   reads.assert_not_called()
 def test_source_failure_prevents_transport(self):
  with patch.dict(q.os.environ,self.context(),clear=True),patch.object(q,'source',side_effect=q.Refused),patch.object(q,'read') as reads:
   with self.assertRaises(q.Refused):q.run('publication')
   reads.assert_not_called()
