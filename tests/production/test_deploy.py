import contextlib,copy,io,json,os,pathlib,sys,tarfile,tempfile,time,unittest
from unittest.mock import patch
from test_authorization import sample
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'ops/production'))
import deploy as d
from authorization import canonical,digest,REPO,REPO_ID,OWNER,OWNER_ID,WORKFLOW,Rejected
class Invoked(BaseException):pass
class DeploymentVerifier(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=pathlib.Path(self.tmp.name);self.build=self.root/'build';self.build.mkdir();self.m=sample();m=self.m;self.rollback={'id':m['rollback']['version_id']};m['rollback']['version_digest']=digest(canonical(self.rollback))
  f=self.root/WORKFLOW;f.parent.mkdir(parents=True);f.write_text('trusted workflow');(self.root/'wrangler.toml').write_text('trusted config')
  files=[WORKFLOW,'wrangler.toml'];m['workflow_sha256']=digest(f.read_bytes());m['config_digest']=digest((self.root/'wrangler.toml').read_bytes());m['source_digest']=digest(canonical({f:digest((self.root/f).read_bytes())for f in files}))
  inv={'node':digest(b'not executed')};ib=canonical(inv);m['toolchain_digest']=digest(ib)
  with tarfile.open(self.build/'toolchain.tar.gz','w:gz')as t:
   for name,b in [('node',b'not executed'),('inventory.json',ib)]:
    x=tarfile.TarInfo(name);x.size=len(b);x.mode=0o755;t.addfile(x,io.BytesIO(b))
  m['artifact_digest']=digest((self.build/'toolchain.tar.gz').read_bytes());policy=canonical({'account':m['account'],'worker':m['worker']});(self.build/'policy.json').write_bytes(policy);m['policy_digest']=digest(policy);self.save()
  self.env={'GITHUB_REPOSITORY':REPO,'GITHUB_REPOSITORY_ID':str(REPO_ID),'GITHUB_ACTOR':OWNER,'GITHUB_ACTOR_ID':str(OWNER_ID),'GITHUB_SHA':m['source_sha'],'GITHUB_RUN_ID':str(m['run_id']),'GITHUB_RUN_ATTEMPT':'1','GITHUB_REF':'refs/heads/main','GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_WORKFLOW_SHA':m['workflow_sha'],'GITHUB_WORKFLOW_REF':REPO+'/'+WORKFLOW+'@refs/heads/main','GSX_PRODUCTION_ENABLED':'true','GSX_PRODUCTION_REVIEWED_SHA':m['source_sha'],'GSX_PRODUCTION_CF_API_TOKEN':'synthetic','GSX_PRODUCTION_CF_ACCOUNT_ID':m['account'],'GH_READ_TOKEN':'synthetic-gh','GSX_ARTIFACT_ID':'1','GSX_CHECK_RUN_ID':'11','NODE_OPTIONS':'untrusted','PUBLICATION_PRIVATE_KEY':'synthetic-not-real'}
  self.artifact={'workflow_run':{'id':m['run_id'],'head_sha':m['source_sha']},'expired':False,'name':f'gsx-production-{m["run_id"]}-1'}
 def save(self):(self.build/'authorization.json').write_bytes(canonical(self.m))
 def call(self,baseline=None,execute=Invoked):
  stack=contextlib.ExitStack();self.addCleanup(stack.close)
  stack.enter_context(patch.dict(os.environ,self.env,clear=True));stack.enter_context(patch.object(d,'ROOT',self.root));stack.enter_context(patch.object(d,'CANCELLED',False));stack.enter_context(patch.object(d.os,'chdir'))
  stack.enter_context(patch.object(d,'git',side_effect=lambda *a:self.m['source_tree'] if a[0]=='rev-parse'else WORKFLOW+'\nwrangler.toml'))
  stack.enter_context(patch.object(d,'cfget',return_value=self.rollback));stack.enter_context(patch.object(d.API,'get',return_value=self.artifact));stack.enter_context(patch.object(d,'baseline',side_effect=baseline or(lambda *a:copy.deepcopy(self.m['baseline']))));stack.enter_context(patch.object(d,'fresh',return_value=time.monotonic()+10))
  exe=stack.enter_context(patch.object(d.os,'execve',side_effect=execute));return exe
 def test_single_invocation_and_clean_environment(self):
  exe=self.call()
  with self.assertRaises(Invoked):d.deploy(self.build)
  self.assertEqual(exe.call_count,1);args=exe.call_args.args;self.assertIn('--require',args[1]);self.assertNotIn('NODE_OPTIONS',args[2]);self.assertNotIn('PUBLICATION_PRIVATE_KEY',args[2]);self.assertEqual(args[2]['CLOUDFLARE_API_TOKEN'],'synthetic')
 def test_uncertain_invocation_no_retry(self):
  exe=self.call(execute=OSError('uncertain'))
  with self.assertRaises(OSError):d.deploy(self.build)
  self.assertEqual(exe.call_count,1)
 def test_missing_token(self):
  self.env['GSX_PRODUCTION_CF_API_TOKEN']='';exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_whitespace_token(self):
  self.env['GSX_PRODUCTION_CF_API_TOKEN']='  ';exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_legacy_only_not_accepted(self):
  self.env.pop('GSX_PRODUCTION_CF_API_TOKEN');self.env['CLOUDFLARE_API_TOKEN']='synthetic-legacy';exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_stale_baseline(self):
  exe=self.call(baseline=lambda *a:{})
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_baseline_changes_in_final_segment(self):
  n=0
  def baseline(*a):
   nonlocal n;n+=1;return self.m['baseline']if n==1 else {}
  exe=self.call(baseline=baseline)
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_artifact_substitution(self):
  (self.build/'toolchain.tar.gz').write_bytes(b'tamper');exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_policy_substitution(self):
  (self.build/'policy.json').write_bytes(b'tamper');exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_source_substitution(self):
  (self.root/'wrangler.toml').write_text('wrong');exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_wrong_workflow_revision(self):
  self.env['GITHUB_WORKFLOW_SHA']='0'*40;exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_other_run_artifact(self):
  self.artifact['workflow_run']['id']=123;exe=self.call()
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
 def test_cancel_after_last_guard_before_exec(self):
  import signal
  exe=self.call()
  with patch.object(d.os,'chdir',side_effect=lambda *a:d.cancel(signal.SIGTERM,None)):
   with self.assertRaisesRegex(Rejected,'cancelled before CLI invocation'):d.deploy(self.build)
  exe.assert_not_called()
 def test_cancel_during_baseline(self):
  def baseline(*a):d.CANCELLED=True;return self.m['baseline']
  exe=self.call(baseline=baseline)
  with self.assertRaises(Rejected):d.deploy(self.build)
  exe.assert_not_called()
