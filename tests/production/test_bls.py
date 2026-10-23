import hashlib,importlib.util,json,os,pathlib,sys,tempfile,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'ops/production'))
from authorization import REPO,REPO_ID,Rejected
spec=importlib.util.spec_from_file_location('bls_writer',ROOT/'ops/production/bls-pr.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
class SnapshotPR(unittest.TestCase):
 def run_writer(self,branch=False,forward='ahead',reverse='diverged',changed=None,unchanged=False):
  calls=[];head='a'*40;old='b'*40
  def call(_self,method,path,data=None,missing=False):
   calls.append((method,path,data))
   if path.endswith('/git/ref/heads/main'):return {'object':{'sha':head}}
   if '/git/commits/'in path:return {'tree':{'sha':'c'*40}}
   if '/git/ref/heads/automation/'in path:return {'object':{'sha':old}}if branch else None
   if '/compare/'+head+'...'in path:return {'status':forward}
   if '/compare/'+old+'...'in path:return {'status':reverse}
   if '/contents/'in path:return {'sha':'d'*40 if unchanged else 'e'*40}
   if '/pulls?'in path:return [{'number':1}] if unchanged else []
   return {'sha':'d'*40}
  with tempfile.TemporaryDirectory()as td:
   out=pathlib.Path(td)/'.bls-artifact';out.mkdir();b=b'export const snapshot = {};\n';(out/'bls-official-snapshot.js').write_bytes(b)
   (out/'manifest.json').write_text(json.dumps({'changed':['bls-official-snapshot.js']if changed is None else changed,'run_id':'7','source_sha':head,'sha256':hashlib.sha256(b).hexdigest()}))
   cwd=os.getcwd()
   try:
    os.chdir(td)
    with patch.dict(os.environ,{'GITHUB_REPOSITORY':REPO,'GITHUB_REPOSITORY_ID':str(REPO_ID),'GITHUB_REF':'refs/heads/main','GITHUB_RUN_ATTEMPT':'1','GITHUB_RUN_ID':'7','GITHUB_SHA':head},clear=True),patch.object(mod.API,'call',call):mod.main()
   finally:os.chdir(cwd)
  return calls
 def test_new_pr_snapshot_only(self):
  calls=self.run_writer();trees=[d for m,p,d in calls if m=='POST'and p.endswith('/git/trees')];self.assertEqual([x['path']for x in trees[0]['tree']],['bls-official-snapshot.js']);self.assertFalse(any(m=='PATCH'for m,p,d in calls));self.assertEqual(len([1 for m,p,d in calls if m=='POST'and p.endswith('/pulls')]),1)
 def test_existing_branch_fast_forward(self):
  calls=self.run_writer(branch=True);updates=[d for m,p,d in calls if m=='PATCH'];self.assertEqual(updates,[{'sha':'d'*40,'force':False}]);self.assertFalse(any(m!='GET'and p.endswith('/heads/main')for m,p,d in calls))
 def test_merged_branch_restarts_from_current_main(self):
  calls=self.run_writer(branch=True,forward='behind',reverse='ahead');commit=[d for m,p,d in calls if m=='POST'and p.endswith('/git/commits')][0];self.assertEqual(commit['parents'],['a'*40])
 def test_existing_identical_proposal_no_commit_or_ref_update(self):
  calls=self.run_writer(branch=True,unchanged=True);self.assertFalse(any(m=='POST'and p.endswith('/git/commits')for m,p,d in calls));self.assertFalse(any(m=='PATCH'for m,p,d in calls))
 def test_divergence_no_overwrite(self):
  with self.assertRaises(Rejected):self.run_writer(branch=True,forward='diverged',reverse='diverged')
 def test_wrong_generated_scope_rejected(self):
  with self.assertRaises(Rejected):self.run_writer(changed=['wrangler.toml'])
