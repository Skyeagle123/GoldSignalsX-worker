import importlib.util,io,json,os,pathlib,subprocess,sys,tarfile,tempfile,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'ops/production'))
from authorization import Rejected,parse
from deploy import unpack
from prepare import inventory
class Controls(unittest.TestCase):
 def test_config_smoke_unchanged(self):
  # Preserve the qualified deployment interface without banning legitimate
  # future BLS snapshot/application PRs. Whole-candidate scope is separately
  # recorded and checked in preservation.json during this review.
  for f in ['wrangler.toml','.github/scripts/post-deploy-smoke.mjs']:
   original=subprocess.check_output(['git','show','5820a9c7fb10f6d1e6c9c944a3490edc36da9c48:'+f],cwd=ROOT);self.assertEqual(original,(ROOT/f).read_bytes())
 def test_no_legacy_secret_reference(self):
  y=(ROOT/'.github/workflows/deploy-worker.yml').read_text();self.assertNotIn('secrets.CLOUDFLARE_',y);self.assertIn('secrets.GSX_PRODUCTION_CF_API_TOKEN',y)
 def test_manual_no_cancel(self):
  s=(ROOT/'.github/workflows/deploy-worker.yml').read_text();self.assertIn('workflow_dispatch:',s);self.assertNotIn('  push:',s);self.assertNotIn('  schedule:',s);self.assertIn('cancel-in-progress: false',s)
 def test_environment_only_deploy(self):
  s=(ROOT/'.github/workflows/deploy-worker.yml').read_text().split('  deploy:');self.assertEqual(len(s),2);self.assertNotIn('secrets.',s[0]);self.assertNotIn('environment:',s[0]);self.assertIn('name: goldsignalsx-production',s[1])
 def test_no_install_in_deploy(self):
  s=(ROOT/'.github/workflows/deploy-worker.yml').read_text().split('  deploy:')[1];self.assertNotIn('npm ',s);self.assertNotIn('npx',s);self.assertNotIn('wrangler-action',s)
 def test_immutable_actions(self):
  import re
  for f in ['deploy-worker.yml','update-bls-official-snapshot.yml','production-security-tests.yml']:
   for value in re.findall(r'uses:\s*(\S+)',(ROOT/'.github/workflows'/f).read_text()):self.assertRegex(value,r'^[\w/-]+@[0-9a-f]{40}$')
 def test_required_check_unfiltered(self):
  s=(ROOT/'.github/workflows/production-security-tests.yml').read_text();self.assertNotIn('paths:',s);self.assertIn('name: Production security offline',s)
 def test_bls_schedule_and_no_direct_push(self):
  s=(ROOT/'.github/workflows/update-bls-official-snapshot.yml').read_text();self.assertIn('17 */6 * * *',s);self.assertIn('workflow_dispatch:',s);self.assertNotIn('git push',s)
 def test_bls_writer_exact_scope(self):
  spec=importlib.util.spec_from_file_location('bls',ROOT/'ops/production/bls-pr.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);m.enforce(['bls-official-snapshot.js'])
  for paths in [[],['wrangler.toml'],['bls-official-snapshot.js','goldsignalsx-worker.js']]:
   with self.assertRaises(Rejected):m.enforce(paths)
 def test_artifact_path_traversal(self):
  with tempfile.TemporaryDirectory()as td:
   p=pathlib.Path(td)/'bad.tar.gz'
   with tarfile.open(p,'w:gz')as t:
    m=tarfile.TarInfo('../escape');m.size=1;t.addfile(m,io.BytesIO(b'x'))
   with self.assertRaises(Rejected):unpack(p,pathlib.Path(td)/'out')
 def test_artifact_symlink(self):
  with tempfile.TemporaryDirectory()as td:
   p=pathlib.Path(td)/'bad.tar.gz'
   with tarfile.open(p,'w:gz')as t:
    m=tarfile.TarInfo('link');m.type=tarfile.SYMTYPE;m.linkname='/tmp';t.addfile(m)
   with self.assertRaises(Rejected):unpack(p,pathlib.Path(td)/'out')
 def test_inventory_substitution(self):
  with tempfile.TemporaryDirectory()as td:
   p=pathlib.Path(td);(p/'a').write_text('one');a=inventory(p);(p/'a').write_text('two');self.assertNotEqual(inventory(p),a)
 def test_patch_rejects_wrong_bytes(self):
  spec=importlib.util.spec_from_file_location('patcher',ROOT/'ops/production/patch-toolchain.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  with tempfile.TemporaryDirectory()as td:
   p=pathlib.Path(td)/'cli.js';p.write_text('wrong')
   with self.assertRaises(ValueError):m.patch(p)
 def test_node_strict_json_rejects_duplicates(self):
  p=subprocess.run(['node','-e',"const {parse}=require('./ops/production/strict-json.cjs');parse('{\"a\":1,\"a\":2}')"],cwd=ROOT,capture_output=True);self.assertNotEqual(p.returncode,0)
 def test_missing_credentials_no_fallback(self):
  code="const g=require('./ops/production/request-gate.cjs');g.fetch('/x',{},null,null,Response).then(()=>process.exit(0),()=>process.exit(7))"
  p=subprocess.run(['/tmp/gsx-no-network','node','-e',code],cwd=ROOT,env={'PATH':os.environ['PATH'],'CLOUDFLARE_API_TOKEN':'legacy-synthetic-only'},capture_output=True);self.assertEqual(p.returncode,1)
 def test_existing_generator_unchanged(self):
  orig=subprocess.check_output(['git','show','5820a9c7fb10f6d1e6c9c944a3490edc36da9c48:scripts/update-bls-official-snapshot.mjs'],cwd=ROOT);self.assertEqual(orig,(ROOT/'scripts/update-bls-official-snapshot.mjs').read_bytes())
 def test_locked_origins_and_integrity(self):
  from prepare import validate_lock,OPS
  ident=parse((OPS/'toolchain/identities.json').read_bytes());validate_lock(ident)
  ident['lock_sha256']='0'*64
  with self.assertRaises(Rejected):validate_lock(ident)
 def test_publisher_unchanged(self):
  for f in ['.github/workflows/publish-accepted-bundle.yml','scripts/publication/publication.py']:
   orig=subprocess.check_output(['git','show','5820a9c7fb10f6d1e6c9c944a3490edc36da9c48:'+f],cwd=ROOT);self.assertEqual(orig,(ROOT/f).read_bytes())
