"""Secret-free reproducible toolchain preparation and same-run authorization."""
import base64,datetime as dt,hashlib,importlib.util,json,os,pathlib,shutil,subprocess,sys,tarfile,tempfile,gzip
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from authorization import *
ROOT=pathlib.Path(__file__).resolve().parents[2];OPS=ROOT/'ops/production'
PROFILE_CONFIG_SHA256='003ba675a52810679d9ead5e468861d5140d7fcbbe497b76835ebdf11eb564f4'
def git(*args):return subprocess.check_output(['git',*args],cwd=ROOT,text=True).strip()
def inventory(root):
 entries={}
 for p in sorted(root.rglob('*')):
  if p.is_file():need(not p.is_symlink(),'artifact symlink');entries[p.relative_to(root).as_posix()]=digest(p.read_bytes())
 return entries
def capture(output):
 spec=importlib.util.spec_from_file_location('capture_policy',OPS/'capture-policy.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod.capture(ROOT,output)
def npm_tree(root):
 entries={}
 for p in sorted(root.rglob('*')):
  if p.is_symlink():need(p.resolve().is_relative_to(root.resolve()),'npm link escape');entries[p.relative_to(root).as_posix()]={'link':p.readlink().as_posix()}
  elif p.is_file():entries[p.relative_to(root).as_posix()]={'sha256':digest(p.read_bytes())}
 return digest(canonical(entries))

def validate_lock(ident):
 lockpath=OPS/'toolchain/package-lock.json';lock=parse(lockpath.read_bytes());need(lock['lockfileVersion']==3,'lock format')
 need(lock['packages']['']['dependencies']=={'wrangler':'3.114.17'},'locked top-level dependency')
 need(lock['packages']['node_modules/wrangler']['integrity']==ident['npm_integrity'],'upstream package integrity')
 for name,p in lock['packages'].items():
  if not name:continue
  need(p.get('resolved','').startswith('https://registry.npmjs.org/') and p.get('integrity','').startswith('sha512-'),'unreviewed dependency origin/integrity')
 need(digest(lockpath.read_bytes())==ident['lock_sha256'],'lock bytes')

def build(out):
 need(digest((ROOT/'wrangler.toml').read_bytes())==PROFILE_CONFIG_SHA256,'unsupported deployment configuration profile')
 ident=parse((OPS/'toolchain/identities.json').read_bytes());validate_lock(ident)
 for f,k in [('patch-toolchain.py','patcher_sha256'),('request-gate.cjs','gate_sha256'),('strict-json.cjs','strict_json_sha256')]:need(digest((OPS/f).read_bytes())==ident[k],'reviewed toolchain source hash')
 node=pathlib.Path(subprocess.check_output(['node','-p','process.execPath'],text=True).strip())
 need(subprocess.check_output(['node','--version'],text=True).strip()=='v'+ident['node'],'Node version')
 need(digest(node.read_bytes())==ident['node_binary_sha256'],'Node binary identity')
 need(subprocess.check_output(['npm','--version'],text=True).strip()==ident['npm'],'npm version')
 npm_cli=pathlib.Path(shutil.which('npm')).resolve();need(digest(npm_cli.read_bytes())==ident['npm_cli_sha256'],'npm entrypoint identity');need(npm_tree(npm_cli.parent.parent)==ident['npm_tree_sha256'],'npm complete-tree identity')
 with tempfile.TemporaryDirectory() as install_home:
  install_env={'PATH':os.environ['PATH'],'HOME':install_home,'CI':'true','npm_config_registry':'https://registry.npmjs.org'}
  # Work's public-registry network may require a host proxy. Preserve only
  # transport settings, never repository/Production credentials. SRI is mandatory.
  for k in os.environ:
   if k.lower() in ('http_proxy','https_proxy','all_proxy','no_proxy','npm_config_proxy','npm_config_http_proxy','npm_config_https_proxy') or k=='NODE_EXTRA_CA_CERTS':install_env[k]=os.environ[k]
  subprocess.run(['npm','ci','--ignore-scripts','--no-audit','--no-fund','--prefix',str(OPS/'toolchain')],check=True,env=install_env)
 subprocess.run([sys.executable,str(OPS/'patch-toolchain.py')],check=True)
 # Source-derived policy is separately approved, not part of the reusable
 # toolchain artifact. Otherwise every BLS snapshot would change its golden hash.
 policy=capture(out.parent/'capture.json')
 shutil.copytree(OPS/'toolchain/node_modules',out/'ops/production/toolchain/node_modules',symlinks=False,ignore=shutil.ignore_patterns('.bin'))
 for p in [OPS/'request-gate.cjs',OPS/'strict-json.cjs']:shutil.copy2(p,out/'ops/production'/p.name)
 license=node.parent.parent/'LICENSE';need(license.is_file() and digest(license.read_bytes())==ident['node_license_sha256'],'Node license identity');shutil.copy2(license,out/'NODE-LICENSE');shutil.copy2(node,out/'node');(out/'inventory.json').write_bytes(canonical(inventory(out)))
 with open(out.parent/'toolchain.tar.gz','wb') as raw:
  with gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0) as z:
   with tarfile.open(fileobj=z,mode='w') as t:
    for p in sorted(out.rglob('*')):
     info=t.gettarinfo(str(p),arcname=p.relative_to(out).as_posix());info.uid=info.gid=0;info.uname=info.gname='';info.mtime=0
     if info.isfile():
      with p.open('rb') as f:t.addfile(info,f)
     elif info.isdir():t.addfile(info)
     else:raise Rejected('unsupported artifact entry')
 return policy
if __name__=='__main__':
 need(os.environ.get('GSX_PRODUCTION_ENABLED')=='true','Production setup disabled')
 for k in os.environ:need(not k.startswith('GSX_PRODUCTION_CF_'),'Production credential in Prepare')
 need(os.environ['GITHUB_REPOSITORY']==REPO and os.environ['GITHUB_REPOSITORY_ID']==str(REPO_ID) and os.environ['GITHUB_ACTOR']==OWNER and os.environ['GITHUB_ACTOR_ID']==str(OWNER_ID),'runner identity')
 need(os.environ['GITHUB_REF']=='refs/heads/main' and os.environ['GITHUB_EVENT_NAME']=='workflow_dispatch' and os.environ['GITHUB_RUN_ATTEMPT']=='1','runner event')
 sha=git('rev-parse','HEAD');need(sha==os.environ['GITHUB_SHA']==os.environ['GSX_PRODUCTION_REVIEWED_SHA'],'initial source authorization')
 api=API(os.environ['GH_READ_TOKEN']);run=api.get(f'/repos/{REPO}/actions/runs/{os.environ["GITHUB_RUN_ID"]}')
 out=ROOT/'.production-build';out.mkdir();artifact=out/'toolchain';artifact.mkdir()
 p=build(artifact);baseline=parse(os.environ['INPUT_BASELINE_JSON'])
 # Existing configuration has a single DO migration. Unknown deployed migration
 # state is never repaired automatically; it requires separate reviewed handling.
 need(baseline['migration_tag']=='v1-gold-feed','unqualified Durable Object baseline')
 account=os.environ['GSX_PRODUCTION_ACCOUNT'];p.update(format='gsx-request-policy-v1',account=account,worker='goldsignalsx-worker',settings={'observability':{'enabled':True,'head_sampling_rate':1}},subdomain={'enabled':True,'previews_enabled':True},schedules=[{'cron':'*/5 * * * *'}])
 (out/'policy.json').write_bytes(canonical(p));files=git('ls-files').splitlines();source_digest=digest(canonical({f:digest((ROOT/f).read_bytes()) for f in files}))
 created=run['created_at'];expires=dt.datetime.fromtimestamp(stamp(created)+900,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
 m=dict(format='gsx-production-authorization-v1',repository=REPO,repository_id=REPO_ID,owner=OWNER,owner_id=OWNER_ID,workflow_path=WORKFLOW,workflow_id=run['workflow_id'],workflow_sha=sha,workflow_sha256=digest((ROOT/WORKFLOW).read_bytes()),source_sha=sha,source_tree=git('rev-parse','HEAD^{tree}'),run_id=int(os.environ['GITHUB_RUN_ID']),event='workflow_dispatch',attempt=1,environment=ENVIRONMENT,environment_id=int(os.environ['GSX_PRODUCTION_ENVIRONMENT_ID']),account=account,worker='goldsignalsx-worker',source_digest=source_digest,config_digest=digest((ROOT/'wrangler.toml').read_bytes()),toolchain_digest=digest((artifact/'inventory.json').read_bytes()),artifact_digest=digest((out/'toolchain.tar.gz').read_bytes()),policy_digest=digest((out/'policy.json').read_bytes()),baseline=baseline,rollback={'version_id':os.environ['INPUT_ROLLBACK'],'version_digest':os.environ['INPUT_ROLLBACK_DIGEST']},created_at=created,expires_at=expires,credential_mode=os.environ['GSX_PRODUCTION_CREDENTIAL_MODE'],migration_t0=os.environ.get('GSX_PRODUCTION_MIGRATION_T0') or None,isolation_qualified=os.environ.get('GSX_PRODUCTION_ISOLATION_READY')=='true')
 run_identity(run,m);d=manifest(m);window(m,Clock());(out/'authorization.json').write_bytes(canonical(m))
 with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as f:f.write('Owner must inspect manifest and approve exactly:\n\n`GSX-PRODUCTION-APPROVE v1 sha256:'+d+'`\n\nExpiry: '+expires+'\n')
