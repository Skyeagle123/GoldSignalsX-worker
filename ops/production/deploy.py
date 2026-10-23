"""One final authorization process, followed by at most one CLI invocation.
No live call is made by importing this module. No retries or automatic rollback.
"""
import os,pathlib,signal,subprocess,sys,tarfile,tempfile,time,urllib.request
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from authorization import *
from prepare import inventory,git
ROOT=pathlib.Path(__file__).resolve().parents[2]
CANCELLED=False
def cancel(signum,frame):
 global CANCELLED
 CANCELLED=True
 # Raise rather than merely set a flag: a signal between the final guard and
 # exec must prevent invocation too. exec keeps the same PID; after exec the
 # preloaded Node gate owns cancellation and non-success exit semantics.
 raise Rejected('cancelled before CLI invocation')
def cfget(account,token,suffix,deadline=None):
 class NoRedirect(urllib.request.HTTPRedirectHandler):
  def redirect_request(self,*a,**k):return None
 timeout=3 if deadline is None else min(3,deadline-time.monotonic());need(timeout>0,'Cloudflare read deadline')
 q=urllib.request.Request('https://api.cloudflare.com/client/v4/accounts/'+account+suffix,headers={'Authorization':'Bearer '+token,'Cache-Control':'no-cache'})
 with urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect()).open(q,timeout=timeout) as r:
  need(r.status==200,'Cloudflare read status');v=parse(r.read(1048577),integers_only=False);need(v['success']is True and not v['errors'],'Cloudflare read evidence');need(not v.get('result_info',{}).get('cursor') and v.get('result_info',{}).get('total_pages',1)<=1,'Cloudflare read pagination');return v['result']
def baseline(account,token,deadline=None):
 s='/workers/scripts/goldsignalsx-worker';v={k:cfget(account,token,s+'/'+k,deadline) for k in ['deployments','settings','subdomain','schedules']}
 scripts=cfget(account,token,'/workers/scripts',deadline);rows=[x for x in scripts if x['id']=='goldsignalsx-worker'];need(len(rows)==1,'Worker baseline identity');return {**{k:digest(canonical(x)) for k,x in v.items()},'migration_tag':rows[0].get('migration_tag')}
def unpack(path,dest):
 need(path.stat().st_size<=100_000_000,'artifact size')
 with tarfile.open(path,'r:gz') as t:
  count=0;size=0;seen=set()
  for m in t:
   count+=1;size+=m.size;p=pathlib.PurePosixPath(m.name)
   need(count<=10000 and size<=500_000_000 and not p.is_absolute() and '..'not in p.parts and m.name not in seen,'artifact paths/bounds');seen.add(m.name)
   need(m.isfile() or m.isdir(),'artifact type')
   out=dest.joinpath(*p.parts)
   if m.isdir():out.mkdir(parents=True,exist_ok=True)
   else:
    out.parent.mkdir(parents=True,exist_ok=True);out.write_bytes(t.extractfile(m).read());out.chmod(m.mode&0o755)
def deploy(build):
 need(not CANCELLED,'cancelled');m=parse((build/'authorization.json').read_bytes());manifest(m)
 need(os.environ.get('GSX_PRODUCTION_ENABLED')=='true' and m['source_sha']==os.environ['GSX_PRODUCTION_REVIEWED_SHA'],'initial constraints')
 need(os.environ['GITHUB_REPOSITORY']==REPO and os.environ['GITHUB_REPOSITORY_ID']==str(REPO_ID) and os.environ['GITHUB_ACTOR']==OWNER and os.environ['GITHUB_ACTOR_ID']==str(OWNER_ID),'runner identity')
 need(os.environ['GITHUB_SHA']==m['source_sha'] and os.environ['GITHUB_RUN_ID']==str(m['run_id']) and os.environ['GITHUB_RUN_ATTEMPT']=='1' and os.environ['GITHUB_REF']=='refs/heads/main' and os.environ['GITHUB_EVENT_NAME']=='workflow_dispatch','runner source/run')
 need(os.environ['GITHUB_WORKFLOW_SHA']==m['workflow_sha'] and os.environ['GITHUB_WORKFLOW_REF']==REPO+'/'+WORKFLOW+'@refs/heads/main','workflow revision')
 token=os.environ.get('GSX_PRODUCTION_CF_API_TOKEN','');account=os.environ.get('GSX_PRODUCTION_CF_ACCOUNT_ID','');need(bool(token.strip()) and account==m['account'],'missing/wrong Production credentials')
 need(digest((ROOT/WORKFLOW).read_bytes())==m['workflow_sha256'] and git('rev-parse','HEAD^{tree}')==m['source_tree'],'source checkout')
 need(digest(canonical({f:digest((ROOT/f).read_bytes()) for f in git('ls-files').splitlines()}))==m['source_digest'],'source bytes')
 need(digest((ROOT/'wrangler.toml').read_bytes())==m['config_digest'],'configuration')
 need(digest((build/'toolchain.tar.gz').read_bytes())==m['artifact_digest'] and digest((build/'policy.json').read_bytes())==m['policy_digest'],'artifact/policy substitution')
 api=API(os.environ['GH_READ_TOKEN']);art=api.get(f'/repos/{REPO}/actions/artifacts/{int(os.environ["GSX_ARTIFACT_ID"])}')
 need(art['workflow_run']['id']==m['run_id'] and art['workflow_run']['head_sha']==m['source_sha'] and not art['expired'] and art['name']==f'gsx-production-{m["run_id"]}-1','same-run artifact')
 clock=Clock();window(m,clock)
 with tempfile.TemporaryDirectory() as td:
  d=pathlib.Path(td);unpack(build/'toolchain.tar.gz',d)
  inv=parse((d/'inventory.json').read_bytes());need(digest((d/'inventory.json').read_bytes())==m['toolchain_digest'],'inventory digest');actual=inventory(d);actual.pop('inventory.json');need(actual==inv,'toolchain bytes')
  policy=parse((build/'policy.json').read_bytes());need(policy['account']==account and policy['worker']==m['worker'],'policy target');policy_path=d/'policy.json';policy_path.write_bytes(canonical(policy))
  rollback=cfget(account,token,'/workers/scripts/goldsignalsx-worker/versions/'+m['rollback']['version_id'])
  need(rollback.get('id')==m['rollback']['version_id'] and digest(canonical(rollback))==m['rollback']['version_digest'],'rollback evidence changed/unavailable')
  need(baseline(account,token)==m['baseline'],'stale Production baseline')
  # No install, artifact handoff or unrelated work after the following checks.
  env={'PATH':'/usr/bin:/bin','HOME':td,'CI':'true','WRANGLER_SEND_METRICS':'false','CLOUDFLARE_SEND_METRICS':'false','GSX_DEPLOY_CONTEXT_FILE':str(policy_path),'GSX_PRODUCTION_CF_API_TOKEN':token,'GSX_PRODUCTION_CF_ACCOUNT_ID':account,'CLOUDFLARE_API_TOKEN':token,'CLOUDFLARE_ACCOUNT_ID':account}
  cmd=[str(d/'node'),'--require',str(d/'ops/production/request-gate.cjs'),str(d/'ops/production/toolchain/node_modules/wrangler/wrangler-dist/cli.js'),'deploy','--config',str(ROOT/'wrangler.toml')]
  # Baseline verification is within the same ten-second budget, not a cached pin.
  start=time.monotonic();deadline=start+10;need(baseline(account,token,deadline)==m['baseline'],'baseline changed')
  rollback=cfget(account,token,'/workers/scripts/goldsignalsx-worker/versions/'+m['rollback']['version_id'],deadline)
  need(rollback.get('id')==m['rollback']['version_id'] and digest(canonical(rollback))==m['rollback']['version_digest'],'rollback evidence changed')
  deadline=fresh(api,m,int(os.environ['GSX_CHECK_RUN_ID']),clock,deadline)
  need(time.monotonic()-start<10 and time.monotonic()<deadline and not CANCELLED,'final freshness/cancellation')
  window(m,clock)
  need(not CANCELLED,'cancelled before invocation')
  # Replace this verifier process. There is no mutable command handoff, child
  # supervisor retry or second invocation after an uncertain result.
  os.chdir(ROOT)
  os.execve(cmd[0],cmd,env)
if __name__=='__main__':
 for s in [signal.SIGINT,signal.SIGTERM]:signal.signal(s,cancel)
 try:deploy(ROOT/'.production-build')
 except Exception as e:print('STOP: '+str(e),file=sys.stderr);raise SystemExit(1)
