"""Run-bound authority. No deployment calls, retries or credential minting here."""
import datetime as dt,hashlib,json,os,re,time,urllib.request,urllib.error
REPO='Skyeagle123/GoldSignalsX-worker';REPO_ID=1085006449;OWNER='Skyeagle123';OWNER_ID=125151458
WORKFLOW='.github/workflows/deploy-worker.yml';ENVIRONMENT='goldsignalsx-production'
FIELDS={'format','repository','repository_id','owner','owner_id','workflow_path','workflow_id','workflow_sha','workflow_sha256','source_sha','source_tree','run_id','event','attempt','environment','environment_id','account','worker','source_digest','config_digest','toolchain_digest','artifact_digest','policy_digest','baseline','rollback','created_at','expires_at','credential_mode','migration_t0','isolation_qualified'}
class Rejected(Exception):pass
def need(ok,why):
 if not ok:raise Rejected(why)
def pairs(xs):
 d={}
 for k,v in xs:
  need(k not in d and k not in ('__proto__','constructor','prototype'),'duplicate/unsafe JSON key');d[k]=v
 return d
def parse(data,integers_only=True):
 need(isinstance(data,(str,bytes)) and len(data)<=1048576,'JSON size')
 return json.loads(data,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(Rejected('JSON constant')),parse_float=(lambda _:(_ for _ in ()).throw(Rejected('JSON float'))) if integers_only else float)
def canonical(x):return json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(b):return hashlib.sha256(b).hexdigest()
def stamp(s):
 need(isinstance(s,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',s),'UTC timestamp')
 return dt.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def manifest(m):
 need(type(m)is dict and set(m)==FIELDS,'manifest schema')
 for k in ['repository_id','owner_id','workflow_id','run_id','attempt','environment_id']:need(type(m[k])is int and m[k]>0,'numeric identity')
 need((m['format'],m['repository'],m['repository_id'],m['owner'],m['owner_id'],m['workflow_path'],m['event'],m['attempt'],m['environment'],m['worker'])==('gsx-production-authorization-v1',REPO,REPO_ID,OWNER,OWNER_ID,WORKFLOW,'workflow_dispatch',1,ENVIRONMENT,'goldsignalsx-worker'),'identity')
 for k in ['workflow_sha','source_sha','source_tree']:need(type(m[k])is str and re.fullmatch('[0-9a-f]{40}',m[k]),'Git identity')
 need(m['workflow_sha']==m['source_sha'],'trusted source')
 for k in ['workflow_sha256','source_digest','config_digest','toolchain_digest','artifact_digest','policy_digest']:need(type(m[k])is str and re.fullmatch('[0-9a-f]{64}',m[k]),'digest')
 need(re.fullmatch('[0-9a-f]{32}',m['account']),'account')
 need(type(m['baseline'])is dict and set(m['baseline'])=={'deployments','settings','subdomain','schedules','migration_tag'},'baseline schema')
 for k in ['deployments','settings','subdomain','schedules']:need(type(m['baseline'][k])is str and re.fullmatch('[0-9a-f]{64}',m['baseline'][k]),'baseline digest')
 need(type(m['baseline']['migration_tag'])is str,'migration baseline')
 need(type(m['rollback'])is dict and set(m['rollback'])=={'version_id','version_digest'},'rollback schema')
 need(type(m['rollback']['version_id'])is str and re.fullmatch('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',m['rollback']['version_id']) and type(m['rollback']['version_digest'])is str and re.fullmatch('[0-9a-f]{64}',m['rollback']['version_digest']),'rollback identity/digest')
 need(m['credential_mode'] in ('migration','steady') and type(m['isolation_qualified'])is bool,'credential mode')
 if m['credential_mode']=='steady':need(m['migration_t0']is None and m['isolation_qualified'],'steady isolation not qualified')
 else:need(stamp(m['migration_t0'])<=stamp(m['created_at'])<stamp(m['migration_t0'])+2400,'migration dispatch window')
 need(stamp(m['expires_at'])==stamp(m['created_at'])+900,'expiry definition')
 return digest(canonical(m))
class API:
 def __init__(self,token):need(bool(token),'GitHub credential missing');self.token=token
 def get(self,path,deadline=None):
  need(path.startswith('/repos/'+REPO+'/'),'API allowlist')
  class NoRedirect(urllib.request.HTTPRedirectHandler):
   def redirect_request(self,*a,**k):return None
  timeout=3 if deadline is None else min(3,deadline-time.monotonic());need(timeout>0,'API deadline')
  q=urllib.request.Request('https://api.github.com'+path,headers={'Authorization':'Bearer '+self.token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28','Cache-Control':'no-cache'})
  with urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect()).open(q,timeout=timeout) as r:
   need(r.status==200 and not r.headers.get('Link'),'incomplete/redirected API evidence');return parse(r.read(1048577))
class Clock:
 def __init__(self):self.wall=time.time();self.mono=time.monotonic();self.last_wall=self.wall;self.last_mono=self.mono
 def check(self,expires):
  mono=time.monotonic();elapsed=mono-self.mono;wall=time.time();need(wall>=self.last_wall and mono>=self.last_mono and abs(wall-(self.wall+elapsed))<=2,'clock discontinuity');need(wall>=stamp(expires)-900,'clock before authenticated run');need(wall<stamp(expires),'authorization expired');self.last_wall=wall;self.last_mono=mono
def window(m,clock):
 clock.check(m['expires_at'])
 if m['credential_mode']=='migration':need(time.time()<stamp(m['migration_t0'])+2400,'migration start cutoff expired')

def run_identity(run,m):
 need(run['id']==m['run_id'] and run['run_attempt']==1 and run['event']=='workflow_dispatch' and run['head_sha']==m['source_sha'] and run['head_branch']=='main','run identity')
 need(run['repository']['id']==REPO_ID and run['repository']['full_name']==REPO,'run repository')
 for k in ['actor','triggering_actor']:need(run[k]['id']==OWNER_ID and run[k]['login']==OWNER,'run Owner')
 need(run['workflow_id']==m['workflow_id'] and run['path']==WORKFLOW and run['created_at']==m['created_at'],'run workflow/time')
 need(run['status']=='in_progress' and run.get('conclusion')is None,'run terminal/cancelled')
def fresh(api,m,check_run_id,clock,deadline=None):
 expected=manifest(m);window(m,clock);start=time.monotonic();deadline=min(deadline,start+10) if deadline is not None else start+10
 base='/repos/'+REPO
 run=api.get(f'{base}/actions/runs/{m["run_id"]}',deadline);run_identity(run,m)
 main=api.get(base+'/git/ref/heads/main',deadline);need(main['object']['sha']==m['source_sha'],'main advanced')
 commit=api.get(base+'/git/commits/'+m['source_sha'],deadline);need(commit['tree']['sha']==m['source_tree'],'source tree')
 reviews=api.get(f'{base}/actions/runs/{m["run_id"]}/approvals',deadline)
 need(type(reviews)is list,'approval response');relevant=[]
 for r in reviews:
  need(type(r)is dict and type(r.get('environments'))is list,'approval schema')
  if any(e.get('id')==m['environment_id'] or e.get('name')==ENVIRONMENT for e in r['environments']):relevant.append(r)
 need(len(relevant)==1,'missing/multiple relevant reviews');r=relevant[0]
 need(r['state']=='approved' and r['user']['login']==OWNER and r['user']['id']==OWNER_ID,'approval/rejection/reviewer')
 need(r['comment']=='GSX-PRODUCTION-APPROVE v1 sha256:'+expected,'approval digest')
 need(sum(e.get('id')==m['environment_id'] and e.get('name')==ENVIRONMENT for e in r['environments'])==1,'approval environment')
 jobs=api.get(f'{base}/actions/runs/{m["run_id"]}/attempts/1/jobs?per_page=100',deadline)
 need(type(jobs['total_count'])is int and jobs['total_count']==len(jobs['jobs']) and jobs['total_count']<=100,'job enumeration')
 ds=[j for j in jobs['jobs'] if j['name']=='Deploy Production'];ps=[j for j in jobs['jobs'] if j['name']=='Prepare Production']
 need(len(ds)==len(ps)==1,'job ambiguity');j=ds[0]
 need(type(j['id'])is int and j['id']>0 and j['run_id']==m['run_id'] and j['run_attempt']==1 and j['head_sha']==m['source_sha'] and j['status']=='in_progress' and j['conclusion']is None,'Deploy job identity/state')
 need(j['check_run_url']==f'https://api.github.com/repos/{REPO}/check-runs/{check_run_id}','check-run cross-check')
 need(ps[0]['status']=='completed' and ps[0]['conclusion']=='success' and ps[0]['run_id']==m['run_id'] and ps[0]['run_attempt']==1 and ps[0]['head_sha']==m['source_sha'],'Prepare completion')
 window(m,clock);need(time.monotonic()<deadline,'final check exceeded ten seconds');return deadline
