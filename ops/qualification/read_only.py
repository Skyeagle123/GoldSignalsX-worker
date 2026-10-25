"""Qualification-only GET client. No deployment, publication, or credential issuance."""
import hashlib,json,os,re,subprocess,sys,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
REPO='Skyeagle123/GoldSignalsX-worker'
ACCOUNT='4bfa1a9c849c3da50520f7a4bcccccab'
STAGE='CONTEXT'
CODES=frozenset(('CONTEXT','SOURCE','RUN_IDENTITY','MAIN_IDENTITY','APPROVAL','JOB_IDENTITY','HTTP_RESPONSE','METADATA','RUN_STATE','INTERNAL'))
class Refused(Exception):
 def __init__(self,code=None):self.code=code or STAGE

def stage(code):
 global STAGE
 STAGE=code

def integer(value):return type(value) is int and value>0

def need(ok):
 if not ok:raise Refused()
def source():
 stage('SOURCE')
 env={'PATH':'/usr/bin:/bin','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_NO_REPLACE_OBJECTS':'1'}
 def git(*args):return subprocess.check_output(['git',*args],cwd=ROOT,env=env,text=True).strip()
 sha=git('rev-parse','HEAD');need(sha==os.environ.get('GSX_QUALIFICATION_REVIEWED_SHA')==os.environ.get('GITHUB_SHA')==os.environ.get('GITHUB_WORKFLOW_SHA'))
 need(os.environ.get('GITHUB_WORKFLOW_REF')==REPO+'/.github/workflows/qualification-only.yml@refs/heads/main')
 files=git('ls-files','-z').split('\0');inventory={}
 for f in filter(None,files):
  p=ROOT/f;need(p.is_file() and not p.is_symlink());inventory[f]=hashlib.sha256(p.read_bytes()).hexdigest()
 digest=hashlib.sha256(json.dumps(inventory,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 need(digest==os.environ.get('GSX_QUALIFICATION_SOURCE_DIGEST'))
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):raise Refused()
def _read(host,path,token,allowed):
 need(path in allowed and host in ('api.github.com','api.cloudflare.com'))
 request=urllib.request.Request('https://'+host+path,method='GET',headers={'Authorization':'Bearer '+token,'Accept':'application/json','Cache-Control':'no-cache'})
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
 with opener.open(request,timeout=3) as response:
  need(response.status==200 and not response.headers.get('Link'));raw=response.read(1048577);need(len(raw)<=1048576)
  def pairs(items):
   result={}
   for key,value in items:
    need(key not in result);result[key]=value
   return result
  return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda value:(_ for _ in ()).throw(Refused()))
def read(host,path,token,allowed):
 try:return _read(host,path,token,allowed)
 except Exception:raise Refused('HTTP_RESPONSE') from None

def bind_job(mode,rid,sha,token,base,paths):
 stage('JOB_IDENTITY')
 need(os.environ.get('GITHUB_JOB')==mode)
 runner=os.environ.get('RUNNER_NAME');need(isinstance(runner,str) and bool(runner))
 listing=base+'/actions/runs/'+rid+'/attempts/1/jobs?per_page=100'
 paths.add(listing);data=read('api.github.com',listing,token,paths)
 need(type(data) is dict and type(data.get('jobs')) is list)
 jobs=data['jobs'];need(type(data.get('total_count')) is int and data['total_count']==len(jobs) and len(jobs)==2)
 need(all(type(j) is dict and integer(j.get('id')) for j in jobs))
 need(len({j['id'] for j in jobs})==len(jobs))
 need(sorted(j.get('name','') for j in jobs)==['production','publication'])
 matches=[j for j in jobs if j.get('name')==mode];need(len(matches)==1)
 job=matches[0];jid=job['id']
 def check(j):
  need(type(j) is dict and type(j.get('id')) is int and j['id']==jid)
  need(type(j.get('run_id')) is int and j['run_id']==int(rid))
  need(j.get('head_sha')==sha and j.get('head_branch')=='main')
  need(j.get('name')==mode and j.get('workflow_name')=='Read-only security qualification')
  need(j.get('status')=='in_progress' and j.get('conclusion') is None and 'conclusion' in j)
  need(j.get('runner_name')==runner and integer(j.get('runner_id')))
  need(j.get('run_url')=='https://api.github.com'+base+'/actions/runs/'+rid)
 check(job)
 detail=base+'/actions/jobs/'+str(jid);paths.add(detail)
 observed=read('api.github.com',detail,token,paths);check(observed)
 need(observed['runner_id']==job['runner_id'])

def validate_run(run,rid):
 stage('RUN_IDENTITY')
 need(type(run) is dict)
 for key,expected in (('id',int(rid)),('run_attempt',1),('workflow_id',396101973)):
  need(integer(run.get(key)) and run[key]==expected)
 for key,expected in (('actor',125151458),('triggering_actor',125151458),('repository',1085006449)):
  record=run.get(key);need(type(record) is dict and integer(record.get('id')) and record['id']==expected)
 need(run.get('path')=='.github/workflows/qualification-only.yml')
 need(run.get('event')=='workflow_dispatch' and run.get('head_branch')=='main')
 need(run.get('head_sha')==os.environ['GITHUB_SHA'])
 stage('RUN_STATE')
 need(run.get('status')=='in_progress' and 'conclusion' in run and run['conclusion'] is None)

def _run(mode):
 need(mode in ('production','publication'))
 need(os.environ.get('GITHUB_REPOSITORY')==REPO and os.environ.get('GITHUB_REPOSITORY_ID')=='1085006449')
 need(os.environ.get('GITHUB_ACTOR_ID')=='125151458' and os.environ.get('GITHUB_TRIGGERING_ACTOR')=='Skyeagle123')
 need(os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch' and os.environ.get('GITHUB_REF')=='refs/heads/main' and os.environ.get('GITHUB_RUN_ATTEMPT')=='1')
 need(not any(os.environ.get(k) for k in ('GSX_PRODUCTION_CF_API_TOKEN','GSX_PUBLICATION_APP_PRIVATE_KEY','CLOUDFLARE_API_TOKEN')))
 source();stage('RUN_IDENTITY');rid=os.environ.get('GITHUB_RUN_ID','');need(bool(re.fullmatch('[1-9][0-9]*',rid)))
 stage('RUN_IDENTITY')
 base='/repos/'+REPO;paths={base+'/actions/runs/'+rid,base+'/actions/runs/'+rid+'/approvals',base+'/git/ref/heads/main'}
 token=os.environ['GH_READ_TOKEN'];run=read('api.github.com',base+'/actions/runs/'+rid,token,paths)
 validate_run(run,rid)
 stage('MAIN_IDENTITY')
 need(read('api.github.com',base+'/git/ref/heads/main',token,paths)['object']['sha']==os.environ['GITHUB_SHA'])
 bind_job(mode,rid,os.environ['GITHUB_SHA'],token,base,paths)
 stage('APPROVAL')
 eid={'production':25273664884,'publication':25273747086}[mode]
 reviews=read('api.github.com',base+'/actions/runs/'+rid+'/approvals',token,paths)
 need(type(reviews) is list and all(type(r) is dict and type(r.get('environments')) is list and all(type(e) is dict and integer(e.get('id')) for e in r['environments']) for r in reviews))
 need(all(len({e['id'] for e in r['environments']})==len(r['environments']) for r in reviews))
 relevant=[r for r in reviews if any(e.get('id')==eid and e.get('name')=='goldsignalsx-'+mode for e in r.get('environments',[]))]
 need(len(relevant)==1 and relevant[0]['state']=='approved' and type(relevant[0]['user']['id']) is int and relevant[0]['user']['id']==125151458)
 stage('RUN_STATE')
 current=read('api.github.com',base+'/actions/runs/'+rid,token,paths)
 validate_run(current,rid)
 if mode=='production':
  stage('METADATA')
  # Separate metadata-only credential; NEVER the deployment credential.
  need(os.environ.get('GSX_QUALIFICATION_READ_POLICY_VERIFIED')=='true')
  token=os.environ['GSX_QUALIFICATION_CF_READ_TOKEN'];prefix='/client/v4/accounts/'+ACCOUNT+'/workers/scripts/goldsignalsx-worker/'
  allowed={prefix+x for x in ('settings','schedules','subdomain')}
  for path in sorted(allowed):
   result=read('api.cloudflare.com',path,token,allowed);need(result.get('success') is True and result.get('errors')==[])
 print('READ-ONLY QUALIFICATION PASS; deployment/publication NOT authorized')
def run(mode):
 stage('CONTEXT')
 try:return _run(mode)
 except Refused:raise
 except Exception:raise Refused() from None

def main():
 try:run(sys.argv[1])
 except Exception as error:
  code=error.code if isinstance(error,Refused) and error.code in CODES else 'INTERNAL'
  safe_stage=STAGE if STAGE in CODES else 'INTERNAL'
  print('READ-ONLY QUALIFICATION STOP code='+code+' stage='+safe_stage,file=sys.stderr)
  return 1
 return 0
if __name__=='__main__':sys.exit(main())
