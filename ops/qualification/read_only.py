"""Qualification-only GET client. No deployment, publication, or credential issuance."""
import hashlib,json,os,re,subprocess,sys,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
REPO='Skyeagle123/GoldSignalsX-worker'
ACCOUNT='4bfa1a9c849c3da50520f7a4bcccccab'
class Refused(Exception):pass
def need(ok):
 if not ok:raise Refused()
def source():
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
def read(host,path,token,allowed):
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
def run(mode):
 need(mode in ('production','publication'))
 need(os.environ.get('GITHUB_REPOSITORY')==REPO and os.environ.get('GITHUB_REPOSITORY_ID')=='1085006449')
 need(os.environ.get('GITHUB_ACTOR_ID')=='125151458' and os.environ.get('GITHUB_TRIGGERING_ACTOR')=='Skyeagle123')
 need(os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch' and os.environ.get('GITHUB_REF')=='refs/heads/main' and os.environ.get('GITHUB_RUN_ATTEMPT')=='1')
 need(not any(os.environ.get(k) for k in ('GSX_PRODUCTION_CF_API_TOKEN','GSX_PUBLICATION_APP_PRIVATE_KEY','CLOUDFLARE_API_TOKEN')))
 source();rid=os.environ.get('GITHUB_RUN_ID','');need(bool(re.fullmatch('[1-9][0-9]*',rid)))
 base='/repos/'+REPO;paths={base+'/actions/runs/'+rid,base+'/actions/runs/'+rid+'/approvals',base+'/git/ref/heads/main'}
 token=os.environ['GH_READ_TOKEN'];run=read('api.github.com',base+'/actions/runs/'+rid,token,paths)
 need(run['head_sha']==os.environ['GITHUB_SHA'] and run['run_attempt']==1 and run['event']=='workflow_dispatch' and run['status']=='in_progress')
 need(run['actor']['id']==run['triggering_actor']['id']==125151458)
 need(run['path']=='.github/workflows/qualification-only.yml' and run['repository']['id']==1085006449)
 need(read('api.github.com',base+'/git/ref/heads/main',token,paths)['object']['sha']==os.environ['GITHUB_SHA'])
 eid={'production':25273664884,'publication':25273747086}[mode]
 reviews=read('api.github.com',base+'/actions/runs/'+rid+'/approvals',token,paths)
 relevant=[r for r in reviews if any(e.get('id')==eid and e.get('name')=='goldsignalsx-'+mode for e in r.get('environments',[]))]
 need(len(relevant)==1 and relevant[0]['state']=='approved' and relevant[0]['user']['id']==125151458)
 if mode=='production':
  # Separate metadata-only credential; NEVER the deployment credential.
  need(os.environ.get('GSX_QUALIFICATION_READ_POLICY_VERIFIED')=='true')
  token=os.environ['GSX_QUALIFICATION_CF_READ_TOKEN'];prefix='/client/v4/accounts/'+ACCOUNT+'/workers/scripts/goldsignalsx-worker/'
  allowed={prefix+x for x in ('settings','schedules','subdomain')}
  for path in sorted(allowed):
   result=read('api.cloudflare.com',path,token,allowed);need(result.get('success') is True and result.get('errors')==[])
 print('READ-ONLY QUALIFICATION PASS; deployment/publication NOT authorized')
if __name__=='__main__':
 try:run(sys.argv[1])
 except Exception:print('READ-ONLY QUALIFICATION STOP',file=sys.stderr);sys.exit(1)
