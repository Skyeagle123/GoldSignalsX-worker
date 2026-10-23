"""Narrow BLS PR writer. Never executes generated snapshot content."""
import base64,json,os,pathlib,re,sys,urllib.request,urllib.error
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from authorization import REPO,REPO_ID,need,parse,digest
BRANCH='automation/bls-official-snapshot';PATH='bls-official-snapshot.js'
class API:
 def call(self,method,path,data=None,missing=False):
  need(path.startswith('/repos/'+REPO+'/'),'repository')
  class NoRedirect(urllib.request.HTTPRedirectHandler):
   def redirect_request(self,*a,**kw):return None
  q=urllib.request.Request('https://api.github.com'+path,data=json.dumps(data).encode() if data is not None else None,method=method,headers={'Authorization':'Bearer '+os.environ['GH_WRITE_TOKEN'],'Accept':'application/vnd.github+json','Content-Type':'application/json','X-GitHub-Api-Version':'2022-11-28'})
  try:
   with urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect()).open(q,timeout=10) as r:
    need(not r.headers.get('Link'),'pagination');return parse(r.read(1048577))
  except urllib.error.HTTPError as e:
   if missing and e.code==404:return None
   raise
def enforce(changed):need(changed==[PATH],'BLS generated artifact may change snapshot only')
def main():
 need(os.environ['GITHUB_REPOSITORY']==REPO and os.environ['GITHUB_REPOSITORY_ID']==str(REPO_ID) and os.environ['GITHUB_REF']=='refs/heads/main' and os.environ['GITHUB_RUN_ATTEMPT']=='1','writer identity')
 p=pathlib.Path('.bls-artifact');m=parse((p/'manifest.json').read_bytes());enforce(m['changed']);need(m['run_id']==os.environ['GITHUB_RUN_ID'] and m['source_sha']==os.environ['GITHUB_SHA'],'artifact run/source')
 b=(p/PATH).read_bytes();need(len(b)<=2_000_000 and digest(b)==m['sha256'],'snapshot integrity')
 api=API();base='/repos/'+REPO;head=api.call('GET',base+'/git/ref/heads/main')['object']['sha'];need(head==m['source_sha'],'main advanced; fresh generation needed')
 maincommit=api.call('GET',base+'/git/commits/'+head);blob=api.call('POST',base+'/git/blobs',{'encoding':'base64','content':base64.b64encode(b).decode()})
 tree=api.call('POST',base+'/git/trees',{'base_tree':maincommit['tree']['sha'],'tree':[{'path':PATH,'mode':'100644','type':'blob','sha':blob['sha']}]})
 branch=api.call('GET',base+'/git/ref/heads/'+BRANCH,missing=True);parent=branch['object']['sha'] if branch else head
 if branch:
  proposed=api.call('GET',base+'/contents/'+PATH+'?ref='+parent)
  if proposed['sha']==blob['sha']:
   existing=api.call('GET',base+'/pulls?state=open&base=main&head=Skyeagle123:'+BRANCH+'&per_page=100');need(len(existing)==1,'unchanged proposal has no unique open PR; Owner resolution required')
   print('Existing BLS PR already contains this exact snapshot; no commit/update');return
 if branch:
  forward=api.call('GET',base+'/compare/'+head+'...'+parent)['status']
  if forward not in ('ahead','identical'):
   reverse=api.call('GET',base+'/compare/'+parent+'...'+head)['status']
   need(reverse in ('ahead','identical'),'BLS branch diverged: Owner must resolve existing PR; no force or automatic merge');parent=head
 need(api.call('GET',base+'/git/ref/heads/main')['object']['sha']==head,'main changed before branch update')
 commit=api.call('POST',base+'/git/commits',{'message':'Refresh verified official BLS snapshot','tree':tree['sha'],'parents':[parent]})
 # Fixed branch, normal fast-forward only. Never force-update or auto-merge.
 if branch:api.call('PATCH',base+'/git/refs/heads/'+BRANCH,{'sha':commit['sha'],'force':False})
 else:api.call('POST',base+'/git/refs',{'ref':'refs/heads/'+BRANCH,'sha':commit['sha']})
 need(api.call('GET',base+'/git/ref/heads/main')['object']['sha']==head,'main changed; inspect BLS branch before PR')
 prs=api.call('GET',base+'/pulls?state=open&base=main&head=Skyeagle123:'+BRANCH+'&per_page=100');need(len(prs)<=1,'ambiguous BLS PR')
 if not prs:api.call('POST',base+'/pulls',{'title':'Refresh official BLS snapshot','head':BRANCH,'base':'main','body':'Snapshot-only official-source refresh. Owner review and CI are required; no automatic approval or merge.','draft':False})
 print('BLS snapshot PR prepared; review/CI required')
if __name__=='__main__':main()
