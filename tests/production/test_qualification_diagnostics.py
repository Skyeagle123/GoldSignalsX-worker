"""Synthetic transport only; no secrets, production credentials or live requests."""
import contextlib,copy,hashlib,io,json,os,pathlib,socket,subprocess,sys,tempfile,unittest,urllib.error
from unittest.mock import patch
import test_qualification as fixtures
q=fixtures.q;ROOT=fixtures.ROOT

def tracked_files(root):
 raw=subprocess.check_output(['git','ls-files','-z'],cwd=root,env={'PATH':'/usr/bin:/bin','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_NO_REPLACE_OBJECTS':'1'})
 return [os.fsdecode(f) for f in raw.split(b'\0') if f]

class SourceInventory(unittest.TestCase):
 def setUp(self):
  temporary=tempfile.TemporaryDirectory(prefix='gsx-source-inventory-');self.addCleanup(temporary.cleanup);self.root=pathlib.Path(temporary.name)
  self.git_env={'PATH':'/usr/bin:/bin','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_NO_REPLACE_OBJECTS':'1'}
  self.git('init','--quiet')
  self.names=['tracked.txt','space name.txt','line\nbreak.txt','unicodé.txt']
  for name in self.names:(self.root/name).write_bytes(b'reviewed source\n')
  (self.root/'.gitignore').write_text('ops/production/toolchain/node_modules/\n')
  self.git('add','--','.gitignore',*self.names)
  self.inventory={f:hashlib.sha256((self.root/f).read_bytes()).hexdigest() for f in tracked_files(self.root)}
  self.network=patch.object(socket,'socket',side_effect=AssertionError('NETWORK FORBIDDEN'));self.network.start();self.addCleanup(self.network.stop)
 def git(self,*args):
  return subprocess.check_output(['git',*args],cwd=self.root,env=self.git_env)
 def exercise(self,reject=False,digest=None):
  original=subprocess.check_output
  def checked(args,**kwargs):
   if args==['git','rev-parse','HEAD']:return 'a'*40
   return original(args,**kwargs)
  env={**fixtures.QualificationIsolation().context(),'GSX_QUALIFICATION_REVIEWED_SHA':'a'*40,'GITHUB_WORKFLOW_SHA':'a'*40,'GITHUB_WORKFLOW_REF':q.REPO+'/.github/workflows/qualification-only.yml@refs/heads/main','GSX_QUALIFICATION_SOURCE_DIGEST':digest or hashlib.sha256(json.dumps(self.inventory,sort_keys=True,separators=(',',':')).encode()).hexdigest()}
  with patch.object(q,'ROOT',self.root),patch.dict(os.environ,env,clear=True),patch.object(q.subprocess,'check_output',side_effect=checked),patch.object(q,'read') as api:
   if reject:
    for mode in ('production','publication'):
     with self.subTest(mode=mode),self.assertRaises(q.Refused) as caught:q.run(mode)
     self.assertEqual(caught.exception.code,'SOURCE')
   else:q.source()
   api.assert_not_called()
 def test_clean_tracked_inventory_and_nul_paths(self):
  self.assertEqual(set(tracked_files(self.root)),set(self.inventory));self.assertIn('line\nbreak.txt',tracked_files(self.root));self.exercise()
 def test_installed_dependencies_and_untracked_files_excluded(self):
  before=tracked_files(self.root)
  modules=self.root/'ops/production/toolchain/node_modules';(modules/'acorn/bin').mkdir(parents=True);(modules/'.bin').mkdir()
  (modules/'acorn/bin/acorn').write_bytes(b'offline dependency fixture')
  (modules/'.bin/acorn').symlink_to('../acorn/bin/acorn')
  (self.root/'untracked.txt').write_text('untracked')
  (self.root/'untracked-link').symlink_to('tracked.txt')
  (self.root/'untracked-broken-link').symlink_to('absent')
  self.assertTrue((modules/'.bin/acorn').is_file());self.assertTrue((modules/'.bin/acorn').is_symlink())
  self.assertEqual(tracked_files(self.root),before);self.exercise()
 def test_tracked_missing_file_refused_before_api(self):
  (self.root/'tracked.txt').unlink();self.assertIn('tracked.txt',tracked_files(self.root));self.exercise(reject=True)
 def test_tracked_symlink_refused_before_api(self):
  path=self.root/'tracked.txt';path.unlink();path.symlink_to('space name.txt')
  self.assertTrue(path.is_file());self.exercise(reject=True)
  # A symlink actually recorded in the index must also remain in the inventory.
  self.git('add','--','tracked.txt');self.assertIn('tracked.txt',tracked_files(self.root));self.exercise(reject=True)
 def test_tracked_non_regular_entries_refused_before_api(self):
  path=self.root/'tracked.txt';path.unlink();path.mkdir();self.exercise(reject=True)
  path.rmdir();os.mkfifo(path);self.exercise(reject=True)
 def test_tracked_digest_mismatch_refused_before_api(self):
  self.exercise(reject=True,digest='0'*64)
  (self.root/'tracked.txt').write_bytes(b'changed source');self.exercise(reject=True)

class Diagnostics(unittest.TestCase):
 def setUp(self):
  self.fixture=fixtures.QualificationIsolation();self.calls=[]
  self.network=patch.object(socket,'socket',side_effect=AssertionError('NETWORK FORBIDDEN'));self.network.start();self.addCleanup(self.network.stop)
 def execute(self,mode='publication',env=None,mutate=None,source=None):
  context={**self.fixture.context(),'GITHUB_JOB':mode,**(env or {})}
  fake=self.fixture.fake(mode)
  def read(host,path,*args):
   self.calls.append((host,path));value=copy.deepcopy(fake(host,path,*args))
   return mutate(path,value) if mutate else value
  with patch.dict(os.environ,context,clear=True),patch.object(q,'source',side_effect=source),patch.object(q,'read',side_effect=read),contextlib.redirect_stdout(io.StringIO()):q.run(mode)
 def rejected(self,code,**kwargs):
  with self.assertRaises(q.Refused) as caught:self.execute(**kwargs)
  self.assertEqual(caught.exception.code,code)
  self.assertFalse(any(h=='api.cloudflare.com' for h,p in self.calls))
 def test_context_and_source_codes(self):
  self.rejected('CONTEXT',env={'GITHUB_ACTOR_ID':'0'})
  self.rejected('SOURCE',source=lambda:(q.stage('SOURCE'),q.need(False)))
 def test_run_fields(self):
  fields=['id','run_attempt','head_sha','event','actor','triggering_actor','repository','path']
  for field in fields:
   for value in (None,{},[],False,'secret-DO-NOT-LOG'):
    with self.subTest(field=field,value=type(value).__name__):
     self.calls=[]
     self.rejected('RUN_IDENTITY',mutate=lambda p,v:dict(v,**{field:value}) if p.endswith('/runs/12') else v)
 def test_run_state(self):
  for state in ('waiting','queued','completed','cancelled',None):
   self.calls=[];self.rejected('RUN_STATE',mutate=lambda p,v:dict(v,status=state) if p.endswith('/runs/12') else v)
 def test_main(self):
  self.rejected('MAIN_IDENTITY',mutate=lambda p,v:{} if p.endswith('/main') else v)
 def test_job_local_identity(self):self.rejected('JOB_IDENTITY',env={'GITHUB_JOB':'production'})
 def test_job_schema_and_ambiguity(self):
  cases=[{}, {'jobs':[],'total_count':0},{'jobs':[self.fixture.job('publication')]*2,'total_count':2},{'jobs':[self.fixture.job('publication')],'total_count':2}]
  for payload in cases:
   self.calls=[];self.rejected('JOB_IDENTITY',mutate=lambda p,v:payload if '/attempts/' in p else v)
 def test_job_detail_fields(self):
  for field in self.fixture.job('publication'):
   self.calls=[];self.rejected('JOB_IDENTITY',mutate=lambda p,v:{k:x for k,x in v.items() if k!=field} if '/actions/jobs/' in p else v)
 def test_job_detail_mismatch(self):
  for field,value in [('id',99),('run_id',99),('head_sha','b'*40),('name','production'),('status','completed'),('runner_id',99),('runner_name','other')]:
   self.calls=[];self.rejected('JOB_IDENTITY',mutate=lambda p,v:dict(v,**{field:value}) if '/actions/jobs/' in p else v)
 def test_approval(self):
  for payload in (None,{},[],[{}],[{'environments':[None]}],self.fixture.fake('publication')('x','/approvals')*2):
   self.calls=[];self.rejected('APPROVAL',mutate=lambda p,v:payload if p.endswith('/approvals') else v)
 def test_final_cancellation(self):
  count=0
  def mutate(p,v):
   nonlocal count
   if p.endswith('/runs/12'):
    count+=1
    if count==2:v['status']='completed'
   return v
  self.rejected('RUN_STATE',mutate=mutate)
 def test_metadata(self):
  with self.assertRaises(q.Refused) as caught:self.execute('production',env={'GSX_QUALIFICATION_READ_POLICY_VERIFIED':'false'})
  self.assertEqual(caught.exception.code,'METADATA')
 def test_http_failures(self):
  class Response:
   status=200;headers={}
   def __enter__(self):return self
   def __exit__(self,*args):pass
   def read(self,n):return b'{}'
  for failure in (TimeoutError('secret'),OSError('secret'),urllib.error.HTTPError('https://secret',403,'secret',{},None)):
   with patch.object(q.urllib.request,'build_opener') as transport:
    transport.return_value.open.side_effect=failure
    with self.assertRaises(q.Refused) as caught:q.read('api.github.com','/x','synthetic',{'/x'})
    self.assertEqual(caught.exception.code,'HTTP_RESPONSE')
  for raw,headers,status in [(b'{"a":1,"a":2}',{},200),(b'NaN',{},200),(b'invalid',{},200),(b' '*1048577,{},200),(b'{}',{'Link':'secret'},200),(b'{}',{},204)]:
   response=Response();response.headers=headers;response.status=status;response.read=lambda n:raw[:n]
   with patch.object(q.urllib.request,'build_opener') as transport:
    transport.return_value.open.return_value=response
    with self.assertRaises(q.Refused):q.read('api.github.com','/x','synthetic',{'/x'})
 def test_source_real_inventory(self):
  files=tracked_files(ROOT)
  inv={f:hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in files};digest=hashlib.sha256(json.dumps(inv,sort_keys=True,separators=(',',':')).encode()).hexdigest()
  env={'GSX_QUALIFICATION_REVIEWED_SHA':'a'*40,'GITHUB_SHA':'a'*40,'GITHUB_WORKFLOW_SHA':'a'*40,'GITHUB_WORKFLOW_REF':q.REPO+'/.github/workflows/qualification-only.yml@refs/heads/main','GSX_QUALIFICATION_SOURCE_DIGEST':digest}
  with patch.dict(os.environ,env,clear=True),patch.object(q.subprocess,'check_output',side_effect=lambda args,**kw:'a'*40 if args[1]=='rev-parse' else '\0'.join(files)+'\0'):
   q.source()
   for field in env:
    with patch.dict(os.environ,{field:'wrong'}):
     with self.assertRaises(q.Refused) as caught:q.source()
     self.assertEqual(caught.exception.code,'SOURCE')
 def test_cli_redaction(self):
  for code in sorted(q.CODES):
   q.stage(code)
   output=io.StringIO()
   with patch.object(q,'run',side_effect=q.Refused(code)),contextlib.redirect_stderr(output):self.assertEqual(q.main(),1)
   self.assertEqual(output.getvalue(),'READ-ONLY QUALIFICATION STOP code='+code+' stage='+code+'\n')
  q.stage('CONTEXT')
  for error in (RuntimeError('TOKEN-secret Authorization: Bearer secret https://sensitive'),q.Refused('secret')):
   output=io.StringIO()
   with patch.object(q,'run',side_effect=error),contextlib.redirect_stderr(output):self.assertEqual(q.main(),1)
   self.assertEqual(output.getvalue(),'READ-ONLY QUALIFICATION STOP code=INTERNAL stage=CONTEXT\n')
 def test_success_through_http_parser(self):
  class Response:
   status=200;headers={}
   def __init__(self,value):self.raw=json.dumps(value).encode()
   def __enter__(self):return self
   def __exit__(self,*args):pass
   def read(self,n):return self.raw[:n]
  for mode in ('production','publication'):
   calls=[];fake=self.fixture.fake(mode)
   def opened(request,timeout):
    from urllib.parse import urlsplit
    u=urlsplit(request.full_url);calls.append((u.hostname,u.path));self.assertEqual(request.get_method(),'GET')
    return Response(fake(u.hostname,u.path+('?' +u.query if u.query else '')))
   with patch.dict(os.environ,{**self.fixture.context(),'GITHUB_JOB':mode},clear=True),patch.object(q,'source'),patch.object(q.urllib.request,'build_opener') as transport,contextlib.redirect_stdout(io.StringIO()):
    transport.return_value.open.side_effect=opened;q.run(mode)
   self.assertEqual(sum(h=='api.cloudflare.com' for h,p in calls),3 if mode=='production' else 0)
 def test_workflow_isolation_contract(self):
  import yaml
  workflow=yaml.load((ROOT/'.github/workflows/qualification-only.yml').read_text(),Loader=yaml.BaseLoader)
  self.assertEqual(workflow['permissions'],{'contents':'read','actions':'read'})
  self.assertEqual(set(workflow['on']),{'workflow_dispatch'})
  for mode,job in workflow['jobs'].items():
   self.assertEqual(job['name'],mode);self.assertEqual(job['steps'][0]['with']['persist-credentials'],'false')
   self.assertEqual(job['steps'][1]['run'],'python3 -B -I ops/qualification/read_only.py '+mode)
   self.assertEqual(len(job['steps']),2)
  self.assertEqual(workflow['jobs']['publication']['steps'][1]['env'],{'GH_READ_TOKEN':'${{ github.token }}'})
 def test_strict_run_identity_initial_and_final(self):
  missing=object()
  expected={'id':12,'run_attempt':1,'workflow_id':396101973,'actor':125151458,'triggering_actor':125151458,'repository':1085006449}
  for mode in ('production','publication'):
   for nth in (1,2):
    for key,value in expected.items():
     for bad in (True,False,float(value),str(value),None,{},[],value+1,missing):
      with self.subTest(mode=mode,response=nth,key=key,bad=repr(bad)):
       count=0
       def mutate(path,payload):
        nonlocal count
        if path.endswith('/runs/12'):
         count+=1
         if count==nth:
          target=payload[key] if key in ('actor','triggering_actor','repository') else payload
          field='id' if key in ('actor','triggering_actor','repository') else key
          if bad is missing:target.pop(field)
          else:target[field]=bad
        return payload
       self.calls=[];self.rejected('RUN_IDENTITY',mode=mode,mutate=mutate)
 def test_complete_final_identity(self):
  for nth in (1,2):
   for key,bad in [('path','other.yml'),('event','push'),('head_sha','b'*40),('head_branch','other')]:
    for remove in (False,True):
     with self.subTest(response=nth,key=key,missing=remove):
      count=0
      def mutate(path,payload):
       nonlocal count
       if path.endswith('/runs/12'):
        count+=1
        if count==nth:
         if remove:payload.pop(key)
         else:payload[key]=bad
       return payload
      self.calls=[];self.rejected('RUN_IDENTITY',mode='production',mutate=mutate)
 def test_strict_approval_numeric_identity(self):
  for field,value in [('environment',25273664884),('owner',125151458)]:
   for bad in (True,False,float(value),str(value),None,{},[],value+1):
    with self.subTest(field=field,bad=repr(bad)):
     def mutate(path,payload):
      if path.endswith('/approvals'):
       target=payload[0]['environments'][0] if field=='environment' else payload[0]['user'];target['id']=bad
      return payload
     self.calls=[];self.rejected('APPROVAL',mode='production',mutate=mutate)
  def duplicate(path,payload):
   if path.endswith('/approvals'):payload[0]['environments']*=2
   return payload
  self.calls=[];self.rejected('APPROVAL',mode='production',mutate=duplicate)
 def test_strict_job_numeric_identity(self):
  for key,value in [('id',21),('run_id',12),('runner_id',3)]:
   for bad in (True,False,float(value),str(value),None,{},[],value+1):
    with self.subTest(key=key,bad=repr(bad)):
     def mutate(path,payload):
      if '/actions/jobs/' in path:payload[key]=bad
      return payload
     self.calls=[];self.rejected('JOB_IDENTITY',mode='production',mutate=mutate)
 def test_final_terminal_and_missing_state(self):
  for key,bad in [('status','completed'),('status','cancelled'),('status',None),('conclusion','success'),('conclusion','cancelled')]:
   count=0
   def mutate(path,payload):
    nonlocal count
    if path.endswith('/runs/12'):
     count+=1
     if count==2:payload[key]=bad
    return payload
   self.calls=[];self.rejected('RUN_STATE',mode='production',mutate=mutate)
