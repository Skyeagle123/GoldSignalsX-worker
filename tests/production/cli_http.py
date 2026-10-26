"""Actual pinned CLI, real HTTP bytes over preconnected AF_UNIX sockets.
Child inherits seccomp: no IPv4/IPv6 sockets or external egress. No cloud calls.
The HTTP server parses received headers/body and records mutations independently.
"""
import selectors,concurrent.futures,hashlib,json,os,pathlib,socket,subprocess,tempfile,threading,time,unittest
ROOT=pathlib.Path(__file__).resolve().parents[2]
CLI=ROOT/'ops/production/toolchain/node_modules/wrangler/wrangler-dist/cli.js'
ACCOUNT='a'*32;V='12345678-1234-1234-1234-123456789abc';WORKER='goldsignalsx-worker'
def scenario(fault='',at='versions',legacy=False,control=False,timeout=8,attack='',floats=False):
 calls=[];lock=threading.Lock();fail=False;p=None
 with tempfile.TemporaryDirectory() as td:
  policy=json.loads(pathlib.Path('/tmp/gsx-policy.json').read_text())
  if legacy:policy['metadata']['migrations']={'new_tag':'v1-gold-feed','steps':[{'new_sqlite_classes':['GoldFeed']}]}
  policy.update(format='gsx-request-policy-v1',account=ACCOUNT,worker=WORKER,settings={'observability':{'enabled':True,'head_sampling_rate':1}},subdomain={'enabled':True,'previews_enabled':True},schedules=[{'cron':'*/5 * * * *'}])
  if attack=='tamper':policy['modules'][next(iter(policy['modules']))]='0'*64
  pp=pathlib.Path(td)/'policy.json';pp.write_text(json.dumps(policy))
  pairs=[socket.socketpair() for _ in range(32)]
  def serve(s):
   nonlocal fail
   f=None;entry=None
   try:
    s.settimeout(15);f=s.makefile('rb');line=f.readline().decode();method,url,_=line.strip().split(' ');headers={}
    while (line:=f.readline()) not in (b'\r\n',b''):
     k,v=line.decode().split(':',1);headers[k.lower()]=v.strip()
    body=f.read(int(headers.get('content-length',0)));url=url.split('?')[0];kind=url.split('/')[-1]
    if method=='PUT' and kind==WORKER:kind='legacy'
    status=200;result={};raw=None
    if fault=='cancel-before' and method=='GET':
     while p is None:time.sleep(0.001)
     p.terminate();return
    if method!='GET':
     with lock:
      entry={'method':method,'path':url,'kind':kind,'bytes':len(body),'body':body,'content_type':headers.get('content-type'),'after_failure':fail,'server_committed':not bool(kind==at and fault) or fault=='committed-loss'};calls.append(entry)
     if kind==at and fault:
      with lock:fail=True
      if fault=='cancel':
       while p is None:time.sleep(0.001)
       p.terminate();return
      if fault in ('disconnect','committed-loss'):return
      if fault=='timeout':time.sleep(13);return
      if fault in ('old-schedules','wrong-schedules'):
       result=[{'cron':'*/5 * * * *'}] if fault=='old-schedules' else {'schedules':[{'cron':'0 * * * *'}]}
       raw=json.dumps({'success':True,'errors':[],'result':result}).encode()
      elif fault in ('bad-id','missing-resources'):
       wrong={'id':'-'*36 if fault=='bad-id' else V,'resources':{'script':{'etag':'etag'}}}
       if kind=='legacy':wrong={'deployment_id':'a'*31}
       if fault=='missing-resources':wrong={'id':V}
       raw=json.dumps({'success':True,'errors':[],'result':wrong}).encode()
      elif fault in ('malformed','500-malformed'):raw=b'{';status=500 if fault=='500-malformed' else 200
      elif fault=='duplicate-json':raw=b'{"success":true,"success":true,"errors":[],"result":{}}'
      else:status=int(fault) if fault.isdigit() else 307;raw=b'{"success":false,"errors":[{"code":1000,"message":"injected"}],"result":null}'
    elif url.endswith('/scripts'):result=[{'id':WORKER,**({} if legacy else {'migration_tag':'v1-gold-feed'})}]
    elif url.endswith('/deployments'):result={'deployments':[]}
    elif url.endswith('/subdomain'):result={'subdomain':'samer-mourtada'} if url.endswith('/workers/subdomain') else {'enabled':False,'previews_enabled':False}
    elif '/services/' in url:result={'default_environment':{'script':{} if legacy else {'migration_tag':'v1-gold-feed'}}}
    else:result={} if legacy else {'migration_tag':'v1-gold-feed'}
    if method!='GET':
     if kind=='versions':result={'id':V,'resources':{'script':{'etag':'etag'}},'startup_time_ms':0.25 if floats else 1}
     elif kind=='deployments':result={'id':V}
     elif kind=='legacy':result={'deployment_id':V.replace('-',''),'startup_time_ms':0.25 if floats else 1}
     elif kind=='subdomain':result={'enabled':True,'previews_enabled':True}
     elif kind=='schedules':result={'schedules':[{'cron':'*/5 * * * *'}]}
    raw=raw if raw is not None else json.dumps({'success':True,'errors':[],'messages':[],'result':result}).encode()
    s.sendall(f'HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nLocation: https://example.invalid/\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n'.encode()+raw)
    # Keep the receive half open until Node finishes its request write.
    # Closing both halves immediately can cause an artificial late EPIPE.
    s.shutdown(socket.SHUT_WR);trailing=f.read()
    if entry is not None:entry['trailing_bytes']=len(trailing)
   except (OSError,ValueError):pass
   finally:
    if f is not None:f.close()
    s.close()
  selector=selectors.DefaultSelector();stop=threading.Event();workers=[]
  for a,b in pairs:selector.register(a,selectors.EVENT_READ)
  def dispatch():
   while not stop.is_set():
    for key,_ in selector.select(0.05):
     a=key.fileobj;selector.unregister(a)
     try:
      if not a.recv(1,socket.MSG_PEEK):a.close();continue
     except OSError:continue
     worker=threading.Thread(target=serve,args=(a,),daemon=True);workers.append(worker);worker.start()
  dispatcher=threading.Thread(target=dispatch,daemon=True);dispatcher.start()
  env={'PATH':os.environ['PATH'],'HOME':td,'CI':'true','WRANGLER_SEND_METRICS':'false','CLOUDFLARE_SEND_METRICS':'false','CLOUDFLARE_API_TOKEN':'synthetic-not-a-secret','CLOUDFLARE_ACCOUNT_ID':ACCOUNT,'GSX_PRODUCTION_CF_API_TOKEN':'synthetic-not-a-secret','GSX_PRODUCTION_CF_ACCOUNT_ID':ACCOUNT,'GSX_DEPLOY_CONTEXT_FILE':str(pp),'GSX_TEST_ATTACK':attack,'GSX_UPSTREAM_TRANSPORT':str(ROOT/'tests/production/unix-transport.cjs'),'GSX_FAKE_FDS':json.dumps([b.fileno() for a,b in pairs])}
  rd,wr=os.pipe();env['GSX_EXECUTION_AUTHORITY_FD']=str(rd)
  cmd=['/tmp/gsx-no-network','node','--require',str(ROOT/'tests/production/unix-transport.cjs'),*(['--require',str(ROOT/'ops/production/request-gate.cjs')] if not control else []),str(CLI)+('.upstream' if control else ''),'deploy','--config',str(ROOT/'wrangler.toml')]
  p=subprocess.Popen(cmd,cwd=ROOT,env=env,pass_fds=tuple(b.fileno() for a,b in pairs)+(rd,),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  from deadline_fixture import packet
  import shutil
  from authorization import canonical
  os.close(rd)
  try:
   if not control:os.write(wr,canonical(packet(p.pid,pp.read_bytes(),str(pathlib.Path(shutil.which('node')).resolve()))))
  finally:os.close(wr)
  for a,b in pairs:b.close()
  try:out=p.communicate(timeout=timeout)[0]
  except subprocess.TimeoutExpired:p.terminate();out=p.communicate(timeout=5)[0]
  stop.set();dispatcher.join(timeout=2);selector.close()
  for a,b in pairs:a.close()
  for worker in workers:worker.join(timeout=16)
  if os.environ.get('GSX_HTTP_TRACE_PATH'):
   trace={'fault':fault,'at':at,'legacy':legacy,'control':control,'attack':attack,'exit':p.returncode,'mutations':[{k:v for k,v in c.items() if k not in ('body','content_type')}|{'body_sha256':hashlib.sha256(c['body']).hexdigest()} for c in calls]}
   with open(os.environ['GSX_HTTP_TRACE_PATH'],'a') as f:f.write(json.dumps(trace,sort_keys=True)+'\n')
  return p.returncode,calls,out.decode(errors='replace')
def prepare_control():
 import urllib.request,tarfile,io
 original=CLI.with_name('cli.js.upstream');b=original.read_bytes()
 # Repeated calls see the original immutable backup, never re-patch a control.
 backup=CLI.with_name('cli.js.original')
 if backup.exists():b=backup.read_bytes()
 else:backup.write_bytes(b)
 assert hashlib.sha256(b).hexdigest()=='3ddc7ba0e400b3ad75e49a4bb18dc9b6b6727873920efe4797940b67bdbb790a'
 s=b.decode();a=s.index('async function performApiFetch(');z=s.index('__name(performApiFetch,',a)
 s=s[:a]+'''async function performApiFetch(resource, init2 = {}, queryParams, abortSignal) {
  return require(process.env.GSX_UPSTREAM_TRANSPORT).fetch(resource, init2, queryParams, abortSignal, import_undici3.Response);
}
'''+s[z:];original.write_text(s)
if __name__=='__main__':
 code,calls,out=scenario();print(out);print(code,[{k:v for k,v in c.items() if k!='body'} for c in calls]);raise SystemExit(code)
