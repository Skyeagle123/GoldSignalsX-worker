'use strict';
// Exclusive network path for the pinned deployment profile. No redirect,
// proxy, retry Agent, OAuth, fallback endpoint or concurrent mutation.
const https=require('node:https'),fs=require('node:fs'),crypto=require('node:crypto');
const {parse,canonical}=require('./strict-json.cjs');
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
let context,chain=Promise.resolve(),failed=false,seen=new Set(),mode,version;
let stopped=false,activeRequest;const originalExit=process.exit.bind(process);
process.exit=(code)=>originalExit(stopped?143:failed?1:code);
process.prependListener('exit',()=>{if(stopped)process.exitCode=143;else if(failed||(!seen.has('upload-version')&&!seen.has('upload-legacy')))process.exitCode=1;});
for(const s of ['SIGINT','SIGTERM'])process.prependListener(s,()=>{stopped=true;failed=true;activeRequest?.destroy(Error('cancelled'));});
function fail(){failed=true;throw Error('GSX deployment request rejected; inspect state; no retry');}
function ctx(){if(context)return context;let p=process.env.GSX_DEPLOY_CONTEXT_FILE;if(!p)fail();context=parse(fs.readFileSync(p,'utf8'));
 if(Object.keys(context).sort().join(',')!=='account,format,metadata,modules,schedules,settings,subdomain,worker'||context.format!=='gsx-request-policy-v1'||!/^[a-f0-9]{32}$/.test(context.account)||context.worker!=='goldsignalsx-worker'||!context.modules||!context.metadata)fail();
 if(!process.env.GSX_PRODUCTION_CF_API_TOKEN?.trim()||process.env.GSX_PRODUCTION_CF_ACCOUNT_ID!==context.account)fail();return context;}
// Linux CLOCK_MONOTONIC is shared across exec by Python monotonic and pinned
// Node hrtime. Wall time is checked as well; neither clock may move backward.
let authority,lastWall,lastMono;
function freshness(){
 let wall=Date.now(),mono=process.hrtime.bigint();
 if(!authority){
  const raw=process.env.GSX_EXECUTION_AUTHORITY_FD;
  if(!/^[0-9]+$/.test(raw||'')||!Number.isSafeInteger(Number(raw))||Number(raw)<3)fail();
  const fd=Number(raw);let bytes;
  try{
   if(!fs.fstatSync(fd).isFIFO())fail();
   const b=Buffer.alloc(4097);let n=0,k;
   while(n<b.length&&(k=fs.readSync(fd,b,n,b.length-n,null))>0)n+=k;
   if(n===0||n>4096)fail();bytes=b.subarray(0,n);
  }finally{fs.closeSync(fd);delete process.env.GSX_EXECUTION_AUTHORITY_FD;}
  const a=parse(bytes.toString('utf8')),m=a.manifest;
  if(Object.keys(a).sort().join(',')!=='artifact_id,check_run_id,clock_mono_ns,clock_wall_ms,deadline_mono_ns,executable,final_start_ns,format,manifest,manifest_digest,pid')fail();
  const integer=x=>Number.isSafeInteger(x)&&x>0,hex=(x,n)=>typeof x==='string'&&new RegExp('^[a-f0-9]{'+n+'}$').test(x);
  if(a.format!=='gsx-execution-authority-v1'||a.pid!==process.pid||a.executable!==process.execPath||!integer(a.artifact_id)||!integer(a.check_run_id)||!m||typeof m!=='object')fail();
  for(const k of ['repository_id','owner_id','workflow_id','run_id','attempt','environment_id'])if(!integer(m[k]))fail();
  if(m.repository!=='Skyeagle123/GoldSignalsX-worker'||m.repository_id!==1085006449||m.owner!=='Skyeagle123'||m.owner_id!==125151458||m.attempt!==1||m.event!=='workflow_dispatch'||m.workflow_path!=='.github/workflows/deploy-worker.yml'||m.environment!=='goldsignalsx-production'||m.account!==ctx().account||m.worker!==ctx().worker)fail();
  for(const k of ['source_sha','source_tree','workflow_sha'])if(!hex(m[k],40))fail();
  for(const k of ['source_digest','artifact_digest','toolchain_digest','policy_digest','workflow_sha256'])if(!hex(m[k],64))fail();
  if(m.workflow_sha!==m.source_sha||!hex(a.manifest_digest,64)||sha(Buffer.from(canonical(m)))!==a.manifest_digest||sha(fs.readFileSync(process.env.GSX_DEPLOY_CONTEXT_FILE))!==m.policy_digest)fail();
  const timestamp=x=>typeof x==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(x)?Date.parse(x):NaN;
  const created=timestamp(m.created_at),expires=timestamp(m.expires_at);
  if(!Number.isSafeInteger(created)||expires!==created+900000)fail();
  let cutoff=expires;
  if(m.credential_mode==='migration'){
   const t0=timestamp(m.migration_t0);if(!Number.isSafeInteger(t0)||created<t0||created>=t0+2400000)fail();cutoff=Math.min(cutoff,t0+2400000);
  }else if(m.credential_mode!=='steady'||m.migration_t0!==null||m.isolation_qualified!==true)fail();
  if(!integer(a.clock_wall_ms)||typeof a.clock_mono_ns!=='string'||typeof a.deadline_mono_ns!=='string'||!/^\d+$/.test(a.clock_mono_ns)||!/^\d+$/.test(a.deadline_mono_ns))fail();
  if(typeof a.final_start_ns!=='string'||!/^\d+$/.test(a.final_start_ns))fail();
  const origin=BigInt(a.clock_mono_ns),deadline=BigInt(a.deadline_mono_ns),start=BigInt(a.final_start_ns);
  if(start<origin||start>mono||deadline>start+10000000000n)fail();
  if(origin<=0n||deadline<=origin||deadline>mono+10000000000n||a.clock_wall_ms<created||a.clock_wall_ms>=cutoff)fail();
  authority=Object.freeze({origin,deadline,wall:a.clock_wall_ms,created,cutoff});lastWall=a.clock_wall_ms;lastMono=origin;
 }
 // Handoff reads and validation consume the original budget. Sample again
 // after that work, immediately before the operative expiry check.
 wall=Date.now();mono=process.hrtime.bigint();
 const a=authority;
 if(stopped||failed||wall<lastWall||mono<lastMono||mono<a.origin||Math.abs(wall-(a.wall+Number(mono-a.origin)/1e6))>2000||wall<a.created||wall>=a.cutoff||mono>=a.deadline)fail();
 lastWall=wall;lastMono=mono;
}
function wire(path,method,body,headers,signal){return new Promise((resolve,reject)=>{
 if(stopped)return reject(Error('cancelled'));
 if(method!=='GET')freshness();
 const req=https.request({hostname:'api.cloudflare.com',port:443,path:'/client/v4'+path,method,headers,agent:false,timeout:12000,signal},res=>{
  const chunks=[];let length=0;res.on('data',b=>{length+=b.length;if(length>1048576)res.destroy(Error('response limit'));else chunks.push(b);});
  res.on('aborted',()=>reject(Error('response lost')));res.on('error',reject);res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,bytes:Buffer.concat(chunks)}));});
 activeRequest=req;req.on('close',()=>{if(activeRequest===req)activeRequest=undefined;});req.on('timeout',()=>req.destroy(Error('timeout')));req.on('error',reject);if(method!=='GET'){try{freshness();}catch(e){req.destroy(e);return reject(e);}}req.end(body);
 });}
async function operation(resource,init,query,signal,Response){
 const c=ctx(),S=`/accounts/${c.account}/workers/scripts/${c.worker}`,A=`/accounts/${c.account}`;
 if(failed||stopped||signal?.aborted)fail();
 if(!resource.startsWith('/')||resource.includes('..')||resource.includes('%')||resource.includes('?')||resource.includes('#'))fail();
 let method=init.method||'GET',id;
 if(method==='GET'){
  const allowed=[S,S+'/settings',S+'/deployments',S+'/subdomain',A+'/workers/subdomain',A+'/workers/scripts',A+'/workers/services/'+c.worker];
  if(!allowed.includes(resource)||query?.toString())fail();
 }else{
  id=({['POST '+S+'/versions']:'upload-version',['POST '+S+'/deployments']:'activate',['PATCH '+S+'/script-settings']:'settings',['PUT '+S]:'upload-legacy',['POST '+S+'/subdomain']:'subdomain',['PUT '+S+'/schedules']:'schedules'})[method+' '+resource];
  if(!id||seen.has(id))fail();seen.add(id);
  if(query?.toString()!==(id==='upload-legacy'?'excludeScript=true':undefined)&&query?.toString()!=='')fail();
  if(id.startsWith('upload-')){if(mode)fail();mode=id;
   if(!init.body||typeof init.body.entries!=='function')fail();let fields=new Set(),mods={};
   for(const [k,v]of init.body.entries()){if(fields.has(k))fail();fields.add(k);if(k==='metadata'){if(typeof v!=='string'||canonical(parse(v))!==canonical(c.metadata))fail();}else{if(typeof v.arrayBuffer!=='function')fail();mods[k]={sha256:sha(Buffer.from(await v.arrayBuffer())),type:v.type,name:v.name};}}
   if(!fields.has('metadata')||canonical(mods)!==canonical(c.modules))fail();
  }else{let b=parse(String(init.body));
   if(id==='activate'){if(mode!=='upload-version'||!version||canonical(b)!==canonical({strategy:'percentage',versions:[{version_id:version,percentage:100}],annotations:{}}))fail();}
   if((id==='upload-version'||id==='upload-legacy')&&data.result.startup_time_ms!==undefined&&(!Number.isFinite(data.result.startup_time_ms)||data.result.startup_time_ms<0))fail();
 if(id==='settings'&&(mode!=='upload-version'||canonical(b)!==canonical(c.settings)))fail();
   if(id==='subdomain'&&canonical(b)!==canonical(c.subdomain))fail();
   if(id==='schedules'&&canonical(b)!==canonical(c.schedules))fail();
  }
 }
 let body,headers={'Authorization':'Bearer '+process.env.GSX_PRODUCTION_CF_API_TOKEN,'User-Agent':'gsx-wrangler/3.114.17-retry-safe-v1'};
 if(init.body!==undefined){let encoded=new Response(init.body);body=Buffer.from(await encoded.arrayBuffer());let contentType=encoded.headers.get('content-type');if(contentType)headers['Content-Type']=contentType;
  if(typeof init.body==='string')headers['Content-Type']='application/json';headers['Content-Length']=String(body.length);}
 if(failed||stopped||signal?.aborted)fail();
 const r=await wire(resource+(query?'?'+query.toString():''),method,body,headers,signal);
 if(r.status<200||r.status>=300)fail();let data=parse(r.bytes.toString('utf8'),{integersOnly:false});
 if(data.success!==true||!Object.hasOwn(data,'result')||!Array.isArray(data.errors)||data.errors.length)fail();
 if(id==='upload-version'){if(!UUID.test(data.result?.id)||typeof data.result?.resources?.script?.etag!=='string'||!data.result.resources.script.etag)fail();version=data.result.id;}
 if(id==='activate'&&!UUID.test(data.result?.id))fail();
 if(id==='upload-legacy'&&!/^[a-f0-9]{32}$/.test(data.result?.deployment_id))fail();
 if((id==='upload-version'||id==='upload-legacy')&&data.result.startup_time_ms!==undefined&&(!Number.isFinite(data.result.startup_time_ms)||data.result.startup_time_ms<0))fail();
 if(id==='settings'&&(!data.result||typeof data.result!=='object'||Array.isArray(data.result)))fail();
 if(id==='subdomain'&&(data.result?.enabled!==c.subdomain.enabled||data.result?.previews_enabled!==c.subdomain.previews_enabled))fail();
 if(id==='schedules'&&(!data.result||!Array.isArray(data.result.schedules)||canonical(data.result.schedules.map(x=>({cron:x.cron})))!==canonical(c.schedules)))fail();
 if(failed||stopped)fail();return new Response(r.bytes,{status:r.status,headers:{'Content-Type':'application/json'}});
}
exports.fetch=function(resource,init={},query,signal,Response){const result=chain.then(()=>operation(resource,init,query,signal,Response));chain=result.then(()=>{},()=>{failed=true;});return result;};
