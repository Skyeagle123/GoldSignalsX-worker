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
function wire(path,method,body,headers,signal){return new Promise((resolve,reject)=>{
 if(stopped)return reject(Error('cancelled'));
 const req=https.request({hostname:'api.cloudflare.com',port:443,path:'/client/v4'+path,method,headers,agent:false,timeout:12000,signal},res=>{
  const chunks=[];let length=0;res.on('data',b=>{length+=b.length;if(length>1048576)res.destroy(Error('response limit'));else chunks.push(b);});
  res.on('aborted',()=>reject(Error('response lost')));res.on('error',reject);res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,bytes:Buffer.concat(chunks)}));});
 activeRequest=req;req.on('close',()=>{if(activeRequest===req)activeRequest=undefined;});req.on('timeout',()=>req.destroy(Error('timeout')));req.on('error',reject);req.end(body);
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
