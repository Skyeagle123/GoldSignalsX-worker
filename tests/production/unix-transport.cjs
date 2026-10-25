// Test harness only. Production artifact never includes this module.
const https=require('node:https'),http=require('node:http'),net=require('node:net');
const fds=JSON.parse(process.env.GSX_FAKE_FDS);let next=0;function localRequest(options,cb){const agent=new http.Agent({keepAlive:false});agent.createConnection=()=>new net.Socket({fd:fds[next++],readable:true,writable:true});return http.request({...options,hostname:'local.invalid',port:80,agent},cb);}
https.request=function(options,callback){if(options.hostname!=='api.cloudflare.com')throw Error('external host');
 return localRequest({...options},callback);};
// The upstream-control CLI uses undici. Route its CF fetch through Unix HTTP,
// without changing its retry or deployment semantics.
const Module=require('node:module'),original=Module._load;
Module._load=function(name,...rest){const value=original.call(this,name,...rest);if(name.endsWith('request-gate.cjs')&&!value.__testWrapped){value.__testWrapped=true;const send=value.fetch;value.fetch=(r,i,q,a,R)=>{process.stderr.write('GSX-TEST-API '+JSON.stringify({resource:r,method:i?.method||'GET'})+'\n');if(i?.method==='POST'&&r.endsWith('/subdomain')&&process.env.GSX_TEST_ATTACK==='duplicate')return Promise.all([send(r,i,q,a,R),send(r,i,q,a,R)]).then(x=>x[0]);if(i?.method==='POST'&&r.endsWith('/versions')&&process.env.GSX_TEST_ATTACK==='fallback')return send(r.replace('/versions','/routes'),i,q,a,R);return send(r,i,q,a,R);};}if(name==='undici'&&!value.__gsxUnix){
 const fetch=value.fetch;Object.defineProperty(value,'__gsxUnix',{value:true});value.fetch=async(url,init={})=>{
  const u=new URL(url);if(u.hostname!=='api.cloudflare.com')throw Error('external host');
  const r=new value.Response(init.body);const b=init.body?Buffer.from(await r.arrayBuffer()):undefined;
  return new Promise((resolve,reject)=>{const req=localRequest({path:u.pathname+u.search,method:init.method||'GET',headers:{...init.headers,...(b?{'content-type':r.headers.get('content-type'),'content-length':b.length}:{})}},res=>{let chunks=[];res.on('data',x=>chunks.push(x));res.on('error',reject);res.on('end',()=>resolve(new value.Response(Buffer.concat(chunks),{status:res.statusCode,headers:res.headers})));});req.on('error',reject);req.end(b);});};
 }return value;};
// Transport-only upstream control: no retry, deployment or payload changes.
exports.fetch=async function(resource,init={},query,signal,Response){
 const encoded=new Response(init.body);const b=init.body!==undefined?Buffer.from(await encoded.arrayBuffer()):undefined;
 return new Promise((resolve,reject)=>{const req=localRequest({path:'/client/v4'+resource+(query?'?'+query.toString():''),method:init.method||'GET',headers:b?{'content-type':typeof init.body==='string'?'application/json':encoded.headers.get('content-type'),'content-length':b.length}:{}},res=>{
  let chunks=[];res.on('data',b=>chunks.push(b));res.on('aborted',()=>reject(new TypeError('response lost')));res.on('error',()=>reject(new TypeError('response lost')));res.on('end',()=>resolve(new Response(Buffer.concat(chunks),{status:res.statusCode,headers:res.headers})));});req.on('error',()=>reject(new TypeError('response lost')));req.end(b);});};
