'use strict';
const fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto'),{EventEmitter}=require('node:events'),assert=require('node:assert/strict');
const {canonical}=require('../../ops/production/strict-json.cjs');
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const source=fs.readFileSync(require.resolve('../../ops/production/request-gate.cjs'),'utf8');
const validationDelays={'pipe-before-deadline':9999,'policy-before-deadline':9999,'pipe-exact-deadline':10000,'policy-exact-deadline':10000,'pipe-after-deadline':10001,'policy-after-deadline':10001,'delayed-invalid-json':10001,'pipe-run-expiry':1000,'policy-run-expiry':1000,'pipe-migration-cutoff':1000,'policy-migration-cutoff':1000};
const validationSuccess=['pipe-before-deadline','policy-before-deadline'];
const records=[];
const names=['valid','startup-delay','discovery-delay','payload-delay','request-construction-delay','exact-deadline','between-mutations','missing-fd','environment-deadline-only','replayed-pipe','regular-file','empty-pipe','oversized','invalid-json','duplicate-json','wrong-pid','wrong-executable','wrong-source','wrong-policy','wrong-manifest-digest','artifact-bool','check-float','run-string','wrong-attempt','missing-time','extended-deadline','future-origin','wrong-expiry','run-expired','migration-expired','cancel-before','cancel-during','redirect','response-loss','duplicate-operation','after-failure','wall-backward','mono-backward','clock-skew','provider-acceptance-late',...Object.keys(validationDelays)];
async function test(name){
 let wall=1800000000000,mono=100000000000n,sends=0,ends=0,offset=0,closed=0;const handlers={};
 const c={format:'gsx-request-policy-v1',account:'a'.repeat(32),worker:'goldsignalsx-worker',metadata:{},modules:{},settings:{},subdomain:{enabled:true,previews_enabled:true},schedules:[]};const policy=Buffer.from(canonical(c));
 const m={repository:'Skyeagle123/GoldSignalsX-worker',repository_id:1085006449,owner:'Skyeagle123',owner_id:125151458,workflow_id:3,run_id:7,attempt:1,environment_id:5,event:'workflow_dispatch',workflow_path:'.github/workflows/deploy-worker.yml',environment:'goldsignalsx-production',account:c.account,worker:c.worker,source_sha:'1'.repeat(40),source_tree:'2'.repeat(40),workflow_sha:'1'.repeat(40),source_digest:'1'.repeat(64),artifact_digest:'2'.repeat(64),toolchain_digest:'3'.repeat(64),workflow_sha256:'4'.repeat(64),policy_digest:hash(policy),created_at:new Date(wall-10000).toISOString().replace('.000Z','Z'),expires_at:new Date(wall+890000).toISOString().replace('.000Z','Z'),credential_mode:'steady',migration_t0:null,isolation_qualified:true};
 if(name.endsWith('-run-expiry')){m.created_at=new Date(wall-899000).toISOString().replace('.000Z','Z');m.expires_at=new Date(wall+1000).toISOString().replace('.000Z','Z');}
 if(name.endsWith('-migration-cutoff')){m.credential_mode='migration';m.migration_t0=new Date(wall-2399000).toISOString().replace('.000Z','Z');}
 const q={format:'gsx-execution-authority-v1',pid:123,manifest:m,manifest_digest:hash(canonical(m)),artifact_id:9,check_run_id:11,executable:'/verified/node',clock_wall_ms:wall,clock_mono_ns:String(mono),final_start_ns:String(mono),deadline_mono_ns:String(mono+10000000000n)};
 const advance=ms=>{wall+=ms;mono+=BigInt(ms)*1000000n;};
 if(name==='wrong-pid')q.pid++;
 if(name==='wrong-executable')q.executable='/other/node';
 if(name==='wrong-source')m.source_sha='a'.repeat(40);
 if(name==='wrong-policy')m.policy_digest='f'.repeat(64);
 if(name==='wrong-manifest-digest')q.manifest_digest='f'.repeat(64);
 if(name==='artifact-bool')q.artifact_id=true;
 if(name==='check-float')q.check_run_id=1.5;
 if(name==='run-string')m.run_id='7';
 if(name==='wrong-attempt')m.attempt=2;
 if(name==='missing-time')delete q.clock_wall_ms;
 if(name==='extended-deadline')q.deadline_mono_ns=String(mono+11000000000n);
 if(name==='future-origin')q.clock_mono_ns=String(mono+1n);
 if(name==='wrong-expiry')m.expires_at=m.created_at;
 if(name==='run-expired'){m.created_at=new Date(wall-900000).toISOString().replace('.000Z','Z');m.expires_at=new Date(wall).toISOString().replace('.000Z','Z');q.manifest_digest=hash(canonical(m));}
 if(name==='migration-expired'){m.credential_mode='migration';m.migration_t0=new Date(wall-2400000).toISOString().replace('.000Z','Z');q.manifest_digest=hash(canonical(m));}
 let policyReads=0;let bytes=Buffer.from(canonical(q));if(name==='empty-pipe')bytes=Buffer.alloc(0);if(name==='oversized')bytes=Buffer.alloc(4098,32);if(['invalid-json','delayed-invalid-json'].includes(name))bytes=Buffer.from('{');if(name==='duplicate-json')bytes=Buffer.from('{"pid":1,"pid":1}');
 if(name==='replayed-pipe')offset=bytes.length;
 const fakefs={readFileSync:(p,enc)=>{policyReads++;if(name.startsWith('policy-')&&policyReads===2)advance(validationDelays[name]);return enc?policy.toString():policy;},fstatSync:()=>({isFIFO:()=>name!=='regular-file'}),readSync:(fd,b,o,n)=>{if(offset===0&&(name.startsWith('pipe-')||name==='delayed-invalid-json'))advance(validationDelays[name]);const count=Math.min(n,bytes.length-offset);bytes.copy(b,o,offset,offset+count);offset+=count;return count;},closeSync:()=>closed++};
 const fakeprocess={env:{GSX_EXECUTION_AUTHORITY_FD:['missing-fd','environment-deadline-only'].includes(name)?undefined:'9',GSX_AUTHORIZATION_DEADLINE:'9999999999999',GSX_DEPLOY_CONTEXT_FILE:'policy',GSX_PRODUCTION_CF_API_TOKEN:'synthetic',GSX_PRODUCTION_CF_ACCOUNT_ID:c.account},pid:123,execPath:'/verified/node',hrtime:{bigint:()=>mono},exit(){},prependListener:(n,f)=>{handlers[n]=f;}};
 const https={request:(opts,cb)=>{sends++;if(name==='request-construction-delay')advance(10001);const req=new EventEmitter();req.destroy=e=>queueMicrotask(()=>req.emit('error',e));req.end=()=>{ends++;if(name==='cancel-during')handlers.SIGTERM();if(name==='provider-acceptance-late')advance(11000);queueMicrotask(()=>{if(name==='response-loss'){req.emit('error',Error('synthetic response loss'));return;}const res=new EventEmitter();res.statusCode=name==='redirect'?307:200;res.headers={};res.destroy=e=>res.emit('error',e);cb(res);const result={id:'12345678-1234-1234-1234-123456789abc',resources:{script:{etag:'etag'}}};res.emit('data',Buffer.from(JSON.stringify({success:true,errors:[],result})));res.emit('end');});};return req;}};
 const sandbox={require:n=>n==='node:https'?https:n==='node:fs'?fakefs:n==='node:crypto'?crypto:require('../../ops/production/strict-json.cjs'),process:fakeprocess,Buffer,Date:{now:()=>wall,parse:Date.parse},exports:{}};vm.runInNewContext(source,sandbox);
 const S='/accounts/'+c.account+'/workers/scripts/'+c.worker;
 const upload=()=>{const f=new FormData();f.set('metadata','{}');return sandbox.exports.fetch(S+'/versions',{method:'POST',body:f},undefined,undefined,DelayResponse);};
 class DelayResponse extends Response{async arrayBuffer(){const b=await super.arrayBuffer();if(name==='payload-delay')advance(11000);return b;}}
 if(name==='startup-delay')advance(11000);
 if(name==='exact-deadline')advance(10000);
 if(name==='wall-backward')wall--;
 if(name==='mono-backward')mono--;
 if(name==='clock-skew')wall+=2001;
 if(name==='cancel-before')handlers.SIGTERM();
 if(name==='discovery-delay'){await sandbox.exports.fetch(S+'/settings',{},undefined,undefined,Response);advance(11000);sends=0;ends=0;}
 let error=false;try{await upload();if(name==='between-mutations'){advance(10000);await sandbox.exports.fetch(S+'/deployments',{method:'POST',body:JSON.stringify({strategy:'percentage',versions:[{version_id:'12345678-1234-1234-1234-123456789abc',percentage:100}],annotations:{}})},undefined,undefined,Response);}if(name==='duplicate-operation')await upload();if(name==='after-failure'){await sandbox.exports.fetch('/forbidden',{method:'POST'},undefined,undefined,Response);}}catch{error=true;}
 const success=['valid','provider-acceptance-late',...validationSuccess].includes(name);assert.equal(error,!success,name);
 const one=['valid','between-mutations','cancel-during','redirect','response-loss','duplicate-operation','after-failure','provider-acceptance-late',...validationSuccess].includes(name);assert.equal(ends,one?1:0,name+' sent bytes');
 assert.equal(sends,one||name==='request-construction-delay'?1:0,name+' request constructions');
 records.push({name,refused:error,request_constructions:sends,body_sends:ends});
 if(error){const n=sends;await assert.rejects(upload());assert.equal(sends,n,name+' no request after failure');}
 if(name==='valid'){assert.equal(closed,1);assert.equal(fakeprocess.env.GSX_EXECUTION_AUTHORITY_FD,undefined);}
}
(async()=>{for(const name of names)await test(name);console.log(JSON.stringify({tests:names.length,result:'PASS',real_network_requests:0,scenarios:records}));})().catch(e=>{console.error(e);process.exitCode=1;});
