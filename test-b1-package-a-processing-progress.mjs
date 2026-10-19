import {test} from 'node:test';
import assert from 'node:assert/strict';
import {seedOfficial,fakeBucket} from './test-fixtures/b1-package-a.mjs';
import {prepareMarketEvidence} from './measurement-market-envelope.js';
import {ingestMarketEvidence} from './measurement-market-store.js';
import {processCapturedOutcome,runRecovery} from './measurement-recovery.js';
import {resolveProcessingSubject,completeEvaluationObligation,scanProgressIdentity} from './measurement-processing-binding.js';
import {createMeasurementD1Adapter,leaseJob} from './measurement-d1-adapter.js';
import {admitCorrection} from './measurement-corrections.js';
import {deliver} from './test-fixtures/b1-package-a.mjs';
import {lifecycle,confirmation} from './test-fixtures/b1-m2.mjs';
import measurement from './measurement-worker.js';
const q=(x,s,...a)=>x.raw.prepare(s).get(...a);
const checkpoint=x=>{const r=q(x,'SELECT * FROM pa_processing WHERE scope_id=?',x.scopeId);return r?JSON.parse(r.payload):null;};
const job=(x,id=x.scopeId)=>q(x,'SELECT * FROM pa_jobs WHERE id=?','projection:'+id);
async function setup(count=12){let clock=0;const x=await seedOfficial({clock:()=>clock});clock=x.at+10000;let serial=0;
 x.clock=()=>clock;x.advance=(n=2000)=>clock+=n;
 x.packet=async(index=serial++,price=4100)=>{const at=x.at+index;return (await prepareMarketEvidence({cohort:x.snapshot.scope.cohort,source:'mt5',session:'progress',from:at,to:at,observedAt:at,ticks:[{ts:at,price,measurement:{provider:'mt5',sessionId:'progress',sequence:index+100000,providerTimestamp:at}}]}))[0];};
 x.append=async()=>{const p=await x.packet();await ingestMarketEvidence(x.db,p,{now:clock});return p;};
 for(let i=0;i<count;i++)await x.append();
 x.scan=(options={})=>processCapturedOutcome(x.db,x.scopeId,{now:clock,rebuild:true,...options});
 x.prefix=async()=>{for(let i=0;i<32;i++){const r=await x.scan();if(checkpoint(x)?.scanProgress.records>0)return r;x.advance();}assert.fail('no sample prefix');};
 x.recover=()=>runRecovery(createMeasurementD1Adapter(x.binding),{now:clock});
 return x;
}
async function drain(x){for(let i=0;i<96;i++){x.advance();await x.recover();if(job(x,x.evaluationId)?.state==='DONE'&&job(x)?.state==='DONE')return;}assert.fail('processing did not catch up');}

test('Processing progress: exact Astra 12 records plus one append per eight-record opportunity catches up',async()=>{const x=await setup();try{
 const fronts=[];let total=12,complete=false;for(let pass=0;pass<32;pass++){const r=await x.scan(),p=checkpoint(x);fronts.push(p.scanProgress.records);assert(p.scanProgress.records<=(total));if(pass)assert(fronts[pass]>=fronts[pass-1]);if(r.status==='COMPLETED_AT_REVISION'){complete=true;assert.equal(p.scanProgress.records,total);break;}await x.append();total++;x.advance();}
 assert(complete,'catch-up must not require arrivals to stop');assert(fronts.some(n=>n>8));await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');console.log(JSON.stringify({astra12PlusOne:fronts}));
 }finally{x.close();}});

test('Processing progress: actual scheduled Worker catches backlog larger than one invocation while Queue arrivals continue',async()=>{const x=await setup(80),oldNow=Date.now,b=fakeBucket();Date.now=x.clock;try{
 const env={MEASUREMENT_ENVIRONMENT:'synthetic-b1-package-a',MEASUREMENT_EXPECTED_D1_ID:'LOCAL_ONLY_NO_CLOUD_ID',MEASUREMENT_DB:x.binding,MEASUREMENT_ARCHIVE:b.bucket,MEASUREMENT_ARCHIVE_NAMESPACE:'synthetic-b1-progress/',MEASUREMENT_QUEUE_NAME:'synthetic-b1-queue',MEASUREMENT_DLQ_NAME:'synthetic-b1-dlq',B1_MEASUREMENT_CONSUMER_ENABLED:'1',B1_ARCHIVE_WRITE_ENABLED:'0',B1_ARCHIVE_EVICTION_ENABLED:'0'};
 let total=80,previous=0,completed=false;const fronts=[];
 for(let pass=0;pass<128;pass++){x.advance();const tasks=[];await measurement.scheduled({},env,{waitUntil:p=>tasks.push(p)});await Promise.all(tasks);const p=checkpoint(x);assert(p);assert(p.scanProgress.records>=previous);previous=p.scanProgress.records;fronts.push(previous);if(pass===0)assert(previous<total,'backlog must exceed one scheduled invocation');assert(previous<=total);if(p.completeForCapturedSnapshot&&job(x,x.evaluationId)?.state==='DONE'){completed=true;assert.equal(previous,total);break;}
 const packet=await x.packet();let ack=0;await measurement.queue({queue:env.MEASUREMENT_QUEUE_NAME,messages:[{body:JSON.stringify({version:'measurement-market-queue-v1',...packet}),ack(){ack++;},retry(){assert.fail('valid arrival retried');}}]},env);assert.equal(ack,1);total++;
 }
 assert(completed);assert(fronts.length>1);assert.equal(q(x,'SELECT COUNT(*) n FROM pa_processing WHERE scope_id=?',x.evaluationId).n,0);console.log(JSON.stringify({scheduledContinuingFrontiers:fronts,arrivalsIncluded:total-80}));
 }finally{Date.now=oldNow;x.close();}});

test('Processing progress: overload preserves verified finite-snapshot work and truthful backlog',async()=>{const x=await setup(20);try{let prior=0;for(let n=0;n<8;n++){const r=await x.scan(),p=checkpoint(x);assert.equal(r.status,'BACKLOG_PENDING');assert.equal(p.completeForCapturedSnapshot,false);assert(p.scanProgress.records>=prior);prior=p.scanProgress.records;assert.notEqual(job(x,x.evaluationId).state,'DONE');for(let k=0;k<10;k++)await x.append();x.advance();}assert(checkpoint(x).scanProgress.state.generation>=8);}finally{x.close();}});

test('Processing progress: identical frontier delivery is idempotent; changed semantic content conflicts',async()=>{const x=await setup();try{await x.prefix();const before=checkpoint(x);const same=await x.packet(7);assert.equal((await ingestMarketEvidence(x.db,same,{now:x.clock()})).status,'DUPLICATE');assert.equal(checkpoint(x).scanProgress.prefixDigest,before.scanProgress.prefixDigest);await assert.rejects(ingestMarketEvidence(x.db,await x.packet(7,9999),{now:x.clock()}),/pa_market_sample_conflict/);x.advance();await assert.rejects(x.scan(),/pa_processing_market_pending|pa_fence_failed|pa_processing_lifecycle_pending/);assert.notEqual(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});

test('Processing progress: same-millisecond late record before frontier restarts rather than skipping',async()=>{const x=await setup();try{await x.prefix();const before=checkpoint(x);const packet=await x.packet(-1,9000);await ingestMarketEvidence(x.db,packet,{now:x.clock()});x.advance();await x.prefix();const p=checkpoint(x);assert.equal(p.completeForCapturedSnapshot,false);await drain(x);assert(checkpoint(x).folded.state.global.mfe>before.folded.state.global.mfe);}finally{x.close();}});

test('Processing progress: replayed checkpoint cannot manufacture a fold or cursor',async()=>{const x=await setup();try{await x.prefix();const p=checkpoint(x);p.folded.state.global.mfe=123456;x.raw.prepare('UPDATE pa_processing SET payload=? WHERE scope_id=?').run(JSON.stringify(p),x.scopeId);x.advance();await assert.rejects(x.scan(),/pa_processing_scan_integrity/);assert.notEqual(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});

for(const target of ['evaluation','signal','market'])test('Processing progress: '+target+' correction invalidates prefix',async()=>{const x=await setup();try{await x.prefix();const p=checkpoint(x),market=q(x,"SELECT id FROM pa_records WHERE kind='MARKET_EXPORT' ORDER BY id LIMIT 1").id;await admitCorrection(x.db,{id:'prefix-'+target,scopeId:target==='evaluation'?x.evaluationId:target==='signal'?x.scopeId:'market-scope:'+market,predecessor:target==='market'?market:'decision:'+x.evaluationId,reason:'prefix correction'},{now:x.clock()});x.advance();await assert.rejects(x.scan(),/pa_processing_lifecycle_pending|pa_fence_failed|pa_projection_fence/);assert.notEqual(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});

for(const change of ['confirmation','tp1','tp2','sl','expired'])test('Processing progress: lifecycle '+change+' restarts incompatible fold',async()=>{const x=await setup();try{await x.prefix();const prior=checkpoint(x);const packet=change==='confirmation'?await confirmation(x.scopeId,'progress-link'):await lifecycle(x.scopeId,change);await deliver(x.db,packet,x.clock());x.advance();await x.prefix();const p=checkpoint(x);assert(p.scanProgress.records<20);assert.notDeepEqual(p.scanProgress.identity,prior.scanProgress.identity);}finally{x.close();}});

for(const change of ['append','backfill','correction','quarantine','binding','epoch','lease','lifecycle'])test('Processing progress: '+change+' during scan cannot commit stale checkpoint/completion',async()=>{const x=await setup(2);try{let injected=false;
 await assert.rejects(x.scan({hook:async phase=>{if(phase!=='OUTCOME_PREPARED'||injected)return;injected=true;
 if(change==='append')await x.append();
 if(change==='backfill')await ingestMarketEvidence(x.db,await x.packet(-2),{now:x.clock()});
 if(change==='correction')await admitCorrection(x.db,{id:'during-scan',scopeId:x.evaluationId,predecessor:'decision:'+x.evaluationId,reason:'correction race'},{now:x.clock()});
 if(change==='quarantine')x.raw.prepare("UPDATE pa_scopes SET quarantine=1 WHERE id=(SELECT scope_id FROM pa_records WHERE kind='MARKET_EXPORT' LIMIT 1)").run();
 if(change==='binding')x.raw.prepare('DELETE FROM pa_bindings WHERE evaluation_id=?').run(x.evaluationId);
 if(change==='epoch')x.raw.exec("UPDATE pa_meta SET epoch='new-epoch'");
 if(change==='lease')x.raw.prepare('UPDATE pa_jobs SET generation=generation+1 WHERE id=?').run('projection:'+x.scopeId);
 if(change==='lifecycle')await deliver(x.db,await lifecycle(x.scopeId,'tp1'),x.clock());
 }}),/pa_fence_failed/);assert(injected);assert.equal(checkpoint(x),null);assert.notEqual(job(x,x.evaluationId).state,'DONE');
 }finally{x.close();}});

for(const flag of ['quarantine','conflict','recovery_required'])test('Processing progress: prefix market '+flag+' cannot be silently reused',async()=>{const x=await setup();try{await x.prefix();const p=checkpoint(x);x.raw.prepare('UPDATE pa_scopes SET '+flag+'=1 WHERE id=?').run('market-scope:'+q(x,"SELECT id FROM pa_records WHERE kind='MARKET_EXPORT' ORDER BY id LIMIT 1").id);x.advance();await assert.rejects(x.scan(),/pa_fence_failed/);assert.equal(checkpoint(x).scanProgress.prefixDigest,p.scanProgress.prefixDigest);}finally{x.close();}});

for(const change of ['epoch','binding','deletion','archive_boundary'])test('Processing progress: '+change+' invalidates prefix reuse',async()=>{const x=await setup(20);try{await x.prefix();const p=checkpoint(x);
 if(change==='epoch')x.raw.exec("UPDATE pa_meta SET epoch='epoch-replaced'");
 if(change==='binding')x.raw.prepare('UPDATE pa_scopes SET revision=revision+1 WHERE id=?').run(x.evaluationId);
 if(change==='deletion')x.raw.prepare('DELETE FROM pa_records WHERE id=?').run(q(x,"SELECT id FROM pa_records WHERE kind='MARKET_EXPORT' ORDER BY id LIMIT 1").id);
 if(change==='archive_boundary')x.raw.exec('UPDATE pa_meta SET catalogue_generation=catalogue_generation+1');
 x.advance();await x.scan();assert(checkpoint(x).scanProgress.records<20);
 }finally{x.close();}});

test('Processing progress: checkpoint response loss and new adapter resume exact immutable prefix once',async()=>{const x=await setup(20);try{let lost=false;const db=createMeasurementD1Adapter({...x.binding,async batch(s){const r=await x.binding.batch(s);if(!lost&&s.some(q=>q.sql.includes('INSERT INTO pa_processing'))){lost=true;throw Error('checkpoint_response_loss');}return r;}});await assert.rejects(processCapturedOutcome(db,x.scopeId,{now:x.clock(),rebuild:true}),/checkpoint_response_loss/);assert(checkpoint(x).scanProgress.records<20);await x.append();x.advance();await x.scan();assert(checkpoint(x).scanProgress.records<=20);await drain(x);assert.equal(checkpoint(x).scanProgress.records,21);}finally{x.close();}});

test('Processing progress: two workers and stale frontier replay have one fenced winner',async()=>{const x=await setup(20);try{const lease=await leaseJob(x.db,'projection:'+x.scopeId,'shared',{now:x.clock()});let selected=0,release;const ready=new Promise(r=>release=r);const hook=async phase=>{if(phase==='OUTCOME_PREPARED'){if(++selected===2)release();await ready;}};const result=await Promise.allSettled([x.db,createMeasurementD1Adapter(x.binding)].map(db=>processCapturedOutcome(db,x.scopeId,{now:x.clock(),lease,rebuild:true,hook})));assert.equal(result.filter(r=>r.status==='fulfilled').length,1);assert(checkpoint(x).scanProgress.records<20);const old=checkpoint(x);await x.append();x.advance();await x.scan();assert(checkpoint(x).scanProgress.records<=20);await assert.rejects(processCapturedOutcome(x.db,x.scopeId,{now:x.clock(),lease,rebuild:true}),/pa_fence_failed/);assert(checkpoint(x).scanProgress.records<=20);assert(checkpoint(x).scanProgress.state.generation>old.scanProgress.state.generation);}finally{x.close();}});

test('Processing progress: append after canonical result prevents stale evaluation acknowledgement',async()=>{const x=await setup(2);try{await x.scan();const resolution=await resolveProcessingSubject(x.db,x.evaluationId,{now:x.clock()}),lease=await leaseJob(x.db,'projection:'+x.evaluationId,'evaluation',{now:x.clock()});await assert.rejects(completeEvaluationObligation(x.db,resolution,lease,{now:x.clock(),hook:()=>x.append()}),/pa_fence_failed/);assert.notEqual(job(x,x.evaluationId).state,'DONE');x.advance(60001);await drain(x);assert.equal(checkpoint(x).scanProgress.records,3);}finally{x.close();}});

test('Processing progress: compatibility changes only market revision, never pending/conflict or ownership',()=>{const a={context:{wire:JSON.stringify(['epoch',[],[],[1,0,0],0,[]])},projectionDigest:'projection'};const b=structuredClone(a);b.context.wire=JSON.stringify(['epoch',[],[],[2,0,0],0,[]]);assert.deepEqual(scanProgressIdentity(a),scanProgressIdentity(b));for(const index of [1,2]){const c=structuredClone(b),parts=JSON.parse(c.context.wire);parts[3][index]=1;c.context.wire=JSON.stringify(parts);assert.notDeepEqual(scanProgressIdentity(a),scanProgressIdentity(c));}});
