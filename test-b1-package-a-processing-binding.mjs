import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {seedOfficial,packageDatabase,deliver,fakeBucket} from './test-fixtures/b1-package-a.mjs';
import {lifecycle,confirmation} from './test-fixtures/b1-m2.mjs';
import {loadM1Workers} from './test-fixtures/b1-m1.mjs';
import {syntheticWorkload} from './test-fixtures/b1-processing-producer.mjs';
import measurement from './measurement-worker.js';
import {runRecovery,processCapturedOutcome} from './measurement-recovery.js';
import {resolveProcessingSubject,completeEvaluationObligation} from './measurement-processing-binding.js';
import {createMeasurementD1Adapter,leaseJob} from './measurement-d1-adapter.js';
import {rebuildD1Projection} from './measurement-independent-store.js';
import {createPrivateArchiveAdapter,publishArchive} from './measurement-archive.js';
import {reconcilePublicationEvictionAuthority} from './measurement-eviction-reconciliation.js';
import {verifyArchiveParity,evictPublication} from './measurement-independent-retention.js';
import {createIndependentReporting} from './measurement-reporting.js';
import {admitCorrection} from './measurement-corrections.js';
const sql=(x,q,...args)=>x.raw.prepare(q).get(...args);
const job=(x,id)=>sql(x,'SELECT * FROM pa_jobs WHERE id=?','projection:'+id);
async function drain(x,hook=null){for(let i=0;i<12;i++)await runRecovery(createMeasurementD1Adapter(x.binding),{now:x.at+i*2000,hook});}
async function selected(x){const r=await resolveProcessingSubject(x.db,x.evaluationId,{now:x.at});assert.equal(r.role,'OFFICIAL_EVALUATION');return r;}
async function canonical(x){return processCapturedOutcome(x.db,x.scopeId,{now:x.at,rebuild:true});}

test('Processing binding: genuine 1 cycle / 7 census / 1 Official / 5 links through Queue and scheduled Worker',async()=>{
 let clock=Date.now();const saved=Date.now;Date.now=()=>clock;const x=await packageDatabase({clock:()=>clock}),bucket=fakeBucket();try{
 const w=await syntheticWorkload((await loadM1Workers()).after,{runId:'native-alias-schedule',tick:1,at:clock});
 const env={MEASUREMENT_ENVIRONMENT:'synthetic-b1-package-a',MEASUREMENT_EXPECTED_D1_ID:'LOCAL_ONLY_NO_CLOUD_ID',MEASUREMENT_DB:x.binding,MEASUREMENT_ARCHIVE:bucket.bucket,MEASUREMENT_ARCHIVE_NAMESPACE:'synthetic-b1-processing/',MEASUREMENT_QUEUE_NAME:'synthetic-b1-queue',MEASUREMENT_DLQ_NAME:'synthetic-b1-dlq',B1_MEASUREMENT_CONSUMER_ENABLED:'1',B1_ARCHIVE_WRITE_ENABLED:'0',B1_ARCHIVE_EVICTION_ENABLED:'0'};
 const send=async ms=>measurement.queue({queue:env.MEASUREMENT_QUEUE_NAME,messages:ms.map(({body})=>({body,ack(){},retry(){throw Error('unexpected_retry');}}))},env);
 await send(w.messages);
 const completed=new Set();
 for(let n=0;n<12;n++){clock+=120000;const tasks=[];await measurement.scheduled({},env,{waitUntil:p=>tasks.push(p)});await Promise.all(tasks);for(const j of x.raw.prepare("SELECT scope_id FROM pa_jobs WHERE class='processing' AND state='DONE'").all())completed.add(j.scope_id);}
 assert.equal(sql(x,"SELECT COUNT(*) n FROM pa_events WHERE state='COMMITTED'").n,14);
 const e='native-alias-schedule:1:3',s='native-alias-schedule:1:30m:buy';assert.notEqual(e,s);
 assert(completed.has(e));assert(completed.has(s));assert(!job(x,e)||job(x,e).state==='DONE');assert(!job(x,s)||job(x,s).state==='DONE');
 assert.equal(await rebuildD1Projection(x.db,e,{now:clock}),null);
 assert.equal(sql(x,"SELECT COUNT(*) n FROM pa_records WHERE kind='CENSUS'").n,7);
 assert.equal(sql(x,'SELECT COUNT(*) n FROM pa_processing WHERE scope_id=?',e).n,0);
 const p=JSON.parse(sql(x,'SELECT payload FROM pa_processing WHERE scope_id=?',s).payload);assert.equal(p.subjectId,s);assert.equal(p.evaluationId,e);assert(p.completeForCapturedSnapshot);assert(p.processingAuthority);
 assert.equal(sql(x,"SELECT COUNT(*) n FROM pa_jobs WHERE class='processing' AND state!='DONE'").n,0);
 const report=await createIndependentReporting(x.db,{tokenKey:'x'.repeat(32),clock:()=>clock});const page=await report.query({view:'CENSUS',cohort:w.cohort,definition:'engine-v1',from:0,to:clock,asOf:clock});assert.equal(page.censusDenominator,7);assert.equal(page.denominatorScope,'PAGE_ONLY');
 console.log(JSON.stringify({genuineProducer:{events:14,census:7,evaluation:e,signal:s,processingJobs:'DONE',evaluationLifecycle:null,officialResultSubject:p.subjectId}}));
 }finally{x.close();Date.now=saved;}
});

test('Processing binding: authenticated roles and no duplicate Official subject',async()=>{const x=await seedOfficial();try{assert.notEqual(x.evaluationId,x.scopeId);assert.equal((await selected(x)).subjectId,x.scopeId);assert.equal((await resolveProcessingSubject(x.db,x.scopeId,{now:x.at})).role,'OFFICIAL_SIGNAL');await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');assert.equal(job(x,x.scopeId).state,'DONE');assert.equal(sql(x,'SELECT COUNT(*) n FROM pa_processing WHERE scope_id=?',x.evaluationId).n,0);assert.equal(sql(x,'SELECT COUNT(*) n FROM pa_processing WHERE scope_id=?',x.scopeId).n,1);}finally{x.close();}});
for(const missing of ['binding','decision','creation'])test('Processing binding: missing '+missing+' stays PENDING and never DONE',async()=>{const x=await seedOfficial();try{
 if(missing==='binding')x.raw.prepare('DELETE FROM pa_bindings WHERE evaluation_id=?').run(x.evaluationId);
 if(missing==='decision')x.raw.prepare('DELETE FROM pa_records WHERE id=?').run('decision:'+x.evaluationId);
 if(missing==='creation')x.raw.prepare('DELETE FROM pa_records WHERE id=?').run('event:'+x.creation.envelope.eventId);
 await drain(x);assert.equal(job(x,x.evaluationId).state,'PENDING');assert.equal(sql(x,'SELECT COUNT(*) n FROM pa_processing WHERE scope_id=?',x.evaluationId).n,0);
 }finally{x.close();}});

test('Processing binding: direct returned PENDING is not scheduler DONE',async()=>{const x=await seedOfficial();try{x.raw.prepare('DELETE FROM pa_records WHERE id=?').run('decision:'+x.evaluationId);const r=await runRecovery(x.db,{now:x.at});assert(r.outcomes.some(o=>o.status==='PENDING'));assert.equal(job(x,x.evaluationId).state,'PENDING');}finally{x.close();}});
for(const event of ['tp1','tp2','sl','expired'])test('Processing binding: '+event+' invalidates and requeues evaluation and signal',async()=>{const x=await seedOfficial();try{await drain(x);const prior=sql(x,'SELECT payload FROM pa_processing WHERE scope_id=?',x.scopeId).payload;await deliver(x.db,await lifecycle(x.scopeId,event),x.at+30000);assert.equal(job(x,x.evaluationId).state,'PENDING');assert.equal(job(x,x.scopeId).state,'PENDING');x.at+=30000;await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');assert.notEqual(sql(x,'SELECT payload FROM pa_processing WHERE scope_id=?',x.scopeId).payload,prior);}finally{x.close();}});

test('Processing binding: confirmation requeues both scopes, repeated delivery preserves census',async()=>{const x=await seedOfficial();try{await drain(x);const p=await confirmation(x.scopeId,'confirmation-child');await deliver(x.db,p,x.at+30000);assert.equal(job(x,x.evaluationId).state,'PENDING');x.at+=30000;await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');await deliver(x.db,p,x.at+1);assert.equal(sql(x,"SELECT COUNT(*) n FROM pa_records WHERE kind='CENSUS'").n,7);}finally{x.close();}});

for(const target of ['evaluation','signal'])test('Processing binding: '+target+' correction admission invalidates linked work',async()=>{const x=await seedOfficial();try{await drain(x);await admitCorrection(x.db,{id:'correction-'+target,scopeId:target==='evaluation'?x.evaluationId:x.scopeId,predecessor:'decision:'+x.evaluationId,reason:'captured correction'},{now:x.at});assert.equal(job(x,x.evaluationId).state,'PENDING');assert.equal(job(x,x.scopeId).state,'PENDING');await assert.rejects(canonical(x),/pa_processing_lifecycle_pending|pa_fence_failed|pa_projection_fence/);assert.equal(job(x,x.evaluationId).state,'PENDING');}finally{x.close();}});

test('Processing binding: genuine pending obligation blocks protected eviction',async()=>{const x=await seedOfficial(),b=fakeBucket(),archive=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-processing-pending/'});try{const p=await publishArchive(x.db,archive,x.scopeId,{now:x.at});await verifyArchiveParity(x.db,archive,p.id);const count=sql(x,'SELECT COUNT(*) n FROM pa_records').n;await assert.rejects(evictPublication(x.db,archive,p.id,{now:x.at}),/pa_fence_failed/);assert.equal(sql(x,'SELECT COUNT(*) n FROM pa_records').n,count);}finally{x.close();}});

test('Processing binding: completed obligation permits fresh current-head publication and eviction',async()=>{const x=await seedOfficial(),b=fakeBucket(),archive=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-processing-complete/'});try{
 const {recoverRootCheckpoint}=await import('./measurement-archive.js');
 const handlers={archive:async({row,now,lease})=>row.id.startsWith('root-checkpoint:')?recoverRootCheckpoint(x.db,archive,row.event_id,{now,lease}):publishArchive(x.db,archive,row.scope_id,{now,domain:row.event_id==='CENSUS'?'CENSUS':'EVIDENCE'})};
 for(let i=0;i<12;i++)await runRecovery(x.db,{now:x.at+i*2000,handlers});
 assert.equal(job(x,x.evaluationId).state,'DONE');
 const p=await publishArchive(x.db,archive,x.scopeId,{now:x.at+30000});await verifyArchiveParity(x.db,archive,p.id);
 await runRecovery(x.db,{now:x.at+30001,handlers});
 assert.equal(sql(x,'SELECT root_key FROM pa_meta').root_key,p.root_key);
 assert.equal((await evictPublication(x.db,archive,p.id,{now:x.at+30001})).status,'EVICTED');
 }finally{x.close();}});

for(const change of ['evaluation_revision','signal_revision','market','pending','correction','conflict','quarantine','epoch','creation_removed','restore','lease','binding_membership'])test('Processing binding: fresh completion rejects concurrent '+change,async()=>{const x=await seedOfficial();try{const r=await selected(x);await canonical(x);const lease=await leaseJob(x.db,'projection:'+x.evaluationId,'evaluation-worker',{now:x.at});assert(lease);let changed=false;
 await assert.rejects(completeEvaluationObligation(x.db,r,lease,{now:x.at,hook:async()=>{changed=true;
 if(change==='evaluation_revision'||change==='signal_revision')x.raw.prepare('UPDATE pa_scopes SET revision=revision+1 WHERE id=?').run(change==='evaluation_revision'?x.evaluationId:x.scopeId);
 if(change==='market')x.raw.prepare('INSERT INTO pa_cohort_state(cohort,revision) VALUES(?,999) ON CONFLICT(cohort) DO UPDATE SET revision=revision+1').run(r.scope.cohort);
 if(['pending','correction','conflict','quarantine'].includes(change))x.raw.prepare('UPDATE pa_scopes SET '+(change==='correction'?'corrections':change)+'=1 WHERE id=?').run(x.evaluationId);
 if(change==='epoch')x.raw.exec("UPDATE pa_meta SET epoch='replaced-epoch'");
 if(change==='creation_removed')x.raw.prepare('DELETE FROM pa_records WHERE id=?').run(r.creation.id);
 if(change==='restore')x.raw.exec("UPDATE pa_scopes SET recovery_required=1");
 if(change==='lease')x.raw.prepare("UPDATE pa_jobs SET generation=generation+1,owner='replacement' WHERE id=?").run(lease.id);
 if(change==='binding_membership')x.raw.prepare('DELETE FROM pa_bindings WHERE evaluation_id=?').run(x.evaluationId);
 }}),/pa_fence_failed/);assert(changed);assert.notEqual(job(x,x.evaluationId).state,'DONE');assert(sql(x,'SELECT COUNT(*) n FROM pa_records').n>0);
 }finally{x.close();}});

test('Processing binding: two workers yield one completion, loser cannot reuse lease',async()=>{const x=await seedOfficial();try{const r=await selected(x);await canonical(x);const lease=await leaseJob(x.db,'projection:'+x.evaluationId,'A',{now:x.at});const results=await Promise.allSettled([x.db,createMeasurementD1Adapter(x.binding)].map(db=>completeEvaluationObligation(db,r,lease,{now:x.at})));assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});
for(const phase of ['PROCESSING_BINDING_RESOLVED','PROJECTION_PREPARED','OUTCOME_PREPARED','EVALUATION_COMPLETION_PREPARED'])test('Processing binding: crash at '+phase+' safely resumes',async()=>{const x=await seedOfficial();try{let crashed=false;for(let i=0;i<4&&!crashed;i++)await runRecovery(x.db,{now:x.at+i*2000,hook:async p=>{if(p===phase&&!crashed){crashed=true;throw Error('simulated_crash');}}});assert(crashed);x.at+=60001;await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');assert.equal(job(x,x.scopeId).state,'DONE');}finally{x.close();}});

test('Processing binding: completion response loss reconciles as DONE after adapter restart',async()=>{const x=await seedOfficial();try{const r=await selected(x);await canonical(x);const lease=await leaseJob(x.db,'projection:'+x.evaluationId,'A',{now:x.at});let lost=false;const db=createMeasurementD1Adapter({...x.binding,async batch(s){const out=await x.binding.batch(s);if(s.some(s=>s.sql.includes("UPDATE pa_jobs SET state='DONE'"))){lost=true;throw Error('response_loss');}return out;}});await assert.rejects(completeEvaluationObligation(db,r,lease,{now:x.at}),/response_loss/);assert(lost);assert.equal(job(x,x.evaluationId).state,'DONE');await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});

test('Processing binding: no new test marks processing jobs DONE with direct SQL',async()=>{const source=await fs.readFile(new URL(import.meta.url),'utf8');assert(!/raw\.(?:exec|prepare)\([^\n]*UPDATE pa_jobs SET state='DONE'/.test(source));});

test('Processing binding: unrelated catalogue advancement does not invalidate completed canonical work',async()=>{const x=await seedOfficial();try{await canonical(x);x.raw.exec("UPDATE pa_meta SET root_key='unrelated-root',catalogue_generation=catalogue_generation+1");const r=await selected(x),lease=await leaseJob(x.db,'projection:'+x.evaluationId,'A',{now:x.at});assert.equal((await completeEvaluationObligation(x.db,r,lease,{now:x.at})).status,'COMPLETED_AT_REVISION');assert.equal(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});

for(const order of ['census-first','creation-first','creation-late'])test('Processing binding: real producer ordering '+order,async()=>{const x=await packageDatabase();try{const at=Date.now(),w=await syntheticWorkload((await loadM1Workers()).after,{runId:'ordering-'+order,tick:1,at});const cycle=w.packets.find(p=>p.envelope.kind==='DECISION_CYCLE'),creation=w.packets.find(p=>p.envelope.kind==='OFFICIAL_CREATION'),census=w.packets.filter(p=>p.envelope.kind==='EVALUATION_CENSUS');const e=creation.envelope.payload.evaluationId,s=creation.envelope.payload.officialSignalId;
 if(order==='census-first')for(const p of census)await deliver(x.db,p,at);
 if(order==='creation-first')await deliver(x.db,creation,at);
 await deliver(x.db,cycle,at);
 if(order==='creation-late'){for(let i=0;i<3;i++)await runRecovery(x.db,{now:at+i*2000});assert.equal(job(x,e).state,'PENDING');}
 if(order!=='creation-first')await deliver(x.db,creation,at+10000);
 for(const p of census)await deliver(x.db,p,at+10000);
 for(let i=0;i<12;i++)await runRecovery(x.db,{now:at+20000+i*2000});
 assert.equal(job(x,e).state,'DONE');assert.equal(job(x,s).state,'DONE');assert.equal(sql(x,"SELECT COUNT(*) n FROM pa_records WHERE kind='CENSUS'").n,7);
 }finally{x.close();}});

for(const phase of ['PROCESSING_BINDING_RESOLVED','PROJECTION_PREPARED','OUTCOME_PREPARED'])for(const change of ['source','epoch','quarantine','ingress'])test('Processing binding: '+change+' race at '+phase+' cannot publish current outcome',async()=>{const x=await seedOfficial();try{let changed=false;await assert.rejects(processCapturedOutcome(x.db,x.scopeId,{now:x.at,rebuild:true,hook:async p=>{if(p!==phase||changed)return;changed=true;if(change==='source')x.raw.prepare('UPDATE pa_scopes SET revision=revision+1 WHERE id=?').run(x.evaluationId);if(change==='epoch')x.raw.exec("UPDATE pa_meta SET epoch='racing-epoch'");if(change==='quarantine')x.raw.prepare('UPDATE pa_scopes SET quarantine=1 WHERE id=?').run(x.evaluationId);if(change==='ingress')x.raw.prepare('UPDATE pa_scopes SET pending=1 WHERE id=?').run(x.evaluationId);}}),/pa_fence_failed|pa_processing_lifecycle_pending|pa_projection_fence/);assert(changed);assert.equal(sql(x,'SELECT COUNT(*) n FROM pa_processing').n,0);assert.notEqual(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});

test('Processing binding: other signal bindings cannot choose an arbitrary originating decision',async()=>{const x=await seedOfficial();try{const b=sql(x,'SELECT * FROM pa_bindings WHERE evaluation_id=?',x.evaluationId);x.raw.prepare("INSERT INTO pa_scopes(id,cohort,kind) VALUES('aaa-other',?,'CANDIDATE')").run(b.cohort);x.raw.prepare("INSERT INTO pa_bindings VALUES('aaa-other','not-the-origin',?,?,?,NULL)").run(x.scopeId,b.first_event,b.cohort);const r=await resolveProcessingSubject(x.db,x.scopeId,{now:x.at});assert.equal(r.binding.evaluation_id,x.evaluationId);await canonical(x);assert.equal(JSON.parse(sql(x,'SELECT payload FROM pa_processing WHERE scope_id=?',x.scopeId).payload).evaluationId,x.evaluationId);}finally{x.close();}});

test('Processing binding: contradictory binding digest fails closed',async()=>{const x=await seedOfficial();try{const b=sql(x,'SELECT * FROM pa_bindings WHERE evaluation_id=?',x.evaluationId);x.raw.prepare('DELETE FROM pa_bindings WHERE evaluation_id=?').run(x.evaluationId);x.raw.prepare('INSERT INTO pa_bindings VALUES(?,?,?,?,?,?)').run(b.evaluation_id,'contradiction',b.signal_id,b.first_event,b.cohort,b.cycle_key);await assert.rejects(canonical(x),/pa_processing_binding_conflict/);assert.equal(sql(x,'SELECT COUNT(*) n FROM pa_processing').n,0);}finally{x.close();}});

test('Processing binding: failed persistence and contradictory terminals retain lifecycle reducer truth',async()=>{const x=await seedOfficial();try{await deliver(x.db,await lifecycle(x.scopeId,'sl',{persisted:false}),x.at);await canonical(x);assert.equal(JSON.parse(sql(x,'SELECT payload FROM pa_projections WHERE scope_id=?',x.scopeId).payload).status,'active');await deliver(x.db,await lifecycle(x.scopeId,'tp2'),x.at+60000);await deliver(x.db,await lifecycle(x.scopeId,'sl',{at:x.at+1}),x.at+60000);await assert.rejects(processCapturedOutcome(x.db,x.scopeId,{now:x.at+60000,rebuild:true}),/pa_processing_lifecycle_pending/);assert.notEqual(job(x,x.evaluationId).state,'DONE');}finally{x.close();}});

test('Processing binding: compacted DONE signal job does not strand a durable completion checkpoint',async()=>{const x=await seedOfficial();try{await canonical(x);x.raw.prepare("DELETE FROM pa_jobs WHERE id=? AND state='DONE'").run('projection:'+x.scopeId);const r=await selected(x),lease=await leaseJob(x.db,'projection:'+x.evaluationId,'A',{now:x.at});assert.equal((await completeEvaluationObligation(x.db,r,lease,{now:x.at})).status,'COMPLETED_AT_REVISION');}finally{x.close();}});

test('Processing binding: busy signal lease cannot be borrowed or acknowledged',async()=>{const x=await seedOfficial();try{const busy=await leaseJob(x.db,'projection:'+x.scopeId,'signal-owner',{now:x.at});assert(busy);const result=await processCapturedOutcome(x.db,x.evaluationId,{now:x.at});assert.equal(result.status,'PENDING');assert.notEqual(job(x,x.evaluationId).state,'DONE');assert.equal(job(x,x.scopeId).owner,'signal-owner');}finally{x.close();}});

test('Processing binding: evaluation completion refuses the signal lease',async()=>{const x=await seedOfficial();try{const r=await selected(x);await canonical(x);await assert.rejects(completeEvaluationObligation(x.db,r,{id:'projection:'+x.scopeId},{now:x.at}),/pa_processing_lease_scope/);}finally{x.close();}});

test('Processing binding: lifecycle arriving during completion leaves durable pending work',async()=>{const x=await seedOfficial();try{const r=await selected(x);await canonical(x);const lease=await leaseJob(x.db,'projection:'+x.evaluationId,'A',{now:x.at});const p=await lifecycle(x.scopeId,'tp1');await assert.rejects(completeEvaluationObligation(x.db,r,lease,{now:x.at,hook:()=>deliver(x.db,p,x.at+60000)}),/pa_fence_failed/);assert.notEqual(job(x,x.evaluationId).state,'DONE');x.at+=120000;await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');assert.equal(JSON.parse(sql(x,'SELECT payload FROM pa_projections WHERE scope_id=?',x.scopeId).payload).status,'tp1');
 }finally{x.close();}});

test('Processing binding: archive-only restore and new target epoch rebuild canonical ownership',async()=>{const source=await seedOfficial(),target=await packageDatabase(),b=fakeBucket(),archive=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-processing-restore/'});let closed=false;try{const p=await publishArchive(source.db,archive,source.scopeId,{now:source.at}),{hydrateArchive}=await import('./measurement-restore.js'),{archiveReader}=await import('./measurement-archive.js');const scopeId=source.scopeId,evaluationId=source.evaluationId,at=source.at;source.close();closed=true;
 const r=await hydrateArchive(target.db,archive,p.root_key,{now:at,environment:'synthetic-b1-package-a',authorization:{operation:'restore',environment:'synthetic-b1-package-a',expiresAt:at+60000}});assert.equal(r.status,'COMPLETE');const reader=archiveReader(archive,{rootKey:p.root_key});const done=await processCapturedOutcome(target.db,evaluationId,{now:at,archive:reader,rebuild:true});assert.equal(done.status,'COMPLETED_AT_REVISION');assert.equal(sql(target,'SELECT COUNT(*) n FROM pa_processing WHERE scope_id=?',evaluationId).n,0);assert.equal(sql(target,'SELECT COUNT(*) n FROM pa_processing WHERE scope_id=?',scopeId).n,1);assert.deepEqual(target.raw.prepare('PRAGMA foreign_key_check').all(),[]);
 }finally{if(!closed)source.close();target.close();}});

test('Processing binding: lost canonical result response resumes without repeating its protected result',async()=>{const x=await seedOfficial();try{let lost=false;const db=createMeasurementD1Adapter({...x.binding,async batch(s){const out=await x.binding.batch(s);if(!lost&&s.some(s=>s.sql.includes('INSERT INTO pa_processing'))){lost=true;throw Error('canonical_response_loss');}return out;}});
 await assert.rejects(processCapturedOutcome(db,x.evaluationId,{now:x.at}),/canonical_response_loss/);assert(lost);const before=sql(x,"SELECT COUNT(*) n FROM pa_records WHERE scope_id=? AND kind='RESULT'",x.scopeId).n;assert.equal(before,1);assert.notEqual(job(x,x.evaluationId).state,'DONE');x.at+=60001;await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');assert.equal(sql(x,"SELECT COUNT(*) n FROM pa_records WHERE scope_id=? AND kind='RESULT'",x.scopeId).n,before);
 }finally{x.close();}});

test('Processing binding: incomplete canonical checkpoint cannot acknowledge evaluation',async()=>{const x=await seedOfficial();try{const {prepareMarketEvidence}=await import('./measurement-market-envelope.js'),{ingestMarketEvidence}=await import('./measurement-market-store.js');for(let i=0;i<10;i++){const p=(await prepareMarketEvidence({cohort:x.snapshot.scope.cohort,source:'mt5',session:'backlog',from:x.at+i,to:x.at+i,observedAt:x.at+i,ticks:[{ts:x.at+i,price:4100+i/10,measurement:{provider:'mt5',sessionId:'backlog',sequence:i,providerTimestamp:x.at+i}}]}))[0];await ingestMarketEvidence(x.db,p,{now:x.at+10});}
 x.at+=20;const r=await processCapturedOutcome(x.db,x.evaluationId,{now:x.at});assert.equal(r.status,'PENDING');assert.equal(job(x,x.scopeId).state,'PENDING');assert.notEqual(job(x,x.evaluationId).state,'DONE');const partial=JSON.parse(sql(x,'SELECT payload FROM pa_processing WHERE scope_id=?',x.scopeId).payload);assert.equal(partial.completeForCapturedSnapshot,false);x.at+=60001;await drain(x);assert.equal(job(x,x.evaluationId).state,'DONE');assert.equal(JSON.parse(sql(x,'SELECT payload FROM pa_processing WHERE scope_id=?',x.scopeId).payload).completeForCapturedSnapshot,true);
 }finally{x.close();}});
