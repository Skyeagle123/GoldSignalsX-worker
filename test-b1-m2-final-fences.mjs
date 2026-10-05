import assert from 'node:assert/strict';
import {test} from 'node:test';
import {database,cyclePackets,now} from './test-fixtures/b1-m2.mjs';
import {createOfflineMeasurementConsumer} from './measurement-consumer.js';
import {collectMeasurementFromProjection} from './measurement-collector.js';
import {rebuildLifecycleProjection,fenceLifecycleFailure,clearLifecycleFailure,lifecycleCapability} from './measurement-lifecycle.js';
import {wrapOfflineMeasurementAdapter} from './measurement-adapter.js';
import {canonicalSerialize,digestPayload} from './signal-evidence.js';
import {censusSnapshot} from './evidence-codec.js';

const packets=await cyclePackets(),cycle=packets[0],creation=packets.find(p=>p.envelope.kind==='OFFICIAL_CREATION');
const officialId=creation.envelope.semanticId;
const candidate=packets.find(p=>p.envelope.kind==='EVALUATION_CENSUS'&&p.envelope.payload.decisionEvidence.kind==='CANDIDATE');
const evalId=candidate.envelope.semanticId;
const option={asOf:now+86400001,ticks:[],bars:[],maxSubjects:1};
async function edited(packet,change){const e=JSON.parse(packet.wire);change(e);
 e.semanticKey=canonicalSerialize([e.producerNamespace,e.kind,e.semanticId]);
 e.payloadDigest=await digestPayload(canonicalSerialize({clocks:{occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt},payload:e.payload},2*1024**2));
 e.eventId=`m1:${await digestPayload(canonicalSerialize([e.producerNamespace,e.kind,e.semanticId,e.payloadDigest]))}`;
 return canonicalSerialize(e,2*1024**2);
}
const badCandidate=()=>edited(candidate,e=>{e.payload.decisionEvidence.engine.levels.entry=4200;e.payload.census=censusSnapshot(e.payload.decisionEvidence);});
const badOfficial=()=>edited(creation,e=>{e.semanticId='30m:1790865000000:other';e.payload.officialSignalId=e.semanticId;e.payload.decisionEvidence.officialSignalId=e.semanticId;e.payload.decisionEvidence.engine.levels.entry=4200;});
async function setup(){const {db,binding}=await database();const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});await consumer.ingest(cycle.wire);return {db,binding,consumer};}
function snapshot(db,subject=evalId){return Object.fromEntries(['signal_outcome_evidence','signal_measurement_state','measurement_final_results'].map(t=>[t,db.prepare(`SELECT * FROM ${t} WHERE subject_id=? ORDER BY 1`).all(subject)]));}
function pausedAdapter(base,match){let release,ready;const gate=new Promise(r=>release=r),selected=new Promise(r=>ready=r);let hits=0;
 const adapter=wrapOfflineMeasurementAdapter(base,{async beforeBatch(work,protectedWrite){if(protectedWrite&&match(work)){hits++;ready();await gate;}}});
 return {adapter,release,selected,get hits(){return hits;}};
}
for(const durable of [true,false])test(`final Candidate conflict blocks irreversible writes, durable=${durable}`,async()=>{
 const {db,binding,consumer}=await setup();const before=snapshot(db);const broken=wrapOfflineMeasurementAdapter(binding,{beforeBatch(){throw new Error('INTENT_STORAGE_FAILURE');}});
 const result=await createOfflineMeasurementConsumer(durable?binding:broken,{clock:()=>now+86400000}).ingest(await badCandidate());
 assert.equal(result.conflictStatus,durable?'RECORDED':'RECONCILIATION_PENDING');
 const collected=await collectMeasurementFromProjection(binding,option);assert(collected.ok);assert.deepEqual(snapshot(db),before);db.close();
});
for(const recover of [false,true])test(`final Candidate selected stale lease rejects after suspension, recovered=${recover}`,async()=>{
 const {db,binding,consumer}=await setup();const before=snapshot(db),held=pausedAdapter(binding,()=>true);
 const work=collectMeasurementFromProjection(held.adapter,option);await held.selected;
 const broken=wrapOfflineMeasurementAdapter(binding,{beforeBatch(){throw new Error('INTENT_STORAGE_FAILURE');}});
 assert.equal((await createOfflineMeasurementConsumer(broken,{clock:()=>now+86400000}).ingest(await badCandidate())).conflictStatus,'RECONCILIATION_PENDING');
 if(recover)assert.equal((await consumer.ingest(await badCandidate())).conflictStatus,'RECORDED');
 held.release();const result=await work;assert(result.ok);assert.equal(result.updated,0);assert.deepEqual(snapshot(db),before);db.close();
});
test('final Candidate suspension after SQL mutations rolls back transaction',async()=>{
 const {db,binding}=await setup(),before=snapshot(db);let fired=false;
 const racing=wrapOfflineMeasurementAdapter(binding,{afterStatement({sql},protectedWrite){if(protectedWrite&&!fired&&sql.startsWith('INSERT INTO measurement_outcome_records')){fired=true;fenceLifecycleFailure(binding,null,evalId);}}});
 const result=await collectMeasurementFromProjection(racing,option);assert(fired);assert(result.ok);assert.equal(result.updated,0);assert.deepEqual(snapshot(db),before);db.close();
});
test('final Candidate latest hook before COMMIT invalidates the native transaction',async()=>{
 const {db,binding}=await setup(),before=snapshot(db);let fired=false;
 const racing=wrapOfflineMeasurementAdapter(binding,{beforeCommit(work,protectedWrite){if(protectedWrite&&!fired&&work.some(s=>s.sql.startsWith('INSERT INTO measurement_outcome_records'))){fired=true;fenceLifecycleFailure(binding,null,evalId);}}});
 const result=await collectMeasurementFromProjection(racing,option);assert(fired);assert(result.ok);assert.equal(result.updated,0);assert.deepEqual(snapshot(db),before);db.close();
});
test('final stale empty binding discovery cannot publish COMPLETE after new binding and failed conflict lookup',async()=>{
 const {db,binding}=await database();const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});let once=true;
 const racing=wrapOfflineMeasurementAdapter(binding,{prepare(sql){const p=binding.prepare(sql);return {...p,bind(...args){const s=p.bind(...args);return {...s,async all(){
   if(once&&sql.startsWith('SELECT evaluation_id FROM measurement_decision_bindings')){
    once=false;const empty=await s.all();assert.equal(empty.results.length,0);assert((await consumer.ingest(cycle.wire)).ok);assert((await consumer.ingest(creation.wire)).ok);
    let lookup=0;const failed=wrapOfflineMeasurementAdapter(binding,{prepare(q){const p=binding.prepare(q);if(q!=='SELECT payload_digest,official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=?')return p;
     return {...p,bind(...args){const s=p.bind(...args);return {...s,async first(){if(++lookup===1)return null;throw new Error('LOOKUP_FAILED');}};}};}});
    const result=await createOfflineMeasurementConsumer(failed,{clock:()=>now+86400000}).ingest(await badOfficial());assert.equal(result.projectionStatus,'PENDING');return empty;
   }return s.all();
  }};}};}});
 let result;try{result=await rebuildLifecycleProjection(racing,officialId,{rebuiltAt:now+86400001});}catch(e){assert.match(String(e),/binding_changed|capability_pending|rebuild_raced|quarantine_pending/);}
 assert.notEqual(result?.integrityStatus,'COMPLETE');assert(lifecycleCapability(binding).pendingEvaluations.has(creation.envelope.payload.evaluationId));
 const collected=await collectMeasurementFromProjection(binding,option);assert.equal(collected.deferredOfficialSubjects,1);db.close();
});

test('final two selected Candidate batches retain invalid generations through reconciliation; unrelated candidate progresses',async()=>{
 const {db,binding,consumer}=await setup();const before=snapshot(db),held=pausedAdapter(binding,()=>true);
 const a=collectMeasurementFromProjection(held.adapter,option),b=collectMeasurementFromProjection(held.adapter,option);
 await held.selected;for(let i=0;i<100&&held.hits<2;i++)await new Promise(r=>setImmediate(r));assert.equal(held.hits,2);
 const broken=wrapOfflineMeasurementAdapter(binding,{beforeBatch(){throw new Error('INTENT_FAILURE');}});
 assert.equal((await createOfflineMeasurementConsumer(broken,{clock:()=>now+86400000}).ingest(await badCandidate())).conflictStatus,'RECONCILIATION_PENDING');
 assert.equal((await consumer.ingest(await badCandidate())).conflictStatus,'RECORDED');held.release();
 for(const result of await Promise.all([a,b])){assert(result.ok);assert.equal(result.updated,0);}
 assert.deepEqual(snapshot(db),before);
 const other=await collectMeasurementFromProjection(binding,option);assert(other.ok);assert.equal(other.updated,1);
 assert.deepEqual(snapshot(db),before);db.close();
});

function bindExtra(db,signalId,suffix){db.prepare('INSERT INTO measurement_decision_bindings(evaluation_id,payload_digest,official_signal_id,first_event_id) VALUES(?,?,?,?)')
 .run(`test-extra:${suffix}`,'a'.repeat(64),signalId,cycle.envelope.eventId);}
function interleavedDiscovery(binding,operation){let once=true;return wrapOfflineMeasurementAdapter(binding,{prepare(sql){const p=binding.prepare(sql);return {...p,bind(...args){const s=p.bind(...args);return {...s,async all(){if(once&&sql.startsWith('SELECT evaluation_id FROM measurement_decision_bindings')){once=false;const prior=await s.all();await operation();return prior;}return s.all();}};}};}});}
test('final empty-to-one canonical binding invalidates discovery without conflict',async()=>{
 const {db,binding}=await database();const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});
 const racing=interleavedDiscovery(binding,async()=>{await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);});
 await assert.rejects(rebuildLifecycleProjection(racing,officialId,{rebuiltAt:now+86400001}),/binding_changed/);
 assert.equal((await rebuildLifecycleProjection(binding,officialId,{rebuiltAt:now+86400001})).integrityStatus,'COMPLETE');db.close();
});
test('final one-to-additional binding invalidates stale rebuild while fresh rebuild completes',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(creation.wire);
 const racing=interleavedDiscovery(binding,()=>bindExtra(db,officialId,'one'));
 await assert.rejects(rebuildLifecycleProjection(racing,officialId,{rebuiltAt:now+86400001}),/binding_changed/);
 const recovered=await rebuildLifecycleProjection(binding,officialId,{rebuiltAt:now+86400001});assert.equal(recovered.integrityStatus,'COMPLETE');db.close();
});
test('final suspension after binding discovery invalidates publication, unchanged set completes',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(creation.wire);
 const racing=interleavedDiscovery(binding,()=>fenceLifecycleFailure(binding,null,creation.envelope.payload.evaluationId));
 await assert.rejects(rebuildLifecycleProjection(racing,officialId,{rebuiltAt:now+86400001}),/capability_pending/);
 clearLifecycleFailure(binding,null,creation.envelope.payload.evaluationId);
 assert.equal((await rebuildLifecycleProjection(binding,officialId,{rebuiltAt:now+86400001})).integrityStatus,'COMPLETE');db.close();
});
test('final concurrent rebuilds across dependency change reject stale rebuild',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(creation.wire);let resume,arrived;const gate=new Promise(r=>resume=r),ready=new Promise(r=>arrived=r);
 const racing=interleavedDiscovery(binding,async()=>{arrived();await gate;});
 const stale=rebuildLifecycleProjection(racing,officialId,{rebuiltAt:now+86400001});await ready;
 bindExtra(db,officialId,'concurrent');const current=await rebuildLifecycleProjection(binding,officialId,{rebuiltAt:now+86400001});assert.equal(current.integrityStatus,'COMPLETE');
 resume();await assert.rejects(stale,/binding_changed/);db.close();
});
