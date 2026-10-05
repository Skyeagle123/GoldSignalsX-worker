import assert from 'node:assert/strict';
import {test} from 'node:test';
import {database,cyclePackets,lifecycle,now} from './test-fixtures/b1-m2.mjs';
import {createOfflineMeasurementConsumer} from './measurement-consumer.js';
import {rebuildLifecycleProjection,validateLifecycleProjectionReturn,fenceLifecycleFailure,clearLifecycleFailure} from './measurement-lifecycle.js';
import {wrapOfflineMeasurementAdapter,measurementAdapterMetrics} from './measurement-adapter.js';
const packets=await cyclePackets(),cycle=packets[0],creation=packets.find(p=>p.envelope.kind==='OFFICIAL_CREATION');
const signalId=creation.envelope.semanticId,evaluationId=creation.envelope.payload.evaluationId;
const rebuiltAt=now+86400001;
async function setup(){const d=await database();const consumer=createOfflineMeasurementConsumer(d.binding,{clock:()=>now+86400000});
 assert((await consumer.ingest(cycle.wire)).ok);assert((await consumer.ingest(creation.wire)).ok);return {...d,consumer};}
function afterFinalRead(binding,operation){let once=true;return wrapOfflineMeasurementAdapter(binding,{prepare(sql){
 const p=binding.prepare(sql);return {...p,bind(...args){const s=p.bind(...args);return {...s,async first(){
  const captured=await s.first();if(once&&sql.startsWith('SELECT p.fact_count,p.payload_digest,p.integrity_status')){
   once=false;assert.equal(captured.integrity_status,'COMPLETE');assert.equal(captured.fact_count,1);await operation();
  }return captured;
 }};}};
}});}
async function terminalConflict(consumer,db){assert.equal((await consumer.ingest((await lifecycle(signalId,'sl')).wire)).projectionStatus,'CURRENT');
 assert.equal((await consumer.ingest((await lifecycle(signalId,'tp2')).wire)).projectionStatus,'CONFLICT');
 const stored=db.prepare('SELECT status,integrity_status,fact_count,source_revision FROM measurement_lifecycle_projection WHERE signal_id=?').get(signalId);
 assert.deepEqual({...stored},{status:null,integrity_status:'CONFLICT',fact_count:3,source_revision:6});}
test('final return exact Astra SL then TP2 after captured COMPLETE read rejects old projection',async()=>{
 const {db,binding,consumer}=await setup();const delayed=afterFinalRead(binding,()=>terminalConflict(consumer,db));
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_projection_rebuild_raced/);db.close();
});
test('final return revision change with failed newer publication rejects old COMPLETE',async()=>{
 const {db,binding}=await setup();const unavailable=wrapOfflineMeasurementAdapter(binding,{beforeBatch(work){if(work.some(s=>s.sql.startsWith('INSERT INTO measurement_lifecycle_projection')))throw new Error('PROJECTION_UNAVAILABLE');}});
 const consumer=createOfflineMeasurementConsumer(unavailable,{clock:()=>now+86400000});const delayed=afterFinalRead(binding,async()=>{
  assert.equal((await consumer.ingest((await lifecycle(signalId,'tp1')).wire)).projectionStatus,'PENDING');
 });await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_projection_rebuild_raced/);db.close();
});
test('final return durable quarantine after captured read rejects old COMPLETE',async()=>{
 const {db,binding}=await setup();const delayed=afterFinalRead(binding,()=>{db.prepare("INSERT INTO measurement_decision_quarantine VALUES(?,'PENDING',?)").run(evaluationId,rebuiltAt);});
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_projection_rebuild_raced/);db.close();
});
test('final return evaluation suspension after captured read rejects old COMPLETE',async()=>{
 const {db,binding}=await setup();const delayed=afterFinalRead(binding,()=>fenceLifecycleFailure(binding,null,evaluationId));
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_quarantine_capability_pending/);db.close();
});
test('final return binding set change after captured read rejects old COMPLETE',async()=>{
 const {db,binding}=await setup();const delayed=afterFinalRead(binding,()=>{db.prepare('INSERT INTO measurement_decision_bindings VALUES(?,?,?,?)').run('new-binding','a'.repeat(64),signalId,cycle.envelope.eventId);});
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_projection_binding_changed/);db.close();
});
test('final return unchanged dependencies allow healthy COMPLETE',async()=>{
 const {db,binding}=await setup();const p=await rebuildLifecycleProjection(afterFinalRead(binding,()=>{}),signalId,{rebuiltAt});
 assert.equal(p.integrityStatus,'COMPLETE');assert.equal(p.status,'active');assert.equal(p.factCount,1);assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(),[]);db.close();
});
test('final return newer valid COMPLETE after captured read rejects old active snapshot',async()=>{
 const {db,binding,consumer}=await setup();const delayed=afterFinalRead(binding,async()=>{assert.equal((await consumer.ingest((await lifecycle(signalId,'tp1')).wire)).projectionStatus,'CURRENT');});
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_projection_rebuild_raced/);
 assert.equal(db.prepare('SELECT status FROM measurement_lifecycle_projection WHERE signal_id=?').get(signalId).status,'tp1');db.close();
});
test('final return concurrent rebuild keeps newer authoritative result and rejects older return',async()=>{
 const {db,binding,consumer}=await setup();let newer;
 const delayed=afterFinalRead(binding,async()=>{await consumer.ingest((await lifecycle(signalId,'tp1')).wire);newer=await rebuildLifecycleProjection(binding,signalId,{rebuiltAt});});
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_projection_rebuild_raced/);
 assert.equal(newer.integrityStatus,'COMPLETE');assert.equal(newer.status,'tp1');assert.equal(newer.factCount,2);db.close();
});
test('final return duplicate consumer never reports CURRENT from stale captured COMPLETE',async()=>{
 const {db,binding,consumer}=await setup();const delayed=afterFinalRead(binding,()=>terminalConflict(consumer,db));
 const answer=await createOfflineMeasurementConsumer(delayed,{clock:()=>rebuiltAt}).ingest(creation.wire);
 assert.equal(answer.status,'DUPLICATE');assert.equal(answer.durable,true);assert.equal(answer.projectionStatus,'PENDING');assert(answer.retryable);
 assert.equal(db.prepare('SELECT integrity_status FROM measurement_lifecycle_projection WHERE signal_id=?').get(signalId).integrity_status,'CONFLICT');db.close();
});
test('final return detects current projection conflict even without a lifecycle revision change',async()=>{
 const {db,binding}=await setup();const delayed=afterFinalRead(binding,()=>{db.prepare("UPDATE measurement_lifecycle_projection SET integrity_status='CONFLICT',status=NULL WHERE signal_id=?").run(signalId);});
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_projection_rebuild_raced/);db.close();
});
test('final return retains invalid generation after suspension is cleared during final read',async()=>{
 const {db,binding}=await setup();const delayed=afterFinalRead(binding,()=>{fenceLifecycleFailure(binding,null,evaluationId);clearLifecycleFailure(binding,null,evaluationId);});
 await assert.rejects(rebuildLifecycleProjection(delayed,signalId,{rebuiltAt}),/measurement_quarantine_capability_pending/);db.close();
});
test('final caller validation rejects stale or copied snapshots without retaining generation tokens',async()=>{
 const {db,binding,consumer}=await setup(),other=await database();const p=await rebuildLifecycleProjection(binding,signalId,{rebuiltAt});
 assert.equal(measurementAdapterMetrics(binding).generationTokens,0);validateLifecycleProjectionReturn(wrapOfflineMeasurementAdapter(binding),p);
 assert.throws(()=>validateLifecycleProjectionReturn(binding,{...p}),/measurement_projection_return_required/);
 assert.throws(()=>validateLifecycleProjectionReturn(other.binding,p),/measurement_projection_return_required/);
 await consumer.ingest((await lifecycle(signalId,'tp1')).wire);assert.throws(()=>validateLifecycleProjectionReturn(binding,p),/measurement_projection_rebuild_raced/);
 assert.equal(measurementAdapterMetrics(binding).generationTokens,0);db.close();other.db.close();
});
