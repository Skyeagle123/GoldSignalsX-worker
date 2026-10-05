import assert from 'node:assert/strict';
import {test} from 'node:test';
import fs from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import {database,cyclePackets,lifecycle,confirmation,now} from './test-fixtures/b1-m2.mjs';
import {createOfflineMeasurementConsumer,validateMeasurementWire} from './measurement-consumer.js';
import {rebuildLifecycleProjection} from './measurement-lifecycle.js';
import {collectMeasurementFromProjection} from './measurement-collector.js';
import {decodeStoredEvidence,decodeProcessingState} from './evidence-codec.js';
import {buildMeasurementEnvelope} from './measurement-envelope.js';
import {loadM1Workers,cycleEnvironment} from './test-fixtures/b1-m1.mjs';
const packets=await cyclePackets(),cycle=packets[0],creation=packets.find(p=>p.envelope.kind==='OFFICIAL_CREATION');
assert(creation);const id=creation.envelope.semanticId,created=creation.envelope.payload.decisionEvidence;
async function setup(){const d=await database();let time=now+86400000;return {...d,consumer:createOfflineMeasurementConsumer(d.binding,{clock:()=>time}),time:t=>{time=t;}};}
const projection=(db,id)=>db.prepare('SELECT * FROM measurement_lifecycle_projection WHERE signal_id=?').get(id);

test('M2 fresh separate migration chain, historical migrations unchanged and no trading table',async()=>{
 const {db}=await setup();assert.equal(db.prepare('SELECT version FROM measurement_schema_meta').get().version,4);
 assert.equal(db.prepare("SELECT count(*) n FROM sqlite_master WHERE name='production_signals'").get().n,0);
 for(const file of ['0001_measurement_evidence.sql','0002_measurement_storage_tiers.sql','0003_measurement_dependency_closure.sql'])assert.equal(await fs.readFile(new URL('migrations/'+file,import.meta.url),'utf8'),execFileSync('git',['show',`86c918740aa9be007536aa33a6f0614c8b880c68:migrations/${file}`],{encoding:'utf8'}));db.close();
});
test('M2 actual M1 cycle and census durable atomic ingestion, duplicate delivery and semantic conflict',async()=>{
 const {db,consumer}=await setup();assert.equal((await consumer.ingest(cycle.wire)).status,'ACCEPTED');
 const receipt=db.prepare('SELECT * FROM measurement_ingress_receipts').get();
 assert.equal((await consumer.ingest(cycle.wire)).status,'DUPLICATE');assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,1);
 for(const packet of packets.slice(1))assert.equal((await consumer.ingest(packet.wire)).ok,true);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,7);
 assert.equal(receipt.observed_at,cycle.envelope.clocks.observedAt);assert.equal(receipt.received_at,now+86400000);assert.equal(receipt.ingested_at,receipt.received_at);
 const changed=await buildMeasurementEnvelope({...creation.envelope,payload:creation.envelope.payload}).catch(()=>null);assert.equal(changed,null); // Builder rejects wire headers as options.
 const conflict=await buildMeasurementEnvelope({kind:creation.envelope.kind,semanticId:id,payload:{...creation.envelope.payload,decisionEvidence:{...created,quote:{...created.quote,bid:4000}}},occurredAt:created.createdAt,observedAt:created.capturedAt,preparedAt:now+2000});
 assert.equal((await consumer.ingest(conflict.wire)).status,'INTEGRITY_CONFLICT');db.close();
});
test('M2 census before cycle is explicitly retryable, never claims durable processing',async()=>{
 const {db,consumer}=await setup();const census=packets.find(p=>p.envelope.kind==='EVALUATION_CENSUS');
 assert.deepEqual(await consumer.ingest(census.wire),{ok:false,status:'DEPENDENCY_PENDING',retryable:true,durable:false,captureGap:true});
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,0);
 assert.equal((await consumer.ingest(cycle.wire)).ok,true);assert.equal((await consumer.ingest(census.wire)).ok,true);db.close();
});
test('M2 creation/lifecycle permutations rebuild identically, including lifecycle before creation',async()=>{
 const tp1=await lifecycle(id,'tp1'),tp2=await lifecycle(id,'tp2',{at:now+120000});
 let expected;
 for(const order of [[creation,tp1,tp2],[tp2,tp1,creation],[tp1,creation,tp2]]){
  const {db,consumer,binding}=await setup();for(const packet of order)assert.equal((await consumer.ingest(packet.wire)).ok,true);
  const before=projection(db,id),p=await decodeStoredEvidence(before);assert.equal(p.status,'tp2');assert.equal(p.closedAt,now+120000);assert.equal(p.tp1At,now+60000);
  expected??=p;assert.deepEqual(p,expected);db.prepare('DELETE FROM measurement_lifecycle_projection').run();
  const restored=await rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000});assert.deepEqual(restored,p);
  assert.equal((await consumer.ingest(tp1.wire)).status,'DUPLICATE');assert.deepEqual(await decodeStoredEvidence(projection(db,id)),p);db.close();
 }
});
test('M2 TP1→SL, direct SL and expiry truthful terminal projections',async()=>{
 for(const events of [['tp1','sl'],['sl'],['expired']]){
  const {db,consumer}=await setup();await consumer.ingest(creation.wire);for(let i=0;i<events.length;i++)await consumer.ingest((await lifecycle(id,events[i],{at:now+60000*(i+1)})).wire);
  const p=await decodeStoredEvidence(projection(db,id));assert.equal(p.status,events.at(-1)==='sl'?'sl':'expired');assert.equal(p.closedAt,now+events.length*60000);db.close();
 }
});
test('M2 projection matches unchanged persistence status/closing clock from real local M1 lifecycle fact',async()=>{
 const {after}=await loadM1Workers(),local=cycleEnvironment(after),{db,consumer}=await setup();
 const at=now+60000,closedAt=at+5000,levels=created.engine.levels;
 const signal={id,tf:created.timeframe,side:created.direction,origin:'server',createdAt:created.createdAt,signalBarTs:now-1800000,
  ...levels,status:'stopped',updatedAt:closedAt,closedAt,triggerEvent:'sl',triggeredAt:at,triggerPrice:levels.sl,triggerSource:'mt5'};
 try{
  await after.ensurePerformanceSchema(local.env);const persistence=await after.recordProductionPerformanceSafely(local.env,signal,'sl');assert(persistence.ok);
  const raw=await lifecycle(id,'sl',{at});const e=raw.envelope;
  const packet=await buildMeasurementEnvelope({kind:e.kind,semanticId:e.semanticId,payload:{...e.payload,timeframe:signal.tf,direction:signal.side,closedAt,
   level:levels.sl,observedPrice:levels.sl,trigger:{price:levels.sl,at,source:'mt5'},performancePersistence:persistence},occurredAt:at,observedAt:persistence.signal.updated_at,preparedAt:persistence.signal.updated_at});
  // Local producer oracle clock may be beyond the historical fixture clock.
  const actualConsumer=createOfflineMeasurementConsumer((await import('./test-fixtures/b1-sqlite.mjs')).transactionalBinding(db),{clock:()=>Date.now()+10000});
  assert.equal((await actualConsumer.ingest(packet.wire)).ok,true);
  const expected=local.db.database.prepare('SELECT status,closed_at FROM production_signals WHERE signal_id=?').get(id),p=await decodeStoredEvidence(projection(db,id));
  assert.equal(p.status,expected.status);assert.equal(p.closedAt,expected.closed_at);assert.equal(p.observedClosedAt,closedAt);assert.notEqual(p.closedAt,p.observedClosedAt);
 }finally{db.close();local.db.database.close();}
});
test('M2 incompatible terminal facts stay immutable and make projection unavailable',async()=>{
 const {db,consumer}=await setup();for(const packet of [creation,await lifecycle(id,'tp2'),await lifecycle(id,'sl')])assert.equal((await consumer.ingest(packet.wire)).ok,true);
 const p=projection(db,id);assert.equal(p.integrity_status,'CONFLICT');assert.equal(p.status,null);assert.equal(p.closed_at,null);
 assert.throws(()=>db.prepare("UPDATE measurement_lifecycle_facts SET signal_id='other'").run(),/immutable/);
 assert.throws(()=>db.prepare('DELETE FROM measurement_lifecycle_facts').run(),/immutable/);db.close();
});
test('M2 first receipt/durable availability never backdated by old observation or redelivery',async()=>{
 const {db,consumer,time}=await setup();time(now+100000);const tp1=await lifecycle(id,'tp1');await consumer.ingest(tp1.wire,{receivedAt:now+90000});
 const r=db.prepare('SELECT * FROM measurement_ingress_receipts').get();assert.equal(r.occurred_at,now+60000);assert.equal(r.observed_at,now+60010);assert.equal(r.received_at,now+90000);assert.equal(r.ingested_at,now+100000);
 time(now+200000);await consumer.ingest(tp1.wire);assert.equal(projection(db,id).available_at,now+100000);db.close();
});
test('M2 confirmations preserve ownership and link persistence before/after Primary',async()=>{
 for(const order of ['before','after']){const {db,consumer}=await setup();const fact=await confirmation(id,'confirmation-id');
  for(const packet of order==='before'?[fact,creation]:[creation,fact])assert.equal((await consumer.ingest(packet.wire)).ok,true);
  const p=await decodeStoredEvidence(projection(db,'confirmation-id'));assert.equal(p.owningPrimaryId,id);assert.equal(p.linkPersistence,'SUCCEEDED');assert.equal(p.status,null);assert.equal(projection(db,id).status,'active');db.close();
 }
 const {db,consumer}=await setup();await consumer.ingest((await confirmation(id,'failed-confirmation',{persisted:false})).wire);
 assert.equal((await decodeStoredEvidence(projection(db,'failed-confirmation'))).linkPersistence,'FAILED');db.close();
});
test('M2 failed persistence and partial facts do not manufacture authoritative status/levels',async()=>{
 const {db,consumer}=await setup();await consumer.ingest((await lifecycle(id,'sl',{persisted:false})).wire);let p=await decodeStoredEvidence(projection(db,id));assert.equal(p.status,null);assert.equal(p.closedAt,null);assert.equal(p.levels.entry,null);
 const partial=await lifecycle('partial','tp1',{partial:true});assert.equal((await consumer.ingest(partial.wire)).ok,true);p=await decodeStoredEvidence(projection(db,'partial'));assert.equal(p.createdAt,null);assert.equal(p.integrityStatus,'PARTIAL');db.close();
});
test('M2 Official persistence failure stays explicit, and counterfeit census is rejected',async()=>{
 const {db,consumer}=await setup();const e=creation.envelope,performance={performance:'FAILED',reason:'performance_record_failed'};
 const failed=await buildMeasurementEnvelope({kind:e.kind,semanticId:e.semanticId,payload:{...e.payload,performancePersistence:performance,decisionEvidence:{...e.payload.decisionEvidence,officialPersistence:performance}},occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt,preparedAt:e.clocks.preparedAt});
 assert.equal((await consumer.ingest(failed.wire)).ok,true);const p=await decodeStoredEvidence(projection(db,id));assert.equal(p.officialPersisted,true);assert.equal(p.performancePersistence,'FAILED');assert.equal(p.status,null);
 const census=packets.find(p=>p.envelope.kind==='EVALUATION_CENSUS').envelope;
 const wrong=await buildMeasurementEnvelope({kind:census.kind,semanticId:census.semanticId,payload:{...census.payload,census:[]},occurredAt:census.clocks.occurredAt,observedAt:census.clocks.observedAt,preparedAt:census.clocks.preparedAt});
 assert.equal((await consumer.ingest(wrong.wire)).error,'measurement_census_conflict');db.close();
});
test('M2 public wire boundary rejects invalid identities, digest/version/namespace and clocks before writes',async()=>{
 const {db,consumer}=await setup();
 for(const [key,value] of [['version',2],['producerNamespace','other'],['eventId','wrong'],['payloadDigest','0'.repeat(64)],['semanticId',''],['semanticKey','wrong'],['measurementOnly',false]]){
  const e=structuredClone(creation.envelope);e[key]=value;const result=await consumer.ingest(JSON.stringify(e));assert.equal(result.ok,false,key);assert.equal(result.durable,false);assert.equal(result.captureGap,true);
 }
 for(const wire of ['{',null,' '.repeat(2*1024*1024+1)])assert.equal((await consumer.ingest(wire)).ok,false);
 const future=await buildMeasurementEnvelope({kind:'LIFECYCLE_FACT',semanticId:'production:x:tp1',payload:(await lifecycle('x','tp1')).envelope.payload,occurredAt:now+60000,observedAt:now+60010,preparedAt:now+86400001});
 assert.equal((await consumer.ingest(future.wire)).ok,false);
 assert.equal((await consumer.ingest(creation.wire,{receivedAt:now+86400001})).ok,false);
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,0);db.close();
});
test('M2 conflicting fact/digest never overwrites accepted immutable evidence',async()=>{
 const {db,consumer}=await setup();const original=await lifecycle(id,'tp1');await consumer.ingest(original.wire);
 const changed=await buildMeasurementEnvelope({kind:'LIFECYCLE_FACT',semanticId:original.envelope.semanticId,payload:{...original.envelope.payload,observedPrice:4567},occurredAt:now+60000,observedAt:now+60010,preparedAt:now+70000});
 assert.equal((await consumer.ingest(changed.wire)).status,'INTEGRITY_CONFLICT');assert.equal(db.prepare('SELECT count(*) n FROM measurement_lifecycle_facts').get().n,1);db.close();
});
test('M2 concurrent identical/conflicting redelivery is protected by atomic receipt constraints',async()=>{
 const {db,binding}=await database();let tail=Promise.resolve();
 const atomic={...binding,batch(statements){const work=tail.then(()=>binding.batch(statements));tail=work.catch(()=>{});return work;}};
 const a=createOfflineMeasurementConsumer(atomic,{clock:()=>now+86400000}),b=createOfflineMeasurementConsumer(atomic,{clock:()=>now+86400001});
 const fact=await lifecycle(id,'tp1');const results=await Promise.all([a.ingest(fact.wire),b.ingest(fact.wire)]);assert(results.every(r=>r.ok));
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,1);assert.equal(db.prepare('SELECT count(*) n FROM measurement_lifecycle_facts').get().n,1);
 assert(results.every(r=>r.ingestedAt===db.prepare('SELECT ingested_at FROM measurement_ingress_receipts').get().ingested_at));
 const e=(await lifecycle(id,'tp2')).envelope,change=await buildMeasurementEnvelope({kind:e.kind,semanticId:e.semanticId,payload:{...e.payload,observedPrice:4000},occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt,preparedAt:e.clocks.preparedAt});
 const pair=await Promise.all([a.ingest(JSON.stringify(e)),b.ingest(change.wire)]);assert.equal(pair.filter(r=>r.ok).length,1);assert.equal(pair.filter(r=>r.status==='INTEGRITY_CONFLICT').length,1);db.close();
});
test('M2 atomic ingestion rolls back evidence and receipt on write failure',async()=>{
 const {db,consumer}=await setup();db.exec("CREATE TRIGGER fail_receipt BEFORE INSERT ON measurement_ingress_receipts BEGIN SELECT RAISE(ABORT,'local_failure'); END;");
 assert.equal((await consumer.ingest(cycle.wire)).ok,false);assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,0);assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,0);db.close();
});
test('M2 measurement-only collector processes subjects with no production_signals table/query',async()=>{
 const {db,binding,consumer,calls}=await setup();for(const p of [cycle,creation,await lifecycle(id,'tp1')])assert.equal((await consumer.ingest(p.wire)).ok,true);
 calls.length=0;let result=await collectMeasurementFromProjection(binding,{asOf:now+60000,ticks:[],bars:[]});assert.equal(result.ok,true);assert.equal(result.subjects,0);
 result=await collectMeasurementFromProjection(binding,{asOf:now+86400000,ticks:[],bars:[],maxSubjects:1});assert.equal(result.ok,true);assert.equal(result.updated,1);
 assert(calls.every(sql=>!/production_signals|GSX_DB/.test(sql)));const state=await decodeProcessingState(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id));assert.equal(state.lifecycleEvidenceBasis,'IMMUTABLE_CAPTURED_MEASUREMENT_FACTS');db.close();
});
test('M2 stale/budget-pending projection is explicit and cannot be used as current truth',async()=>{
 const {db,binding,consumer}=await setup();for(const p of [creation,await lifecycle(id,'tp1')])await consumer.ingest(p.wire);
 await assert.rejects(rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000,maxFacts:1}),/work_exceeded/);
 // Facts remain available; cache deletion/replay is safe and deterministic.
 db.prepare('DELETE FROM measurement_lifecycle_projection').run();assert.equal((await consumer.ingest(creation.wire)).projectionStatus,'CURRENT');db.close();
});
test('M2 unavailable clocks remain distinct from null; no reporting writes or trading imports',async()=>{
 const {db,consumer}=await setup();const gap=await buildMeasurementEnvelope({kind:'CAPTURE_GAP',semanticId:'gap',payload:{captureGap:true,durable:false,measurementOnly:true,decisionUse:false},occurredAt:undefined,observedAt:null,preparedAt:now});
 assert.equal((await consumer.ingest(gap.wire)).ok,true);const receipt=db.prepare('SELECT * FROM measurement_ingress_receipts').get();assert.equal(receipt.clock_unavailable,1);assert.equal(receipt.observed_at,null);
 for(const name of ['measurement-consumer.js','measurement-lifecycle.js']){const text=await fs.readFile(new URL(name,import.meta.url),'utf8');assert(!/\bfetch\s*\(|GSX_DB|production_signals|cloudflare:|https?:\/\//.test(text));}
 const worker=await fs.readFile(new URL('goldsignalsx-worker.js',import.meta.url),'utf8');assert(!worker.includes('measurement-consumer'));db.close();
});
