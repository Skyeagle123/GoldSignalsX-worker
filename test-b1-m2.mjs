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
 const receipt=db.prepare('SELECT r.*,s.ingested_at FROM measurement_ingress_receipts r JOIN measurement_ingress_recovery s ON s.event_id=r.event_id').get();
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
 const r=db.prepare('SELECT r.*,s.ingested_at FROM measurement_ingress_receipts r JOIN measurement_ingress_recovery s ON s.event_id=r.event_id').get();assert.equal(r.occurred_at,now+60000);assert.equal(r.observed_at,now+60010);assert.equal(r.received_at,now+90000);assert.equal(r.ingested_at,now+100000);
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
 assert(results.every(r=>r.ingestedAt===db.prepare('SELECT ingested_at FROM measurement_ingress_recovery').get().ingested_at));
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
 assert.equal((await consumer.ingest(gap.wire)).ok,true);const receipt=db.prepare('SELECT r.*,s.ingested_at FROM measurement_ingress_receipts r JOIN measurement_ingress_recovery s ON s.event_id=r.event_id').get();assert.equal(receipt.clock_unavailable,1);assert.equal(receipt.observed_at,null);
 for(const name of ['measurement-consumer.js','measurement-lifecycle.js']){const text=await fs.readFile(new URL(name,import.meta.url),'utf8');assert(!/\bfetch\s*\(|GSX_DB|production_signals|cloudflare:|https?:\/\//.test(text));}
 const worker=await fs.readFile(new URL('goldsignalsx-worker.js',import.meta.url),'utf8');assert(!worker.includes('measurement-consumer'));db.close();
});

// Public-ingress regressions for the six independently reproduced M2 defects.
async function edited(packet,change){
 const e=JSON.parse(packet.wire);change(e);
 const {canonicalSerialize,digestPayload}=await import('./signal-evidence.js');
 e.payloadDigest=await digestPayload(canonicalSerialize({clocks:{occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt},payload:e.payload},2*1024**2));
 e.eventId=`m1:${await digestPayload(canonicalSerialize([e.producerNamespace,e.kind,e.semanticId,e.payloadDigest]))}`;
 return canonicalSerialize(e,2*1024**2);
}
function faultBinding(binding,match){
 let armed=true;
 return {...binding,prepare(sql){
  const original=binding.prepare(sql);
  return {...original,bind(...v){
   const statement=original.bind(...v);
   return {...statement,
    async run(){if(armed&&match(sql)){armed=false;throw new Error('PROJECTION_UNAVAILABLE');}return statement.run();},
    async first(){if(armed&&match(sql)){armed=false;throw new Error('POST_COMMIT_TEST_FAILURE');}return statement.first();},
    async all(){if(armed&&match(sql)){armed=false;throw new Error('RECEIPT_LOOKUP_UNAVAILABLE');}return statement.all();}
   };
  }};
 }};
}
test('M2-R1 SL + failed projection defers Official without cursor/outcomes; recovery resumes safely',async()=>{
 const {db,binding,consumer,time}=await setup();time(now+7200000);for(const p of [cycle,creation])assert.equal((await consumer.ingest(p.wire)).ok,true);
 const sl=await lifecycle(id,'sl');
 const broken=createOfflineMeasurementConsumer(faultBinding(binding,s=>s.startsWith('INSERT INTO measurement_lifecycle_projection')),{clock:()=>now+7200000});
 assert.equal((await broken.ingest(sl.wire)).projectionStatus,'PENDING');
 const before=db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id);
 const levels=created.engine.levels;
 const bars=Array.from({length:120},(_,i)=>({t:now+i*60000,o:levels.entry,h:i?levels.tp2+1:levels.entry,l:i?levels.entry:levels.sl-1,c:levels.entry,v:1,provider:'mt5'}));
 const deferred=await collectMeasurementFromProjection(binding,{asOf:now+7260000,bars,ticks:[],maxSubjects:1});assert(deferred.ok);assert.equal(deferred.deferredOfficialSubjects,1);assert(deferred.captureGap);
 assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence WHERE subject_id=?').get(id).n,0);
 assert.equal((await consumer.ingest(sl.wire)).projectionStatus,'CURRENT');
 const result=await collectMeasurementFromProjection(binding,{asOf:now+7260000,bars,ticks:[],maxSubjects:1});assert(result.ok);assert.equal(result.updated,1);
 for(const row of db.prepare('SELECT * FROM signal_outcome_evidence WHERE subject_id=?').all(id)){
  const {decodeStoredOutcome}=await import('./evidence-codec.js');const outcome=await decodeStoredOutcome(row);
  if(outcome.occurredTo!=null)assert(outcome.occurredTo<=now+60000);
  assert.notEqual(outcome.eventType,'TP2');
 }
 db.close();
});
test('M2-R1 missing/partial/conflicted Official projection remains pending without cursor writes',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);const before=db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id);
 for(const mode of ['absent','partial','conflict']){
  if(mode==='partial'){await consumer.ingest(creation.wire);db.prepare("UPDATE measurement_lifecycle_projection SET integrity_status='PARTIAL'").run();}
  if(mode==='conflict')db.prepare("UPDATE measurement_lifecycle_projection SET integrity_status='CONFLICT'").run();
  assert((await collectMeasurementFromProjection(binding,{asOf:now+86400000,bars:[],ticks:[]})).ok);
  assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);
 }db.close();
});
test('M2-R2 identical canonical evidence reconciles cycle/census/Official in either order',async()=>{
 for(const order of [[cycle,creation],[creation,cycle]]){const {db,consumer}=await setup();for(const p of order)assert((await consumer.ingest(p.wire)).ok);
  const census=packets.find(p=>p.envelope.semanticId===created.evaluationId);assert((await consumer.ingest(census.wire)).ok);
  assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_bindings WHERE evaluation_id=?').get(created.evaluationId).n,1);
  assert.equal(projection(db,id).integrity_status,'COMPLETE');db.close();}
});
test('M2-R2 cross-kind Entry/levels/direction/timeframe conflicts quarantine both delivery orders',async()=>{
 for(const mutation of [d=>d.engine.levels.entry=4200,d=>d.engine.levels.tp2+=1,d=>d.direction='sell',d=>d.timeframe='15m']){
  const bad=await edited(creation,e=>mutation(e.payload.decisionEvidence));
  for(const reverse of [false,true]){const {db,consumer}=await setup();assert((await consumer.ingest(reverse?bad:cycle.wire)).ok);
   const conflict=await consumer.ingest(reverse?cycle.wire:bad);assert.equal(conflict.status,'INTEGRITY_CONFLICT');assert.equal(conflict.conflictStatus,'RECORDED');
   assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_conflicts').get().n,1);
   if(reverse)assert.equal(projection(db,id).integrity_status,'CONFLICT');
   else {await consumer.ingest(creation.wire);assert.equal(projection(db,id).integrity_status,'CONFLICT');}
   db.close();}
 }
});
test('M2-R2 concurrent cross-kind conflicting delivery has one canonical winner, unusable projection',async()=>{
 const {db,binding}=await database();let tail=Promise.resolve();const atomic={...binding,batch(ss){const work=tail.then(()=>binding.batch(ss));tail=work.catch(()=>{});return work;}};
 const a=createOfflineMeasurementConsumer(atomic,{clock:()=>now+86400000}),b=createOfflineMeasurementConsumer(atomic,{clock:()=>now+86400000});
 const bad=await edited(creation,e=>e.payload.decisionEvidence.engine.levels.entry=4200);
 const results=await Promise.all([a.ingest(cycle.wire),b.ingest(bad)]);
 assert.equal(results.filter(r=>r.ok).length,1);assert.equal(results.filter(r=>r.status==='INTEGRITY_CONFLICT').length,1);
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_conflicts').get().n,1);
 await a.ingest(creation.wire);assert.equal(projection(db,id).integrity_status,'CONFLICT');db.close();
});
test('M2-R3 malformed pins/links/ownership/scalars reject before any receipt or evidence',async()=>{
 const mutations=[
  e=>e.payload.officialPins[0].blockIds=123,
  e=>e.payload.officialPins[0].blockIds=null,
  e=>e.payload.officialPins[0].officialId='other-owner',
  e=>e.payload.officialPins[0].blockIds=[],
  e=>e.payload.links[0].ownerId='other-cycle',
  e=>e.payload.links[0].blockIds=['missing-block'],
  e=>e.payload.records.find(r=>r.type==='decision').values.evaluated_at+=864000000,
  e=>e.payload.records.find(r=>r.type==='decision').values.candidate_key='unrelated-key',
  e=>e.payload.records.find(r=>r.type==='cycle').values.evaluated_at+=1,
  e=>e.clocks.observedAt+=1,
  e=>e.payload.records.find(r=>r.type==='decision').payload.inputManifest.primary.references[0].blockId='other-block',
 ];
 for(const mutation of mutations){const {db,consumer}=await setup();const r=await consumer.ingest(await edited(cycle,mutation));assert.equal(r.status,'REJECTED',JSON.stringify(r));
  assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,0);assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,0);db.close();}
});
test('M2-R3 exact tags and decoded finite clocks reject ambiguous/Infinity/future/invalid values',async()=>{
 const fact=await lifecycle(id,'tp1');
 for(const value of [{$unavailable:'undefined',$number:'Infinity'},{$number:'Infinity'},{$number:'NaN'},{$number:'wat'},now+86400001,-1,1.25]){
  const {db,consumer}=await setup();const wire=await edited(fact,e=>{e.payload.occurredAt=value;e.clocks.occurredAt=value;});
  assert.equal((await consumer.ingest(wire)).status,'REJECTED');assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,0);db.close();
 }
});
test('M2-R4 five-second persistence delay establishes availability after commit, duplicate preserves fence',async()=>{
 const {db,binding}=await database();let time=now+100000;
 const slow={...binding,async batch(ss){time+=5000;return binding.batch(ss);}};
 const consumer=createOfflineMeasurementConsumer(slow,{clock:()=>time}),fact=await lifecycle(id,'tp1');
 const r=await consumer.ingest(fact.wire,{receivedAt:now+90000});assert(r.ok);assert.equal(r.ingestedAt,now+105000);
 assert.equal(db.prepare('SELECT processing_started_at FROM measurement_ingress_receipts').get().processing_started_at,now+100000);
 assert.equal(projection(db,id).available_at,now+105000);time+=100000;
 assert.equal((await consumer.ingest(fact.wire)).ingestedAt,now+105000);
 await rebuildLifecycleProjection(slow,id,{rebuiltAt:time});assert.equal(projection(db,id).available_at,now+105000);db.close();
});
test('M2-R4 failed availability-fence write keeps evidence unavailable until idempotent replay',async()=>{
 const {db,binding}=await database();const fact=await lifecycle(id,'tp1');
 const broken=createOfflineMeasurementConsumer(faultBinding(binding,s=>s.startsWith('UPDATE measurement_ingress_recovery')),{clock:()=>now+100000});
 const result=await broken.ingest(fact.wire);assert(result.ok);assert.equal(result.durable,true);assert.equal(result.acknowledgementStatus,'RECONCILIATION_PENDING');
 assert.equal(db.prepare('SELECT ingested_at FROM measurement_ingress_recovery').get().ingested_at,null);assert.equal(projection(db,id),undefined);
 const fixed=createOfflineMeasurementConsumer(binding,{clock:()=>now+200000});assert.equal((await fixed.ingest(fact.wire)).ingestedAt,now+200000);assert.equal(projection(db,id).available_at,now+200000);db.close();
});
test('M2-R5 post-commit receipt read failure is durable/pending, retry remains one immutable fact',async()=>{
 const {db,binding}=await database();
 const broken=createOfflineMeasurementConsumer(faultBinding(binding,s=>s.startsWith('SELECT r.*,s.ingested_at,s.processing_status')),{clock:()=>now+100000});
 const fact=await lifecycle(id,'tp1'),r=await broken.ingest(fact.wire);assert(r.ok);assert.equal(r.durable,true);assert.equal(r.acknowledgementStatus,'RECONCILIATION_PENDING');assert.notEqual(r.status,'REJECTED');
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_lifecycle_facts').get().n,1);
 const fixed=createOfflineMeasurementConsumer(binding,{clock:()=>now+200000});assert.equal((await fixed.ingest(fact.wire)).status,'DUPLICATE');assert.equal(db.prepare('SELECT count(*) n FROM measurement_lifecycle_facts').get().n,1);db.close();
});
test('M2-R5 lost commit acknowledgement is explicitly unknown and recoverable',async()=>{
 const {db,binding}=await database();let once=true;
 const uncertain={...binding,async batch(ss){const r=await binding.batch(ss);if(once){once=false;throw new Error('COMMIT_ACK_LOST');}return r;}};
 const consumer=createOfflineMeasurementConsumer(uncertain,{clock:()=>now+100000});const fact=await lifecycle(id,'tp1');
 const r=await consumer.ingest(fact.wire);assert.equal(r.status,'DURABILITY_UNKNOWN');assert.equal(r.durable,null);assert(r.retryable);
 assert.equal((await consumer.ingest(fact.wire)).status,'DUPLICATE');assert.equal(db.prepare('SELECT count(*) n FROM measurement_lifecycle_facts').get().n,1);db.close();
});
test('M2-R6 4032-subject capacity preserves census + pending gap; replay initializes exactly once',async()=>{
 const {db,consumer}=await setup();const fill=db.prepare("INSERT INTO signal_measurement_state(subject_id,state_version,payload_json,updated_at) VALUES(?,1,'{}',?)");
 for(let i=0;i<4032;i++)fill.run('capacity:'+i,now);
 const first=await consumer.ingest(cycle.wire);assert(first.ok);assert.equal(first.durable,true);assert.equal(first.processingStatus,'PENDING');assert(first.captureGap);assert(first.processingCaptureGaps.length>0);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,7);
 assert.equal(db.prepare("SELECT processing_status FROM measurement_ingress_recovery WHERE event_id=?").get(cycle.envelope.eventId).processing_status,'PENDING');
 db.prepare("DELETE FROM signal_measurement_state WHERE subject_id LIKE 'capacity:%'").run();
 const repaired=await consumer.ingest(cycle.wire);assert.equal(repaired.status,'DUPLICATE');assert.equal(repaired.processingStatus,'CURRENT');assert.equal(repaired.processingCaptureGaps.length,0);
 const before=db.prepare('SELECT * FROM signal_measurement_state ORDER BY subject_id').all();assert.equal(before.length,7);
 await consumer.ingest(cycle.wire);assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state ORDER BY subject_id').all(),before);
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,1);db.close();
});

test('M2-R2 changed census is quarantined before historical writer preflight rejects it',async()=>{
 const {db,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);
 const census=packets.find(p=>p.envelope.kind==='EVALUATION_CENSUS'&&p.envelope.semanticId===created.evaluationId);
 const wire=await edited(census,e=>{e.payload.decisionEvidence.engine.levels.entry=4200;});
 // Keep the compact census consistent: the defect is cross-kind evidence, not
 // a counterfeit compact census. Use the established codec, not a new reducer.
 const {censusSnapshot}=await import('./evidence-codec.js');
 const packet={wire};const valid=await edited(packet,e=>{e.payload.census=censusSnapshot(e.payload.decisionEvidence);});
 const result=await consumer.ingest(valid);assert.equal(result.status,'INTEGRITY_CONFLICT');assert.equal(result.conflictStatus,'RECORDED');
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_conflicts').get().n,1);assert.equal(projection(db,id).integrity_status,'CONFLICT');db.close();
});

test('M2-R4 unfenced/old-availability cache cannot become Official collection truth',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);
 const before=db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id);
 // A rebuildable cache from an older availability contract is insufficient,
 // even when its count and COMPLETE state happen to match current facts.
 db.prepare('UPDATE measurement_lifecycle_projection SET available_at=? WHERE signal_id=?').run(now,id);
 assert.equal((await collectMeasurementFromProjection(binding,{asOf:now+86400000,bars:[],ticks:[]})).deferredOfficialSubjects,1);
 assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);
 await rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000});
 assert.throws(()=>db.prepare('DELETE FROM measurement_ingress_recovery WHERE event_id=?').run(creation.envelope.eventId),/immutable/);
 // Simulate an existing pre-fence local receipt, without changing its immutable
 // evidence or cache. Only the local test fixture bypasses the new delete guard.
 db.exec('DROP TRIGGER measurement_availability_no_delete');db.prepare('DELETE FROM measurement_ingress_recovery WHERE event_id=?').run(creation.envelope.eventId);
 assert.equal((await collectMeasurementFromProjection(binding,{asOf:now+86400000,bars:[],ticks:[]})).deferredOfficialSubjects,1);
 assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);db.close();
});

test('M2-R5 unavailable receipt lookup is retryable, not a false rejection of an accepted event',async()=>{
 const {db,binding,consumer}=await setup();const fact=await lifecycle(id,'tp1');assert((await consumer.ingest(fact.wire)).ok);
 const broken=createOfflineMeasurementConsumer(faultBinding(binding,s=>s.startsWith('SELECT r.*,s.ingested_at FROM measurement_ingress_receipts')),{clock:()=>now+86400000});
 const result=await broken.ingest(fact.wire);assert.equal(result.status,'RETRYABLE');assert.equal(result.durable,null);assert(result.retryable);
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_lifecycle_facts').get().n,1);assert.equal((await consumer.ingest(fact.wire)).status,'DUPLICATE');db.close();
});

// Final residuals: exercise real public operations with deterministic interleavings.
function interleaveBinding(binding,{match,operation,method='first',afterRead=false}){
 let once=true;
 return {...binding,prepare(sql){const original=binding.prepare(sql);return {...original,bind(...values){const statement=original.bind(...values);
  return {...statement,async [method](){if(once&&match(sql)){once=false;const prior=afterRead?await statement[method]():null;await operation();if(afterRead)return prior;}return statement[method]();}};
 }};}};
}
function lateTp2Bars(){const levels=created.engine.levels;return Array.from({length:120},(_,i)=>({t:now+i*60000,o:levels.entry,h:i?levels.tp2+1:levels.entry,l:levels.entry,c:levels.entry,v:1,provider:'mt5'}));}
async function badCreation(){return edited(creation,e=>e.payload.decisionEvidence.engine.levels.entry=4200);}

test('M2 residual R1 selected active Official is deferred atomically after SL and failed refresh',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);
 const before=db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),sl=await lifecycle(id,'sl');
 const broken=createOfflineMeasurementConsumer(faultBinding(binding,s=>s.startsWith('INSERT INTO measurement_lifecycle_projection')),{clock:()=>now+86400000});
 const racing=interleaveBinding(binding,{match:s=>s==='SELECT * FROM measurement_rich_evidence WHERE evidence_id=?',operation:async()=>assert.equal((await broken.ingest(sl.wire)).projectionStatus,'PENDING')});
 const result=await collectMeasurementFromProjection(racing,{asOf:now+7260000,bars:lateTp2Bars(),ticks:[],maxSubjects:1});
 assert(result.ok);assert.equal(result.updated,0);assert.equal(result.deferredOfficialSubjects,1);assert(result.captureGap);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence WHERE subject_id=?').get(id).n,0);
 assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_collector_guards').get().n,0);
 assert.equal((await consumer.ingest(sl.wire)).projectionStatus,'CURRENT');
 assert((await collectMeasurementFromProjection(binding,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1})).ok);
 const {decodeStoredOutcome}=await import('./evidence-codec.js');
 for(const row of db.prepare('SELECT * FROM signal_outcome_evidence WHERE subject_id=?').all(id)){const p=await decodeStoredOutcome(row);assert.notEqual(p.eventType,'TP2');if(p.occurredTo!=null)assert(p.occurredTo<=now+60000);}
 const count=db.prepare('SELECT count(*) n FROM signal_outcome_evidence WHERE subject_id=?').get(id).n;
 assert((await collectMeasurementFromProjection(binding,{asOf:now+86400002,bars:lateTp2Bars(),ticks:[],maxSubjects:1})).ok);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence WHERE subject_id=?').get(id).n,count);db.close();
});
test('M2 residual R1 conflict arriving after selection defers outcomes and cursor',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const bad=await badCreation();
 const before=db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id);
 const racing=interleaveBinding(binding,{match:s=>s==='SELECT * FROM measurement_rich_evidence WHERE evidence_id=?',operation:async()=>assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED')});
 const result=await collectMeasurementFromProjection(racing,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1});
 assert(result.ok);assert.equal(result.updated,0);assert.equal(result.deferredOfficialSubjects,1);
 assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence WHERE subject_id=?').get(id).n,0);db.close();
});
test('M2 residual R1 mutation guard detects a fact arriving immediately before DB batch',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const before=db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id);
 let once=true;const racing={...binding,prepare(sql){const original=binding.prepare(sql);return {...original,bind(...v){return {...original.bind(...v),testSql:sql};}};},async batch(statements){
  if(once&&statements[0]?.testSql?.startsWith('INSERT INTO measurement_collector_guards')){once=false;assert((await consumer.ingest((await lifecycle(id,'sl')).wire)).ok);}return binding.batch(statements);
 }};
 const result=await collectMeasurementFromProjection(racing,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1});
 assert.equal(once,false);assert(result.ok);assert.equal(result.updated,0);assert.equal(result.deferredOfficialSubjects,1);assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence WHERE subject_id=?').get(id).n,0);db.close();
});
test('M2 residual R2 stale rebuild cannot overwrite committed quarantine',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const bad=await badCreation();
 const racing=interleaveBinding(binding,{match:s=>s.startsWith('SELECT c.evaluation_id FROM measurement_decision_conflicts'),method:'all',afterRead:true,operation:async()=>{
  assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED');assert.equal(projection(db,id).integrity_status,'CONFLICT');
 }});
 const result=await createOfflineMeasurementConsumer(racing,{clock:()=>now+86400000}).ingest(creation.wire);
 assert.notEqual(result.projectionStatus,'CURRENT');assert.equal(projection(db,id).integrity_status,'CONFLICT');
 assert.equal((await consumer.ingest(creation.wire)).projectionStatus,'CONFLICT');db.close();
});
test('M2 residual R2 CAS fences conflict appearing after all rebuild reads before publication',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const bad=await badCreation();
 let once=true;const racing={...binding,prepare(sql){const p=binding.prepare(sql);return {...p,bind(...v){return {...p.bind(...v),publication:sql.startsWith('INSERT INTO measurement_lifecycle_projection')};}};},async batch(statements){if(once&&statements[0]?.publication){once=false;assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED');}return binding.batch(statements);}};
 const result=await createOfflineMeasurementConsumer(racing,{clock:()=>now+86400000}).ingest(creation.wire);
 assert.equal(result.projectionStatus,'PENDING');assert.equal(projection(db,id).integrity_status,'CONFLICT');db.close();
});
test('M2 residual R2 failed immutable conflict marker leaves durable pending quarantine',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const bad=await badCreation();
 const broken=createOfflineMeasurementConsumer(faultBinding(binding,s=>s.startsWith('INSERT INTO measurement_decision_conflicts')),{clock:()=>now+86400000});
 const r=await broken.ingest(bad);assert(r.retryable);assert.equal(r.conflictStatus,'RECONCILIATION_PENDING');assert.equal(r.projectionStatus,'PENDING');
 assert.equal(db.prepare('SELECT state FROM measurement_decision_quarantine').get().state,'PENDING');assert.equal(projection(db,id).integrity_status,'CONFLICT');
 assert.equal((await consumer.ingest(creation.wire)).projectionStatus,'PENDING');
 assert.equal((await collectMeasurementFromProjection(binding,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1})).deferredOfficialSubjects,1);
 assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED');assert.equal(db.prepare('SELECT state FROM measurement_decision_quarantine').get().state,'CONFLICT');
 assert.equal((await consumer.ingest(creation.wire)).projectionStatus,'CONFLICT');db.close();
});
test('M2 residual R2 failed quarantine transaction never claims successful quarantine',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);
 const unavailable={...binding,async batch(){throw new Error('QUARANTINE_STORAGE_UNAVAILABLE');}};
 const r=await createOfflineMeasurementConsumer(unavailable,{clock:()=>now+86400000}).ingest(await badCreation());
 assert(r.retryable);assert.equal(r.conflictStatus,'RECONCILIATION_PENDING');assert.equal(r.quarantineDurability,'UNKNOWN_OR_PENDING');assert.equal(r.projectionStatus,'PENDING');
 assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_conflicts').get().n,0);
 assert.equal((await consumer.ingest(creation.wire)).projectionStatus,'PENDING');
 await assert.rejects(rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400001}),/capability_pending/);
 const before=db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id);
 const collected=await collectMeasurementFromProjection(binding,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1});assert.equal(collected.deferredOfficialSubjects,1);
 assert.deepEqual(db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').get(id),before);
 assert.equal((await consumer.ingest(await badCreation())).conflictStatus,'RECORDED');assert.equal(projection(db,id).integrity_status,'CONFLICT');db.close();
});
test('M2 residual R2 identical concurrent rebuilds remain deterministic and usable',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(creation.wire);
 const results=await Promise.all([rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000}),rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000})]);
 assert.deepEqual(results[0],results[1]);assert.equal(projection(db,id).integrity_status,'COMPLETE');db.close();
});
test('M2 residual R3 numeric/contradictory/malformed nested links reject without writes',async()=>{
 const packet=await confirmation('primary','child');
 const bad=[e=>e.payload.link=123,e=>e.payload.link.signalId='other',e=>e.payload.link.primarySignalId=123,e=>e.payload.linkPersistence={status:'SUCCEEDED'},e=>e.payload.link={type:'later-confirmation',confirmationSignalId:'child',primarySignalId:123,tf:'5m',side:'buy',conf:80,score:80,signalBarTs:now-300000,confirmedAt:now+30}];
 for(const mutate of bad){const {db,consumer}=await setup();const r=await consumer.ingest(await edited(packet,mutate));assert.equal(r.status,'REJECTED');
  assert.equal(db.prepare('SELECT count(*) n FROM measurement_ingress_receipts').get().n,0);assert.equal(db.prepare('SELECT count(*) n FROM measurement_lifecycle_facts').get().n,0);db.close();}
});
test('M2 residual R3 full captured links and unavailable/failure states remain exact',async()=>{
 const packet=await confirmation('primary','child');
 const full={type:'later-confirmation',confirmationSignalId:'child',primarySignalId:'primary',tf:'5m',side:'buy',conf:80,score:75,signalBarTs:now-300000,confirmedAt:now+30};
 for(const [link,persistence] of [[packet.envelope.payload.link,'SUCCEEDED'],[full,'SUCCEEDED'],[full,'FAILED'],[null,null],[{$unavailable:'undefined'},{$unavailable:'undefined'}]]){
  const {db,consumer}=await setup();const wire=await edited(packet,e=>{e.payload.link=link;e.payload.linkPersistence=persistence;});assert((await consumer.ingest(wire)).ok);
  const fact=await decodeStoredEvidence(db.prepare('SELECT * FROM measurement_lifecycle_facts').get());
  const {parseEvidence,canonicalSerialize}=await import('./signal-evidence.js');assert.equal(canonicalSerialize(fact),canonicalSerialize(parseEvidence(wire).payload));
  const p=await decodeStoredEvidence(projection(db,'child'));assert.equal(p.integrityStatus,'PARTIAL');assert.equal(p.status,null);assert.equal(p.owningPrimaryId,'primary');db.close();
 }
});
test('M2 residual migration upgrades populated 0005 and fences old caches until rebuild',async()=>{
 const {DatabaseSync}=await import('node:sqlite');const db=new DatabaseSync(':memory:');db.exec('PRAGMA foreign_keys=ON');
 for(const file of ['0001_measurement_evidence.sql','0002_measurement_storage_tiers.sql','0003_measurement_dependency_closure.sql','0004_measurement_ingress.sql','0005_measurement_ingress_recovery.sql'])db.exec(await fs.readFile(new URL('migrations/'+file,import.meta.url),'utf8'));
 const {transactionalBinding}=await import('./test-fixtures/b1-sqlite.mjs'),binding=transactionalBinding(db);
 // Populate the previous schema with its exact previous consumer/projection.
 const urls=[];try{
  const root=new URL('./',import.meta.url);
  for(const [original,name] of [['measurement-lifecycle.js','.m2-upgrade-lifecycle.mjs'],['measurement-consumer.js','.m2-upgrade-consumer.mjs']]){
   const url=new URL(name,root);urls.push(url);let source=execFileSync('git',['show',`abe2a408f8e8fdf2292df31646aac4960458a9dd:${original}`],{encoding:'utf8'});
   if(original==='measurement-consumer.js')source=source.replace("'./measurement-lifecycle.js'","'./.m2-upgrade-lifecycle.mjs'");await fs.writeFile(url,source);
  }
  const old=await import(urls[1].href);const consumer=old.createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});assert((await consumer.ingest(cycle.wire)).ok);assert((await consumer.ingest(creation.wire)).ok);
  const before=db.prepare('SELECT * FROM measurement_lifecycle_facts').all();
  db.exec(await fs.readFile(new URL('migrations/0006_measurement_commit_fences.sql',import.meta.url),'utf8'));
  assert.deepEqual(db.prepare('SELECT * FROM measurement_lifecycle_facts').all(),before);assert.equal(projection(db,id).source_revision,0);
  assert.equal((await collectMeasurementFromProjection(binding,{asOf:now+86400001,bars:[],ticks:[],maxSubjects:1})).deferredOfficialSubjects,1);
  await rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400001});assert(projection(db,id).source_revision>0);assert.equal(db.prepare('PRAGMA foreign_key_check').all().length,0);
 }finally{await Promise.all(urls.map(url=>fs.unlink(url)));db.close();}
});
test('M2 residual R2 failed quarantine blocks an older rebuild through shared offline capability',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(creation.wire);const before=projection(db,id),bad=await badCreation();
 const unavailable={...binding,async batch(){throw new Error('QUARANTINE_UNAVAILABLE');}};
 const racing=interleaveBinding(binding,{match:s=>s.startsWith('SELECT c.evaluation_id FROM measurement_decision_conflicts'),method:'all',afterRead:true,operation:async()=>{
  const r=await createOfflineMeasurementConsumer(unavailable,{clock:()=>now+86400000}).ingest(bad);assert.equal(r.projectionStatus,'PENDING');
 }});
 const r=await createOfflineMeasurementConsumer(racing,{clock:()=>now+86400000}).ingest(creation.wire);assert.equal(r.projectionStatus,'PENDING');assert(r.retryable);
 assert.deepEqual(projection(db,id),before);assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED');db.close();
});
test('M2 residual R2 SQL publication fence independently rejects stale or quarantined COMPLETE',async()=>{
 const {db,consumer}=await setup();await consumer.ingest(creation.wire);const row=projection(db,id);await consumer.ingest(await badCreation());
 assert.throws(()=>db.prepare("UPDATE measurement_lifecycle_projection SET source_revision=?,payload_blob=?,integrity_status='COMPLETE' WHERE signal_id=?").run(row.source_revision,row.payload_blob,id),/rebuild_raced/);
 assert.equal(projection(db,id).integrity_status,'CONFLICT');db.close();
});

function heldCollector(binding){
 let release;const gate=new Promise(resolve=>release=resolve),waiters=[];
 const paused={...binding,prepare(sql){const p=binding.prepare(sql);return {...p,bind(...v){return {...p.bind(...v),heldCollectorGuard:sql.startsWith('INSERT INTO measurement_collector_guards')};}};},async batch(ss){if(ss[0]?.heldCollectorGuard){waiters.push(true);await gate;}return binding.batch(ss);}};
 return {paused,release,async selected(n=1){for(let i=0;i<1000&&waiters.length<n;i++)await new Promise(resolve=>setImmediate(resolve));assert.equal(waiters.length,n,'collector reached dispatch after capability check');}};
}
function irreversibleSnapshot(db){return Object.fromEntries(['signal_outcome_evidence','signal_measurement_state','measurement_final_results'].map(table=>[table,db.prepare(`SELECT * FROM ${table} WHERE subject_id=? ORDER BY 1`).all(id)]));}
for(const recoverBeforeRelease of [false,true])test(`M2 final R1 two dispatched stale batches fail after nondurable intent; recovery before release=${recoverBeforeRelease}`,async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);
 const before=irreversibleSnapshot(db),held=heldCollector(binding),options={asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1};
 const a=collectMeasurementFromProjection(held.paused,options),b=collectMeasurementFromProjection(held.paused,options);await held.selected(2);
 const broken={...binding,async batch(){throw new Error('INTENT_STORAGE_FAILURE');}};
 const bad=await badCreation(),r=await createOfflineMeasurementConsumer(broken,{clock:()=>now+86400000}).ingest(bad);
 assert.equal(r.conflictStatus,'RECONCILIATION_PENDING');assert.equal(r.quarantineDurability,'UNKNOWN_OR_PENDING');assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_quarantine').get().n,0);
 if(recoverBeforeRelease)assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED');
 held.release();for(const result of await Promise.all([a,b])){assert(result.ok);assert.equal(result.updated,0);assert.equal(result.deferredOfficialSubjects,1);}
 assert.deepEqual(irreversibleSnapshot(db),before);assert.equal(db.prepare('SELECT count(*) n FROM measurement_collector_guards').get().n,0);
 if(!recoverBeforeRelease)assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED');
 const count=db.prepare('SELECT count(*) n FROM measurement_decision_conflicts').get().n;await consumer.ingest(bad);assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_conflicts').get().n,count);assert.equal(db.prepare('PRAGMA foreign_key_check').all().length,0);db.close();
});
test('M2 final R2 validated contradiction skips failed repeated binding lookup and persists suspension before marker failure',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);
 let redundant=0;const broken=faultBinding({...binding,prepare(sql){if(sql==='SELECT payload_digest,official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=?')redundant++;return binding.prepare(sql);}},s=>s.startsWith('INSERT INTO measurement_decision_conflicts'));
 const r=await createOfflineMeasurementConsumer(broken,{clock:()=>now+86400000}).ingest(await badCreation());assert.equal(redundant,1);assert.equal(r.projectionStatus,'PENDING');
 assert.equal(db.prepare('SELECT state FROM measurement_decision_quarantine').get().state,'PENDING');assert.equal(projection(db,id).integrity_status,'CONFLICT');
 assert.equal((await consumer.ingest(creation.wire)).projectionStatus,'PENDING');await assert.rejects(rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000}),/quarantine_pending/);
 const before=irreversibleSnapshot(db);assert.equal((await collectMeasurementFromProjection(binding,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1})).deferredOfficialSubjects,1);assert.deepEqual(irreversibleSnapshot(db),before);
 assert.equal((await consumer.ingest(await badCreation())).conflictStatus,'RECORDED');db.close();
});
test('M2 final R2 nondurable suspension prevents duplicate CURRENT, rebuild and collection until deterministic reconciliation',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const bad=await badCreation();
 let richer=0;const broken={...binding,prepare(sql){if(sql==='SELECT payload_digest,official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=?'){richer++;if(richer>1)throw new Error('REPEATED_BINDING_LOOKUP_FAILURE');}return binding.prepare(sql);},async batch(){throw new Error('SUSPENSION_PERSISTENCE_FAILURE');}};
 const r=await createOfflineMeasurementConsumer(broken,{clock:()=>now+86400000}).ingest(bad);assert.equal(r.projectionStatus,'PENDING');assert.equal(r.durable,false);assert.equal(richer,1);
 for(const packet of [creation,cycle])assert.equal((await consumer.ingest(packet.wire)).projectionStatus,'PENDING');
 await assert.rejects(rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000}),/capability_pending/);
 const before=irreversibleSnapshot(db);const collected=await collectMeasurementFromProjection(binding,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1});assert.equal(collected.deferredOfficialSubjects,1);assert.deepEqual(irreversibleSnapshot(db),before);
 assert.equal((await consumer.ingest(bad)).conflictStatus,'RECORDED');assert.equal(db.prepare('SELECT state FROM measurement_decision_quarantine').get().state,'CONFLICT');await consumer.ingest(bad);assert.equal(db.prepare('SELECT count(*) n FROM measurement_decision_conflicts').get().n,1);db.close();
});
test('M2 final R1 paused Official dispatch does not block unrelated candidate progress',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const held=heldCollector(binding);
 const work=collectMeasurementFromProjection(held.paused,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1});await held.selected();
 const broken={...binding,async batch(){throw new Error('INTENT_STORAGE_FAILURE');}};await createOfflineMeasurementConsumer(broken,{clock:()=>now+86400000}).ingest(await badCreation());
 // Local suspension is subject-specific: other candidates commit while the
 // original Official dispatch is still held at the adapter boundary.
 const other=await collectMeasurementFromProjection(binding,{asOf:now+86400002,bars:lateTp2Bars(),ticks:[],maxSubjects:8});assert(other.ok);assert(other.updated>0);assert.equal(held.paused.measurementCommitChecks,true);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence WHERE subject_id=?').get(id).n,0);
 held.release();const result=await work;assert.equal(result.updated,0);assert.equal(result.deferredOfficialSubjects,1);db.close();
});
test('M2 final R1 missing transaction-check adapter fails closed before Official evidence',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const before=irreversibleSnapshot(db);
 const result=await collectMeasurementFromProjection({...binding,measurementCommitChecks:false},{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1});assert.equal(result.ok,false);assert.equal(result.error,'measurement_commit_adapter_required');assert.deepEqual(irreversibleSnapshot(db),before);db.close();
});
test('M2 final R1 same-subject healthy concurrent collectors complete without a coordination lock',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);
 const options={asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1};
 const results=await Promise.all([collectMeasurementFromProjection(binding,options),collectMeasurementFromProjection(binding,options)]);for(const r of results){assert(r.ok);assert.equal(r.updated,1);}
 const rows=db.prepare('SELECT event_id FROM signal_outcome_evidence WHERE subject_id=?').all(id);assert(rows.length>0);assert.equal(new Set(rows.map(r=>r.event_id)).size,rows.length);assert.equal(db.prepare('SELECT count(*) n FROM measurement_collector_guards').get().n,0);db.close();
});
test('M2 final R1 suspension after SQL mutations rolls back the whole transaction before COMMIT',async()=>{
 const {db,binding,consumer}=await setup();await consumer.ingest(cycle.wire);await consumer.ingest(creation.wire);const bad=await badCreation(),before=irreversibleSnapshot(db);
 const broken={...binding,async batch(){throw new Error('QUARANTINE_FAILURE_INSIDE_INFLIGHT_TRANSACTION');}};const rejecting=createOfflineMeasurementConsumer(broken,{clock:()=>now+86400000});let fired=false;
 const racing={...binding,prepare(sql){const p=binding.prepare(sql);return {...p,bind(...v){const statement=p.bind(...v);return {...statement,async run(){const result=await statement.run();if(!fired&&sql.startsWith('INSERT INTO measurement_outcome_records')){fired=true;assert.equal((await rejecting.ingest(bad)).projectionStatus,'PENDING');}return result;}};}};}};
 const r=await collectMeasurementFromProjection(racing,{asOf:now+86400001,bars:lateTp2Bars(),ticks:[],maxSubjects:1});assert(fired);assert(r.ok);assert.equal(r.updated,0);assert.equal(r.deferredOfficialSubjects,1);assert.deepEqual(irreversibleSnapshot(db),before);db.close();
});
