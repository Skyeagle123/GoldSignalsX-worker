import assert from 'node:assert/strict';
import {test} from 'node:test';
import {database,cyclePackets,lifecycle,now} from './test-fixtures/b1-m2.mjs';
import {createOfflineMeasurementConsumer} from './measurement-consumer.js';
import {collectMeasurementFromProjection} from './measurement-collector.js';
import {wrapOfflineMeasurementAdapter} from './measurement-adapter.js';
import {canonicalSerialize,digestPayload} from './signal-evidence.js';
import {censusSnapshot} from './evidence-codec.js';
import {buildMeasurementEnvelope} from './measurement-envelope.js';
import {loadM1Workers,completeJournal,provenance} from './test-fixtures/b1-m1.mjs';
import {prepareCycleEnvelopes} from './measurement-producer.js';
import * as sm from './signal-measurement.js';
import * as engine from './signal-engine.js';
import {fragmentMeasurementPacket,validateTransportFragment,reassembleMeasurementFragments,digestTransportBytes,fragmentIdentity,FRAGMENT_DATA_BYTES,MAX_EVENT_BYTES,MAX_FRAGMENTS,serializedBytes} from './measurement-transport-fragment.js';
import {createOfflineMultipartTransport,createMockDurableTransportStore,createVolatileTransportStore,classifyMeasurementDelivery,MOCK_RETENTION_MS} from './measurement-transport-multipart.js';

const packets=await cyclePackets(),cycle=packets[0],candidate=packets.find(p=>p.envelope.kind==='EVALUATION_CENSUS'&&p.envelope.payload.decisionEvidence.kind==='CANDIDATE');
const small=await lifecycle('30m:1790865000000:buy','tp1'),other=await lifecycle('30m:1790865000000:buy','tp2');
const options={receivedAt:now+86400000};
const frames=await fragmentMeasurementPacket(cycle,options),smallFrames=await fragmentMeasurementPacket(small,options),otherFrames=await fragmentMeasurementPacket(other,options);
const id=cycle.envelope.eventId,smallId=small.envelope.eventId;
const accepted={ok:true,status:'ACCEPTED',durable:true};
function gate(){let release,ready;return {wait:new Promise(r=>release=r),arrived:new Promise(r=>ready=r),release:()=>release(),ready:()=>ready()};}
function runtime(extra={}){const calls=[],consumer={async ingest(wire){calls.push(wire);return accepted;}};return {calls,transport:createOfflineMultipartTransport({consumer,clock:()=>now+86400000,...extra})};}
async function deliver(t,fs=frames){let result;for(const f of fs)result=await t.receive(f);return result;}
async function reframed(raw,source=JSON.parse(smallFrames[0])){
 const bytes=new TextEncoder().encode(raw),count=Math.ceil(bytes.length/FRAGMENT_DATA_BYTES),wireDigest=await digestTransportBytes(bytes),result=[];
 for(let ordinal=0;ordinal<count;ordinal++){
  const chunk=bytes.subarray(ordinal*FRAGMENT_DATA_BYTES,(ordinal+1)*FRAGMENT_DATA_BYTES),f={...source,totalBytes:bytes.length,count,ordinal,wireDigest,fragmentBytes:chunk.length,fragmentDigest:await digestTransportBytes(chunk),data:Buffer.from(chunk).toString('base64')};
  f.fragmentId='m3:'+await fragmentIdentity(f);result.push(canonicalSerialize(f,60000));
 }return result;
}
async function conflictingCandidate(){const e=JSON.parse(candidate.wire);e.payload.decisionEvidence.engine.levels.entry=4200;e.payload.census=censusSnapshot(e.payload.decisionEvidence);
 e.payloadDigest=await digestPayload(canonicalSerialize({clocks:{occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt},payload:e.payload},MAX_EVENT_BYTES));
 e.eventId='m1:'+await digestPayload(canonicalSerialize([e.producerNamespace,e.kind,e.semanticId,e.payloadDigest]));return {wire:canonicalSerialize(e,MAX_EVENT_BYTES)};
}

test('M3 deterministic <=60,000-byte framing preserves every original wire byte',async()=>{
 assert.deepEqual(await fragmentMeasurementPacket(cycle,options),frames);assert(frames.length>1);assert.equal(MAX_FRAGMENTS,50);
 for(const f of frames){assert(serializedBytes(f)<=60000);await validateTransportFragment(f);}
 const manifest=JSON.parse(frames[0]),pieces=new Map(frames.map(f=>{const x=JSON.parse(f);return [x.ordinal,f];}));
 assert.equal(await reassembleMeasurementFragments(manifest,pieces,options),cycle.wire);
 const {transport,calls}=runtime();await deliver(transport);assert.deepEqual(calls,[cycle.wire]);
});
test('M3 fragment identity ignores receipt/retry/preparation time; exact-wire mismatch conflicts',async()=>{
 const e=JSON.parse(small.wire);e.clocks.preparedAt+=1;const changed=await fragmentMeasurementPacket({wire:canonicalSerialize(e,MAX_EVENT_BYTES)},options);
 assert.equal(JSON.parse(changed[0]).fragmentId,JSON.parse(smallFrames[0]).fragmentId);
 const {transport,calls}=runtime();await deliver(transport,smallFrames);const conflict=await transport.receive(changed[0]);
 assert.equal(conflict.classification,'INTEGRITY_CONFLICT');assert.equal(calls.length,1);assert.equal(transport.inspect(smallId).status,'ACKNOWLEDGED');
});
for(const order of ['reverse','last-first','random'])test(`M3 ${order} delivery completes exactly once`,async()=>{
 const fs=frames.map((f,i)=>({f,i,key:((i+7)*2654435761)>>>0}));
 if(order==='reverse')fs.reverse();if(order==='last-first')fs.unshift(fs.pop());if(order==='random')fs.sort((a,b)=>a.key-b.key);
 const {transport,calls}=runtime();assert.equal((await deliver(transport,fs.map(x=>x.f))).status,'ACKNOWLEDGED');assert.deepEqual(calls,[cycle.wire]);
});
test('M3 interleaved independent events complete without mixing manifests',async()=>{
 const {transport,calls}=runtime();await transport.receive(frames.at(-1));await deliver(transport,otherFrames);await deliver(transport,frames.slice(0,-1).reverse());
 assert.equal(calls.length,2);assert.deepEqual(new Set(calls),new Set([cycle.wire,other.wire]));
});
for(const keep of ['first','last','missing-one','missing-many','duplicate-subset'])test(`M3 incomplete ${keep} exposes exact missing ordinals and never calls M2`,async()=>{
 const {transport,calls}=runtime();let fs=keep==='first'?[frames[0]]:keep==='last'?[frames.at(-1)]:keep==='missing-one'?frames.slice(0,-1):frames.filter((_,i)=>i%2===0);
 if(keep==='duplicate-subset')fs=[...fs,...fs];await deliver(transport,fs);await transport.receive(otherFrames[0]);
 const pending=transport.inspect(id);assert.equal(pending.status,'STAGING');assert.equal(pending.missingOrdinals.length,frames.length-new Set(fs).size);assert.equal(calls.length,1); // independent complete event only
 assert.equal(calls[0],other.wire);
});
test('M3 identical fragment duplicates do not amplify staging or completion',async()=>{
 const {transport,calls}=runtime();await transport.receive(frames[0]);const bytes=transport.metrics().stagingBytes;
 await Promise.all(Array.from({length:25},()=>transport.receive(frames[0])));assert.equal(transport.metrics().stagingBytes,bytes);
 await deliver(transport,frames.slice(1));await deliver(transport);assert.equal(calls.length,1);assert(transport.metrics().duplicateFragments>=25);
});
test('M3 concurrent final-fragment and duplicate arrivals share one per-event delivery',async()=>{
 const g=gate();let calls=0;const t=createOfflineMultipartTransport({clock:()=>now+86400000,consumer:{async ingest(){calls++;g.ready();await g.wait;return accepted;}}});
 await deliver(t,frames.slice(0,-1));const work=Array.from({length:12},()=>t.receive(frames.at(-1)));await g.arrived;assert.equal(calls,1);g.release();
 for(const r of await Promise.all(work))assert.equal(r.status,'ACKNOWLEDGED');assert.equal(calls,1);
});
test('M3 same ordinal with conflicting bytes is rejected without replacing original',async()=>{
 const {transport,calls}=runtime();await transport.receive(frames[0]);const f=JSON.parse(frames[0]),bytes=Buffer.from(f.data,'base64');bytes[0]^=1;
 f.data=bytes.toString('base64');f.fragmentDigest=await digestTransportBytes(bytes);const result=await transport.receive(canonicalSerialize(f,60000));
 assert.equal(result.classification,'INTEGRITY_CONFLICT');assert.equal(transport.inspect(id).receivedFragments,1);
 await deliver(transport,frames.slice(1));assert.deepEqual(calls,[cycle.wire]);
});
const mutations={version:f=>f.version=2,namespace:f=>f.namespace='other',event:f=>f.eventId='invalid',ordinal:f=>f.ordinal=-1,sparse:f=>f.ordinal=49,count:f=>f.count=1e9,total:f=>f.totalBytes=MAX_EVENT_BYTES+1,length:f=>f.fragmentBytes=1,digest:f=>f.fragmentDigest='0'.repeat(64),identity:f=>f.fragmentId='m3:'+'0'.repeat(64),shape:f=>f.extra=true,base64:f=>f.data='!'};
for(const [name,mutate] of Object.entries(mutations))test(`M3 malformed ${name} rejected before allocation/consumer persistence`,async()=>{
 const {transport,calls}=runtime();const f=JSON.parse(frames[0]);mutate(f);const result=await transport.receive(JSON.stringify(f));
 assert.equal(result.status,'DLQ');assert.equal(calls.length,0);assert.equal(transport.metrics().events,0);assert.equal(transport.metrics().stagingBytes,0);
});
test('M3 malformed JSON and oversized bounded input preserve truthful DLQ accounting',async()=>{
 const {transport,calls}=runtime();assert.equal((await transport.receive('{')).status,'DLQ');
 const r=await transport.receive('x'.repeat(60001));assert.equal(r.status,'REJECTED');assert.equal(r.originalRetained,false);assert.equal(calls.length,0);
 const p=transport.dlq().poison[0];assert.equal(p.wire,'{');await transport.replayPoison(p.id);assert.equal(transport.dlq().poison[0].attempts,2);
});
for(const poison of ['complete-digest','payload-digest','shape','utf8'])test(`M3 assembled ${poison} never reaches M2`,async()=>{
 const {transport,calls}=runtime();let fs;
 if(poison==='complete-digest'){const f=JSON.parse(smallFrames[0]);f.wireDigest='0'.repeat(64);fs=[canonicalSerialize(f,60000)];}
 else if(poison==='utf8'){const f=JSON.parse(smallFrames[0]);const b=new Uint8Array([255]);Object.assign(f,{totalBytes:1,count:1,ordinal:0,fragmentBytes:1,fragmentDigest:await digestTransportBytes(b),wireDigest:await digestTransportBytes(b),data:'/w=='});f.fragmentId='m3:'+await fragmentIdentity(f);fs=[canonicalSerialize(f,60000)];}
 else{const e=JSON.parse(small.wire);if(poison==='shape')e.payload={unsupported:true};else e.payloadDigest='0'.repeat(64);fs=await reframed(JSON.stringify(e));}
 assert.equal((await deliver(transport,fs)).status,'DLQ');assert.equal(calls.length,0);
});
test('M3 event, staging, validation and poison DLQ bounds fail explicitly',async()=>{
 const {transport}=runtime({store:createVolatileTransportStore({maxEvents:1,maxBytes:60000,maxDlq:1,maxDlqBytes:10})});
 await transport.receive(frames[0]);assert.equal((await transport.receive(otherFrames[0])).status,'CAPACITY_PENDING');
 assert.equal((await transport.receive(frames[1])).status,'CAPACITY_PENDING');assert.equal((await transport.receive('{')).status,'DLQ');
 assert.equal((await transport.receive('[')).retentionReason,'MOCK_DLQ_CAPACITY');assert(transport.metrics().stagingBytes<=60000);assert.equal(transport.discard(id),false);
});
const outcomes=[
 {ok:true,status:'ACCEPTED',durable:true}, {ok:true,status:'DUPLICATE',durable:true},
 {ok:true,status:'ACCEPTED',durable:true,projectionStatus:'PENDING',retryable:true},
 {ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true},
 {ok:false,status:'DURABILITY_UNKNOWN',durable:null,retryable:true},
 {ok:false,status:'INTEGRITY_CONFLICT',durable:false,retryable:true,projectionStatus:'PENDING'},
 {ok:false,status:'INTEGRITY_CONFLICT',durable:true,projectionStatus:'CONFLICT'},
 {ok:false,status:'CONFLICT',durable:true},
 {ok:false,status:'PENDING',durable:false,retryable:true},
 {ok:false,status:'REJECTED',durable:false,error:'MALFORMED'},
 {ok:true,status:'UNKNOWN',durable:true}
];
for(const result of outcomes)test(`M3 truthful M2 propagation ${result.status}/${result.projectionStatus||''}/${result.durable}`,async()=>{
 const {transport}=runtime({consumer:{async ingest(){return result;}}});const delivered=await deliver(transport,smallFrames);
 assert.deepEqual(delivered.consumerResult,result);assert.equal(delivered.durable,false);assert.equal(delivered.classification,classifyMeasurementDelivery(result).classification);
 assert.equal(delivered.status,['ACCEPTED','DUPLICATE'].includes(result.status)&&!result.retryable?'ACKNOWLEDGED':classifyMeasurementDelivery(result).retryable?'RETRY_PENDING':'DLQ');
});
test('M3 retry clocks, limits, DLQ original bytes and controlled replay are deterministic',async()=>{
 let clock=now+86400000,healthy=false,calls=0;const t=createOfflineMultipartTransport({clock:()=>clock,maxAttempts:2,maxReplays:1,retryDelayMs:10,consumer:{async ingest(){calls++;return healthy?accepted:{ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true,error:'NEEDS_CYCLE'};}}});
 assert.equal((await deliver(t,smallFrames)).status,'RETRY_PENDING');const first=t.inspect(smallId);await t.retry(smallId);assert.equal(calls,1);
 clock+=10;assert.equal((await t.retry(smallId)).status,'DLQ');const dlq=t.dlq().events[0];assert.deepEqual(dlq.fragments,smallFrames);assert.equal(dlq.firstFailureAt,first.firstFailureAt);assert.equal(dlq.lastFailureAt,clock);assert.equal(dlq.attempts,2);
 healthy=true;assert.equal((await t.replay(smallId)).status,'ACKNOWLEDGED');assert.equal((await t.replay(smallId)).status,'REPLAY_LIMIT');assert.equal(t.inspect(smallId).attempts,3);
});
test('M3 consumer exception remains retryable, never a false acknowledgement',async()=>{
 const {transport}=runtime({consumer:{async ingest(){throw new Error('offline injected failure');}}});const result=await deliver(transport,smallFrames);
 assert.equal(result.status,'RETRY_PENDING');assert.equal(result.consumerResult.durable,null);
});
test('M3 expired partial cannot complete or replay; expiry accounting remains visible',async()=>{
 let clock=now+86400000;const {transport,calls}=runtime({clock:()=>clock});await transport.receive(frames[0]);clock+=MOCK_RETENTION_MS;
 assert.equal(transport.expire()[0].status,'EXPIRED_INCOMPLETE');await deliver(transport,frames.slice(1));assert.equal(calls.length,0);assert.equal((await transport.replay(id)).status,'EXPIRED_INCOMPLETE');
 const bytes=transport.metrics().stagingBytes;assert.equal(transport.discard(id),true);assert.equal(transport.metrics().discardedBytes,bytes);assert.equal(transport.metrics().stagingBytes,0);
});
test('M3 expired complete retry moves to mock DLQ, explicit replay preserves original clocks',async()=>{
 let clock=now+86400000,healthy=false;const calls=[];const {transport}=runtime({clock:()=>clock,consumer:{async ingest(w){calls.push(w);return healthy?accepted:{ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true};}}});
 await deliver(transport,smallFrames);clock+=MOCK_RETENTION_MS;assert.equal(transport.expire()[0].status,'DLQ');healthy=true;assert.equal((await transport.replay(smallId)).status,'ACKNOWLEDGED');assert.deepEqual(calls,[small.wire,small.wire]);
});
test('M3 crash partial staging: retained mock store resumes; lost volatile object explicitly loses state',async()=>{
 const store=createMockDurableTransportStore(),a=runtime({store});await a.transport.receive(frames[0]);a.transport.crash();
 const b=runtime({store});assert.equal(b.transport.inspect(id).receivedFragments,1);assert.equal((await deliver(b.transport,frames.slice(1))).status,'ACKNOWLEDGED');
 const v=runtime();await v.transport.receive(frames[0]);v.transport.crash();const restarted=runtime();assert.equal(restarted.transport.inspect(id),null);assert.equal(restarted.transport.metrics().events,0);
});
test('M3 restart preserves retry ledger and exact event identity in retained mock store',async()=>{
 const store=createMockDurableTransportStore(),a=runtime({store,retryDelayMs:0,consumer:{async ingest(){return {ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true};}}});
 await deliver(a.transport,smallFrames);a.transport.crash();const b=runtime({store});assert.equal((await b.transport.retry(smallId)).status,'ACKNOWLEDGED');assert.equal(b.transport.inspect(smallId).attempts,2);assert.equal(b.calls[0],small.wire);
});
for(const phase of ['afterReassembly','afterAttemptRecorded','beforeDlq','beforeReplay'])test(`M3 crash/restart at ${phase} cannot let obsolete runtime publish acknowledgement`,async()=>{
 const store=createMockDurableTransportStore();let t;const g=gate(),pending={ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true};
 t=runtime({store,maxAttempts:1,hook:async p=>{if(p===phase){g.ready();await g.wait;}},consumer:{async ingest(){return phase==='beforeDlq'||phase==='beforeReplay'?pending:accepted;}}}).transport;
 let work;if(phase==='beforeReplay'){await deliver(t,smallFrames);work=t.replay(smallId);}else work=deliver(t,smallFrames);
 await g.arrived;t.crash();g.release();await work.catch(e=>assert.match(e.message,/crashed/));
 const b=runtime({store,retryDelayMs:0});let r=b.transport.inspect(smallId);if(r.status==='DLQ')r=await b.transport.replay(smallId);else r=await b.transport.retry(smallId);
 assert.equal(r.status,'ACKNOWLEDGED');assert.equal(b.calls[0],small.wire);
});
test('M3 unrelated evaluation progresses while another is awaiting its M2 acknowledgement',async()=>{
 const g=gate(),calls=[];const t=runtime({consumer:{async ingest(w){calls.push(w);if(w===cycle.wire){g.ready();await g.wait;}return accepted;}}}).transport;
 const stalled=deliver(t);await g.arrived;assert.equal((await deliver(t,otherFrames)).status,'ACKNOWLEDGED');assert.equal(calls.length,2);g.release();await stalled;
});
test('M3 replay versus normal final delivery uses one event-specific completion',async()=>{
 const store=createMockDurableTransportStore();let healthy=false;const g=gate();let calls=0;
 const t=runtime({store,maxAttempts:1,consumer:{async ingest(){calls++;if(healthy){g.ready();await g.wait;return accepted;}return {ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true};}}}).transport;
 await deliver(t,smallFrames);healthy=true;const replay=t.replay(smallId);await g.arrived;const duplicate=t.receive(smallFrames[0]);g.release();await Promise.all([replay,duplicate]);assert.equal(calls,2);
});
test('M3 public M2 ingestion, full-event redelivery and replay are immutable/idempotent',async()=>{
 const {db,binding}=await database();try{
 const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000}),t=runtime({consumer}).transport;
 const first=await t.send(cycle);assert.equal(first.consumerResult.status,'ACCEPTED');const count=()=>db.prepare('SELECT count(*) AS n FROM measurement_ingress_receipts').get().n;
 const n=count();const dup=await t.send(cycle);assert.equal(dup.consumerResult.status,'DUPLICATE');assert.equal(count(),n);
 assert.equal((await t.replay(id)).consumerResult.status,'DUPLICATE');assert.equal(count(),n);
 }finally{db.close();}
});
test('M3 independently represented census survives missing detail; early census dependency remains truthful',async()=>{
 const {db,binding}=await database();try{
 const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000}),t=runtime({consumer,retryDelayMs:0}).transport;
 const early=await t.send(candidate);assert.equal(early.consumerResult.status,'DEPENDENCY_PENDING');assert.equal(early.status,'RETRY_PENDING');
 await t.send(cycle);const captured=await t.retry(candidate.envelope.eventId);assert.equal(captured.status,'ACKNOWLEDGED');
 assert.equal(db.prepare('SELECT count(*) AS n FROM measurement_ingress_receipts WHERE event_id=?').get(candidate.envelope.eventId).n,1);
 assert.equal(db.prepare('SELECT count(*) AS n FROM measurement_ingress_receipts WHERE event_kind=?').get('OFFICIAL_CREATION').n,0);
 }finally{db.close();}
});
for(const durable of [true,false])test(`M3 conflicting replay preserves M2 quarantine and Candidate fence, durable=${durable}`,async()=>{
 const {db,binding}=await database();try{
 const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});await consumer.ingest(cycle.wire);
 const fault=wrapOfflineMeasurementAdapter(binding,{beforeBatch(){throw new Error('M3_INTENT_STORAGE_FAILURE');}});
 const t=runtime({consumer:durable?consumer:createOfflineMeasurementConsumer(fault,{clock:()=>now+86400000}),retryDelayMs:0}).transport;
 const bad=await conflictingCandidate(),result=await t.send(bad);assert.equal(result.classification,'INTEGRITY_CONFLICT');
 assert.equal(result.consumerResult.conflictStatus,durable?'RECORDED':'RECONCILIATION_PENDING');
 const evalId=candidate.envelope.semanticId;const before=()=>db.prepare('SELECT count(*) AS n FROM signal_outcome_evidence WHERE subject_id=?').get(evalId).n;
 const n=before();await collectMeasurementFromProjection(binding,{asOf:now+86400001,ticks:[],bars:[],maxSubjects:1});assert.equal(before(),n);
 await t.replay(JSON.parse(bad.wire).eventId);assert.equal(before(),n);
 }finally{db.close();}
});
test('M3 crash after public M2 commit before acknowledgement replays as DUPLICATE',async()=>{
 const {db,binding}=await database();try{
 const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000}),store=createMockDurableTransportStore(),g=gate();
 const a=runtime({store,consumer,hook:async phase=>{if(phase==='beforeAcknowledgement'){g.ready();await g.wait;}}}).transport;
 const work=a.send(cycle);await g.arrived;a.crash();g.release();assert.equal((await work).status,'CRASHED');
 const b=runtime({store,consumer}).transport;const result=await b.retry(id);assert.equal(result.consumerResult.status,'DUPLICATE');assert.equal(result.status,'ACKNOWLEDGED');
 assert.equal(db.prepare('SELECT count(*) AS n FROM measurement_ingress_receipts WHERE event_id=?').get(id).n,1);
 }finally{db.close();}
});
test('M3 concurrent Candidate, Official and conflicting evidence retain public M2 semantics',async()=>{
 const {db,binding}=await database();try{
 const consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000}),t=runtime({consumer}).transport;
 await t.send(cycle);const official=packets.find(p=>p.envelope.kind==='OFFICIAL_CREATION');
 const results=await Promise.all([t.send(candidate),t.send(official)]);for(const r of results)assert(r.consumerResult.ok);
 const bad=await conflictingCandidate();const [duplicate,conflict]=await Promise.all([t.send(candidate),t.send(bad)]);
 assert.equal(duplicate.consumerResult.status,'DUPLICATE');assert.equal(conflict.classification,'INTEGRITY_CONFLICT');
 assert.equal(db.prepare('SELECT count(*) AS n FROM measurement_ingress_receipts WHERE event_id=?').get(candidate.envelope.eventId).n,1);
 }finally{db.close();}
});
test('M3 independent databases and stores progress without cross-capability coupling',async()=>{
 const a=await database(),b=await database();try{
 const ta=runtime({consumer:createOfflineMeasurementConsumer(a.binding,{clock:()=>now+86400000})}).transport;
 const tb=runtime({consumer:createOfflineMeasurementConsumer(b.binding,{clock:()=>now+86400000})}).transport;
 for(const r of await Promise.all([ta.send(cycle),tb.send(cycle)]))assert.equal(r.consumerResult.status,'ACCEPTED');
 }finally{a.db.close();b.db.close();}
});
test('M3 retained mock store rejects copied store admission and aborts bounded admission',async()=>{
 const store=createMockDurableTransportStore();assert.throws(()=>runtime({store:{...store}}),/runtime_invalid/);
 const {transport,calls}=runtime({store});const controller=new AbortController();controller.abort();
 assert.equal((await transport.receive(frames[0],{signal:controller.signal})).status,'ABORTED');assert.equal(calls.length,0);
});
test('M3 UTF-8 boundaries preserve complete original bytes and M1 upper bound rejects oversize',async()=>{
 const p=await buildMeasurementEnvelope({kind:'CAPTURE_GAP',semanticId:'unicode-gap',occurredAt:now,observedAt:now,preparedAt:now,
  payload:{captureGap:true,durable:false,measurementOnly:true,decisionUse:false,note:'a'+'€'.repeat(18000)}});
 const fs=await fragmentMeasurementPacket(p,options);assert.equal(fs.length,2);const {transport,calls}=runtime();await deliver(transport,[...fs].reverse());assert.deepEqual(calls,[p.wire]);
 await assert.rejects(fragmentMeasurementPacket({wire:'x'.repeat(MAX_EVENT_BYTES+1)},options),/event_bound/);
});
test('M3 receive admission bounds 65 concurrent arrivals with explicit backpressure',async()=>{
 const {transport}=runtime();const results=await Promise.all(Array.from({length:65},()=>transport.receive(frames[0])));
 assert.equal(results.filter(r=>r.status==='CAPACITY_PENDING').length,1);assert.equal(transport.inspect(id).receivedFragments,1);assert.equal(transport.metrics().validationsInFlight,0);
});
test('M3 finalized M1 early-rejection packet is completely framed and durably accepted',async()=>{
 const {after}=await loadM1Workers(),source=completeJournal(after,sm),tf='5m',trace={},mtf=engine.HIGHER_SIGNAL_TIMEFRAMES[tf]||[];
 const result=engine.computeServerSignal(source.frames[tf].bars,{tf,mtf:mtf.map(tf=>source.frames[tf]),live:source.live,barsSource:'d1',evaluationAt:now,filters:source.filters,dataQuality:{ok:false,reason:'synthetic-quality-failure'}},trace);
 const early=sm.beginDecisionCycle(now,source.frames,source.live,null,source.filters);early.capturedAt=now+1234;early.failedOfficialId=null;sm.observeAttempt(early,{tf,trace,result,requestedMtf:mtf,includedMtf:mtf});
 const [p]=await prepareCycleEnvelopes(sm.decisionJournalObservations(early),provenance,{preparedAt:now+5000});
 const fs=await fragmentMeasurementPacket(p,options);assert.equal(fs.length,5);const {db,binding}=await database();
 try{const transport=runtime({consumer:createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000})}).transport;const r=await deliver(transport,fs);
 assert.equal(r.status,'ACKNOWLEDGED');assert.equal(r.consumerResult.durable,true);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,1);
 }finally{db.close();}
});
test('M3 maximum valid geometry stages actual last fragment without allocating declared total',async()=>{
 const f=JSON.parse(frames[0]);Object.assign(f,{totalBytes:MAX_EVENT_BYTES,count:MAX_FRAGMENTS,ordinal:MAX_FRAGMENTS-1});
 const bytes=new Uint8Array(MAX_EVENT_BYTES-(MAX_FRAGMENTS-1)*FRAGMENT_DATA_BYTES);Object.assign(f,{fragmentBytes:bytes.length,data:Buffer.from(bytes).toString('base64'),fragmentDigest:await digestTransportBytes(bytes)});f.fragmentId='m3:'+await fragmentIdentity(f);
 const {transport,calls}=runtime();const wire=canonicalSerialize(f,60000),r=await transport.receive(wire);
 assert.equal(r.receivedFragments,1);assert.equal(r.missingOrdinals.length,49);assert.equal(r.stagedBytes,serializedBytes(wire));assert.equal(calls.length,0);
});
test('M3 expiry at last supported pre-consumer hook cannot dispatch expired event',async()=>{
 let clock=now+86400000;const {transport,calls}=runtime({clock:()=>clock,hook:phase=>{if(phase==='afterAttemptRecorded')clock+=MOCK_RETENTION_MS;}});
 assert.equal((await deliver(transport,smallFrames)).status,'DLQ');assert.equal(calls.length,0);assert.equal(transport.inspect(smallId).classification,'TRANSPORT_EXPIRED');
});
