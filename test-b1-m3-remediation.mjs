import assert from 'node:assert/strict';
import {test} from 'node:test';
import {database,lifecycle,now} from './test-fixtures/b1-m2.mjs';
import {loadM1Workers,completeJournal,cycleEnvironment,provenance,BASELINE} from './test-fixtures/b1-m1.mjs';
import * as sm from './signal-measurement.js';
import * as engine from './signal-engine.js';
import {prepareCycleEnvelopes,installOfflineMeasurementProducer} from './measurement-producer.js';
import {buildMeasurementEnvelope} from './measurement-envelope.js';
import {createOfflineTransport} from './measurement-transport.js';
import {createOfflineMeasurementConsumer} from './measurement-consumer.js';
import {fragmentMeasurementPacket,reassembleMeasurementFragments} from './measurement-transport-fragment.js';
import {createOfflineMultipartTransport,createMockDurableTransportStore} from './measurement-transport-multipart.js';
const workers=await loadM1Workers();
const accepted={ok:true,status:'ACCEPTED',durable:true},at=now+86400000;
function gate(){let release,ready;return {wait:new Promise(r=>release=r),arrived:new Promise(r=>ready=r),release:()=>release(),ready:()=>ready()};}
const small=await lifecycle('30m:1790865000000:buy','tp1');
const fs=await fragmentMeasurementPacket(small,{receivedAt:at});
function runtime(extra={}){return createOfflineMultipartTransport({clock:()=>at,consumer:{async ingest(){return accepted;}},...extra});}
async function earlyPackets(mutate=null,finalized=true){
 const {after}=workers,source=completeJournal(after,sm),tf='5m',trace={},mtf=engine.HIGHER_SIGNAL_TIMEFRAMES[tf]||[];
 const result=engine.computeServerSignal(source.frames[tf].bars,{tf,mtf:mtf.map(tf=>source.frames[tf]),live:source.live,barsSource:'d1',evaluationAt:now,filters:source.filters,dataQuality:{ok:false,reason:'synthetic-quality-failure'}},trace);
 const j=sm.beginDecisionCycle(now,source.frames,source.live,null,source.filters);j.capturedAt=now+1234;sm.observeAttempt(j,{tf,trace,result,requestedMtf:mtf,includedMtf:mtf});
 if(finalized)j.failedOfficialId=null;
 const packets=[...await prepareCycleEnvelopes(sm.decisionJournalObservations(j),provenance,{preparedAt:now+5000})];
 if(mutate){const p=packets[0],payload=structuredClone(p.envelope.payload);mutate(payload);packets[0]=await buildMeasurementEnvelope({kind:p.envelope.kind,semanticId:p.envelope.semanticId,...p.envelope.clocks,payload});}
 return packets;
}
test('compat: original fixture is byte-preserved but mislabeled rejection remains rejected; finalized census accepted',async()=>{
 const [old]=await earlyPackets(null,false);assert.equal(old.wireBytes,182996);assert.equal(old.envelope.eventId,'m1:90d36c2167ef5f39063e8c9cff0acfa31462d7eb06e50716a1ef4955db351f38');
 const frames=await fragmentMeasurementPacket(old,{receivedAt:at});assert.equal(frames.length,5);assert.equal(Buffer.concat(frames.map(f=>Buffer.from(JSON.parse(f).data,'base64'))).toString(),old.wire);
 const {db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>at});assert.equal((await c.ingest(old.wire)).status,'REJECTED');
 const packets=await earlyPackets(),t=runtime({consumer:c});const p=packets[0],f=await fragmentMeasurementPacket(p,{receivedAt:at});assert.equal(f.length,5);
 assert.equal(await reassembleMeasurementFragments(JSON.parse(f[0]),new Map(f.map(x=>[JSON.parse(x).ordinal,x])),{receivedAt:at}),p.wire);
 for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'ACCEPTED');
 for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'DUPLICATE');
 assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,1);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_measurement_state').get().n,0);
 }finally{db.close();}
});
// Mutate the journal observation before envelopes/digests are created: validation, not digest rejection.
for(const [name,edit] of Object.entries({outcome:d=>d.outcome='OFFICIAL_PERSISTED',computed:d=>{d.levelsStatus='COMPUTED_BY_ENGINE';d.engine.levels={entry:4100,tp1:4110,tp2:4120,sl:4090};},hiddenLevels:d=>d.engine.levels={entry:4100},result:d=>d.engine.result.side='buy',gates:d=>d.engine.gates.forEach(g=>g.result='PASS'),identity:d=>d.engine.identity.tf='1m',official:d=>{d.kind='OFFICIAL';d.officialSignalId='forged';}}))test('compat: rejects contradictory none '+name,async()=>{
 const packets=await earlyPackets(obs=>{for(const r of obs.records||[])if(r.type==='decision')edit(r.payload);});
 const {db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>at});assert.equal((await c.ingest(packets[0].wire)).status,'REJECTED');}finally{db.close();}
});
test('compat: actual flat-bar Worker emits seven rejected census entries exactly once without outcome state',async()=>{
 const {after}=workers,x=cycleEnvironment(after,{fullArrays:true}),oldNow=Date.now,oldFetch=globalThis.fetch;const {db,binding}=await database();
 try{Date.now=()=>now;globalThis.fetch=()=>{throw Error('NO_NETWORK');};x.db.database.exec('UPDATE bars_v2 SET o=4100,h=4100,l=4100,c=4100');Object.assign(x.env,{B1_PRODUCER_CAPTURE_ENABLED:'1',B1_CODE_COMMIT:BASELINE,B1_MEASUREMENT_EFFECTIVE_AT:provenance.measurementEffectiveAt});
 const captured=createOfflineTransport(),producer=installOfflineMeasurementProducer(x.env,{transport:captured});await after.runSignalCycle(x.env,null,{nyFilterOn:false,pivotFilterOn:false});await producer.whenIdle();const packets=captured.packets();
 const census=packets.filter(p=>p.envelope.kind==='EVALUATION_CENSUS');assert.equal(census.length,7);for(const p of census){const d=p.envelope.payload.decisionEvidence;assert.equal(d.direction,'none');assert.equal(d.outcome,'ENGINE_REJECTED');}
 const clock=Math.max(at,...packets.flatMap(p=>Object.values(p.envelope.clocks).filter(Number.isFinite)))+86400000,c=createOfflineMeasurementConsumer(binding,{clock:()=>clock}),t=runtime({clock:()=>clock,consumer:c});
 for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'ACCEPTED');for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'DUPLICATE');
 assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,7);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,8);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_measurement_state').get().n,0);
 }finally{Date.now=oldNow;globalThis.fetch=oldFetch;x.db.database.close();db.close();}
});
const [partial]=await earlyPackets();const parts=await fragmentMeasurementPacket(partial,{receivedAt:at}),id=partial.envelope.eventId;
for(const mode of ['replay','late','sweep','hook'])test('expiry: elapsed incomplete '+mode+' cannot revive and accounts once',async()=>{
 let clock=at,calls=0;const t=runtime({clock:()=>clock,retentionMs:10,hook:phase=>{if(mode==='hook'&&phase==='beforeReplay')clock+=11;},consumer:{async ingest(){calls++;return accepted;}}});await t.receive(parts[0]);
 if(mode!=='hook')clock+=11;if(mode==='sweep')t.expire();if(mode==='late')await t.receive(parts[1]);else await t.replay(id);
 for(let i=0;i<3;i++){assert.equal((await t.replay(id)).status,'EXPIRED_INCOMPLETE');t.expire();}
 for(const f of parts.slice(1))assert.equal((await t.receive(f)).status,'EXPIRED_INCOMPLETE');assert.equal(calls,0);assert.equal(t.metrics().expired,1);
});
test('expiry: complete durable event redelivery still invokes real M2 idempotency',async()=>{
 let clock=at;const {db,binding}=await database();try{const [p]=await earlyPackets(),t=runtime({clock:()=>clock,retentionMs:10,consumer:createOfflineMeasurementConsumer(binding,{clock:()=>clock})});
 assert.equal((await t.send(p)).consumerResult.status,'ACCEPTED');clock+=11;assert.equal((await t.send(p)).consumerResult.status,'DUPLICATE');assert.equal(t.metrics().expired,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,1);
 }finally{db.close();}
});
for(const [kind,count] of [['receive',64],['send',16]])test('restart: abandoned '+kind+' charges released once, late completions harmless',async()=>{
 const store=createMockDurableTransportStore(),g=gate(),a=runtime({store,consumer:{async ingest(){g.ready();await g.wait;return accepted;}}});const pending=Array.from({length:count},()=>kind==='receive'?a.receive(fs[0]):a.send(small));await g.arrived;
 assert.equal(a.metrics()[kind==='receive'?'validationsInFlight':'framingsInFlight'],count);a.crash();a.crash();assert.equal(a.metrics().validationsInFlight,0);assert.equal(a.metrics().framingsInFlight,0);
 const b=runtime({store});assert.equal((await b.send(small)).consumerResult.status,'ACCEPTED');g.release();await Promise.allSettled(pending);assert.equal(b.metrics().validationsInFlight,0);assert.equal(b.metrics().framingsInFlight,0);
});
test('restart: crash only releases owned charges across live runtimes',async()=>{
 const store=createMockDurableTransportStore(),ga=gate(),gb=gate();const a=runtime({store,hook:async p=>{if(p==='afterFragmentStaged'){ga.ready();await ga.wait;}}}),b=runtime({store,consumer:{async ingest(){gb.ready();await gb.wait;return accepted;}}});
 const pa=a.receive(parts[0]);await ga.arrived;const pb=b.send(small);await gb.arrived;assert.equal(a.metrics().validationsInFlight,2);a.crash();assert.equal(b.metrics().validationsInFlight,1);assert.equal(b.metrics().framingsInFlight,1);assert.equal(b.metrics().ownedValidations,1);a.crash();ga.release();await Promise.allSettled([pa]);assert.equal(b.metrics().validationsInFlight,1);gb.release();await pb;assert.equal(b.metrics().validationsInFlight,0);assert.equal(b.metrics().framingsInFlight,0);
});
for(const phase of ['beforeAcknowledgement','beforeDlq'])test('restart: crash during '+phase+' transition preserves accounting',async()=>{
 const store=createMockDurableTransportStore(),g=gate(),a=runtime({store,maxAttempts:phase==='beforeDlq'?1:3,hook:async p=>{if(p===phase){g.ready();await g.wait;}},consumer:{async ingest(){return {ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true};}}});const p=a.send(small);await g.arrived;a.crash();assert.equal(a.metrics().validationsInFlight,0);assert.equal(a.metrics().framingsInFlight,0);const b=runtime({store});assert.equal((await b.retry(small.envelope.eventId)).status,'ACKNOWLEDGED');g.release();await p;assert.equal(b.inspect(small.envelope.eventId).status,'ACKNOWLEDGED');assert.equal(b.metrics().validationsInFlight,0);assert.equal(b.metrics().framingsInFlight,0);
});
test('expiry: sweep during paused replay accounts exactly once',async()=>{
 let clock=at;const g=gate(),t=runtime({clock:()=>clock,retentionMs:10,hook:async p=>{if(p==='beforeReplay'){g.ready();await g.wait;}}});await t.receive(parts[0]);const replay=t.replay(id);await g.arrived;clock+=11;t.expire();g.release();assert.equal((await replay).status,'EXPIRED_INCOMPLETE');assert.equal(t.metrics().expired,1);assert.equal(t.inspect(id).replayCount,0);
});
