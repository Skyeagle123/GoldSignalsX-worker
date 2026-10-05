import assert from 'node:assert/strict';
import {test} from 'node:test';
import {database,now} from './test-fixtures/b1-m2.mjs';
import {loadM1Workers,completeJournal,cycleEnvironment,provenance,BASELINE} from './test-fixtures/b1-m1.mjs';
import * as sm from './signal-measurement.js';
import * as engine from './signal-engine.js';
import {prepareCycleEnvelopes,installOfflineMeasurementProducer} from './measurement-producer.js';
import {buildMeasurementEnvelope} from './measurement-envelope.js';
import {createOfflineTransport} from './measurement-transport.js';
import {createOfflineMultipartTransport} from './measurement-transport-multipart.js';
import {canonicalSerialize} from './signal-evidence.js';
import {censusSnapshot} from './evidence-codec.js';
import {createOfflineMeasurementConsumer} from './measurement-consumer.js';
const workers=await loadM1Workers(),source=completeJournal(workers.after,sm),at=now+86400000;
const profiles={
 minimum:q=>q.bars=q.bars.slice(-20),quality:(_q,o)=>o.dataQuality={ok:false,reason:'fixture-rejection'},
 atr:q=>q.bars=q.bars.map(b=>({...b,o:4100,h:4100,l:4100,c:4100})),
 age:q=>q.bars=q.bars.map(b=>({...b,t:b.t-3600000})),receipt:(_q,o)=>o.live.receivedAt=now-20001,provider:(_q,o)=>o.live.ts=now-90001,
 news:(_q,o)=>o.news={ok:true,stale:false,safety:{calendarBlockTechnicalSignal:true,newsBlockTechnicalSignal:false}},
 score:(q,o)=>{q.bars=q.bars.map((b,i)=>{const c=4100+(i%2)*.01;return {...b,o:c,h:c+.4,l:c-.4,c};});o.mtf=[];},
 candle:q=>{const b=q.bars.at(-1),p=q.bars.at(-2);Object.assign(b,{o:p.c,c:p.c,h:p.c+.5,l:p.c-.5});},
 mtf:(_q,o)=>o.mtf=[],opposition:(_q,o)=>{o.mtf=o.mtf.map(f=>({...f,bars:f.bars.map((b,i)=>{const c=4200-i*.4;return {...b,o:c+.3,h:c+.4,l:c-.1,c};})}));},
 ny:(_q,o)=>o.filters={nyFilterOn:true,nyStart:'00:00',nyEnd:'00:01',pivotFilterOn:false},pivot:(_q,o)=>o.filters={nyFilterOn:false,pivotFilterOn:true,pivotDistance:50},
 priceSource:(_q,o)=>o.barsSource='other',priceAlignment:(_q,o)=>o.live.price=5000,
 // Nonfinite ATR is expressly rejected by Engine; its tagged codec representation is supported.
 nonfiniteAtr:q=>q.bars=q.bars.map(b=>({...b,h:1e308,l:-1e308,c:0})),
};
profiles.unavailableQuality=(q,o)=>{profiles.score(q,o);o.dataQuality={ok:undefined};};
const prepared=new Map();
async function profile(name){
 if(prepared.has(name))return prepared.get(name);
 const tf='5m',frames=structuredClone(source.frames),q=frames[tf],trace={},o={tf,mtf:(engine.HIGHER_SIGNAL_TIMEFRAMES[tf]||[]).map(tf=>frames[tf]),live:{...source.live},barsSource:'d1',evaluationAt:now,filters:{nyFilterOn:false,pivotFilterOn:false},dataQuality:q.quality};profiles[name](q,o);
 const result=engine.computeServerSignal(q.bars,o,trace);assert.equal(result.side,'none',name);
 const j=sm.beginDecisionCycle(now,frames,o.live,o.news||null,o.filters);j.capturedAt=now+1234;j.failedOfficialId=null;sm.observeAttempt(j,{tf,trace,result,requestedMtf:o.requestedMtf??o.mtf.map(f=>f.tf),includedMtf:o.mtf.map(f=>f.tf)});
 const packets=await prepareCycleEnvelopes(sm.decisionJournalObservations(j),provenance,{preparedAt:now+5000});const value={packets,trace,result};prepared.set(name,value);return value;
}
let workerPackets;
async function flatWorker(){
 if(workerPackets)return workerPackets;
 const x=cycleEnvironment(workers.after,{fullArrays:true}),clock=Date.now,fetch=globalThis.fetch;try{Date.now=()=>now;globalThis.fetch=()=>{throw Error('NO_NETWORK');};x.db.database.exec('UPDATE bars_v2 SET o=4100,h=4100,l=4100,c=4100');Object.assign(x.env,{B1_PRODUCER_CAPTURE_ENABLED:'1',B1_CODE_COMMIT:BASELINE,B1_MEASUREMENT_EFFECTIVE_AT:provenance.measurementEffectiveAt});const capture=createOfflineTransport(),p=installOfflineMeasurementProducer(x.env,{transport:capture});await workers.after.runSignalCycle(x.env,null,{nyFilterOn:false,pivotFilterOn:false});await p.whenIdle();workerPackets=capture.packets();return workerPackets;}finally{Date.now=clock;globalThis.fetch=fetch;x.db.database.close();}
}

// Each tamper changes one captured semantic field; outer digests are rebuilt.
const atrEdits={
 operand:d=>d.engine.gates[2].operands.atr=1,
 primary:d=>d.engine.indicators.atr=1,
 profile:d=>d.engine.indicators.marketProfile.atr=1,
 missingPrimary:d=>delete d.engine.indicators.atr,
 missingProfile:d=>delete d.engine.indicators.marketProfile.atr,
 missingProfileObject:d=>delete d.engine.indicators.marketProfile,
 malformedPrimary:d=>d.engine.indicators.atr='0',
 malformedProfile:d=>d.engine.indicators.marketProfile.atr={$number:'unsupported'},
 nonfiniteContradiction:d=>d.engine.indicators.marketProfile.atr=NaN,
};
async function changed(p,edit){const payload=structuredClone(p.envelope.payload);edit(payload.records.find(r=>r.type==='decision').payload);return buildMeasurementEnvelope({kind:p.envelope.kind,semanticId:p.envelope.semanticId,...p.envelope.clocks,payload});}
function ceiling(packets){return Math.max(at,...packets.flatMap(p=>Object.values(p.envelope.clocks).filter(Number.isFinite)))+86400000;}
async function rejected(p,error,clock){const {db,binding}=await database();try{
 const r=await createOfflineMeasurementConsumer(binding,{clock:()=>clock}).ingest(p.wire);assert.equal(r.status,'REJECTED');assert.equal(r.durable,false);assert.equal(r.error,error);
 for(const table of ['measurement_ingress_receipts','signal_decision_evidence','signal_measurement_state','signal_outcome_evidence'])assert.equal(db.prepare('SELECT COUNT(*) n FROM '+table).get().n,0,table);
}finally{db.close();}}
for(const [name,edit]of Object.entries(atrEdits))test('ATR cross-reference actual Worker single-field '+name,async()=>{
 const packets=await flatWorker();await rejected(await changed(packets[0],edit),name==='malformedProfile'?'measurement_codec_tag_invalid':'measurement_rejection_atr_invalid',ceiling(packets));
});
for(const name of ['atr','nonfiniteAtr'])test('ATR cross-reference consistent genuine '+name,async()=>{
 const {packets}=await profile(name),{db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>at});
 for(const p of packets)assert.equal((await c.ingest(p.wire)).status,'ACCEPTED');for(const p of packets)assert.equal((await c.ingest(p.wire)).status,'DUPLICATE');
 assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,1);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_measurement_state').get().n,0);
}finally{db.close();}});
for(const value of [NaN,Infinity,-Infinity,-0])test('ATR consistent canonical numeric '+String(value),async()=>{
 const p=(await profile('atr')).packets[0],packet=await changed(p,d=>{d.engine.gates[2].operands.atr=value;d.engine.indicators.atr=value;d.engine.indicators.marketProfile.atr=value;}),{db,binding}=await database();try{
 const r=await createOfflineMeasurementConsumer(binding,{clock:()=>at}).ingest(packet.wire);assert.equal(r.status,'ACCEPTED');assert.equal(r.durable,true);
 }finally{db.close();}
});
const mtfEdits={
 frameDirection:d=>d.engine.mtf[0].result.direction='bullish',
 copyDirection:d=>d.engineMtf.frames[0].result.direction='bullish',
 detailDirection:d=>d.engine.mtf[0].detail.result.direction='bullish',
 confirmationCount:d=>d.engine.scoring.confirm=1,
 oppositionCount:d=>d.engine.scoring.oppositions=1,
 resultBull:d=>d.engine.result.mtf.bull=1,
 resultBear:d=>d.engine.result.mtf.bear=1,
 resultNeutral:d=>d.engine.result.mtf.neutral=1,
 confirmationGate:d=>d.engine.gates[10].operands.confirm=1,
 oppositionGate:d=>d.engine.gates[11].operands.oppositions=1,
 gateCount:d=>d.engine.gates[11].operands.count=1,
 copyGate:d=>d.engineMtf.frames[0].admissionGateResults[0].operands.confirm=1,
 malformedDirection:d=>d.engine.mtf[0].result.direction='unavailable',
 missingDirection:d=>delete d.engine.mtf[0].result.direction,
 duplicateIdentity:d=>d.engine.mtf[1].tf=d.engine.mtf[0].tf,
 missingIdentity:d=>delete d.engine.mtf[0].tf,
 missingFrame:d=>d.engine.mtf.pop(),
 reorderedFrames:d=>d.engine.mtf.reverse(),
 includedIdentity:d=>d.engineMtf.included[0]='1m',
 malformedStrength:d=>d.engine.mtf[0].result.strength=Infinity,
};
for(const [name,edit]of Object.entries(mtfEdits))test('MTF cross-reference genuine Engine/M1 single-field '+name,async()=>{
 const p=(await profile('opposition')).packets[0];const expected=['confirmationCount','confirmationGate'].includes(name)?'mtf-confirmation':name==='oppositionCount'||name==='oppositionGate'||name==='gateCount'||name==='missingFrame'?'mtf-opposition':name==='resultBull'||name==='resultBear'?'result':'mtf_cross_reference';
 await rejected(await changed(p,edit),'measurement_rejection_'+expected+'_invalid',at);
});
profiles.shortMtf=(q,o)=>{o.mtf=o.mtf.map(f=>{f.bars=f.bars.slice(-20);return f;});};
profiles.neutralMtf=(q,o)=>{o.mtf=o.mtf.map(f=>{f.bars=f.bars.map(b=>({...b,o:4100,h:4100,l:4100,c:4100}));return f;});};
profiles.multiMtf=(q,o)=>{profiles.opposition(q,o);profiles.news(q,o);};
profiles.excludedMtf=(q,o)=>{o.requestedMtf=o.mtf.map(f=>f.tf);for(const f of o.mtf)f.quality={ok:false,reason:'fixture-unavailable-higher-frame'};o.mtf=[];};
for(const name of ['opposition','mtf','shortMtf','neutralMtf','excludedMtf','atr'])test('MTF genuine control '+name,async()=>{
 const {packets}=await profile(name),{db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>at}),t=createOfflineMultipartTransport({consumer:c,clock:()=>at});
 for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'ACCEPTED');for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'DUPLICATE');
 assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,1);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_measurement_state').get().n,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_outcome_evidence').get().n,0);
}finally{db.close();}});
test('MTF coherent changed aggregates cannot conceal contradictory frame copies',async()=>{
 const p=(await profile('multiMtf')).packets[0];await rejected(await changed(p,d=>{
 d.engine.mtf[0].result.direction='bullish';d.engine.result.mtf={bull:1,bear:1,neutral:0};d.engine.scoring.confirm=1;d.engine.scoring.oppositions=1;
 for(const [i,key]of [[10,'confirm'],[11,'oppositions']])d.engine.gates[i].operands[key]=1;
 d.engine.gates[11].operands.confirm=1;for(const i of [10,11]){d.engine.gates[i].result='PASS';d.engine.gates[i].reasonCode=d.engine.gates[i].id+'_passed';}
 }),'measurement_rejection_mtf_cross_reference_invalid',at);
});
test('public M3 rejects contradictory MTF before any M2 persistence',async()=>{
 const p=await changed((await profile('opposition')).packets[0],mtfEdits.frameDirection),{db,binding}=await database();try{const t=createOfflineMultipartTransport({consumer:createOfflineMeasurementConsumer(binding,{clock:()=>at}),clock:()=>at}),r=await t.send(p);
 assert.equal(r.status,'DLQ');assert.equal(r.reason,'measurement_rejection_mtf_cross_reference_invalid');assert.equal(t.metrics().consumerCalls,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,0);
}finally{db.close();}});
for(const type of ['atr','mtf'])test('malformed '+type+' census preserves existing canonical evidence without quarantine',async()=>{
 const packets=type==='atr'?await flatWorker():(await profile('opposition')).packets,p=packets.find(p=>p.envelope.kind==='EVALUATION_CENSUS'),payload=structuredClone(p.envelope.payload);
 (type==='atr'?atrEdits.profile:mtfEdits.frameDirection)(payload.decisionEvidence);payload.census=censusSnapshot(payload.decisionEvidence);
 const bad=await buildMeasurementEnvelope({kind:p.envelope.kind,semanticId:p.envelope.semanticId,...p.envelope.clocks,payload}),clock=ceiling(packets),{db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>clock});
 for(const p of packets)assert.equal((await c.ingest(p.wire)).status,'ACCEPTED');const before=db.prepare('SELECT * FROM signal_decision_evidence ORDER BY evaluation_id').all(),receipts=db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n;
 const r=await c.ingest(bad.wire);assert.equal(r.status,'REJECTED');assert.equal(r.error,'measurement_rejection_'+(type==='atr'?'atr':'mtf_cross_reference')+'_invalid');
 assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,receipts);assert.deepEqual(db.prepare('SELECT * FROM signal_decision_evidence ORDER BY evaluation_id').all(),before);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_decision_quarantine').get().n,0);
 assert.equal((await c.ingest(p.wire)).status,'DUPLICATE');
}finally{db.close();}});
test('actual Worker seven-rejection census unchanged under final cross references',async()=>{
 const packets=await flatWorker(),clock=ceiling(packets),{db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>clock});for(const p of packets)assert.equal((await c.ingest(p.wire)).status,'ACCEPTED');for(const p of packets)assert.equal((await c.ingest(p.wire)).status,'DUPLICATE');
 assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,7);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,8);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_measurement_state').get().n,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_outcome_evidence').get().n,0);
}finally{db.close();}});
