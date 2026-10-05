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
 const j=sm.beginDecisionCycle(now,frames,o.live,o.news||null,o.filters);j.capturedAt=now+1234;j.failedOfficialId=null;sm.observeAttempt(j,{tf,trace,result,requestedMtf:o.mtf.map(f=>f.tf),includedMtf:o.mtf.map(f=>f.tf)});
 const packets=await prepareCycleEnvelopes(sm.decisionJournalObservations(j),provenance,{preparedAt:now+5000});const value={packets,trace,result};prepared.set(name,value);return value;
}
const coverage=new Set();
for(const name of Object.keys(profiles))test('rejection contract genuine Engine/M1 '+name,async()=>{
 const {packets,trace}=await profile(name);for(const g of trace.gates)if(g.result==='FAIL')coverage.add(g.id);
 const {db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>at}),t=createOfflineMultipartTransport({consumer:c,clock:()=>at});
 for(const p of packets){const delivered=await t.send(p);assert.equal(delivered.consumerResult?.status,'ACCEPTED',name+':'+JSON.stringify(delivered));}
 for(const p of packets)assert.equal((await t.send(p)).consumerResult?.status,'DUPLICATE',name);
 assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,1);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_measurement_state').get().n,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_outcome_evidence').get().n,0);
 }finally{db.close();}
});
test('genuine fixtures cover every supported failed Engine gate',()=>{assert.deepEqual([...coverage].sort(),['minimum-bars','candle-quality','atr','candle-age','receipt-age','provider-age','news-calendar','score','margin','candle-confirmation','mtf-confirmation','mtf-opposition','ny-session','pivot','price-source','price-alignment'].sort());});
let workerPackets;
async function flatWorker(){
 if(workerPackets)return workerPackets;
 const x=cycleEnvironment(workers.after,{fullArrays:true}),clock=Date.now,fetch=globalThis.fetch;try{Date.now=()=>now;globalThis.fetch=()=>{throw Error('NO_NETWORK');};x.db.database.exec('UPDATE bars_v2 SET o=4100,h=4100,l=4100,c=4100');Object.assign(x.env,{B1_PRODUCER_CAPTURE_ENABLED:'1',B1_CODE_COMMIT:BASELINE,B1_MEASUREMENT_EFFECTIVE_AT:provenance.measurementEffectiveAt});const capture=createOfflineTransport(),p=installOfflineMeasurementProducer(x.env,{transport:capture});await workers.after.runSignalCycle(x.env,null,{nyFilterOn:false,pivotFilterOn:false});await p.whenIdle();workerPackets=capture.packets();return workerPackets;}finally{Date.now=clock;globalThis.fetch=fetch;x.db.database.close();}
}
const edits={
 passOperand:d=>d.engine.gates[2].operands.atr=1,missingOperands:d=>delete d.engine.gates[2].operands,missingAtr:d=>delete d.engine.gates[2].operands.atr,
 indicatorContradiction:d=>d.engine.indicators.atr=1,wrongReason:d=>d.engine.gates[2].reasonCode='atr_passed',
 laterEvaluated:d=>{const g=d.engine.gates[3];g.result='PASS';g.reasonCode='candle-age_passed';g.operands={age:0,maxAge:420000};},
 numericString:d=>d.engine.gates[2].operands.atr='0',numericNull:d=>d.engine.gates[2].operands.atr=null,numericObject:d=>d.engine.gates[2].operands.atr={},
 nanContradiction:d=>d.engine.gates[2].operands.atr=NaN,infinityContradiction:d=>d.engine.gates[2].operands.atr=Infinity,
 unsupportedGate:d=>d.engine.gates[2].id='invented',order:d=>d.engine.gates.reverse(),result:d=>d.engine.result.side='buy',
 levels:d=>d.engine.levels={entry:4100,tp1:4110,tp2:4120,sl:4090},classification:d=>d.outcome='OFFICIAL_PERSISTED',official:d=>{d.kind='OFFICIAL';d.officialSignalId='fake';},identity:d=>d.engine.identity.tf='5m',
};
for(const [name,edit] of Object.entries(edits))test('actual Worker single-element semantic tamper '+name,async()=>{
 const packets=await flatWorker(),p=packets.find(p=>p.envelope.kind==='DECISION_CYCLE'),payload=structuredClone(p.envelope.payload),d=payload.records.find(r=>r.type==='decision').payload;
 assert.equal(d.evaluationId,'cycle:1790867400000:0');assert.equal(d.direction,'none');assert.equal(d.engine.gates[2].operands.atr,0);edit(d);
 const changed=await buildMeasurementEnvelope({kind:p.envelope.kind,semanticId:p.envelope.semanticId,...p.envelope.clocks,payload}),ceiling=Math.max(at,...Object.values(p.envelope.clocks).filter(Number.isFinite))+86400000;
 const {db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>ceiling}),r=await c.ingest(changed.wire);assert.equal(r.status,'REJECTED');assert.equal(r.durable,false);const expected=['result','levels','classification','official','identity'].includes(name)?'measurement_decision_identity_invalid':['unsupportedGate','order','wrongReason'].includes(name)?'measurement_rejection_gate_identity_invalid':name==='laterEvaluated'?'measurement_rejection_gate_order_invalid':'measurement_rejection_atr_invalid';assert.equal(r.error,expected);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,0);
 }finally{db.close();}
});
test('actual Worker 7/7 rejected evaluations preserve original census and clocks exactly once',async()=>{
 const packets=await flatWorker(),ceiling=Math.max(at,...packets.flatMap(p=>Object.values(p.envelope.clocks).filter(Number.isFinite)))+86400000;
 const {db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>ceiling}),t=createOfflineMultipartTransport({consumer:c,clock:()=>ceiling});
 const before=packets.map(p=>p.wire);for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'ACCEPTED');for(const p of packets)assert.equal((await t.send(p)).consumerResult.status,'DUPLICATE');assert.deepEqual(packets.map(p=>p.wire),before);
 assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,7);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,8);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_measurement_state').get().n,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_outcome_evidence').get().n,0);
 }finally{db.close();}
});
const passEdits={
 'minimum-bars':o=>o.count=40,'candle-quality':o=>o.quality={ok:true},atr:o=>o.atr=1,'candle-age':o=>o.age=0,'receipt-age':o=>o.receiptAge=0,'provider-age':o=>o.providerAge=0,
 'news-calendar':o=>{o.calendarBlocked=false;o.gdeltBlocked=false;},score:o=>o.score=7.4,margin:o=>o.margin=2,'candle-confirmation':o=>o.candle=true,'mtf-confirmation':o=>o.confirm=o.required,'mtf-opposition':o=>o.oppositions=0,
 'ny-session':o=>o.nyBlocked=false,pivot:o=>o.pivotBlocked=false,'price-source':o=>o.consistent=true,'price-alignment':o=>o.aligned=true,
};
for(const id of Object.keys(passEdits))for(const mode of ['passOperand','missingOperands'])test('supported failed gate '+id+' rejects '+mode,async()=>{
 let p;
 for(const name of Object.keys(profiles)){const value=await profile(name);if(value.trace.gates.some(g=>g.id===id&&g.result==='FAIL')){p=value.packets[0];break;}}
 assert(p,id);const payload=structuredClone(p.envelope.payload),d=payload.records.find(r=>r.type==='decision').payload,g=d.engine.gates.find(g=>g.id===id);
 if(mode==='missingOperands')delete g.operands;else passEdits[id](g.operands);
 const changed=await buildMeasurementEnvelope({kind:p.envelope.kind,semanticId:p.envelope.semanticId,...p.envelope.clocks,payload});const {db,binding}=await database();try{const r=await createOfflineMeasurementConsumer(binding,{clock:()=>at}).ingest(changed.wire);assert.equal(r.status,'REJECTED');// Source consistency consumes alignment evidence first; malformed alignment
 // can therefore invalidate that earlier recorded dependency.
 assert.equal(r.error,'measurement_rejection_'+(id==='price-alignment'?'price-source':id)+'_invalid');assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,0);}finally{db.close();}
});
test('malformed Worker ATR through M3 is rejected before public consumer persistence',async()=>{
 const packets=await flatWorker(),p=packets[0],payload=structuredClone(p.envelope.payload);payload.records.find(r=>r.type==='decision').payload.engine.gates[2].operands.atr=1;
 const changed=await buildMeasurementEnvelope({kind:p.envelope.kind,semanticId:p.envelope.semanticId,...p.envelope.clocks,payload}),clock=Math.max(at,...Object.values(p.envelope.clocks).filter(Number.isFinite))+86400000;
 const {db,binding}=await database();try{const t=createOfflineMultipartTransport({clock:()=>clock,consumer:createOfflineMeasurementConsumer(binding,{clock:()=>clock})}),r=await t.send(changed);assert.equal(r.status,'DLQ');assert.equal(r.reason,'measurement_rejection_atr_invalid');assert.equal(t.metrics().consumerCalls,0);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,0);}finally{db.close();}
});
test('invalid census with recomputed census/outer digests cannot alter accepted canonical evidence',async()=>{
 const packets=await flatWorker(),cycle=packets[0],p=packets.find(p=>p.envelope.kind==='EVALUATION_CENSUS'),payload=structuredClone(p.envelope.payload);payload.decisionEvidence.engine.gates[2].operands.atr=1;payload.census=censusSnapshot(payload.decisionEvidence);
 const changed=await buildMeasurementEnvelope({kind:p.envelope.kind,semanticId:p.envelope.semanticId,...p.envelope.clocks,payload}),clock=Math.max(at,...packets.flatMap(p=>Object.values(p.envelope.clocks).filter(Number.isFinite)))+86400000;
 const {db,binding}=await database();try{const c=createOfflineMeasurementConsumer(binding,{clock:()=>clock});assert.equal((await c.ingest(cycle.wire)).status,'ACCEPTED');const before=db.prepare('SELECT * FROM signal_decision_evidence ORDER BY evaluation_id').all();const r=await c.ingest(changed.wire);assert.equal(r.status,'REJECTED');assert.equal(r.error,'measurement_rejection_atr_invalid');assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,1);assert.deepEqual(db.prepare('SELECT * FROM signal_decision_evidence ORDER BY evaluation_id').all(),before);}finally{db.close();}
});
test('Worker rejected-evaluation outer digest mismatch fails before persistence',async()=>{
 const packets=await flatWorker(),p=packets[0],e=JSON.parse(p.wire);e.payloadDigest='0'.repeat(64);const clock=Math.max(at,...Object.values(p.envelope.clocks).filter(Number.isFinite))+86400000;
 const {db,binding}=await database();try{const r=await createOfflineMeasurementConsumer(binding,{clock:()=>clock}).ingest(canonicalSerialize(e,2097152));assert.equal(r.status,'REJECTED');assert.equal(r.error,'measurement_envelope_digest_mismatch');assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_ingress_receipts').get().n,0);}finally{db.close();}
});
