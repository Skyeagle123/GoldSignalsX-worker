import assert from 'node:assert/strict';
import {test} from 'node:test';
import fs from 'node:fs/promises';
import {localMeasurementDatabase} from './test-fixtures/b1-outcome-profiles.mjs';
import {transactionalBinding} from './test-fixtures/b1-sqlite.mjs';
import {loadM1Workers,completeJournal,cycleEnvironment,BASELINE,now,provenance} from './test-fixtures/b1-m1.mjs';
import * as sm from './signal-measurement.js';
import {canonicalSerialize,digestPayload,parseEvidence} from './signal-evidence.js';
import {copyFrozenEvidence,buildMeasurementEnvelope,ENVELOPE_VERSION,PRODUCER_BOUNDS} from './measurement-envelope.js';
import {createOfflineTransport} from './measurement-transport.js';
import {createMeasurementProducer,installOfflineMeasurementProducer,prepareCycleEnvelopes,PRODUCER_GATE} from './measurement-producer.js';
const {before,after,oldSm}=await loadM1Workers();
const fact=(payload={signalId:'subject',status:'tp1'},kind='LIFECYCLE_FACT')=>({kind,semanticId:'production:subject:tp1',payload,occurredAt:now-19,observedAt:now-7});
const envelope=input=>buildMeasurementEnvelope({...input,preparedAt:now+31});
const throwFetch=()=>{throw new Error('M1_NO_EXTERNAL_IO');};

test('M1 deterministic transport identity is separate from unchanged semantic identity',async()=>{
 const a=await envelope(fact()),b=await buildMeasurementEnvelope({...fact(),preparedAt:now+99});
 assert.equal(a.envelope.eventId,b.envelope.eventId);assert.equal(a.envelope.semanticId,'production:subject:tp1');
 assert.equal(a.envelope.payloadDigest,await digestPayload(canonicalSerialize({clocks:{occurredAt:now-19,observedAt:now-7},payload:fact().payload})));
 assert.equal(a.envelope.version,ENVELOPE_VERSION);assert.equal(a.envelope.measurementOnly,true);assert.equal(a.envelope.decisionUse,false);
 const c=await envelope(fact({signalId:'subject',status:'tp2'}));assert.equal(a.envelope.semanticKey,c.envelope.semanticKey);assert.notEqual(a.envelope.eventId,c.envelope.eventId);
});
test('M1 exact clocks, null, unavailable, nonfinite values and wire round-trip',async()=>{
 const a=await envelope(fact({nullValue:null,missing:undefined,n:NaN,negativeZero:-0}));
 assert.deepEqual(a.envelope.clocks,{occurredAt:now-19,observedAt:now-7,preparedAt:now+31});
 assert.equal(a.envelope.payload.nullValue,null);assert.deepEqual(a.envelope.payload.missing,{$unavailable:'undefined'});
 const restored=parseEvidence(a.wire);assert(Number.isNaN(restored.payload.n));assert(Object.is(restored.payload.negativeZero,-0));
 assert.equal(a.wireBytes,Buffer.byteLength(a.wire));assert.equal(a.headerBytes+a.payloadBytes,a.wireBytes);
});
test('M1 deeply immutable copies protect nested arrays and Map/Set membership',async()=>{
 const source={a:[{n:1}],map:new Map([['id',{n:2}]]),set:new Set(['id'])};const copy=copyFrozenEvidence(source);
 source.a[0].n=10;source.map.set('id',{n:20});source.set.clear();assert.equal(copy.a[0].n,1);assert.equal(copy.map.get('id').n,2);assert(copy.set.has('id'));
 assert.throws(()=>{copy.a[0].n=3;},TypeError);assert.equal(copy.map.set,undefined);assert.equal(copy.set.add,undefined);
 const p=await envelope(fact({nested:{a:[1]}}));assert.throws(()=>p.envelope.payload.nested.a.push(2));
});
test('M1 version, kind, identity, clock, depth, node and input byte validation',async()=>{
 for(const change of [{kind:'UNKNOWN'},{semanticId:''},{occurredAt:-1},{preparedAt:NaN},{maxWireBytes:PRODUCER_BOUNDS.maxWireBytes+1}])await assert.rejects(buildMeasurementEnvelope({...fact(),preparedAt:now,...change}));
 assert.throws(()=>copyFrozenEvidence({f:()=>0}));const circular={};circular.self=circular;assert.throws(()=>copyFrozenEvidence(circular));
 assert.throws(()=>copyFrozenEvidence('x'.repeat(100),{maxInputBytes:10}));assert.throws(()=>copyFrozenEvidence([1,2],{maxNodes:2}));
 assert.throws(()=>copyFrozenEvidence({a:{b:1}},{maxDepth:1}));
 await assert.rejects(envelope(fact({authorization:'not-a-credential'})),/measurement_sensitive_field/);
});
test('M1 exact wire boundary rejects overflow without dropping a field',async()=>{
 const input=fact({value:'x'.repeat(1000)}),p=await envelope(input);
 assert.equal((await buildMeasurementEnvelope({...input,preparedAt:now+31,maxWireBytes:p.wireBytes})).wire,p.wire);
 await assert.rejects(buildMeasurementEnvelope({...input,preparedAt:now+31,maxWireBytes:p.wireBytes-1}),/exceeded/);
});
test('M1 OFF does not inspect evidence, call clocks, schedule or send',()=>{
 const bad=new Proxy({},{get(){throw new Error('OFF_MUST_NOT_READ');}});
 const p=createMeasurementProducer({clock:()=>{throw new Error('OFF_CLOCK');},schedule:()=>{throw new Error('OFF_SCHEDULE');},transport:{mode:'OFFLINE',send(){throw new Error('OFF_SEND');}}});
 assert.equal(p.submitFact(bad).status,'OFF');assert.equal(p.submitCycle(bad,bad).status,'OFF');assert.equal(p.diagnostics().length,0);
});
test('M1 absent/disabled transport exposes only local nondurable capture gaps',()=>{
 for(const transport of [null,{mode:'DISABLED',send(){throw new Error('must not call');}}]){
  const p=createMeasurementProducer({enabled:true,transport});assert.equal(p.submitFact(fact()).status,'TRANSPORT_UNAVAILABLE');assert.equal(p.diagnostics()[0].durable,false);
 }
});
test('M1 mock success is asynchronous; input mutation cannot alter captured evidence',async()=>{
 const jobs=[],transport=createOfflineTransport(),p=createMeasurementProducer({enabled:true,transport,schedule:f=>jobs.push(f),clock:()=>now});
 const input=fact({signalId:'subject',nested:{price:1}});assert.equal(p.submitFact(input).status,'SCHEDULED');assert.equal(transport.packets().length,0);
 input.payload.nested.price=9;jobs[0]();await p.whenIdle();assert.equal(transport.packets()[0].envelope.payload.nested.price,1);assert.equal(p.diagnostics().length,0);
});
test('M1 synchronous transport throw, asynchronous reject and failed status are isolated',async()=>{
 for(const send of [()=>{throw new Error('sync');},()=>Promise.reject(new Error('async')),()=>({status:'FAILED'})]){
  const p=createMeasurementProducer({enabled:true,transport:{mode:'OFFLINE',send}});assert.equal(p.submitFact(fact()).status,'SCHEDULED');await p.whenIdle();assert.equal(p.diagnostics()[0].status,'DELIVERY_FAILED');
 }
});
test('M1 hanging transport has bounded diagnostic completion and permanently occupied slots',async()=>{
 const p=createMeasurementProducer({enabled:true,transport:{mode:'OFFLINE',send:()=>new Promise(()=>{})},deliveryTimeoutMs:5,maxPending:6});
 for(let i=0;i<4;i++)p.submitFact({...fact(),semanticId:'hang:'+i});await p.whenIdle();assert.equal(p.state().unsettledSends,4);
 assert.equal(p.submitFact(fact()).status,'PRODUCER_CAPACITY_EXCEEDED');assert.equal(p.diagnostics().filter(x=>x.status==='DELIVERY_TIMEOUT').length,4);
});
test('M1 serialization and oversized envelope failures never reach transport',async()=>{
 for(const input of [fact({fn:()=>0}),fact({v:'x'.repeat(5000)})]){
  let sends=0;const p=createMeasurementProducer({enabled:true,maxWireBytes:1000,transport:{mode:'OFFLINE',send(){sends++;}}});p.submitFact(input);await p.whenIdle();assert.equal(sends,0);assert(p.diagnostics().some(x=>['PREPARATION_FAILED','ENVELOPE_EXCEEDED'].includes(x.status)));
 }
});
test('M1 duplicate/conflicting payload handling includes concurrent delivery',async()=>{
 const p=await envelope(fact()),transport=createOfflineTransport();const results=await Promise.all([transport.send(p),transport.send(p)]);
 assert.deepEqual(results.map(x=>x.status),['OFFLINE_ACCEPTED','DUPLICATE']);assert.equal(transport.packets().length,1);
 assert.equal((await transport.send(await envelope(fact({changed:true})))).status,'INTEGRITY_CONFLICT');
 const producer=createMeasurementProducer({enabled:true,transport});producer.submitFact(fact({changed:true}));await producer.whenIdle();assert.equal(producer.diagnostics()[0].status,'INTEGRITY_CONFLICT');
});
test('M1 failed mock delivery permits truthful retry; offline memory is bounded',async()=>{
 let fail=true;const transport=createOfflineTransport({deliver(){if(fail)throw new Error('fixture');},maxPackets:1});const p=await envelope(fact());
 await assert.rejects(transport.send(p));fail=false;assert.equal((await transport.send(p)).status,'OFFLINE_ACCEPTED');
 assert.equal((await transport.send(await envelope({...fact(),semanticId:'other'}))).status,'OFFLINE_CAPACITY_EXCEEDED');
});
test('M1 producer scheduler/diagnostic callback failure is isolated',async()=>{
 for(const opts of [{schedule(){throw new Error('scheduler');}},{schedule:async()=>{throw new Error('async scheduler');}},{onDiagnostic(){throw new Error('diagnostic');}},{onDiagnostic:async()=>{throw new Error('async diagnostic');}}]){
  const p=createMeasurementProducer({enabled:true,transport:{mode:'OFFLINE',send(){throw new Error('transport');}},...opts});assert.doesNotThrow(()=>p.submitFact(fact()));await p.whenIdle();assert(p.diagnostics().length>0);assert.equal(p.state().pending,0);
 }
});
test('M1 all supported fact classes preserve observations without inventing unavailable fields',async()=>{
 for(const kind of ['EVALUATION_CENSUS','OFFICIAL_CREATION','CONFIRMATION_LINK','LIFECYCLE_FACT','CAPTURE_GAP']){
  const p=await envelope(fact({observed:null,measurementOnly:true,decisionUse:false},kind));assert.equal(p.envelope.kind,kind);assert.equal(p.envelope.payload.observed,null);
 }
});
test('M1 extracted preparer preserves exact d95d804b persisted capture records',async()=>{
 for(const options of [{},{heavy:true},{heavy:true,active:true}]){
  const clock=Date.now;Date.now=()=>now+1234;
  const a=completeJournal(before,oldSm,options),b=completeJournal(after,sm,options),da=localMeasurementDatabase(),db=localMeasurementDatabase();
  da.function('julianday',_value=>now/86400000+2440587.5);db.function('julianday',_value=>now/86400000+2440587.5);
  try{
   assert((await oldSm.persistDecisionCycle(transactionalBinding(da),a,provenance)).ok);
   assert((await sm.persistDecisionCycle(transactionalBinding(db),b,provenance)).ok);
   for(const {name} of da.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all())assert.deepEqual(db.prepare(`SELECT * FROM ${name}`).all(),da.prepare(`SELECT * FROM ${name}`).all(),name);
   const packets=await prepareCycleEnvelopes(copyFrozenEvidence(sm.decisionJournalObservations(b)),provenance,{preparedAt:now+5000});
   assert.equal(packets.filter(p=>p.envelope.kind==='EVALUATION_CENSUS').length,7);assert.equal(packets.filter(p=>p.envelope.kind==='OFFICIAL_CREATION').length,options.active?0:1);
  }finally{Date.now=clock;da.close();db.close();}
 }
});

function productionSnapshot(x){return Object.fromEntries(x.db.database.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'production_%' ORDER BY name").all().map(({name})=>[name,x.db.database.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()]));}
const exposure={symbol:'XAUUSD',status:'active',side:'buy',primarySignalId:'owner',primaryTf:'30m',openedAt:now-10000,maxPositions:1,cooldownUntil:0,confirmations:[],blocked:[]};
const activeSignal={id:'owner',tf:'30m',side:'buy',createdAt:now-10000,signalBarTs:now-1800000,entry:4100,tp1:4200,tp2:4300,sl:4000,status:'tp1',tp1Hit:true,updatedAt:now-5000,lastPrice:4100,conf:90};
const cases=[{}, {fullArrays:true}, {stale:true}, {news:{ok:false,stale:true,safety:{calendarBlockTechnicalSignal:true,reason:'fixture-high'}}},
 {exposure}, {exposure:{...exposure,side:'sell'}}, {exposure:{...exposure,status:'cooldown',cooldownUntil:now+1800000}},
 {activeSignal,exposure}, {activeSignal:{...activeSignal,status:'active',tp1Hit:false,tp1:4099.99},exposure}, {activeSignal:{...activeSignal,status:'active',tp1Hit:false,tp1:4099,tp2:4100},exposure},
 {activeSignal:{...activeSignal,status:'active',tp1Hit:false,sl:4101},exposure}, {activeSignal:{...activeSignal,createdAt:now-8*86400000},exposure}, {activeSignal:{...activeSignal,createdAt:Date.UTC(2026,8,26,12)},exposure}, {telegram:true}];
const modes=['off','success','throw','reject','hang','absent','serialize-fail','oversize','callback-fail'];
test('M1 exact-baseline full-cycle differential including fault-injected producer, lifecycle, News, Exposure, cooldown and Telegram',async()=>{
 const clock=Date.now,fetch=globalThis.fetch;Date.now=()=>now;globalThis.fetch=throwFetch;
 try{for(const options of cases)for(const mode of modes){
  const a=cycleEnvironment(before,options),b=cycleEnvironment(after,options);
  const transport=mode==='absent'?null:mode==='throw'?{mode:'OFFLINE',send(){throw new Error('fixture');}}:mode==='reject'?{mode:'OFFLINE',send:()=>Promise.reject(new Error('fixture'))}:mode==='hang'?{mode:'OFFLINE',send:()=>new Promise(()=>{})}:createOfflineTransport();
  b.env[PRODUCER_GATE]=mode==='off'?'0':'1';b.env.B1_CODE_COMMIT=BASELINE;b.env.B1_MEASUREMENT_EFFECTIVE_AT=provenance.measurementEffectiveAt;
  const producer=installOfflineMeasurementProducer(b.env,{transport,deliveryTimeoutMs:5,maxWireBytes:mode==='oversize'?1000:PRODUCER_BOUNDS.maxWireBytes,...(mode==='callback-fail'?{schedule(){throw new Error('fixture');}}:{})});
  if(mode==='serialize-fail'){b.env.B1_CODE_COMMIT='invalid-provenance';}
  const original=await before.runSignalCycle(a.env,options.news,{nyFilterOn:false,pivotFilterOn:false});
  const actual=await after.runSignalCycle(b.env,options.news,{nyFilterOn:false,pivotFilterOn:false});
  assert.deepEqual(actual,original,mode);assert.deepEqual(b.writes,a.writes,mode);assert.deepEqual(b.decisions,a.decisions,mode);assert.deepEqual(productionSnapshot(b),productionSnapshot(a),mode);assert.deepEqual(b.cancellations,a.cancellations,mode);assert.deepEqual(b.telegramEvents,a.telegramEvents,mode);
  await producer.whenIdle();assert.equal(producer.state().pending,0);
  if(mode==='success'){assert.equal(producer.diagnostics().length,0);assert(transport.packets().some(p=>p.envelope.kind==='DECISION_CYCLE'));assert.equal(transport.packets().filter(p=>p.envelope.kind==='EVALUATION_CENSUS').length,7);}
  a.db.database.close();b.db.database.close();
 }}finally{Date.now=clock;globalThis.fetch=fetch;}
});
test('M1 exact-baseline Official KV failure/cancellation and independent performance failure parity',async()=>{
 const clock=Date.now,fetch=globalThis.fetch;Date.now=()=>now;globalThis.fetch=throwFetch;
 try{for(const failure of ['kv','performance']){
  const a=cycleEnvironment(before),b=cycleEnvironment(after);b.env[PRODUCER_GATE]='1';b.env.B1_CODE_COMMIT=BASELINE;b.env.B1_MEASUREMENT_EFFECTIVE_AT=provenance.measurementEffectiveAt;
  const producer=installOfflineMeasurementProducer(b.env,{transport:createOfflineTransport()});
  for(const x of [a,b])if(failure==='kv'){const put=x.env.GSX_KV.put;x.env.GSX_KV.put=async(k,v)=>{if(k.startsWith('signal:state:'))throw new Error('KV_FAILURE');return put(k,v);};}else{const prepare=x.db.prepare.bind(x.db);x.db.prepare=sql=>{if(/INSERT[\s\S]*production_signals/.test(sql))throw new Error('PERFORMANCE_FAILURE');return prepare(sql);};}
  const outcomes=[];for(const [module,x]of [[before,a],[after,b]])try{outcomes.push(await module.runSignalCycle(x.env,null,{nyFilterOn:false,pivotFilterOn:false}));}catch(e){outcomes.push({error:e.message});}
  assert.deepEqual(outcomes[1],outcomes[0]);assert.deepEqual(b.writes,a.writes);assert.deepEqual(b.decisions,a.decisions);assert.deepEqual(b.cancellations,a.cancellations);assert.deepEqual(productionSnapshot(b),productionSnapshot(a));await producer.whenIdle();
  a.db.database.close();b.db.database.close();
 }}finally{Date.now=clock;globalThis.fetch=fetch;}
});
test('M1 changed runtime modules contain no transport network/platform API or new migration/configuration',async()=>{
 for(const name of ['measurement-envelope.js','measurement-producer.js','measurement-transport.js']){
  const source=await fs.readFile(new URL(name,import.meta.url),'utf8');assert(!/\bfetch\s*\(|https?:\/\/|cloudflare:|\.GSX_DB\b|\.R2\b|\.queue\s*\(/.test(source),name);
 }
});

test('M1 trading completion does not wait for producer scheduling or a transport promise',async()=>{
 const clock=Date.now,fetch=globalThis.fetch;Date.now=()=>now;globalThis.fetch=throwFetch;
 const x=cycleEnvironment(after);x.env[PRODUCER_GATE]='1';x.env.B1_CODE_COMMIT=BASELINE;x.env.B1_MEASUREMENT_EFFECTIVE_AT=provenance.measurementEffectiveAt;
 const jobs=[];let sends=0;const producer=installOfflineMeasurementProducer(x.env,{schedule:job=>jobs.push(job),deliveryTimeoutMs:5,transport:{mode:'OFFLINE',send(){sends++;return new Promise(()=>{});}}});
 let timer;
 try{
  const result=await Promise.race([after.runSignalCycle(x.env,null,{nyFilterOn:false,pivotFilterOn:false}),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('TRADING_WAITED_FOR_PRODUCER')),1000);})]);
  assert(result.telemetry);assert(jobs.length>0);assert.equal(sends,0);for(const job of jobs)job();await producer.whenIdle();assert(producer.diagnostics().length>0);
 }finally{clearTimeout(timer);Date.now=clock;globalThis.fetch=fetch;x.db.database.close();}
});
test('M1 unavailable clock differs from explicit null; accessor properties are never executed',async()=>{
 const packet=await buildMeasurementEnvelope({kind:'CAPTURE_GAP',semanticId:'gap',payload:{},occurredAt:undefined,observedAt:null,preparedAt:now});
 assert.equal(packet.envelope.clocks.occurredAt,undefined);assert.deepEqual(JSON.parse(packet.wire).clocks.occurredAt,{$unavailable:'undefined'});assert.equal(packet.envelope.clocks.observedAt,null);
 let reads=0;const value={get bad(){reads++;throw new Error('getter');}};assert.throws(()=>copyFrozenEvidence(value));assert.equal(reads,0);
});
test('M1 evidence clock conflicts are detectable while preparation time is not identity',async()=>{
 const a=await envelope(fact()),b=await envelope({...fact(),occurredAt:now});
 assert.equal(a.envelope.semanticKey,b.envelope.semanticKey);assert.notEqual(a.envelope.payloadDigest,b.envelope.payloadDigest);assert.notEqual(a.envelope.eventId,b.envelope.eventId);
});
test('M1 retries preserve first capture time without mutating the source journal',async()=>{
 const j=completeJournal(after,sm);delete j.capturedAt;let time=now;
 const transport=createOfflineTransport(),p=createMeasurementProducer({enabled:true,transport,clock:()=>++time});
 p.submitCycle(j,provenance);await p.whenIdle();const first=transport.packets().map(x=>x.envelope.eventId);
 p.submitCycle(j,provenance);await p.whenIdle();assert.deepEqual(transport.packets().map(x=>x.envelope.eventId),first);assert.equal(j.capturedAt,undefined);assert.equal(p.diagnostics().length,0);
});
test('M1 local capture diagnostics cannot retain unbounded identifiers',()=>{
 const p=createMeasurementProducer({enabled:true});p.submitFact({semanticId:'x'.repeat(5000)});assert.equal(p.diagnostics()[0].semanticId,null);
});
test('M1 enabled shadow capture does not add reads to the trading Date.now clock sequence',async()=>{
 const originalClock=Date.now,fetch=globalThis.fetch;let reads=0;Date.now=()=>{reads++;return now;};globalThis.fetch=throwFetch;
 const a=cycleEnvironment(before),b=cycleEnvironment(after);b.env[PRODUCER_GATE]='1';b.env.B1_CODE_COMMIT=BASELINE;b.env.B1_MEASUREMENT_EFFECTIVE_AT=provenance.measurementEffectiveAt;
 const jobs=[],producer=installOfflineMeasurementProducer(b.env,{transport:createOfflineTransport(),schedule:job=>jobs.push(job)});
 try{
  reads=0;const original=await before.runSignalCycle(a.env,null,{nyFilterOn:false,pivotFilterOn:false});const oldReads=reads;
  reads=0;const actual=await after.runSignalCycle(b.env,null,{nyFilterOn:false,pivotFilterOn:false});assert.equal(reads,oldReads);assert.deepEqual(actual,original);
  for(const job of jobs)job();await producer.whenIdle();
 }finally{Date.now=originalClock;globalThis.fetch=fetch;a.db.database.close();b.db.database.close();}
});
