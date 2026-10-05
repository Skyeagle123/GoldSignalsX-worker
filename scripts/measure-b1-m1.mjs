// Reproducible, offline-only wire/preparation benchmark. No storage resource.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
import {loadM1Workers,completeJournal,cycleEnvironment,now,provenance,BASELINE} from '../test-fixtures/b1-m1.mjs';
import * as sm from '../signal-measurement.js';
import * as engine from '../signal-engine.js';
import {copyFrozenEvidence,buildMeasurementEnvelope,PRODUCER_BOUNDS} from '../measurement-envelope.js';
import {prepareCycleEnvelopes} from '../measurement-producer.js';
import {installOfflineMeasurementProducer} from '../measurement-producer.js';
import {createOfflineTransport} from '../measurement-transport.js';
const output=process.argv[2],iterations=40,warmup=5;
if(!output)throw new Error('local output path required');
const savedClock=Date.now;Date.now=()=>now+1234;
const savedFetch=globalThis.fetch;globalThis.fetch=()=>{throw new Error('M1_BENCHMARK_NO_NETWORK');};
try{
 const {before,after}=await loadM1Workers(),sources={representative:completeJournal(after,sm),heavyValid:completeJournal(after,sm,{heavy:true}),activeOwnership:completeJournal(after,sm,{heavy:true,active:true})};
 // Price actual local persistence results, rather than the capture fixture's
 // minimal successful-persistence stand-in.
 for(const source of Object.values(sources))for(const candidate of source.candidates.filter(c=>source.officialIds.has(c.id))){
  const local=cycleEnvironment(after);
  try{await after.ensurePerformanceSchema(local.env);const result=await after.recordProductionPerformanceSafely(local.env,{...candidate,origin:'server'},'created');assert(result.ok);sm.rememberOfficialPersistence(candidate,'created',result);}
  finally{local.db.database.close();}
 }
 const wire=[],costs={};
 const summarize=v=>{const a=v.slice().sort((x,y)=>x-y);return {median:a[Math.floor(a.length/2)],p95:a[Math.ceil(a.length*.95)-1],maximum:a.at(-1)};};
 for(const [profile,source]of Object.entries(sources)){
  const samples=[],memory=[];let packets;
  for(let i=-warmup;i<iterations;i++){
   const times={},initial=process.memoryUsage(),initialCpu=process.cpuUsage(),started=performance.now();let t=performance.now();
   const frozen=copyFrozenEvidence(sm.decisionJournalObservations(source)),frozenProvenance=copyFrozenEvidence(provenance);
   times.copyFreezeMs=performance.now()-t;
   packets=await prepareCycleEnvelopes(frozen,frozenProvenance,{preparedAt:now+5000,measure:(name,value)=>{times[name]=(times[name]||0)+value;}});
   times.totalPreparationMs=performance.now()-started;
   const cpu=process.cpuUsage(initialCpu);times.totalProcessCpuMs=(cpu.user+cpu.system)/1000;
   const final=process.memoryUsage();if(i>=0){samples.push(times);memory.push({heapDeltaBytes:final.heapUsed-initial.heapUsed,rssDeltaBytes:final.rss-initial.rss});}
  }
  costs[profile]={iterations,warmup,stages:Object.fromEntries(Object.keys(samples[0]).map(k=>[k,summarize(samples.map(x=>x[k]))])),
   memory:{heapDeltaBytes:summarize(memory.map(x=>x.heapDeltaBytes)),rssDeltaBytes:summarize(memory.map(x=>x.rssDeltaBytes))},batchWireBytes:packets.reduce((n,p)=>n+p.wireBytes,0),
   inputCopyAccounting:'8 MiB charged scalar/key budget and 200000 nodes; charged bytes are not heap bytes'};
  for(const packet of packets)wire.push({profile,kind:packet.envelope.kind,semanticId:packet.envelope.semanticId,
   canonicalLogicalBytes:packet.payloadBytes,payloadBytes:packet.payloadBytes,wireBytes:packet.wireBytes,headerBytes:packet.headerBytes,fragmentationNeeded:packet.requiresFutureFragmentation});
 }
 // Real early Engine rejection, not a manufactured successful candidate.
 const source=sources.representative,tf='5m',trace={},includedMtf=engine.HIGHER_SIGNAL_TIMEFRAMES[tf]||[];
 const result=engine.computeServerSignal(source.frames[tf].bars,{tf,mtf:includedMtf.map(tf=>source.frames[tf]),live:source.live,barsSource:'d1',evaluationAt:now,filters:source.filters,dataQuality:{ok:false,reason:'synthetic-quality-failure'}},trace);
 const early=sm.beginDecisionCycle(now,source.frames,source.live,null,source.filters);early.capturedAt=now+1234;
 sm.observeAttempt(early,{tf,trace,result,requestedMtf:includedMtf,includedMtf});
 const earlyPackets=await prepareCycleEnvelopes(copyFrozenEvidence(sm.decisionJournalObservations(early)),provenance,{preparedAt:now+5000});
 for(const p of earlyPackets)wire.push({profile:'earlyRejection',kind:p.envelope.kind,semanticId:p.envelope.semanticId,canonicalLogicalBytes:p.payloadBytes,payloadBytes:p.payloadBytes,wireBytes:p.wireBytes,headerBytes:p.headerBytes,fragmentationNeeded:p.requiresFutureFragmentation});
 // Exact observed confirmation link from the unchanged worker helper.
 const primary=sources.heavyValid.candidates.find(c=>sources.heavyValid.officialIds.has(c.id)),confirmation=sources.heavyValid.candidates.find(c=>sources.heavyValid.confirmationIds.has(c.id));
 const linked=after.linkMtfConfirmation(primary,confirmation,primary.id,now);
 assert(linked);const facts=[{profile:'confirmation',kind:'CONFIRMATION_LINK',semanticId:`${primary.id}:confirmation:${confirmation.id}`,occurredAt:now,observedAt:null,
  payload:{primarySignalId:primary.id,confirmationSignalId:confirmation.id,link:linked.mtfConfirmations.at(-1),linkPersistence:'SUCCEEDED',measurementOnly:true,decisionUse:false}}];
 const base={...primary,origin:'server',updatedAt:now,lastPrice:primary.entry};
 for(const [name,bar,at]of [
  ['tp1',{t:now,h:base.tp1+.001,l:base.entry-.001,c:base.tp1},now+60000],
  ['tp2',{t:now,h:base.tp2+.001,l:base.entry-.001,c:base.tp2},now+60000],
  ['sl',{t:now,h:base.entry+.001,l:base.sl-.001,c:base.sl},now+60000],
  ['expired',null,now+engine.signalExpiryMs(base.tf)+1]]){
  const signal=engine.updateSignalLifecycle(base,bar,bar?.c??base.entry,at,'mt5'),event=name;
  assert.equal(signal.status,name==='sl'?'stopped':name);
  const occurredAt=signal.triggeredAt??signal.closedAt??signal.updatedAt;
  const local=cycleEnvironment(after),captureClock=Date.now;Date.now=()=>at;let persistence;
  try{await after.ensurePerformanceSchema(local.env);persistence=await after.recordProductionPerformanceSafely(local.env,signal,event);}finally{Date.now=captureClock;local.db.database.close();}
  assert(persistence.ok);
  facts.push({profile:'lifecycle-'+name,kind:'LIFECYCLE_FACT',semanticId:`production:${signal.id}:${event}`,occurredAt,observedAt:at,
   payload:{signalId:signal.id,event,createdAt:signal.createdAt,timeframe:signal.tf,direction:signal.side,occurredAt,
    observedPrice:signal.triggerPrice??signal.lastPrice,level:signal[event==='expired'?'entry':event],trigger:{price:signal.triggerPrice??null,at:signal.triggeredAt??null,source:signal.triggerSource??null},
    status:signal.status,closedAt:signal.closedAt??null,performancePersistence:persistence,orderingQuality:'NOT_ASSERTED',measurementOnly:true,decisionUse:false}});
 }
 facts.push({profile:'capture-gap',kind:'CAPTURE_GAP',semanticId:'local-gap:cycle',occurredAt:now,observedAt:now+100,
  payload:{status:'TRANSPORT_UNAVAILABLE',cycleId:source.cycleId,captureGap:true,durable:false,measurementOnly:true,decisionUse:false}});
 for(const fact of facts){const {profile,...fields}=fact;const p=await buildMeasurementEnvelope({...fields,preparedAt:now+5000});wire.push({profile,kind:p.envelope.kind,semanticId:p.envelope.semanticId,canonicalLogicalBytes:p.payloadBytes,payloadBytes:p.payloadBytes,wireBytes:p.wireBytes,headerBytes:p.headerBytes,fragmentationNeeded:p.requiresFutureFragmentation});}
 const groups={};for(const row of wire){const key=row.profile+':'+row.kind;(groups[key]||=[]).push(row.wireBytes);}
 // Paired local latency comparison ends when runSignalCycle settles: no
 // deferred preparation is charged to this pre-completion measurement.
 const criticalSamples=[],deferredSamples=[];
 for(let i=-warmup;i<iterations;i++){
  const sample={};
  for(const [kind,module]of (i%2?[['enabled',after],['baseline',before]]:[['baseline',before],['enabled',after]])){
   const x=cycleEnvironment(module,{telegram:true}),enabled=kind==='enabled';
   x.env.B1_PRODUCER_CAPTURE_ENABLED=enabled?'1':'0';x.env.B1_CODE_COMMIT=BASELINE;x.env.B1_MEASUREMENT_EFFECTIVE_AT=provenance.measurementEffectiveAt;
   const producer=installOfflineMeasurementProducer(x.env,{transport:createOfflineTransport()});
   try{
    const started=performance.now();await module.runSignalCycle(x.env,null,{nyFilterOn:false,pivotFilterOn:false});
    const completed=performance.now();sample[kind+'TradingMs']=completed-started;
    if(enabled){await producer.whenIdle();sample.deferredEndToIdleMs=performance.now()-completed;
     assert.equal(producer.diagnostics().length,0);}
   }finally{x.db.database.close();}
  }
  if(i>=0){criticalSamples.push({...sample,preTradingIncrementMs:sample.enabledTradingMs-sample.baselineTradingMs});deferredSamples.push(sample.deferredEndToIdleMs);}
 }
 const report={baseline:BASELINE,node:process.version,iterations,warmup,bounds:PRODUCER_BOUNDS,wire,wireDistributions:Object.fromEntries(Object.entries(groups).map(([k,a])=>[k,{count:a.length,minimum:Math.min(...a),...summarize(a)}])),
  maximumTestedEnvelopeBytes:Math.max(...wire.map(x=>x.wireBytes)),costs,
  tradingBoundaryCost:{samples:iterations,profiles:['exact baseline','M1 enabled, offline mock'],
   baselineTradingMs:summarize(criticalSamples.map(x=>x.baselineTradingMs)),
   enabledTradingMs:summarize(criticalSamples.map(x=>x.enabledTradingMs)),
   preTradingIncrementMs:summarize(criticalSamples.map(x=>x.preTradingIncrementMs)),
   deferredEndToIdleMs:summarize(deferredSamples)},
  limitations:['Tested maxima are not universal evidence maxima. Wire/input/node bounds are enforced failure boundaries, not truncation.','The paired preTradingIncrementMs is a noisy local end-to-end latency difference, not isolated instruction CPU or a hard worst-case bound; negative samples are possible. DeferredEndToIdle includes local timer/transport as well as preparation.','Local elapsed timing includes hashing awaits and GC; process CPU includes runtime/GC/native hashing work and is not Worker isolate CPU. Neither is a Production guarantee.','capturePreparationMs includes reused baseline manifest/snapshot construction and its own canonicalization/hashing; envelope-specific stages are separately timed.','Memory deltas can be negative because GC runs; they are not per-envelope heap maxima.','No transport fragmentation, network, durable capture, infrastructure or Production access.']};
 await fs.writeFile(output,JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({maximum:report.maximumTestedEnvelopeBytes,costs:report.costs},null,2));
}finally{Date.now=savedClock;globalThis.fetch=savedFetch;}
