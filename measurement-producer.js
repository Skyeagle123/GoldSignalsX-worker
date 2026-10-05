// M1 offline shadow producer. No awaited trading dependency or durable outbox.
import {prepareDecisionCycle,decisionJournalObservations} from './signal-measurement.js';
import {censusSnapshot} from './evidence-codec.js';
import {copyFrozenEvidence,buildMeasurementEnvelope,PRODUCER_BOUNDS} from './measurement-envelope.js';

export const PRODUCER_GATE='B1_PRODUCER_CAPTURE_ENABLED';
const injected=new WeakMap(),defaults=new WeakMap();
export function measurementProducerEnabled(env){try{return String(env?.[PRODUCER_GATE]??'0').trim()==='1';}catch{return false;}}

export async function prepareCycleEnvelopes(journal,provenance,{preparedAt,maxWireBytes=PRODUCER_BOUNDS.maxWireBytes,measure=null}={}){
 const started=performance.now();
 const prepared=await prepareDecisionCycle(journal,provenance);
 measure?.('capturePreparationMs',performance.now()-started);
 const packet=(kind,semanticId,payload,occurredAt,observedAt)=>buildMeasurementEnvelope({kind,semanticId,payload,occurredAt,observedAt,preparedAt,maxWireBytes,measure});
 const packets=[await packet('DECISION_CYCLE',journal.cycleId,{records:prepared.records,officialPins:prepared.officialPins,links:prepared.links},journal.evaluatedAt,prepared.capturedAt)];
 for(const record of prepared.records.filter(r=>r.type==='decision')){
  const s=record.payload;
  packets.push(await packet('EVALUATION_CENSUS',s.evaluationId,{values:record.values,census:censusSnapshot(s),decisionEvidence:s},s.evaluatedAt??journal.evaluatedAt,s.capturedAt));
  if(s.kind==='OFFICIAL')packets.push(await packet('OFFICIAL_CREATION',s.officialSignalId,{evaluationId:s.evaluationId,officialSignalId:s.officialSignalId,
   officialPersisted:s.exposure.officialPersisted,performancePersistence:s.officialPersistence,decisionEvidence:s},s.createdAt,s.capturedAt));
 }
 // Whole shadow delivery is bounded too, not merely each individual envelope.
 if(packets.reduce((sum,p)=>sum+p.wireBytes,0)>maxWireBytes)throw new Error('measurement_envelope_batch_exceeded');
 return Object.freeze(packets);
}

export function createMeasurementProducer({enabled=false,transport=null,schedule=fn=>queueMicrotask(fn),clock=()=>new Date().getTime(),
 onDiagnostic=null,deliveryTimeoutMs=1000,maxPending=PRODUCER_BOUNDS.maxPending,maxWireBytes=PRODUCER_BOUNDS.maxWireBytes}={}){
 if(!Number.isInteger(deliveryTimeoutMs)||deliveryTimeoutMs<1||deliveryTimeoutMs>1000||!Number.isInteger(maxPending)||maxPending<3||maxPending>PRODUCER_BOUNDS.maxPending||!Number.isInteger(maxWireBytes)||maxWireBytes<1||maxWireBytes>PRODUCER_BOUNDS.maxWireBytes)throw new Error('measurement_producer_bound_invalid');
 const tasks=new Set(),diagnostics=[],captureTimes=new WeakMap();let pending=0,unsettledSends=0,pendingCycles=0,pendingFacts=0,unsettledCycles=0,unsettledFacts=0,deferred=0;
 function diagnostic(status,semanticId=null){
  if(typeof semanticId!=='string'||semanticId.length>1024||new TextEncoder().encode(semanticId).length>1024)semanticId=null;
  const value=Object.freeze({status,semanticId,measurementOnly:true,decisionUse:false,captureGap:true,durable:false});
  diagnostics.push(value);if(diagnostics.length>32)diagnostics.shift();
  try{const callback=onDiagnostic?.(value);if(callback&&typeof callback.then==='function')Promise.resolve(callback).catch(()=>{});}catch{}return value;
 }
 function submit(input,cycle,provenance){
  if(!enabled)return Object.freeze({status:'OFF',durable:false});
  try{if(!transport||transport.mode!=='OFFLINE'||typeof transport.send!=='function')return diagnostic('TRANSPORT_UNAVAILABLE',input?.semanticId??input?.cycleId??null);}
  catch{return diagnostic('TRANSPORT_UNAVAILABLE');}
  // Reserve cycle capacity independently: six same-cycle confirmations must
  // not consume the slot for their completed seven-attempt census.
  if(cycle?pendingCycles+unsettledCycles>=PRODUCER_BOUNDS.maxPendingCycles:pendingFacts+unsettledFacts>=maxPending-PRODUCER_BOUNDS.maxPendingCycles)return diagnostic('PRODUCER_CAPACITY_EXCEEDED',input?.semanticId??input?.cycleId??null);
  let frozen,frozenProvenance;
  try{
   const source=cycle?decisionJournalObservations(input):input;
   if(cycle&&source.capturedAt==null&&!captureTimes.has(input))captureTimes.set(input,clock());
   frozen=copyFrozenEvidence(cycle?{...source,capturedAt:source.capturedAt??captureTimes.get(input)}:source,cycle?{}:{maxInputBytes:PRODUCER_BOUNDS.maxFactInputBytes});
   if(cycle)frozenProvenance=copyFrozenEvidence(provenance);
  }catch{return diagnostic('PREPARATION_FAILED',input?.semanticId??input?.cycleId??null);}
  pending++;if(cycle)pendingCycles++;else pendingFacts++;
  const release=()=>{pending--;if(cycle)pendingCycles--;else pendingFacts--;};
  let resolveDone;const done=new Promise(resolve=>{resolveDone=resolve;});tasks.add(done);
  let started=false;
  const job=()=>{
   if(started)return;started=true;
   (async()=>{
    let packets;
    try{const preparedAt=clock();packets=cycle?await prepareCycleEnvelopes(frozen,frozenProvenance,{preparedAt,maxWireBytes}):
     [await buildMeasurementEnvelope({...frozen,preparedAt,maxWireBytes:Math.min(maxWireBytes,PRODUCER_BOUNDS.maxFactWireBytes)})];
    }catch(error){diagnostic(/exceeded$/.test(String(error?.message))?'ENVELOPE_EXCEEDED':'PREPARATION_FAILED',frozen.semanticId??frozen.cycleId);return;}
    for(const packet of packets){
     const controller=new AbortController();let timer;
     if(cycle?unsettledCycles>=PRODUCER_BOUNDS.maxPendingCycles:unsettledFacts>=maxPending-PRODUCER_BOUNDS.maxPendingCycles){diagnostic('PRODUCER_CAPACITY_EXCEEDED',packet.envelope.semanticId);return;}
     unsettledSends++;if(cycle)unsettledCycles++;else unsettledFacts++;
     // A timed-out uncooperative transport keeps its slot until it actually
     // settles. Repeated hangs therefore cannot spawn unbounded pending sends.
     const send=Promise.resolve().then(()=>transport.send(packet,{signal:controller.signal}));
     const settled=()=>{unsettledSends--;if(cycle)unsettledCycles--;else unsettledFacts--;};
     send.then(settled,settled);
     try{
      const result=await Promise.race([send,new Promise(resolve=>{timer=setTimeout(()=>{controller.abort();resolve({status:'DELIVERY_TIMEOUT'});},deliveryTimeoutMs);})]);
      if(!['OFFLINE_ACCEPTED','DUPLICATE'].includes(result?.status)){diagnostic(result?.status==='DELIVERY_TIMEOUT'?'DELIVERY_TIMEOUT':result?.status==='INTEGRITY_CONFLICT'?'INTEGRITY_CONFLICT':'DELIVERY_FAILED',packet.envelope.semanticId);return;}
     }catch{diagnostic('DELIVERY_FAILED',packet.envelope.semanticId);return;}
     finally{clearTimeout(timer);}
    }
   })().catch(()=>diagnostic('PRODUCER_CALLBACK_FAILED',frozen.semanticId??frozen.cycleId)).finally(()=>{release();tasks.delete(done);resolveDone();});
  };
  const schedulingFailed=()=>{if(!started){started=true;release();tasks.delete(done);resolveDone();}diagnostic('PRODUCER_CALLBACK_FAILED',frozen.semanticId??frozen.cycleId);};
  try{const scheduled=schedule(job);if(scheduled&&typeof scheduled.then==='function')Promise.resolve(scheduled).catch(schedulingFailed);}catch{schedulingFailed();}
  return Object.freeze({status:'SCHEDULED',durable:false});
 }
 function deferWork(work){
  if(!enabled)return;
  if(deferred>=PRODUCER_BOUNDS.maxPendingCycles){diagnostic('PRODUCER_CAPACITY_EXCEEDED');return;}
  deferred++;
  let resolveDone;const done=new Promise(resolve=>{resolveDone=resolve;});tasks.add(done);
  // A timer boundary lets the trading promise settle and its continuations run
  // before even the synchronous input copy of an offline producer can begin.
  // This is a nondurable local shadow task, never a delivery guarantee.
  const run=()=>{
   const prior=new Set(tasks);
   Promise.resolve().then(work).then(()=>Promise.all([...tasks].filter(task=>!prior.has(task))))
    .catch(()=>diagnostic('PRODUCER_CALLBACK_FAILED'))
    .finally(()=>{deferred--;tasks.delete(done);resolveDone();});
  };
  try{setTimeout(run,0);}catch{deferred--;tasks.delete(done);resolveDone();diagnostic('PRODUCER_CALLBACK_FAILED');}
 }
 return Object.freeze({submitCycle:(journal,provenance)=>submit(journal,true,provenance),submitFact:fact=>submit(fact,false),
  deferWork,diagnostics:()=>Object.freeze([...diagnostics]),whenIdle:()=>Promise.all([...tasks]),
  state:()=>Object.freeze({pending,unsettledSends})});
}

// Explicit local dependency injection only; no environment transport binding.
export function installOfflineMeasurementProducer(env,options){
 const producer=createMeasurementProducer({...options,enabled:measurementProducerEnabled(env)});injected.set(env,producer);return producer;
}
function producerFor(env){
 if(!measurementProducerEnabled(env))return null;
 if(injected.has(env))return injected.get(env);
 if(!defaults.has(env))defaults.set(env,createMeasurementProducer({enabled:true,onDiagnostic:value=>console.error(JSON.stringify({message:'b1 m1 local capture gap',...value}))}));
 return defaults.get(env);
}
export function observeM1DecisionCycle(env,journal){
 try{producerFor(env)?.submitCycle(journal,{codeCommit:String(env.B1_CODE_COMMIT||''),deploymentId:env.CF_VERSION_METADATA?.id??null,
  measurementEffectiveAt:Number(env.B1_MEASUREMENT_EFFECTIVE_AT)});}catch{}
}
export function observeM1Fact(env,fact){try{producerFor(env)?.submitFact(fact);}catch{}}
export function deferM1Work(env,work){try{producerFor(env)?.deferWork(work);}catch{}}
