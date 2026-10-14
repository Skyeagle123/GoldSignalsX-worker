import {fragmentMeasurementPacket} from './measurement-transport-fragment.js';
import {prepareCycleEnvelopes} from './measurement-producer.js';
import {buildMeasurementEnvelope,copyFrozenEvidence} from './measurement-envelope.js';
import {decisionJournalObservations} from './signal-measurement.js';
import {enabled,need,bytes,LIMITS,independentBindings} from './measurement-independent-contract.js';
import {prepareMarketEvidence} from './measurement-market-envelope.js';
export function createQueueHandoff(queue,{maxInFlight=16}={}){
 need(queue&&typeof queue.send==='function'&&maxInFlight>=1&&maxInFlight<=16,'pa_queue_producer');let inFlight=0;
 return Object.freeze({mode:'QUEUE_HANDOFF',async send(packet){if(inFlight>=maxInFlight)return {status:'CAPACITY_REJECTED',durable:false,captureGap:true};inFlight++;
  try{const frames=await fragmentMeasurementPacket(packet);let accepted=0;for(const body of frames){try{await queue.send(body,{contentType:'text'});accepted++;}catch{return {status:'HANDOFF_ACCEPTANCE_UNKNOWN',queueAcceptedFragments:accepted,expectedFragments:frames.length,durable:null,captureGap:true};}}
   return {status:'QUEUE_ACCEPTED',queueAcceptedFragments:accepted,durable:false,analyticalCompletion:false};
  }finally{inFlight--;}}});
}
const producers=new WeakMap();
let runtimeTasks=0;
export function configureIndependentProducer(env,ctx,{schedule=fn=>setTimeout(fn,0),clock=()=>Date.now(),diagnostic=null}={}){
 independentBindings(env,'trading');if(!enabled(env,'B1_INDEPENDENT_CAPTURE_ENABLED')||!enabled(env,'B1_REAL_QUEUE_HANDOFF_ENABLED'))return null;
 if(producers.has(env))return producers.get(env);const handoff=createQueueHandoff(env.MEASUREMENT_QUEUE);const tasks=new Set(),observations=[],metrics={scheduled:0,accepted:0,unknown:0,failed:0,peakTasks:0};
 const report=v=>{v=Object.fromEntries(['category','status','durable','coverage','captureGap','queueAcceptedFragments','expectedFragments','analyticalCompletion','reason'].filter(k=>Object.hasOwn(v,k)).map(k=>[k,k==='reason'?String(v[k]).slice(0,512):v[k]]));observations.push(v);if(observations.length>32)observations.shift();try{diagnostic?.(v);}catch{}};
 function defer(work){if(tasks.size>=2||runtimeTasks>=16){report({category:'PRODUCER_RESOURCE_FAILURE',durable:false,coverage:'UNKNOWN_CAPTURE'});return {status:'CAPACITY_REJECTED',durable:false};}
  let resolve,started=false,charged=true;const done=new Promise(r=>resolve=r);tasks.add(done);runtimeTasks++;metrics.scheduled++;metrics.peakTasks=Math.max(metrics.peakTasks,tasks.size);const release=()=>{if(!charged)return;charged=false;tasks.delete(done);runtimeTasks--;resolve();};
  try{ctx?.waitUntil?.(done);schedule(()=>{if(!charged)return;started=true;Promise.resolve().then(work).catch(e=>{metrics.failed++;report({category:'PRODUCER_PREPARATION_FAILURE',reason:e.message,durable:false,coverage:'UNKNOWN_CAPTURE'});}).finally(release);});}catch(e){if(!started)release();report({category:'PRODUCER_PREPARATION_FAILURE',reason:e.message,durable:started?null:false});return {status:started?'SCHEDULE_ACCEPTANCE_UNKNOWN':'PREPARATION_FAILED',durable:started?null:false,captureGap:true};}
  return {status:'SCHEDULED',durable:false};
 }
 async function send(p){const result=await handoff.send(p);if(result.status==='QUEUE_ACCEPTED')metrics.accepted++;else{metrics.unknown++;report({category:'HANDOFF_ACCEPTANCE_UNKNOWN',...result,coverage:'UNKNOWN_CAPTURE'});}return result;}
 const api=Object.freeze({defer,
  cycle(journal,pending=[]){return defer(async()=>{const frozen=copyFrozenEvidence(decisionJournalObservations(journal)),provenance={codeCommit:env.B1_CODE_COMMIT,deploymentId:env.CF_VERSION_METADATA?.id??null,measurementEffectiveAt:Number(env.B1_MEASUREMENT_EFFECTIVE_AT)};
   const packets=await prepareCycleEnvelopes(frozen,provenance,{preparedAt:clock()});for(const packet of packets)await send(packet);
   for(const fact of pending.slice(0,14))if(fact.kind==='confirmation')await send(await buildMeasurementEnvelope({kind:'CONFIRMATION_LINK',semanticId:`${fact.primarySignalId}:confirmation:${fact.confirmationSignalId}`,occurredAt:fact.eventAt,observedAt:null,preparedAt:clock(),payload:{primarySignalId:fact.primarySignalId,confirmationSignalId:fact.confirmationSignalId,link:fact.link,linkPersistence:'SUCCEEDED',measurementOnly:true,decisionUse:false}}));
   if(enabled(env,'B1_POST_ENTRY_EXPORT_ENABLED')){
    // Reuse only bars already observed during the trading cycle; no new market fetch.
    let exported=false;const cohort=packets[0].envelope.payload.records.find(r=>r.type==='cohort').values.cohort_id,observedAt=clock(),bars=(journal.frames?.['1m']?.bars??[]).filter(b=>b.provider==='mt5'&&b.t+60000<=observedAt).slice(-2000);
    if(bars.length){exported=true;const session=bars.at(-1).sessionId??null,same=bars.filter(b=>(b.sessionId??null)===session);for(const p of await prepareMarketEvidence({cohort,source:'mt5',session,bars:same,from:same[0].t,to:same.at(-1).t+60000,observedAt,truncated:bars.length===2000||same.length!==bars.length,gaps:[{reason:'BROKER_TICK_COMPLETENESS_UNKNOWN'},...(same.length!==bars.length?[{reason:'BOUNDED_EXPORT_SELECTED_ONE_SESSION'}]:[])]}))await env.MEASUREMENT_QUEUE.send(JSON.stringify({version:'measurement-market-queue-v1',id:p.id,wire:p.wire}),{contentType:'text'});}
    const q=journal.quoteEvidence;if(q?.provider==='mt5'&&typeof q.sessionId==='string'&&Number.isSafeInteger(q.sequence)&&Number.isSafeInteger(q.receivedAt)&&Number.isFinite(q.providerTimestamp)&&Number.isFinite(q.canonicalPrice)){
     exported=true;const tick={ts:q.receivedAt,price:q.canonicalPrice,measurement:{provider:q.provider,sessionId:q.sessionId,sequence:q.sequence,providerTimestamp:q.providerTimestamp,priceBasis:q.priceBasis}};
     for(const p of await prepareMarketEvidence({cohort,source:'mt5',session:q.sessionId,ticks:[tick],from:Math.min(q.providerTimestamp,q.receivedAt),to:Math.max(q.providerTimestamp,q.receivedAt),observedAt,gaps:[{reason:'BOUNDED_CYCLE_QUOTE_SAMPLE; OTHER_TICKS_NOT_EXPORTED'}]}))await env.MEASUREMENT_QUEUE.send(JSON.stringify({version:'measurement-market-queue-v1',id:p.id,wire:p.wire}),{contentType:'text'});
    }
    if(!exported)for(const p of await prepareMarketEvidence({cohort,source:'mt5',session:null,from:Math.min(journal.evaluatedAt??observedAt,observedAt),to:observedAt,observedAt,gaps:[{reason:'NO_SUPPORTED_ALREADY_OBSERVED_POST_ENTRY_INPUTS',missingCount:'UNKNOWN'}]}))await env.MEASUREMENT_QUEUE.send(JSON.stringify({version:'measurement-market-queue-v1',id:p.id,wire:p.wire}),{contentType:'text'});
   }
  });},fact(fact){return defer(async()=>send(await buildMeasurementEnvelope({...copyFrozenEvidence(fact),preparedAt:clock()})));},
  market(input){if(!enabled(env,'B1_POST_ENTRY_EXPORT_ENABLED'))return {status:'OFF',durable:false};return defer(async()=>{for(const p of await prepareMarketEvidence(copyFrozenEvidence(input)))await env.MEASUREMENT_QUEUE.send(JSON.stringify({version:'measurement-market-queue-v1',id:p.id,wire:p.wire}),{contentType:'text'});});},
  whenIdle:()=>Promise.all([...tasks]),metrics:()=>({...metrics,pending:tasks.size}),diagnostics:()=>[...observations]});producers.set(env,api);return api;
}
export function independentCaptureEnabled(env){return enabled(env,'B1_INDEPENDENT_CAPTURE_ENABLED');}
export function independentCohort(env){return env?.B1_MEASUREMENT_PLANE!=null&&env.B1_MEASUREMENT_PLANE!=='legacy'||independentCaptureEnabled(env);}
export function observeIndependentCycle(env,journal,pending){try{return producers.get(env)?.cycle(journal,pending);}catch{return {status:'PREPARATION_FAILED',durable:false};}}
export function observeIndependentFact(env,fact){try{return producers.get(env)?.fact(fact);}catch{return {status:'PREPARATION_FAILED',durable:false};}}
