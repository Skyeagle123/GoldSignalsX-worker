// Offline/mock only. No files, cloud bindings, timers, network or measurement SQL.
import {fragmentMeasurementPacket,validateTransportFragment,reassembleMeasurementFragments,sameFragmentManifest,serializedBytes,MAX_FRAGMENT_ENVELOPE_BYTES} from './measurement-transport-fragment.js';
import {digestPayload,canonicalSerialize,parseEvidence} from './signal-evidence.js';

export const MOCK_RETENTION_MS=14*86400000;
const stores=new WeakMap();
const clockValue=n=>{if(!Number.isSafeInteger(n)||n<0)throw new Error('transport_clock_invalid');return n;};
const bounded=(n,min,max)=>Number.isInteger(n)&&n>=min&&n<=max;
function store({maxEvents=32,maxBytes=16*1024*1024,maxDlq=64,maxDlqBytes=4*1024*1024}={},mode){
 if(!bounded(maxEvents,1,64)||!bounded(maxBytes,1,16*1024*1024)||!bounded(maxDlq,1,64)||!bounded(maxDlqBytes,1,16*1024*1024))throw new Error('transport_store_bound');
 const api=Object.freeze({mode});stores.set(api,{mode,maxEvents,maxBytes,maxDlq,maxDlqBytes,records:new Map(),poison:new Map(),locks:new Map(),bytes:0,dlqBytes:0,validations:0,framings:0,
  metrics:{received:0,duplicateFragments:0,consumerCalls:0,expired:0,discardedBytes:0,peakBytes:0,peakEvents:0,capacityRejected:0}});return api;
}
export const createVolatileTransportStore=options=>store(options,'VOLATILE');
// Persistence means retaining this object in the test harness across runtimes.
// It is not actual durable storage and never survives loss of the object/process.
export const createMockDurableTransportStore=options=>store(options,'MOCK_DURABLE');

export function classifyMeasurementDelivery(result){
 if(['INTEGRITY_CONFLICT','CONFLICT'].includes(result?.status)||result?.projectionStatus==='CONFLICT')return {classification:'INTEGRITY_CONFLICT',retryable:result.retryable===true};
 if(result?.status==='REJECTED')return {classification:'PERMANENT_MALFORMED',retryable:false};
 if(result?.status==='DEPENDENCY_PENDING')return {classification:'DEPENDENCY_PENDING',retryable:true};
 if(result?.status==='DURABILITY_UNKNOWN')return {classification:'DURABILITY_UNKNOWN',retryable:true};
 if(['ACCEPTED','DUPLICATE'].includes(result?.status)&&result.ok===true&&result.durable===true&&!result.retryable&&result.projectionStatus!=='PENDING'&&result.processingStatus!=='PENDING')return {classification:'ACKNOWLEDGED',retryable:false};
 return {classification:'RETRYABLE',retryable:true};
}
const header=f=>Object.freeze(Object.fromEntries(['version','namespace','eventId','eventKind','payloadDigest','wireDigest','totalBytes','count'].map(k=>[k,f[k]])));
const cloneResult=result=>parseEvidence(canonicalSerialize(result,16384));

export function createOfflineMultipartTransport({consumer,store:storage=createVolatileTransportStore(),clock=()=>Date.now(),maxAttempts=3,maxReplays=4,retryDelayMs=1000,retentionMs=MOCK_RETENTION_MS,hook=null}={}){
 const root=stores.get(storage);
 if(!root||!consumer||typeof consumer.ingest!=='function'||typeof clock!=='function'||hook!==null&&typeof hook!=='function'||!bounded(maxAttempts,1,16)||!bounded(maxReplays,1,16)||!bounded(retryDelayMs,0,86400000)||!bounded(retentionMs,1,MOCK_RETENTION_MS))throw new Error('transport_runtime_invalid');
 const runtime={active:true};
 const now=()=>clockValue(clock());
 const live=()=>{if(!runtime.active)throw new Error('transport_runtime_crashed');};
 const instrument=async(phase,id)=>{if(hook)await hook(phase,id);live();};
 function snapshot(r,historical=true){return Object.freeze({eventId:r.manifest.eventId,status:r.status,durable:false,mockPersistence:root.mode,
  receivedFragments:r.pieces.size,declaredFragments:r.manifest.count,missingOrdinals:Object.freeze(Array.from({length:r.manifest.count},(_,i)=>i).filter(i=>!r.pieces.has(i))),
  stagedBytes:r.bytes,expiresAt:r.expiresAt,attempts:r.attempts,cycleAttempts:r.cycleAttempts,replayCount:r.replays,nextAttemptAt:r.nextAt,
  classification:r.classification,reason:r.reason,firstFailureAt:r.firstFailureAt,lastFailureAt:r.lastFailureAt,
  consumerResult:r.lastResult===null?null:cloneResult(r.lastResult),consumerResultObservedAt:r.observedAt,consumerResultHistorical:historical});}
 function failure(r,classification,reason,at){r.classification=classification;r.reason=reason;r.firstFailureAt??=at;r.lastFailureAt=at;}
 function expireRecord(r,at){
  if(at<r.expiresAt||['ACKNOWLEDGED','DLQ','EXPIRED_INCOMPLETE'].includes(r.status))return;
  root.metrics.expired++;failure(r,'TRANSPORT_EXPIRED','transport_retention_expired',at);
  r.status=r.pieces.size===r.manifest.count?'DLQ':'EXPIRED_INCOMPLETE';r.nextAt=null;
 }
 async function reject(wire,reason,classification='PERMANENT_MALFORMED'){
  const at=now();
  if(typeof wire!=='string'||wire.length>MAX_FRAGMENT_ENVELOPE_BYTES||serializedBytes(wire)>MAX_FRAGMENT_ENVELOPE_BYTES)return {status:'REJECTED',classification,reason,durable:false,originalRetained:false,retentionReason:'OVERSIZED_OR_NONWIRE'};
  const id='poison:'+await digestPayload(wire);live();const old=root.poison.get(id);
  if(old){old.attempts=Math.min(Number.MAX_SAFE_INTEGER,old.attempts+1);old.lastFailureAt=at;return {status:'DLQ',dlqId:id,classification,reason,durable:false,originalRetained:true};}
  const bytes=serializedBytes(wire);
  if(root.poison.size>=root.maxDlq||root.dlqBytes+bytes>root.maxDlqBytes){root.metrics.capacityRejected++;return {status:'REJECTED',classification,reason,durable:false,originalRetained:false,retentionReason:'MOCK_DLQ_CAPACITY'};}
  root.poison.set(id,{id,wire,bytes,classification,reason,attempts:1,replays:0,firstFailureAt:at,lastFailureAt:at});root.dlqBytes+=bytes;
  return {status:'DLQ',dlqId:id,classification,reason,durable:false,originalRetained:true};
 }
 async function dispatch(r){
  live();const id=r.manifest.eventId,existing=root.locks.get(id);if(existing)return existing.promise;
  const at=now();expireRecord(r,at);
  if(['DLQ','EXPIRED_INCOMPLETE','ACKNOWLEDGED'].includes(r.status)||r.pieces.size!==r.manifest.count||r.nextAt!==null&&at<r.nextAt)return snapshot(r);
  const lease={owner:runtime,valid:true,promise:null};
  lease.promise=Promise.resolve().then(async()=>{
   const check=()=>{live();if(!lease.valid||root.records.get(id)!==r||root.locks.get(id)!==lease)throw new Error('transport_runtime_crashed');};
   try{
    const wire=await reassembleMeasurementFragments(r.manifest,r.pieces,{receivedAt:now()});check();
    await instrument('afterReassembly',id);check();expireRecord(r,now());if(r.status==='DLQ')return snapshot(r);
    r.attempts=Math.min(Number.MAX_SAFE_INTEGER,r.attempts+1);r.cycleAttempts++;r.status='DELIVERING';r.nextAt=null;
    await instrument('afterAttemptRecorded',id);check();
    expireRecord(r,now());if(r.status==='DLQ')return snapshot(r);
    let result;
    try{root.metrics.consumerCalls++;result=cloneResult(await consumer.ingest(wire,{receivedAt:now()}));}catch{result={ok:false,status:'RETRYABLE',durable:null,retryable:true,error:'transport_consumer_exception'};}
    await instrument('beforeAcknowledgement',id);check();
    r.lastResult=result;r.observedAt=now();const c=classifyMeasurementDelivery(result);
    r.classification=c.classification;
    if(c.classification==='ACKNOWLEDGED'){r.status='ACKNOWLEDGED';r.reason=null;r.nextAt=null;return snapshot(r,false);}
    failure(r,c.classification,result.error||result.projectionError||result.status||'consumer_pending',r.observedAt);
    // A confirmed durable receipt can still have retryable processing/projection state.
    if(!c.retryable||r.cycleAttempts>=maxAttempts){await instrument('beforeDlq',id);check();r.status='DLQ';r.nextAt=null;}
    else{r.status='RETRY_PENDING';r.nextAt=clockValue(r.observedAt+retryDelayMs);}
    return snapshot(r,false);
   }catch(error){
    if(!lease.valid||!runtime.active)return {status:'CRASHED',classification:'DURABILITY_UNKNOWN',durable:false,eventId:id};
    // Complete malformed bytes never reach the public M2 ingress.
    failure(r,/digest|identity|manifest_conflict/.test(error.message)?'INTEGRITY_CONFLICT':'PERMANENT_MALFORMED',String(error.message).slice(0,160),now());r.status='DLQ';r.nextAt=null;return snapshot(r);
   }finally{if(root.locks.get(id)===lease)root.locks.delete(id);}
  });root.locks.set(id,lease);return lease.promise;
 }
 async function receive(wire,{signal}={}){
  live();if(signal?.aborted)return {status:'ABORTED',durable:false};
  if(root.validations>=64){root.metrics.capacityRejected++;return {status:'CAPACITY_PENDING',classification:'RETRYABLE',durable:false};}
  root.validations++;try{
   let f;try{f=(await validateTransportFragment(wire)).manifest;}catch(error){return await reject(wire,String(error.message),/digest|identity/.test(error.message)?'INTEGRITY_CONFLICT':'PERMANENT_MALFORMED');}
   live();if(signal?.aborted)return {status:'ABORTED',durable:false};const at=now();root.metrics.received++;
   let r=root.records.get(f.eventId);
   if(r&&!sameFragmentManifest(r.manifest,f))return await reject(wire,'transport_manifest_conflict','INTEGRITY_CONFLICT');
   if(!r){
    if(root.records.size>=root.maxEvents){root.metrics.capacityRejected++;return {status:'CAPACITY_PENDING',classification:'RETRYABLE',durable:false};}
    r={manifest:header(f),pieces:new Map(),bytes:0,status:'STAGING',expiresAt:clockValue(at+retentionMs),attempts:0,cycleAttempts:0,replays:0,nextAt:null,classification:null,reason:null,firstFailureAt:null,lastFailureAt:null,lastResult:null,observedAt:null};
   }
   expireRecord(r,at);if(r.status==='EXPIRED_INCOMPLETE')return snapshot(r);
   const prior=r.pieces.get(f.ordinal);
   if(prior){
    // The frame's canonical representation is the duplicate identity. Whitespace
    // formatting is normalized, while conflicting fragment bytes are rejected.
    if(prior!==canonicalSerialize(f,MAX_FRAGMENT_ENVELOPE_BYTES))return await reject(wire,'transport_fragment_conflict','INTEGRITY_CONFLICT');
    root.metrics.duplicateFragments++;
   }else{
    const canonical=canonicalSerialize(f,MAX_FRAGMENT_ENVELOPE_BYTES),bytes=serializedBytes(canonical);
    if(root.bytes+bytes>root.maxBytes){root.metrics.capacityRejected++;return {status:'CAPACITY_PENDING',classification:'RETRYABLE',durable:false};}
    root.records.set(f.eventId,r);r.pieces.set(f.ordinal,canonical);r.bytes+=bytes;root.bytes+=bytes;
    root.metrics.peakBytes=Math.max(root.metrics.peakBytes,root.bytes);root.metrics.peakEvents=Math.max(root.metrics.peakEvents,root.records.size);
    await instrument('afterFragmentStaged',f.eventId);
   }
   if(r.status==='ACKNOWLEDGED'||r.status==='DLQ')return snapshot(r);
   if(r.pieces.size===r.manifest.count){if(r.status==='STAGING')r.status='READY';return await dispatch(r);}
   return snapshot(r);
  }finally{root.validations--;}
 }
 async function redeliver(id,{replay=false}={}){
  live();const r=root.records.get(id);if(!r)return {status:'NOT_FOUND',durable:false};
  if(root.locks.has(id))return root.locks.get(id).promise;
  if(r.status==='EXPIRED_INCOMPLETE')return snapshot(r);
  if(replay){
   if(r.replays>=maxReplays)return {status:'REPLAY_LIMIT',durable:false,eventId:id};
   await instrument('beforeReplay',id);
   // Another replay or owner cleanup may have occurred during the optional hook.
   if(root.records.get(id)!==r)return {status:'NOT_FOUND',durable:false};
   if(r.status==='EXPIRED_INCOMPLETE')return snapshot(r);
   if(root.locks.has(id))return root.locks.get(id).promise;
   if(r.replays>=maxReplays)return {status:'REPLAY_LIMIT',durable:false,eventId:id};
   r.replays++;r.expiresAt=clockValue(now()+retentionMs);
  }else if(r.status==='DLQ')return snapshot(r);
  if(replay||r.status==='ACKNOWLEDGED'){r.cycleAttempts=0;r.status='READY';r.nextAt=null;}
  return dispatch(r);
 }
 const api={mode:'OFFLINE',store:storage,receive,
  async send(packet,{signal}={}){
   live();if(signal?.aborted)return {status:'ABORTED',durable:false};
   if(root.framings>=16)return {status:'OFFLINE_CAPACITY_EXCEEDED',durable:false};root.framings++;
   try{
    const fragments=await fragmentMeasurementPacket(packet,{receivedAt:now()});live();
    const eventId=JSON.parse(fragments[0]).eventId,wasAcknowledged=root.records.get(eventId)?.status==='ACKNOWLEDGED';let result;
    for(const fragment of fragments){result=await receive(fragment,{signal});if(['ABORTED','CAPACITY_PENDING','REJECTED'].includes(result.status)||result.dlqId)return result;}
    if(wasAcknowledged)result=await redeliver(eventId);
    return {...result,status:result.status==='ACKNOWLEDGED'?(wasAcknowledged?'DUPLICATE':'OFFLINE_ACCEPTED'):result.status,transportStatus:result.status};
   }finally{root.framings--;}
  },
  retry:id=>redeliver(id),replay:id=>redeliver(id,{replay:true}),
  async replayPoison(id){live();const p=root.poison.get(id);if(!p)return {status:'NOT_FOUND',durable:false};if(p.replays>=maxReplays)return {status:'REPLAY_LIMIT',durable:false};p.replays++;await instrument('beforePoisonReplay',id);return receive(p.wire);},
  inspect:id=>{live();const r=root.records.get(id);return r?snapshot(r):null;},
  expire(){live();const at=now();for(const r of root.records.values())if(!root.locks.has(r.manifest.eventId))expireRecord(r,at);return [...root.records.values()].filter(r=>r.classification==='TRANSPORT_EXPIRED').map(r=>snapshot(r));},
  dlq(){live();return {events:[...root.records.values()].filter(r=>r.status==='DLQ').map(r=>({...snapshot(r),fragments:[...r.pieces.values()]})),
   poison:[...root.poison.values()].map(p=>({...p}))};},
  discard(id){live();if(root.locks.has(id)||root.validations)return false;const r=root.records.get(id);if(!r||!['ACKNOWLEDGED','DLQ','EXPIRED_INCOMPLETE'].includes(r.status))return false;root.bytes-=r.bytes;root.metrics.discardedBytes+=r.bytes;root.records.delete(id);return true;},
  metrics:()=>({...root.metrics,events:root.records.size,stagingBytes:root.bytes,poisonEntries:root.poison.size,poisonBytes:root.dlqBytes,deliveriesInFlight:root.locks.size,validationsInFlight:root.validations}),
  crash(){if(!runtime.active)return;runtime.active=false;for(const [id,lease]of root.locks)if(lease.owner===runtime){lease.valid=false;root.locks.delete(id);const r=root.records.get(id);
    if(r.status==='DELIVERING'){failure(r,'DURABILITY_UNKNOWN','runtime_lost_before_acknowledgement',now());r.status='RETRY_PENDING';r.nextAt=now();}}}
 };return Object.freeze(api);
}
