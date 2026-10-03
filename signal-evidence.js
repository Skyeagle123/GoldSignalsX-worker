export const MEASUREMENT_SCHEMA_VERSION=1;
export const CAPTURE_VERSION='b1-capture-v1';
export const ENGINE_SEMANTICS_VERSION='6721f84-engine-v1';
export const SIGNAL_POLICY_VERSION='post-improvement-2026-09-21';
export const EXPOSURE_POLICY_VERSION='single-primary-until-terminal-v1';
export const FROZEN_BASELINE=Object.freeze({firstSignalId:'5m:1789992000000:buy',effectiveAt:1789992352230,
  official:35,success:8,failure:15,noDecision:11,unresolved:1,recordedDirectional:23});

export function canonicalSerialize(value,maxBytes=65536) {
  const seen=new Set();
  function normalize(v,depth=0){
    if(depth>24)throw new Error('measurement_depth_exceeded');
    if(typeof v==='number')return Number.isFinite(v)&&!Object.is(v,-0)?v:{$number:Object.is(v,-0)?'-0':String(v)};
    if(v===undefined)return {$unavailable:'undefined'};
    if(v===null||typeof v==='boolean'||typeof v==='string')return v;
    if(typeof v!=='object'||seen.has(v))throw new Error('measurement_serialization_invalid');
    seen.add(v);let out;
    if(Array.isArray(v))out=v.map(x=>normalize(x,depth+1));
    else if(Object.getPrototypeOf(v)===Object.prototype||Object.getPrototypeOf(v)===null){
      const keys=Object.keys(v).sort();
      if(keys.some(k=>/token|password|secret|authorization|api.?key/i.test(k)))throw new Error('measurement_sensitive_field');
      out=Object.fromEntries(keys.map(k=>[k,normalize(v[k],depth+1)]));
    }else throw new Error('measurement_object_invalid');
    seen.delete(v);return out;
  }
  const text=JSON.stringify(normalize(value)),bytes=new TextEncoder().encode(text);
  if(bytes.length>maxBytes)throw new Error('measurement_payload_exceeded');
  return text;
}

export function parseEvidence(text){
  return JSON.parse(text,(_key,v)=>v?.$number===undefined?v:
    v.$number==='-0'?-0:v.$number==='NaN'?NaN:v.$number==='Infinity'?Infinity:v.$number==='-Infinity'?-Infinity:v);
}
export async function digestPayload(text){
  const digest=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map(x=>x.toString(16).padStart(2,'0')).join('');
}
export function evaluationIdentity(cycleId,ordinal,tf,candle,direction){
  return {evaluationId:`${cycleId}:${ordinal}`,candidateKey:candle!=null&&['buy','sell'].includes(direction)?`${tf}:${candle}:${direction}`:null};
}

export function cohortManifest({codeCommit,deploymentId=null,measurementEffectiveAt,configFingerprint}){
  if(!/^[0-9a-f]{40}$/.test(codeCommit||'')||!Number.isFinite(measurementEffectiveAt))throw new Error('measurement_provenance_not_configured');
  return {cohortId:`${CAPTURE_VERSION}:${codeCommit}:${deploymentId||'deployment-unknown'}:${measurementEffectiveAt}:${configFingerprint}`,
    codeCommit,deploymentId,engineSemanticsVersion:ENGINE_SEMANTICS_VERSION,signalPolicyVersion:SIGNAL_POLICY_VERSION,
    exposurePolicyVersion:EXPOSURE_POLICY_VERSION,measurementSchemaVersion:MEASUREMENT_SCHEMA_VERSION,
    captureVersion:CAPTURE_VERSION,outcomeReducerVersion:'tp1-first-v1',measurementEffectiveAt,
    policyEffectiveAt:FROZEN_BASELINE.effectiveAt,configFingerprint,measurementOnly:true,decisionUse:false};
}

export function exposureEvidenceState(state){
 try{if(!state)return null;return Object.fromEntries(['symbol','status','side','maxPositions','primarySignalId','primaryTf','openedAt','updatedAt','closedAt','closeReason','cooldownUntil','source'].map(key=>[key,state[key]??null]));}catch{return null;}
}
export function decisionSnapshot({cycleId,ordinal,tf,bars,live,trace,result,decision,officialPersisted=false,
  confirmationPersisted=false,skippedReason=null,admissionContext=null,exposureBefore=null,exposureAfter=null,officialPersistenceFailed=false,
  requestedMtf=[],includedMtf=[],competingCandidateIds=[],createdAt,actualOfficialSignalId=null}){
  const identity=evaluationIdentity(cycleId,ordinal,tf,result?.lastTs??bars?.at(-1)?.t,result?.side);
  return {...identity,cycleId,symbol:'XAUUSD',timeframe:tf,direction:result?.side||null,
    officialSignalId:officialPersisted?(actualOfficialSignalId??identity.candidateKey):null,kind:officialPersisted?'OFFICIAL':'CANDIDATE',
    createdAt,evaluatedAt:trace?.identity?.evaluationAt??null,measurementOnly:true,decisionUse:false,
    sourceCandle:bars?.at(-1)?.t??null,engine:trace||null,
    outcome:skippedReason?'SKIPPED':officialPersistenceFailed?'OFFICIAL_PERSISTENCE_FAILED':officialPersisted?'OFFICIAL_PERSISTED':confirmationPersisted?'CONFIRMATION_PERSISTED':decision?.decision||'ENGINE_REJECTED',
    skippedReason,levelsStatus:trace?.levels?'COMPUTED_BY_ENGINE':'NOT_COMPUTED_BY_ENGINE',
    quote:live?{canonicalPrice:live.price,bid:live.bid??null,ask:live.ask??null,midpoint:live.midpoint??null,
      spread:live.spread??null,selectedPriceBasis:live.priceBasis??'canonical-worker-price',providerTimestamp:live.ts,
      receivedAt:live.receivedAt??null,provider:live.source??null,sessionId:live.sessionId??null,
      sequence:live.sequence??null,sequenceScope:live.sequenceScope??null,timestampInferred:live.timestampInferred??null}:null,
    engineMtf:{requested:requestedMtf,included:includedMtf,excluded:requestedMtf.filter(x=>!includedMtf.includes(x)),frames:trace?.mtf||[]},
    admissionContext,exposure:{before:exposureBefore,after:exposureAfter,decision:decision||null,
      responsiblePrimaryId:decision?.primarySignalId??exposureAfter?.primarySignalId??null,
      competingCandidateIds,reservationAccepted:decision?.decision==='accepted',officialPersisted,officialPersistenceFailed,confirmationPersisted},
    evidenceCompleteness:{entry:skippedReason?'NOT_EVALUATED':'CAPTURED',sourceCompleteness:'UNKNOWN',inputAvailability:'CAPTURE_TIME_ONLY'}};
}
