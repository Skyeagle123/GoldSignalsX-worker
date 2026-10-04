// Pure M1 envelopes. No storage, network, reducer or platform capabilities.
import {canonicalSerialize,digestPayload} from './signal-evidence.js';

export const ENVELOPE_VERSION=1;
export const PRODUCER_NAMESPACE='goldsignalsx:b1';
export const ENVELOPE_KINDS=Object.freeze(['DECISION_CYCLE','EVALUATION_CENSUS','OFFICIAL_CREATION','CONFIRMATION_LINK','LIFECYCLE_FACT','CAPTURE_GAP']);
export const PRODUCER_BOUNDS=Object.freeze({maxWireBytes:2*1024*1024,maxInputBytes:8*1024*1024,maxNodes:200000,maxDepth:24,
 maxFactWireBytes:65536,maxFactInputBytes:65536,maxPending:16,maxPendingCycles:2});
const encoder=new TextEncoder();
const bytes=value=>encoder.encode(value).length;
const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};

// Map/Set contents become private read-only facades for the legacy preparer.
// Object.freeze(new Map()) alone would not protect its entries.
export function copyFrozenEvidence(value,{maxInputBytes=PRODUCER_BOUNDS.maxInputBytes,maxNodes=PRODUCER_BOUNDS.maxNodes,maxDepth=PRODUCER_BOUNDS.maxDepth}={}){
 let nodes=0,charged=0;const seen=new Set();
 const charge=n=>{charged+=n;if(charged>maxInputBytes)throw new Error('measurement_input_exceeded');};
 function copy(v,depth=0){
  if(++nodes>maxNodes||depth>maxDepth)throw new Error('measurement_input_structure_exceeded');
  if(v===null||v===undefined||typeof v==='boolean'||typeof v==='number'){charge(8);return v;}
  if(typeof v==='string'){if(v.length>maxInputBytes)throw new Error('measurement_input_exceeded');charge(bytes(v));return v;}
  if(typeof v!=='object'||seen.has(v))throw new Error('measurement_serialization_invalid');
  seen.add(v);let result;
  if(v instanceof Map){
   if(v.size>maxNodes)throw new Error('measurement_input_structure_exceeded');
   const entries=[...v].map(([k,x])=>Object.freeze([copy(k,depth+1),copy(x,depth+1)])),map=new Map(entries);
   result=Object.freeze({get:k=>map.get(k),has:k=>map.has(k),[Symbol.iterator]:()=>entries[Symbol.iterator]()});
  }else if(v instanceof Set){
   if(v.size>maxNodes)throw new Error('measurement_input_structure_exceeded');
   const entries=[...v].map(x=>copy(x,depth+1)),set=new Set(entries);
   result=Object.freeze({has:k=>set.has(k),[Symbol.iterator]:()=>entries[Symbol.iterator]()});
  }else if(Array.isArray(v)){if(v.length>maxNodes)throw new Error('measurement_input_structure_exceeded');result=Object.freeze(v.map(x=>copy(x,depth+1)));
  }else if(Object.getPrototypeOf(v)===Object.prototype||Object.getPrototypeOf(v)===null){
   const keys=Object.keys(v);if(keys.length>maxNodes)throw new Error('measurement_input_structure_exceeded');
   result=Object.create(null);for(const key of keys){const descriptor=Object.getOwnPropertyDescriptor(v,key);if(!descriptor||!('value' in descriptor))throw new Error('measurement_object_invalid');charge(bytes(key));result[key]=copy(descriptor.value,depth+1);}Object.freeze(result);
  }else throw new Error('measurement_object_invalid');
  seen.delete(v);return result;
 }
 return copy(value);
}

export async function buildMeasurementEnvelope({kind,semanticId,payload,occurredAt,observedAt,preparedAt,
 producerNamespace=PRODUCER_NAMESPACE,maxWireBytes=PRODUCER_BOUNDS.maxWireBytes,measure=null}){
 if(!ENVELOPE_KINDS.includes(kind))throw new Error('measurement_envelope_kind_invalid');
 if(typeof semanticId!=='string'||!semanticId||bytes(semanticId)>1024||typeof producerNamespace!=='string'||!producerNamespace||bytes(producerNamespace)>128)throw new Error('measurement_envelope_identity_invalid');
 if(!Number.isInteger(maxWireBytes)||maxWireBytes<1||maxWireBytes>PRODUCER_BOUNDS.maxWireBytes)throw new Error('measurement_envelope_bound_invalid');
 for(const clock of [occurredAt,observedAt,preparedAt])if(clock!==null&&clock!==undefined&&(!Number.isSafeInteger(clock)||clock<0))throw new Error('measurement_envelope_clock_invalid');
 const time=()=>performance.now();let t=time();
 const logical=canonicalSerialize(payload,maxWireBytes);
 // Occurrence/observation clocks are evidence, unlike transport preparation.
 const digestLogical=canonicalSerialize({clocks:{occurredAt,observedAt},payload:JSON.parse(logical)},maxWireBytes);
 measure?.('canonicalizationMs',time()-t);t=time();
 const payloadDigest=await digestPayload(digestLogical);
 const semanticKey=canonicalSerialize([producerNamespace,kind,semanticId]);
 const eventId=`m1:${await digestPayload(canonicalSerialize([producerNamespace,kind,semanticId,payloadDigest]))}`;
 measure?.('hashingMs',time()-t);t=time();
 // Canonical tagged JSON is the wire representation: null/unavailable/nonfinite
 // values remain distinct. Consumers use the established evidence decoder.
 const envelope=freeze({version:ENVELOPE_VERSION,producerNamespace,kind,semanticId,semanticKey,eventId,payloadDigest,
  measurementOnly:true,decisionUse:false,clocks:{occurredAt,observedAt,preparedAt},payload:JSON.parse(logical)});
 const wire=canonicalSerialize(envelope,maxWireBytes),wireBytes=bytes(wire),payloadBytes=bytes(logical);
 measure?.('serializationMs',time()-t);
 return Object.freeze({envelope,wire,wireBytes,payloadBytes,headerBytes:wireBytes-payloadBytes,
  requiresFutureFragmentation:wireBytes>60000,durable:false});
}
