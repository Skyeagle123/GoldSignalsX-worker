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
function walkEvidence(value,{maxInputBytes=PRODUCER_BOUNDS.maxInputBytes,maxNodes=PRODUCER_BOUNDS.maxNodes,maxDepth=PRODUCER_BOUNDS.maxDepth}={},materialize=true){
 if(!Number.isInteger(maxInputBytes)||maxInputBytes<1||maxInputBytes>PRODUCER_BOUNDS.maxInputBytes||
    !Number.isInteger(maxNodes)||maxNodes<1||maxNodes>PRODUCER_BOUNDS.maxNodes||
    !Number.isInteger(maxDepth)||maxDepth<0||maxDepth>PRODUCER_BOUNDS.maxDepth)throw new Error('measurement_input_bound_invalid');
 let nodes=0,charged=0;const seen=new Set();
 const charge=n=>{charged+=n;if(charged>maxInputBytes)throw new Error('measurement_input_exceeded');};
 function copy(v,depth=0){
  if(++nodes>maxNodes||depth>maxDepth)throw new Error('measurement_input_structure_exceeded');
  if(v===null||v===undefined||typeof v==='boolean'||typeof v==='number'){charge(8);return v;}
  if(typeof v==='string'){if(v.length>maxInputBytes)throw new Error('measurement_input_exceeded');charge(bytes(v));return v;}
  if(typeof v!=='object'||seen.has(v))throw new Error('measurement_serialization_invalid');
  seen.add(v);let result;
  if(v instanceof Map){
   if(Reflect.ownKeys(v).length)throw new Error('measurement_object_invalid');
   const size=Object.getOwnPropertyDescriptor(Map.prototype,'size').get.call(v);
   if(size>maxNodes)throw new Error('measurement_input_structure_exceeded');charge(size);
   const entries=materialize?[]:null;for(const [k,x]of Map.prototype.entries.call(v)){const key=copy(k,depth+1),value=copy(x,depth+1);if(materialize)entries.push(Object.freeze([key,value]));}
   if(materialize){const map=new Map(entries);
    result=Object.freeze({get:k=>map.get(k),has:k=>map.has(k),[Symbol.iterator]:()=>entries[Symbol.iterator]()});}
  }else if(v instanceof Set){
   if(Reflect.ownKeys(v).length)throw new Error('measurement_object_invalid');
   const size=Object.getOwnPropertyDescriptor(Set.prototype,'size').get.call(v);
   if(size>maxNodes)throw new Error('measurement_input_structure_exceeded');charge(size);
   const entries=materialize?[]:null;for(const x of Set.prototype.values.call(v)){const value=copy(x,depth+1);if(materialize)entries.push(value);}
   if(materialize){const set=new Set(entries);result=Object.freeze({has:k=>set.has(k),[Symbol.iterator]:()=>entries[Symbol.iterator]()});}
  }else if(Array.isArray(v)){
   // Enrichment may use Array methods: only the standard data-array prototype is supported.
   if(Object.getPrototypeOf(v)!==Array.prototype)throw new Error('measurement_array_invalid');
   const length=Object.getOwnPropertyDescriptor(v,'length').value;
   if(length>maxNodes)throw new Error('measurement_input_structure_exceeded');charge(length);
   // Dense, data-only evidence: inspect descriptors, never run an index getter.
   // Charge the whole array before allocating its copy or visiting children.
   const keys=Reflect.ownKeys(v);
   if(keys.length!==length+1)throw new Error('measurement_array_invalid');
   result=materialize?[]:null;
   for(let i=0;i<length;i++){
    const descriptor=Object.getOwnPropertyDescriptor(v,String(i));
    if(!descriptor||!('value' in descriptor)||!descriptor.enumerable)throw new Error('measurement_array_invalid');
    const value=copy(descriptor.value,depth+1);if(materialize)result.push(value);
   }
   if(materialize)Object.freeze(result);
  }else if(Object.getPrototypeOf(v)===Object.prototype||Object.getPrototypeOf(v)===null){
   const keys=Reflect.ownKeys(v);if(keys.length>maxNodes)throw new Error('measurement_input_structure_exceeded');charge(keys.length);
   result=materialize?Object.create(null):null;for(const key of keys){if(typeof key!=='string')throw new Error('measurement_object_invalid');const descriptor=Object.getOwnPropertyDescriptor(v,key);if(!descriptor||!('value' in descriptor)||!descriptor.enumerable)throw new Error('measurement_object_invalid');if(/token|password|secret|authorization|api.?key/i.test(key))throw new Error('measurement_sensitive_field');if(key.length>maxInputBytes)throw new Error('measurement_input_exceeded');charge(bytes(key));const value=copy(descriptor.value,depth+1);if(materialize)result[key]=value;}if(materialize)Object.freeze(result);
  }else throw new Error('measurement_object_invalid');
  seen.delete(v);return result;
 }
 return copy(value);
}

// Validation traverses descriptors and budgets without cloning observed values.
export function validateDataEvidence(value,options={}){walkEvidence(value,options,false);}
export function copyFrozenEvidence(value,options={}){return walkEvidence(value,options,true);}

export async function buildMeasurementEnvelope(options){
 if(!options||(Object.getPrototypeOf(options)!==Object.prototype&&Object.getPrototypeOf(options)!==null))throw new Error('measurement_object_invalid');
 const keys=Reflect.ownKeys(options);
 if(keys.length>9)throw new Error('measurement_object_invalid');
 for(const key of keys){
  if(typeof key!=='string'||!['kind','semanticId','payload','occurredAt','observedAt','preparedAt','producerNamespace','maxWireBytes','measure'].includes(key))throw new Error('measurement_object_invalid');
  const descriptor=Object.getOwnPropertyDescriptor(options,key);
  if(!descriptor||!('value' in descriptor)||!descriptor.enumerable)throw new Error('measurement_object_invalid');
 }
 const {kind,semanticId,payload,occurredAt,observedAt,preparedAt,
  producerNamespace=PRODUCER_NAMESPACE,maxWireBytes=PRODUCER_BOUNDS.maxWireBytes,measure=null}=options;
 if(!ENVELOPE_KINDS.includes(kind))throw new Error('measurement_envelope_kind_invalid');
 if(typeof semanticId!=='string'||!semanticId||bytes(semanticId)>1024||typeof producerNamespace!=='string'||!producerNamespace||bytes(producerNamespace)>128)throw new Error('measurement_envelope_identity_invalid');
 if(!Number.isInteger(maxWireBytes)||maxWireBytes<1||maxWireBytes>PRODUCER_BOUNDS.maxWireBytes)throw new Error('measurement_envelope_bound_invalid');
 for(const clock of [occurredAt,observedAt,preparedAt])if(clock!==null&&clock!==undefined&&(!Number.isSafeInteger(clock)||clock<0))throw new Error('measurement_envelope_clock_invalid');
 const time=()=>performance.now();let t=time();
 // Preflight and copy before canonicalization. In particular a sparse array or
 // accessor may not expand or execute before a wire-size rejection.
 const data=copyFrozenEvidence(payload,{maxInputBytes:Math.min(maxWireBytes,PRODUCER_BOUNDS.maxInputBytes)});
 const logical=canonicalSerialize(data,maxWireBytes);
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
