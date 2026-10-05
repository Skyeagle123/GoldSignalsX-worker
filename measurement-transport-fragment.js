// M3 offline framing only. Original M1 wire and M2 validation remain authoritative.
import {PRODUCER_BOUNDS,ENVELOPE_KINDS,PRODUCER_NAMESPACE,buildMeasurementEnvelope,validateDataEvidence} from './measurement-envelope.js';
import {validateMeasurementWire} from './measurement-consumer.js';
import {canonicalSerialize,digestPayload} from './signal-evidence.js';

export const TRANSPORT_VERSION=1;
export const TRANSPORT_NAMESPACE='goldsignalsx:b1:m3';
export const MAX_FRAGMENT_ENVELOPE_BYTES=60000;
export const FRAGMENT_DATA_BYTES=42000;
export const MAX_EVENT_BYTES=PRODUCER_BOUNDS.maxWireBytes;
export const MAX_FRAGMENTS=Math.ceil(MAX_EVENT_BYTES/FRAGMENT_DATA_BYTES);
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true});
const fields=['version','namespace','eventId','eventKind','payloadDigest','wireDigest','totalBytes','count','ordinal','fragmentBytes','fragmentDigest','fragmentId','data'];
const hex=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const fail=reason=>{throw new Error(reason);};
export const serializedBytes=wire=>encoder.encode(wire).length;
export async function digestTransportBytes(bytes){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(b=>b.toString(16).padStart(2,'0')).join('');}
const base64=bytes=>btoa(String.fromCharCode(...bytes));
export const fragmentIdentity=f=>digestPayload(canonicalSerialize([TRANSPORT_NAMESPACE,TRANSPORT_VERSION,f.eventId,f.payloadDigest,f.count,f.ordinal]));
export function sameFragmentManifest(a,b){return ['version','namespace','eventId','eventKind','payloadDigest','wireDigest','totalBytes','count'].every(k=>a[k]===b[k]);}

// Framing preserves M1's envelope contract, including evidence that M2 may
// reject. Rebuilding this envelope verifies identity; no git commit or payload
// is recreated. Full M2 validation still gates completed consumer delivery.
export async function validateTransportEvent(wire){
 if(typeof wire!=='string'||wire.length>MAX_EVENT_BYTES||serializedBytes(wire)>MAX_EVENT_BYTES)fail('transport_event_bound');
 const e=JSON.parse(wire);validateDataEvidence(e);
 if(!e||!e.clocks||e.producerNamespace!==PRODUCER_NAMESPACE)fail('transport_event_identity');
 const rebuilt=await buildMeasurementEnvelope({kind:e.kind,semanticId:e.semanticId,payload:e.payload,
  occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt,preparedAt:e.clocks.preparedAt,producerNamespace:e.producerNamespace});
 if(canonicalSerialize(e,MAX_EVENT_BYTES)!==rebuilt.wire)fail('transport_event_identity');
 return e;
}

export async function fragmentMeasurementPacket(packet,{receivedAt=Date.now()}={}){
 if(!packet||typeof packet.wire!=='string')fail('transport_packet_invalid');
 const wire=packet.wire;
 if(wire.length>MAX_EVENT_BYTES)fail('transport_event_bound');
 const bytes=encoder.encode(wire);if(bytes.length>MAX_EVENT_BYTES)fail('transport_event_bound');
 const e=await validateTransportEvent(wire);
 const count=Math.ceil(bytes.length/FRAGMENT_DATA_BYTES),wireDigest=await digestTransportBytes(bytes),fragments=[];
 for(let ordinal=0;ordinal<count;ordinal++){
  const data=bytes.subarray(ordinal*FRAGMENT_DATA_BYTES,(ordinal+1)*FRAGMENT_DATA_BYTES);
  const f={version:TRANSPORT_VERSION,namespace:TRANSPORT_NAMESPACE,eventId:e.eventId,eventKind:e.kind,payloadDigest:e.payloadDigest,wireDigest,totalBytes:bytes.length,count,ordinal,
   fragmentBytes:data.length,fragmentDigest:await digestTransportBytes(data),fragmentId:null,data:base64(data)};
  f.fragmentId='m3:'+await fragmentIdentity(f);
  fragments.push(canonicalSerialize(f,MAX_FRAGMENT_ENVELOPE_BYTES));
 }
 return Object.freeze(fragments);
}

export async function validateTransportFragment(wire){
 if(typeof wire!=='string'||wire.length>MAX_FRAGMENT_ENVELOPE_BYTES||serializedBytes(wire)>MAX_FRAGMENT_ENVELOPE_BYTES)fail('transport_fragment_bound');
 let f;try{f=JSON.parse(wire);}catch{fail('transport_fragment_json');}
 if(!f||Array.isArray(f)||Object.keys(f).length!==fields.length||fields.some(k=>!Object.hasOwn(f,k)))fail('transport_fragment_shape');
 if(f.version!==TRANSPORT_VERSION||f.namespace!==TRANSPORT_NAMESPACE)fail('transport_fragment_version_namespace');
 if(typeof f.eventId!=='string'||!/^m1:[a-f0-9]{64}$/.test(f.eventId)||!ENVELOPE_KINDS.includes(f.eventKind)||!hex(f.payloadDigest)||!hex(f.wireDigest)||!hex(f.fragmentDigest))fail('transport_fragment_identity');
 if(!Number.isSafeInteger(f.totalBytes)||f.totalBytes<1||f.totalBytes>MAX_EVENT_BYTES||!Number.isInteger(f.count)||f.count!==Math.ceil(f.totalBytes/FRAGMENT_DATA_BYTES)||f.count>MAX_FRAGMENTS||
  !Number.isInteger(f.ordinal)||f.ordinal<0||f.ordinal>=f.count||!Number.isInteger(f.fragmentBytes)||f.fragmentBytes!==Math.min(FRAGMENT_DATA_BYTES,f.totalBytes-f.ordinal*FRAGMENT_DATA_BYTES))fail('transport_fragment_manifest');
 if(typeof f.data!=='string'||f.data.length!==4*Math.ceil(f.fragmentBytes/3)||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(f.data))fail('transport_fragment_data');
 const bytes=Uint8Array.from(atob(f.data),c=>c.charCodeAt(0));
 if(bytes.length!==f.fragmentBytes||base64(bytes)!==f.data||await digestTransportBytes(bytes)!==f.fragmentDigest)fail('transport_fragment_digest');
 if(f.fragmentId!=='m3:'+await fragmentIdentity(f))fail('transport_fragment_identity');
 return {manifest:Object.freeze(f),bytes};
}

export async function reassembleMeasurementFragments(manifest,pieces,{receivedAt}={}){
 if(pieces.size!==manifest.count)fail('transport_fragments_missing');
 // Allocation uses the validated geometry and actual verified bytes, not a declaration alone.
 const data=[];let actual=0;
 for(let i=0;i<manifest.count;i++){
  const piece=pieces.get(i);if(!piece)fail('transport_fragments_missing');
  const verified=await validateTransportFragment(piece);
  if(verified.manifest.ordinal!==i||!sameFragmentManifest(manifest,verified.manifest))fail('transport_manifest_conflict');
  actual+=verified.bytes.length;data.push(verified.bytes);
 }
 if(actual!==manifest.totalBytes||actual>MAX_EVENT_BYTES)fail('transport_complete_length');
 const bytes=new Uint8Array(actual);let offset=0;for(const part of data){bytes.set(part,offset);offset+=part.length;}
 if(await digestTransportBytes(bytes)!==manifest.wireDigest)fail('transport_complete_digest');
 let wire;try{wire=decoder.decode(bytes);}catch{fail('transport_complete_utf8');}
 const envelope=await validateMeasurementWire(wire,{receivedAt});
 if(envelope.producerNamespace!==PRODUCER_NAMESPACE||envelope.eventId!==manifest.eventId||envelope.kind!==manifest.eventKind||envelope.payloadDigest!==manifest.payloadDigest)fail('transport_complete_identity');
 return wire;
}
