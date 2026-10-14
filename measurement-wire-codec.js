import {need,hash,LIMITS,bytes} from './measurement-independent-contract.js';
// Identity encoding makes multipart retry deterministic across runtime/compressor
// upgrades. Readers retain the synthetic v1 gzip fixture for format compatibility.
export async function encodeWire(wire){const raw=new TextEncoder().encode(wire);need(raw.length<=LIMITS.eventBytes,'pa_wire_bound');return {codec:'pa-identity-wire-v1',data:raw};}

export function encodedChunks(data){const out=[];for(let i=0;i<data.length;i+=LIMITS.chunkBytes)out.push(btoa(String.fromCharCode(...data.subarray(i,i+LIMITS.chunkBytes))));return out;}
export async function decodeWire(parts,record){need(parts.length===record.chunk_count&&['pa-gzip-wire-v1','pa-identity-wire-v1'].includes(record.codec),'pa_wire_codec');const data=new Uint8Array(parts.reduce((n,p)=>n+p.length,0));need(data.length===record.encoded_bytes&&data.length<=LIMITS.eventBytes&&record.bytes<=LIMITS.eventBytes,'pa_wire_encoded_bound');let at=0;for(const p of parts){data.set(p,at);at+=p.length;}
 let raw=data;if(record.codec==='pa-gzip-wire-v1'){const reader=new Blob([data]).stream().pipeThrough(new DecompressionStream('gzip')).getReader(),chunks=[];let length=0;try{for(;;){const r=await reader.read();if(r.done)break;length+=r.value.length;if(length>record.bytes||length>LIMITS.eventBytes){await reader.cancel();throw Error('pa_decompression_bound');}chunks.push(r.value);}}finally{reader.releaseLock();}need(length===record.bytes,'pa_wire_length');raw=new Uint8Array(length);let n=0;for(const p of chunks){raw.set(p,n);n+=p.length;}}
 const wire=new TextDecoder('utf-8',{fatal:true}).decode(raw);need(raw.length===record.bytes&&await hash(wire)===record.digest,'pa_record_digest');return wire;
}
