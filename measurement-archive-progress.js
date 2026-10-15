// Bounded private-archive continuation authority. These 64 replaceable slots
// are operational state, never immutable evidence or D1 write authority.
import {need,hash,serialize,bytes,uid} from './measurement-independent-contract.js';
const FORMAT='measurement-archive-progress-v1',TTL=3600000,SLOTS=64;
export function archiveProgressStore(bucket,namespace,admit,metrics){
 const key=slot=>namespace+'progress/'+slot.toString().padStart(2,'0');
 async function read(slot){admit();const object=await bucket.get(key(slot));if(!object)return null;
  need(object.size<=60000&&typeof object.etag==='string','pa_archive_progress_binding');
  const wire=new TextDecoder('utf-8',{fatal:true}).decode(await object.arrayBuffer());need(bytes(wire)===object.size,'pa_archive_progress_read');
  if(metrics){metrics.reads++;metrics.bytesRead+=bytes(wire);}const entry=JSON.parse(wire);need(entry.format===FORMAT&&entry.namespace===namespace&&entry.digest===await hash(serialize(entry.payload)),'pa_archive_progress_integrity');return {entry,etag:object.etag};
 }
 return Object.freeze({
  async load(context,token){
   need(token&&Number.isInteger(token.slot)&&token.slot>=0&&token.slot<SLOTS,'pa_archive_progress_token');
   const saved=await read(token.slot);need(saved&&saved.entry.expiresAt>Date.now(),'pa_archive_progress_expired');
   const e=saved.entry,latest=e.nonce===token.nonce&&e.digest===token.digest,replay=e.previous?.nonce===token.nonce&&e.previous?.digest===token.digest;
   need(e.context===await hash(context)&&(latest||replay),'pa_archive_progress_token');
   return {state:e.payload,slot:token.slot,etag:saved.etag,nonce:e.nonce,digest:e.digest,replay,token:{slot:token.slot,nonce:e.nonce,digest:e.digest}};
  },
  async save(context,state,loaded=null){
   const contextHash=await hash(context),slot=loaded?.slot??parseInt(contextHash.slice(0,8),16)%SLOTS;
   const old=loaded??await read(slot);const oldEntry=old?.entry;
   need(loaded||!oldEntry||oldEntry.context===contextHash||oldEntry.payload.complete||oldEntry.expiresAt<=Date.now(),'pa_archive_progress_capacity');
   if(!loaded&&oldEntry?.context===contextHash&&!oldEntry.payload.complete&&oldEntry.expiresAt>Date.now())return {slot,nonce:oldEntry.nonce,digest:oldEntry.digest};
   const nonce=uid(),digest=await hash(serialize(state)),entry={format:FORMAT,namespace,context:contextHash,nonce,digest,expiresAt:Date.now()+TTL,payload:state,...(loaded?{previous:{nonce:loaded.nonce,digest:loaded.digest}}:{})};
   admit();const wire=serialize(entry);if(metrics){metrics.writes++;metrics.bytesWritten+=bytes(wire);}const result=await bucket.put(key(slot),wire,{onlyIf:old?.etag?{etagMatches:old.etag}:{etagDoesNotMatch:'*'}});
   need(result,'pa_archive_progress_raced');const checked=await read(slot);need(checked?.entry.nonce===nonce&&checked.entry.digest===digest,'pa_archive_progress_raced');
   return {slot,nonce,digest};
  }
 });
}

// Fixed-size multiset fingerprint; paired with count and an actual complete
// predecessor walk. It detects listing/boundary changes, not write authority.
export const emptyFingerprint=()=>({count:0,sum:'0'.repeat(64)});
export async function fingerprintAdd(value,key){const sum=(BigInt('0x'+value.sum)+BigInt('0x'+await hash(['archive-root-v1',key])))%(1n<<256n);need(Number.isSafeInteger(value.count+1),'pa_archive_progress_count');return {count:value.count+1,sum:sum.toString(16).padStart(64,'0')};}
export const sameFingerprint=(a,b)=>a.count===b.count&&a.sum===b.sum;
