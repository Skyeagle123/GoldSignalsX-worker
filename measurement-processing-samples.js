// Container order is enumeration only. No clocks, Engine calls or trading writes.
import {need,hash,serialize,parseEvidence,bytes} from './measurement-independent-contract.js';
import {readRecord} from './measurement-independent-store.js';
import {validateMarketEvidence} from './measurement-market-envelope.js';
import {sampleDigest} from './measurement-market-store.js';
import {validatedMarketPacket} from './measurement-processing-budget.js';

export const SAMPLE_SCAN_VERSION='pa-sample-selection-v2';
export const SAMPLE_ORDER_VERSION='receipt-end-source-session-sequence-id-v1';
export const SAMPLE_REDUCER_VERSION='pa-sample-fold-v2';
const MAX_SELECTION=64,MAX_SELECTION_BYTES=16384;
const packetKeys=new WeakMap();
const compare=(a,b)=>{for(let i=0;i<a.length;i++){if(a[i]===b[i])continue;return a[i]<b[i]?-1:1;}return 0;};
export const compareSampleKeys=compare;
export async function sampleSelectionKey(p,s){
 const tick=s.type==='TICK',v=s.value;
 let prefix=packetKeys.get(p);if(!prefix){prefix=[await hash(p.source),await hash(p.session)];packetKeys.set(p,prefix);}
 return [tick?v.ts:v.t+60000,...prefix,tick?0:1,tick?v.measurement.sequence:0,s.id];
}
export const freshEnumeration=()=>({source:'HOT',after:''});
export function freshSelection(limit=64){need(Number.isInteger(limit)&&limit>=1&&limit<=MAX_SELECTION,'pa_processing_selection_bound');return {enumeration:freshEnumeration(),items:[],more:false,position:0,limit};}
export async function capturedRecordById(db,archive,pin,id){const row=await db.first('SELECT location FROM pa_records WHERE id=? AND ready=1',id);if(row?.location==='LIVE')return readRecord(db,id);need(pin.root&&pin.rootProof?.complete&&archive?.processingRecord,'pa_processing_archive_required');return archive.processingRecord(id,{rootKey:pin.root,proof:pin.rootProof});}

// The caller pins archive membership independently of the hot admission revision.
// A returned record is an atomic decoding unit and counts against the shared budget.
export async function capturedRecord(db,archive,pin,cursor,budget){
 while(budget.remaining>0){
  if(cursor.source==='HOT'){
   const r=await db.first("SELECT id FROM pa_records WHERE kind='MARKET_EXPORT' AND cohort=? AND ready=1 AND revision>? AND revision<=? AND available_at<=? AND id>? ORDER BY id LIMIT 1",pin.cohort,pin.lowerRevision??-1,pin.hotRevision,pin.asOf,cursor.after);
   if(!r){cursor.source='COLD';cursor.after='';continue;}
   if(!budget.invocation)budget.remaining--;const record=await capturedRecordById(db,archive,pin,r.id);need(record,'pa_processing_market_missing');cursor.after=r.id;return record;
  }
  if(cursor.source==='COLD'){
   if(!pin.root||pin.skipCold){cursor.source='END';return null;}
   need(archive?.processingMarketPage,'pa_processing_archive_required');
   const page=await archive.processingMarketPage(pin.cohort,{after:cursor.after,limit:1,rootKey:pin.root,proof:pin.rootProof});
   need(page.records.length<=1,'pa_processing_archive_page_bound');
   const before=cursor.after;cursor.after=page.next??before;if(page.complete)cursor.source='END';
   if(!page.records.length){need(page.complete||cursor.after>before,'pa_processing_archive_pending');continue;}
   if(!budget.invocation)budget.remaining--;const record=page.records[0];
   // Availability is independent of occurrence and of archive publication time.
   if(record.available_at>pin.asOf)continue;
   return record;
  }
  return null;
 }
 return undefined;
}
export async function validatedCapturedRecord(record,pin){
 const p=record[validatedMarketPacket]??await validateMarketEvidence(record.wire);
 need(record.id==='market:'+await hash(record.wire)&&record.kind==='MARKET_EXPORT'&&record.cohort===pin.cohort&&p.cohort===pin.cohort&&record.definition==='engine-v1'&&['pa-identity-wire-v1','pa-gzip-wire-v1'].includes(record.codec)&&record.created_at===p.from&&record.available_at<=pin.asOf,'pa_processing_market_identity');
 return p;
}
export async function selectSampleBlock(db,archive,state,budget,{hook=null}={}){
 const sweep=state.selection;
 while(budget.remaining>0&&sweep.enumeration.source!=='END'){
  const r=await capturedRecord(db,archive,state.selectionPin??state.pin,sweep.enumeration,budget);if(r===undefined)return false;if(!r)break;
  const p=await validatedCapturedRecord(r,state.pin);await hook?.('SAMPLE_RECORD_ENUMERATED',{recordId:r.id});
  for(let i=0;i<p.samples.length;i++){
   const s=p.samples[i],key=await sampleSelectionKey(p,s);
   if(state.cursor&&compare(key,state.cursor)<=0)continue;
   const digest=await sampleDigest(s),item={key,id:s.id,digest,recordId:r.id,recordDigest:r.digest,index:i,availableAt:r.available_at};
   const old=sweep.items.find(x=>x.id===s.id);
   if(old){need(old.digest===digest,'pa_processing_sample_conflict');if(item.availableAt<old.availableAt||item.availableAt===old.availableAt&&item.recordId<old.recordId)Object.assign(old,item);continue;}
   sweep.items.push(item);sweep.items.sort((a,b)=>compare(a.key,b.key));
   need(Number.isInteger(sweep.limit)&&sweep.limit>=1&&sweep.limit<=MAX_SELECTION,'pa_processing_selection_bound');
   while(sweep.items.length>sweep.limit||bytes(serialize(sweep.items))>MAX_SELECTION_BYTES){sweep.items.pop();sweep.more=true;}
   need(sweep.items.length>0,'pa_processing_sample_descriptor_bound');
  }
 }
 return sweep.enumeration.source==='END';
}
export async function selectedSample(db,archive,state,item,budget,cache){
 budget.reserve?.({});
 let cached=cache.get(item.recordId);
 if(!cached){if(!budget.remaining)return undefined;if(!budget.invocation)budget.remaining--;const record=await capturedRecordById(db,archive,state.pin,item.recordId);need(record&&record.digest===item.recordDigest,'pa_processing_market_missing');cached={record,p:await validatedCapturedRecord(record,state.pin)};cache.set(item.recordId,cached);}
 const {record,p}=cached,s=p.samples[item.index];
 need(s&&s.id===item.id&&await sampleDigest(s)===item.digest&&compare(await sampleSelectionKey(p,s),item.key)===0,'pa_processing_sample_integrity');
 const alias=await db.first('SELECT digest FROM pa_samples WHERE id=?',s.id);
 if(alias)need(alias.digest===item.digest,'pa_processing_sample_conflict');
 else if(archive?.processingRecord){if(!budget.remaining)return undefined;if(!budget.invocation)budget.remaining--;const original=await archive.processingRecord('sample:'+s.id,{rootKey:state.pin.root,proof:state.pin.rootProof});need(original&&original.sampleDigest===item.digest,'pa_processing_sample_witness');}
 else need(false,'pa_processing_sample_witness');
 return {sample:s,packet:p,availableAt:item.availableAt};
}

// Incremental extension must examine actual newly admitted SAMPLE identities,
// not compare a new record key with an old record frontier. Repeated packet
// overlap may contain old samples; the durable first witness identifies them.
export async function inspectCapturedDelta(db,archive,state,budget){
 const delta=state.delta;
 while(budget.remaining>0){
  const row=delta.recordId?{id:delta.recordId}:await db.first("SELECT id FROM pa_records WHERE kind='MARKET_EXPORT' AND cohort=? AND ready=1 AND revision>? AND revision<=? AND available_at<=? AND id>? ORDER BY id LIMIT 1",state.pin.cohort,state.pin.hotRevision,delta.hotRevision,delta.asOf,delta.after);
  if(!row)return true;
  if(!budget.invocation)budget.remaining--;const r=await capturedRecordById(db,archive,state.pin,row.id);need(r,'pa_processing_market_missing');delta.recordId=r.id;
  const p=await validatedCapturedRecord(r,{...state.pin,asOf:delta.asOf});
  const originals=new Map();for(let start=0;start<p.samples.length;start+=64){const ids=p.samples.slice(start,start+64).map(s=>s.id);for(const r of await db.all('SELECT s.id,s.digest,s.record_id,r.revision FROM pa_samples s LEFT JOIN pa_records r ON r.id=s.record_id AND r.ready=1 WHERE s.id IN(SELECT value FROM json_each(?))',serialize(ids)))originals.set(r.id,r);}
  for(let index=delta.sampleIndex??0;index<p.samples.length;index++){const s=p.samples[index];
   const key=await sampleSelectionKey(p,s),digest=await sampleDigest(s);
   const original=originals.get(s.id);
   if(original)need(original.digest===digest,'pa_processing_sample_conflict');
   let known=original&&original.revision!=null&&original.revision<=state.pin.hotRevision;
   if(!known&&state.pin.root&&archive?.processingAlias){if(archive.remaining&&archive.remaining()<512){delta.sampleIndex=index;return false;}const old=await archive.processingAlias('sample:'+s.id,{rootKey:state.pin.root,proof:state.pin.rootProof});if(old){need(old.sampleDigest===digest,'pa_processing_sample_conflict');known=true;}}
   if(!known&&state.cursor&&compare(key,state.cursor)<=0)delta.replay=true;
   if(!known)delta.changed=true;
  }
  delta.after=r.id;delta.recordId=null;delta.sampleIndex=0;
 }
 return false;
}
