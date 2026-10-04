// Deterministic authoritative-shaped local evidence, never a Production signal.
import fs from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {measurementWriter} from '../signal-evidence-store.js';
import {transactionalBinding} from './b1-sqlite.mjs';
import {foldPostEntry} from '../post-entry-evidence.js';
export function localMeasurementDatabase(){const db=new DatabaseSync(':memory:');for(const name of ['0001_measurement_evidence.sql','0002_measurement_storage_tiers.sql'])db.exec(fs.readFileSync(new URL('../migrations/'+name,import.meta.url),'utf8'));return db;}
export function recordBytes(row,{virtual=[]}={}){return Object.entries(row).filter(([key])=>!virtual.includes(key)).reduce((n,[_key,v])=>n+(v==null?0:typeof v==='string'?Buffer.byteLength(v):v instanceof Uint8Array?v.length:8),0);}
export async function outcomeProfile(profile='complete',{sessionId='synthetic-session',realistic=false}={}){
 const at=1790608200000,end=at+3600000,db=localMeasurementDatabase(),writer=measurementWriter(transactionalBinding(db));
 const subject={id:'cycle:1790608200000:0',createdAt:at,side:'buy',entry:4100,tp1:4100.4,tp2:4100.8,sl:4099.4,status:'active'};
 const ticks=Array.from({length:3601},(_,i)=>({ts:at+i*1000,price:realistic?Math.round((4100+Math.sin(i/10))*1000)/1000:4100+Math.sin(i/10),measurement:{provider:'mt5',providerTimestamp:at+i*1000,sessionId,sequence:i+1,priceBasis:'canonical-midpoint'}}));
 const bars=Array.from({length:60},(_,i)=>({t:at+i*60000,o:4100,h:4101,l:4099,c:4100,provider:'mt5',sessionId}));
 const selected=profile==='gaps'?ticks.filter((_t,i)=>i%100<3||i===3600):ticks;
 const folded=foldPostEntry(subject,null,{asOf:end,ticks:selected,bars:profile==='complete'?bars:[]});
 for(const event of folded.events)await writer.immutable('outcome',{event_id:event.eventId,subject_id:subject.id,event_type:event.eventType,available_at:event.availableAt,recorded_at:end,occurred_at:event.occurredAt??null,block_ids_json:'[]'},event);
 await writer.finalize(subject.id,'CANDIDATE',{...folded.state,availableAt:end,observedAt:end,occurredFrom:at,occurredTo:end},end);
 const rows=db.prepare('SELECT * FROM signal_outcome_evidence').all();
 return {db,at,end,subject,folded,rows,bytes:rows.reduce((n,r)=>n+recordBytes(r),0),finalBytes:recordBytes(db.prepare('SELECT * FROM measurement_final_results').get(),{virtual:['payload_json']})};
}
