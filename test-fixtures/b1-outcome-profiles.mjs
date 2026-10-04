// Deterministic authoritative-shaped local evidence, never a Production signal.
import fs from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {measurementWriter} from '../signal-evidence-store.js';
import {transactionalBinding} from './b1-sqlite.mjs';
import {foldPostEntry} from '../post-entry-evidence.js';
import {decodeStoredOutcome,hydrateFinalOutcome} from '../evidence-codec.js';
export function localMeasurementDatabase(){const db=new DatabaseSync(':memory:');for(const name of ['0001_measurement_evidence.sql','0002_measurement_storage_tiers.sql','0003_measurement_dependency_closure.sql'])db.exec(fs.readFileSync(new URL('../migrations/'+name,import.meta.url),'utf8'));return db;}
export function recordBytes(row,{virtual=[]}={}){return Object.entries(row).filter(([key])=>!virtual.includes(key)).reduce((n,[_key,v])=>n+(v==null?0:typeof v==='string'?Buffer.byteLength(v):v instanceof Uint8Array?v.length:8),0);}
export async function outcomeProfile(profile='complete',{sessionId='synthetic-session',realistic=false}={}){
 const at=1790608200000,end=at+3600000,db=localMeasurementDatabase(),writer=measurementWriter(transactionalBinding(db),{maxWrites:160});
 const subject={id:'cycle:1790608200000:0',createdAt:at,side:'buy',entry:4100,tp1:4100.4,tp2:4100.8,sl:4099.4,status:'active'};
 const ticks=Array.from({length:3601},(_,i)=>({ts:at+i*1000,price:realistic?Math.round((4100+Math.sin(i/10))*1000)/1000:4100+Math.sin(i/10),measurement:{provider:'mt5',providerTimestamp:at+i*1000,sessionId,sequence:i+1,priceBasis:'canonical-midpoint'}}));
 const bars=Array.from({length:60},(_,i)=>({t:at+i*60000,o:4100,h:4101,l:4099,c:4100,provider:'mt5',sessionId}));
 const incremental=['incremental','session-transition','fragmented','correction'].includes(profile);
 if(incremental)for(let i=0;i<ticks.length;i++){
  ticks[i].price=Math.round((4100+Math.sin(i/10)*(1+i/3600*.3))*1000)/1000;
  ticks[i].measurement.providerTimestamp=ticks[i].ts-(i?97:0);
  if(profile!=='incremental'&&i>=1800){ticks[i].measurement.sessionId='738d8fa5-dc42-4157-b28f-2eb179a0f960';ticks[i].measurement.sequence=i-1799;}
 }
 if(profile!=='incremental'&&incremental)for(let i=30;i<bars.length;i++)bars[i].sessionId='738d8fa5-dc42-4157-b28f-2eb179a0f960';
 const fragmented=['fragmented','correction'].includes(profile);
 const selected=profile==='gaps'?ticks.filter((_t,i)=>i%100<3||i===3600):fragmented?ticks.filter((_t,i)=>i<60||i===3600||i%173<43):ticks;
 const selectedBars=profile==='gaps'?[]:fragmented?bars.filter((_b,i)=>i===0||i%11===0):bars;
 let folded,previous=null;
 for(let time=incremental?at+300000:end;time<=end;time+=300000){
  folded=foldPostEntry(subject,previous,{asOf:time,ticks:selected.filter(t=>t.ts<=time&&(!incremental||t.ts>=time-301000)),bars:selectedBars.filter(b=>b.t+60000<=time)});
  await writer.immutableBatch(folded.events.map(event=>({type:'outcome',values:{event_id:event.eventId,subject_id:subject.id,event_type:event.eventType,available_at:event.availableAt,recorded_at:time,occurred_at:event.occurredAt??null,block_ids_json:'[]'},payload:event})));
  for(const [name,window]of Object.entries(folded.state.quality.windows))if(window.state==='FINALIZED'&&!db.prepare('SELECT event_id FROM signal_outcome_evidence WHERE event_id=?').get(`${subject.id}:window:${name}`)){
   const event={subjectId:subject.id,eventId:`${subject.id}:window:${name}`,evidenceType:'WINDOW_FINALIZED',eventType:'WINDOW_FINALIZED',availableAt:time,observedAt:time,occurredFrom:at,occurredTo:window.measuredTo,window,measurementOnly:true,decisionUse:false};
   await writer.immutable('outcome',{event_id:event.eventId,subject_id:subject.id,event_type:event.eventType,available_at:time,recorded_at:time,block_ids_json:'[]'},event);
  }
  previous=folded.state;
 }
 await writer.finalize(subject.id,'CANDIDATE',{...folded.state,measurementHorizonAt:end,availableAt:end,observedAt:end,occurredFrom:at,occurredTo:end},end);
 if(profile==='correction'){
  const original=Object.values(folded.state.barriers).find(e=>e.eventType==='TP1'&&e.source==='mt5:closed-1m-bar');
  const event={subjectId:subject.id,eventId:`${subject.id}:correction:bar-revision:1`,eventType:'CORRECTION',evidenceType:'CORRECTION',supersedesEventId:original.eventId,reason:'SOURCE_BAR_REVISION',availableAt:end+60000,observedAt:end+60000,occurredFrom:original.occurredFrom,occurredTo:original.occurredTo,source:original.source,sessionId:original.sessionId,priceBasis:original.priceBasis,price:4101.005,level:original.level,orderingQuality:'INTERVAL',evaluatorVersion:'b1-witness-v1',measurementOnly:true,decisionUse:false};
  await writer.immutable('outcome',{event_id:event.eventId,subject_id:subject.id,event_type:event.eventType,available_at:event.availableAt,recorded_at:event.availableAt,block_ids_json:'[]'},event);
 }
 const rows=db.prepare('SELECT * FROM signal_outcome_evidence').all();
 const events=await Promise.all(rows.map(r=>decodeStoredOutcome(r)));for(const event of events)if(event.coverageRef&&!events.some(e=>e.eventId===event.coverageRef&&e.evidenceType==='COVERAGE_CHECKPOINT'))throw new Error('profile_coverage_reference_missing');
 hydrateFinalOutcome(events.find(e=>e.evidenceType==='FINAL_MEASUREMENT'),events);
 const bytes=db.prepare("SELECT SUM(payload) bytes FROM dbstat WHERE name IN ('measurement_subjects','measurement_outcome_records','measurement_outcome_dependencies')").get().bytes;
 const finalBytes=db.prepare("SELECT SUM(payload) bytes FROM dbstat WHERE name='measurement_final_results'").get().bytes;
 return {db,at,end,subject,folded,rows,events,bytes,finalBytes};
}
