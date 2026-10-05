// Scheduled measurement writer only. Never imported by the reader. No schema initializer.
import {measurementWriter} from './signal-evidence-store.js';
import {decodeStoredEvidence,decodeStoredOutcome,decodeProcessingState,restoreSharedDecision} from './evidence-codec.js';
import {parseEvidence} from './signal-evidence.js';
import {foldPostEntry} from './post-entry-evidence.js';
import {buildMarketManifest} from './market-evidence.js';
export async function collectMeasurement(db,{asOf,ticks,bars,maxSubjects=8,maxWrites=80}){
 return collect(db,{asOf,ticks,bars,maxSubjects,maxWrites},false);
}
// M2-only entry: a separate measurement database, no trading-table fallback.
export async function collectMeasurementFromProjection(db,{asOf,ticks,bars,maxSubjects=8,maxWrites=80}){
 return collect(db,{asOf,ticks,bars,maxSubjects,maxWrites},true);
}
async function collect(db,{asOf,ticks,bars,maxSubjects,maxWrites},measurementPlane){
 try{
  // Official lifecycle subjects have priority over research candidates so newly
  // arriving candidates cannot starve their bounded tick-buffer accumulation.
  // Within each class, the least recently processed subject gets the next turn.
  const select=measurementPlane?`SELECT d.evaluation_id,d.kind,d.official_signal_id,d.cohort_id,d.payload_json AS entry_json,
   s.payload_json AS state_json,s.payload_blob AS state_blob,s.codec AS state_codec,s.uncompressed_length AS state_length,s.payload_digest AS state_digest,s.updated_at AS state_updated_at,p.status,p.closed_at
   FROM signal_measurement_state s JOIN signal_decision_evidence d ON d.evaluation_id=s.evaluation_id
   LEFT JOIN measurement_lifecycle_projection p ON p.signal_id=d.official_signal_id AND p.available_at<=?
    AND p.integrity_status!='CONFLICT' AND p.fact_count=(SELECT COUNT(*) FROM measurement_lifecycle_facts f WHERE f.signal_id=p.signal_id)
   WHERE EXISTS(SELECT 1 FROM measurement_ingress_receipts r WHERE r.event_kind='DECISION_CYCLE' AND r.semantic_id=d.cycle_id AND r.ingested_at<=?)
    AND s.kind=? AND s.next_observe_at<=? ORDER BY s.next_observe_at,s.subject_id LIMIT ?`:
   `SELECT d.evaluation_id,d.kind,d.official_signal_id,d.cohort_id,d.payload_json AS entry_json,
   s.payload_json AS state_json,s.payload_blob AS state_blob,s.codec AS state_codec,s.uncompressed_length AS state_length,s.payload_digest AS state_digest,s.updated_at AS state_updated_at,p.status,p.closed_at
   FROM signal_measurement_state s JOIN signal_decision_evidence d ON d.evaluation_id=s.evaluation_id
   LEFT JOIN production_signals p ON p.signal_id=d.official_signal_id
   WHERE s.kind=? AND s.next_observe_at<=? ORDER BY s.next_observe_at,s.subject_id LIMIT ?`;
  const selectClass=(kind,limit)=>db.prepare(select).bind(...(measurementPlane?[asOf,asOf]:[]),kind,asOf,limit).all();
  const official=(await selectClass('OFFICIAL',maxSubjects)).results||[];
  const candidates=official.length<maxSubjects?(await selectClass('CANDIDATE',maxSubjects-official.length)).results||[]:[];
  const rows=[...official,...candidates];
  const writer=measurementWriter(db,{maxWrites});let updated=0;const resourceReviews=[];const blocks=new Map();
  const subjects=[];
  for(const row of rows){
   const rich=await db.prepare('SELECT * FROM measurement_rich_evidence WHERE evidence_id=?').bind(`decision:${row.evaluation_id}`).first();
   if(!rich)continue;
   const entry=await decodeStoredEvidence(rich);
   const shared=await db.prepare('SELECT r.*,c.payload_json AS cohort_json FROM decision_cycle_evidence y JOIN measurement_rich_evidence r ON r.evidence_id=? JOIN measurement_cohorts c ON c.cohort_id=y.cohort_id WHERE y.cycle_id=?').bind(`cycle:${entry.sharedContextRef}`,entry.sharedContextRef).first();
   const defs=new Map();if(entry.definitionRef){const d=await db.prepare('SELECT payload_json FROM measurement_definitions WHERE definition_id=?').bind(entry.definitionRef).first();if(d)defs.set(entry.definitionRef,parseEvidence(d.payload_json));}
   restoreSharedDecision(entry,shared?await decodeStoredEvidence(shared):null,shared?parseEvidence(shared.cohort_json):null,defs);
   const levels=entry.engine?.levels;if(!levels)continue;
   const createdAt=entry.createdAt??entry.evaluatedAt;
   const subject={id:row.official_signal_id||row.evaluation_id,createdAt,entry:levels.entry,tp1:levels.tp1,tp2:levels.tp2,sl:levels.sl,
    side:entry.direction,status:row.kind==='OFFICIAL'?row.status:'active',closedAt:row.closed_at??null,
    measurementHorizonAt:row.kind==='CANDIDATE'?createdAt+3600000:null};
   let previous=row.state_json?await decodeProcessingState({payload_json:row.state_json,payload_blob:row.state_blob,codec:row.state_codec,uncompressed_length:row.state_length,payload_digest:row.state_digest}):{processedThrough:subject.createdAt,covered:[],gaps:[],barriers:{},global:{mfe:null,mae:null}};
   const checkpointRow=await db.prepare("SELECT * FROM signal_outcome_evidence WHERE subject_id=? AND event_type='COVERAGE_CHECKPOINT' ORDER BY available_at DESC,event_id DESC LIMIT 1").bind(subject.id).first();
   let latest=checkpointRow?await decodeStoredOutcome(checkpointRow):null;
   if(latest?.evidenceRef){const r=await db.prepare('SELECT * FROM measurement_rich_evidence WHERE evidence_id=?').bind(latest.evidenceRef).first();if(r)latest=await decodeStoredEvidence(r);}
   if(latest?.checkpointRole==='BARRIER_COVERAGE')latest=null;
   if(latest&&latest.availableAt>=(previous?.outcome?.evidenceAsOf??subject.createdAt))previous={...previous,processedThrough:latest.occurredTo,covered:latest.covered,gaps:latest.gaps,quality:latest.quality,global:latest.global,outcome:latest.outcome};
   const existing=(await db.prepare("SELECT * FROM signal_outcome_evidence WHERE subject_id=? AND event_type IN ('TP1','TP2','SL')").bind(subject.id).all()).results||[];
   for(const record of existing){const event=await decodeStoredOutcome(record);if(event.evidenceType==='BARRIER_OBSERVATION')previous.barriers[`${event.eventType}:${event.source}`]=event;}
   if(latest?.availableAt===asOf){await writer.state(subject.id,row.cohort_id,previous,asOf);updated++;continue;}
   const observationAsOf=Math.min(asOf,subject.measurementHorizonAt??asOf);
   const folded=foldPostEntry(subject,previous,{ticks,bars,asOf:observationAsOf,availableAt:asOf});
   folded.state.measurementHorizonAt=subject.measurementHorizonAt;
   folded.state.productionLifecycleEvidence=row.kind==='OFFICIAL'?(row.status?'AVAILABLE':'UNAVAILABLE'):'NOT_APPLICABLE';
   if(measurementPlane)folded.state.lifecycleEvidenceBasis='IMMUTABLE_CAPTURED_MEASUREMENT_FACTS';
   if(row.kind==='OFFICIAL'&&!row.status){folded.state.nextObservationAt=asOf+86400000;folded.state.captureGap=true;}
   folded.state.measurementStatus=subject.measurementHorizonAt&&asOf>=subject.measurementHorizonAt?'HORIZON_ATTEMPT_COMPLETE; COVERAGE_MAY_BE_INSUFFICIENT':'COLLECTING';
   const selected=bars.filter(b=>b.t>=createdAt&&b.t+60000<=Math.min(observationAsOf,subject.closedAt??observationAsOf)&&b.t+60000>(previous?.processedThrough??createdAt));
   const built=await buildMarketManifest('1m',selected,asOf);for(const b of built.blocks)blocks.set(b.blockId,b);
   subjects.push({row,subject,folded,manifest:built.manifest,observationAsOf});
  }
  for(const b of blocks.values())await writer.immutable('market',{block_id:b.blockId,timeframe:b.tf,from_at:b.from,to_at:b.to,recorded_at:asOf},parseEvidence(b.payload));
  for(const {row,subject,folded,manifest,observationAsOf} of subjects){
   if(folded.events.length)await writer.immutableBatch(folded.events.map(event=>({type:'outcome',values:{event_id:event.eventId,subject_id:subject.id,event_type:event.eventType,
    occurred_at:event.occurredAt??null,available_at:asOf,recorded_at:asOf,block_ids_json:'[]'},payload:event})),
    {links:folded.events.map(event=>({ownerType:'OUTCOME',ownerId:event.eventId,blockIds:manifest.references.map(x=>x.blockId)}))});
   if(row.kind==='OFFICIAL'&&manifest.references.length)await writer.pinOfficial(subject.id,manifest.references.map(r=>r.blockId));
   // Persist each finalized window once. Active cumulative data lives only in
   // bounded mutable processing state; final package is immutable and complete.
   for(const [name,window] of Object.entries(folded.state.quality?.windows||{})){
    if(window.state!=='FINALIZED')continue;
    const event={subjectId:subject.id,eventId:`${subject.id}:window:${name}`,evidenceType:'WINDOW_FINALIZED',eventType:'WINDOW_FINALIZED',availableAt:asOf,
     observedAt:asOf,occurredFrom:subject.createdAt,occurredTo:window.measuredTo??folded.state.processedThrough,window,measurementOnly:true,decisionUse:false};
    const exists=await db.prepare('SELECT event_id FROM signal_outcome_evidence WHERE event_id=?').bind(event.eventId).first();
    if(!exists)await writer.immutable('outcome',{event_id:event.eventId,subject_id:subject.id,event_type:event.eventType,available_at:asOf,recorded_at:asOf,block_ids_json:'[]'},event);
   }
   if(subject.closedAt&&subject.closedAt<=asOf){const lifecycle={subjectId:subject.id,eventId:`${subject.id}:lifecycle:${subject.status}`,evidenceType:'LIFECYCLE_EVENT',
    eventType:subject.status.toUpperCase(),occurredAt:subject.closedAt,availableAt:asOf,observedAt:asOf,orderingQuality:'UNKNOWN',measurementOnly:true,decisionUse:false};
    await writer.immutable('outcome',{event_id:lifecycle.eventId,subject_id:subject.id,event_type:lifecycle.eventType,
     occurred_at:lifecycle.occurredAt,available_at:asOf,recorded_at:asOf,block_ids_json:'[]'},lifecycle);
   }
   const complete=(subject.measurementHorizonAt&&asOf>=subject.measurementHorizonAt)||(subject.closedAt&&subject.closedAt<=asOf);
   if(complete){const finalization=await writer.finalize(subject.id,row.kind,{...folded.state,availableAt:asOf,observedAt:asOf,occurredFrom:subject.createdAt,occurredTo:folded.state.processedThrough,marketManifest:manifest},asOf,{blockIds:manifest.references.map(r=>r.blockId)});resourceReviews.push(...finalization.resourceReviews);}
   else await writer.state(subject.id,row.cohort_id,folded.state,asOf);updated++;
  }
  return {ok:true,updated,resourceReviews,subjects:rows.length,subjectBudget:maxSubjects,budgetLimitReached:rows.length===maxSubjects,
   selectedOldestLagMs:rows.length?Math.max(...rows.map(r=>asOf-(r.state_updated_at??r.evaluated_at))):0,
   unselectedCoverage:'NOT_ASSERTED; SUBJECT_BUDGET_MAY_LEAVE_GAPS',coverageBasis:'CANONICAL_INPUT_STREAM; BROKER_TICK_COMPLETENESS_UNKNOWN'};
 }catch(error){return {ok:false,error:String(error?.message||'measurement_collection_failed'),captureGap:true};}
}
