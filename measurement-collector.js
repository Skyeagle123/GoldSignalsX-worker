// Scheduled measurement writer only. Never imported by the reader. No schema initializer.
import {measurementWriter} from './signal-evidence-store.js';
import {parseEvidence} from './signal-evidence.js';
import {foldPostEntry} from './post-entry-evidence.js';
import {buildMarketManifest} from './market-evidence.js';
export async function collectMeasurement(db,{asOf,ticks,bars,maxSubjects=8,maxWrites=80}){
 try{
  // Official lifecycle subjects have priority over research candidates so newly
  // arriving candidates cannot starve their bounded tick-buffer accumulation.
  // Within each class, the least recently processed subject gets the next turn.
  const select=`SELECT d.evaluation_id,d.kind,d.official_signal_id,d.cohort_id,d.payload_json AS entry_json,
   s.payload_json AS state_json,s.updated_at AS state_updated_at,p.status,p.closed_at
   FROM signal_measurement_state s JOIN signal_decision_evidence d ON d.evaluation_id=s.evaluation_id
   LEFT JOIN production_signals p ON p.signal_id=d.official_signal_id
   WHERE s.kind=? AND s.next_observe_at<=? ORDER BY s.next_observe_at,s.subject_id LIMIT ?`;
  const official=(await db.prepare(select).bind('OFFICIAL',asOf,maxSubjects).all()).results||[];
  const candidates=official.length<maxSubjects?(await db.prepare(select).bind('CANDIDATE',asOf,maxSubjects-official.length).all()).results||[]:[];
  const rows=[...official,...candidates];
  const writer=measurementWriter(db,{maxWrites});let updated=0;const blocks=new Map();
  const subjects=[];
  for(const row of rows){const entry=parseEvidence(row.entry_json),levels=entry.engine?.levels;if(!levels)continue;
   const createdAt=entry.createdAt??entry.evaluatedAt;
   const subject={id:row.official_signal_id||row.evaluation_id,createdAt,entry:levels.entry,tp1:levels.tp1,tp2:levels.tp2,sl:levels.sl,
    side:entry.direction,status:row.kind==='OFFICIAL'?row.status:'active',closedAt:row.closed_at??null,
    measurementHorizonAt:row.kind==='CANDIDATE'?createdAt+3600000:null};
   let previous=row.state_json?parseEvidence(row.state_json):{processedThrough:subject.createdAt,covered:[],gaps:[],barriers:{},global:{mfe:null,mae:null}};
   const checkpointRow=await db.prepare("SELECT payload_json FROM signal_outcome_evidence WHERE subject_id=? AND event_type='COVERAGE_CHECKPOINT' ORDER BY available_at DESC,event_id DESC LIMIT 1").bind(subject.id).first();
   const latest=checkpointRow?parseEvidence(checkpointRow.payload_json):null;
   if(latest&&latest.availableAt>=(previous?.outcome?.evidenceAsOf??subject.createdAt))previous={...previous,processedThrough:latest.occurredTo,covered:latest.covered,gaps:latest.gaps,quality:latest.quality,global:latest.global,outcome:latest.outcome};
   const existing=(await db.prepare("SELECT payload_json FROM signal_outcome_evidence WHERE subject_id=? AND event_type IN ('TP1','TP2','SL')").bind(subject.id).all()).results||[];
   for(const record of existing){const event=parseEvidence(record.payload_json);if(event.evidenceType==='BARRIER_OBSERVATION')previous.barriers[`${event.eventType}:${event.source}`]=event;}
   if(latest?.availableAt===asOf){await writer.state(subject.id,row.cohort_id,previous,asOf);updated++;continue;}
   const observationAsOf=Math.min(asOf,subject.measurementHorizonAt??asOf);
   const folded=foldPostEntry(subject,previous,{ticks,bars,asOf:observationAsOf});
   folded.state.measurementHorizonAt=subject.measurementHorizonAt;
   folded.state.productionLifecycleEvidence=row.kind==='OFFICIAL'?(row.status?'AVAILABLE':'UNAVAILABLE'):'NOT_APPLICABLE';
   if(row.kind==='OFFICIAL'&&!row.status){folded.state.nextObservationAt=asOf+86400000;folded.state.captureGap=true;}
   folded.state.measurementStatus=subject.measurementHorizonAt&&asOf>=subject.measurementHorizonAt?'HORIZON_ATTEMPT_COMPLETE; COVERAGE_MAY_BE_INSUFFICIENT':'COLLECTING';
   const selected=bars.filter(b=>b.t>=createdAt&&b.t+60000<=Math.min(observationAsOf,subject.closedAt??observationAsOf)&&b.t+60000>(previous?.processedThrough??createdAt));
   const built=await buildMarketManifest('1m',selected,asOf);for(const b of built.blocks)blocks.set(b.blockId,b);
   subjects.push({row,subject,folded,manifest:built.manifest});
  }
  for(const b of blocks.values())await writer.immutable('market',{block_id:b.blockId,timeframe:b.tf,from_at:b.from,to_at:b.to,recorded_at:asOf},parseEvidence(b.payload));
  for(const {row,subject,folded,manifest} of subjects){
   for(const event of folded.events)await writer.immutable('outcome',{event_id:event.eventId,subject_id:subject.id,event_type:event.eventType,
    occurred_at:event.occurredAt??null,available_at:asOf,recorded_at:asOf,block_ids_json:JSON.stringify(manifest.references.map(x=>x.blockId))},event);
   const checkpoint={subjectId:subject.id,eventId:`${subject.id}:coverage:${asOf}`,evidenceType:'COVERAGE_CHECKPOINT',eventType:'COVERAGE_CHECKPOINT',
    availableAt:asOf,observedAt:asOf,occurredFrom:subject.createdAt,occurredTo:folded.state.processedThrough,
    outcome:folded.state.outcome,covered:folded.state.covered,gaps:folded.state.gaps,quality:folded.state.quality,global:folded.state.global,
    marketManifest:manifest,orderingQuality:'UNKNOWN',measurementOnly:true,decisionUse:false};
   await writer.immutable('outcome',{event_id:checkpoint.eventId,subject_id:subject.id,event_type:checkpoint.eventType,
    available_at:asOf,recorded_at:asOf,block_ids_json:JSON.stringify(manifest.references.map(x=>x.blockId))},checkpoint);
   if(subject.closedAt&&subject.closedAt<=asOf){const lifecycle={subjectId:subject.id,eventId:`${subject.id}:lifecycle:${subject.status}`,evidenceType:'LIFECYCLE_EVENT',
    eventType:subject.status.toUpperCase(),occurredAt:subject.closedAt,availableAt:asOf,observedAt:asOf,orderingQuality:'UNKNOWN',measurementOnly:true,decisionUse:false};
    await writer.immutable('outcome',{event_id:lifecycle.eventId,subject_id:subject.id,event_type:lifecycle.eventType,
     occurred_at:lifecycle.occurredAt,available_at:asOf,recorded_at:asOf,block_ids_json:'[]'},lifecycle);
   }
   await writer.state(subject.id,row.cohort_id,folded.state,asOf);updated++;
  }
  return {ok:true,updated,subjects:rows.length,subjectBudget:maxSubjects,budgetLimitReached:rows.length===maxSubjects,
   selectedOldestLagMs:rows.length?Math.max(...rows.map(r=>asOf-(r.state_updated_at??r.evaluated_at))):0,
   unselectedCoverage:'NOT_ASSERTED; SUBJECT_BUDGET_MAY_LEAVE_GAPS',coverageBasis:'CANONICAL_INPUT_STREAM; BROKER_TICK_COMPLETENESS_UNKNOWN'};
 }catch(error){return {ok:false,error:String(error?.message||'measurement_collection_failed'),captureGap:true};}
}
