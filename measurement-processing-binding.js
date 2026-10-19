// Canonical processing ownership. Evidence stays in its original scope.
import {need,hash,serialize,parseEvidence,LIMITS,bytes} from './measurement-independent-contract.js';
import {assertion,jobFence,clearAssertions,SQL_CLOCK} from './measurement-d1-adapter.js';
import {readRecord} from './measurement-independent-store.js';
import {validateMeasurementWire} from './measurement-consumer.js';
const pending=reason=>({status:'PENDING',dependencyState:'DEPENDENCY_PENDING',reason});
function selectedBinding(context,b){
 const identity=[b.evaluation_id,b.digest,b.signal_id,b.first_event,b.cohort,b.cycle_key];
 const parts=JSON.parse(context.wire);
 for(const id of [b.evaluation_id,b.signal_id].filter(Boolean))need(parts[2].some(row=>row[0]===id&&row[1]===b.cohort),'pa_processing_binding_conflict');
 need(parts[1].some(row=>serialize(row)===serialize(identity)),'pa_processing_binding_conflict');
 return context;
}
// One SQL snapshot binds membership AND absence. RESULT writes do not change source
// revisions. archive_revision includes our own RESULT and is deliberately not a source
// fence; non-RESULT identities, scope archive pointers and source revisions are fenced.
// Unrelated global catalogue appends do not change this processing obligation.
const contextSQL=`SELECT json_array(m.epoch,
 (SELECT json_group_array(json(v)) FROM (SELECT json_array(b.evaluation_id,b.digest,b.signal_id,b.first_event,b.cohort,b.cycle_key) v FROM pa_bindings b WHERE b.evaluation_id=?1 OR b.signal_id=?2 ORDER BY b.evaluation_id LIMIT 81)),
 (SELECT json_group_array(json(v)) FROM (SELECT json_array(s.id,s.cohort,s.kind,s.revision,s.archive_root,s.pending,s.corrections,s.quarantine,s.conflict,s.recovery_required,s.available_at,s.first_available_at,s.active) v FROM pa_scopes s WHERE s.id=?1 OR s.id=?2 OR s.id IN(SELECT evaluation_id FROM pa_bindings WHERE signal_id=?2) ORDER BY s.id LIMIT 82)),
 (SELECT json_array(revision,pending,conflict) FROM pa_cohort_state WHERE cohort=?3),
 EXISTS(SELECT 1 FROM pa_restores WHERE state='RUNNING'),
 (SELECT json_group_array(json(v)) FROM (SELECT json_array(id,digest,ready,location,available_at) v FROM pa_records WHERE id='decision:'||?1 OR (scope_id=?2 AND kind='OFFICIAL_CREATION') ORDER BY id LIMIT 258))) AS context FROM pa_meta m`;
export async function processingContext(db,evaluationId,signalId,cohort){
 const row=await db.first(contextSQL,evaluationId,signalId,cohort);need(row&&bytes(row.context)<=LIMITS.rowBytes,'pa_processing_binding_bound');
 const parts=JSON.parse(row.context);need(parts[1].length<=80&&parts[2].length<=81,'pa_processing_binding_bound');
 need(parts[5].length<=257,'pa_processing_creation_bound');
 need(!parts[4],'pa_processing_restore_pending');
 return {evaluationId,signalId,cohort,wire:row.context};
}
export function processingContextFence(db,c){return assertion(db,`(${contextSQL.replace(' AS context','')})=?4`,c.evaluationId,c.signalId,c.cohort,c.wire);}
export async function resolveProcessingSubject(db,scopeId,{now,archive=null}={}){
 const scope=await db.first('SELECT * FROM pa_scopes WHERE id=?',scopeId);if(!scope)return pending('missing_scope');
 const rows=await db.all('SELECT * FROM pa_bindings WHERE evaluation_id=? OR signal_id=? ORDER BY evaluation_id LIMIT 81',scopeId,scopeId);need(rows.length<=80,'pa_processing_binding_bound');
 if(!rows.length)return scope.kind==='OFFICIAL'?pending('binding_unavailable'):{status:'NOT_APPLICABLE',scope};
 const direct=rows.find(b=>b.evaluation_id===scopeId);let binding=direct,record,d;
 if(direct){record=await readRecord(db,'decision:'+direct.evaluation_id,{archive});if(!record)return pending('decision_unavailable');d=parseEvidence(record.wire);}
 if(d?.kind==='CANDIDATE'){
  need(d.evaluationId===binding.evaluation_id&&!binding.signal_id&&record.digest===binding.digest&&record.cohort===binding.cohort,'pa_processing_binding_conflict');
  return {status:'RESOLVED',role:'CANDIDATE',scope,binding,record,decision:d,subjectId:scopeId,context:selectedBinding(await processingContext(db,scopeId,null,scope.cohort),binding)};
 }
 const signalId=direct?direct.signal_id:scopeId;if(!signalId)return pending('binding_unavailable');
 const context=await processingContext(db,direct?.evaluation_id??'',signalId,scope.cohort);
 let creations=await db.all("SELECT id FROM pa_records WHERE scope_id=? AND ready=1 AND kind='OFFICIAL_CREATION' ORDER BY id LIMIT 257",signalId);
 const signalScope=await db.first('SELECT * FROM pa_scopes WHERE id=?',signalId);if(!signalScope)return pending('signal_unavailable');
 if(signalScope.archive_root){need(archive?.scopeRecords,'pa_processing_archive_required');const cold=await archive.scopeRecords(signalId,{rootKey:signalScope.archive_root});creations=[...new Map([...creations,...cold.filter(r=>r.kind==='OFFICIAL_CREATION')].map(r=>[r.id,r])).values()];}
 need(creations.length<=LIMITS.scopeRecords,'pa_processing_creation_bound');if(!creations.length)return pending('creation_unavailable');
 let creation=null;
 for(const row of creations){const r=await readRecord(db,row.id,{archive});need(r,'pa_processing_creation_unavailable');const e=await validateMeasurementWire(r.wire,{receivedAt:now});
  need(e.kind==='OFFICIAL_CREATION'&&e.payload.officialSignalId===signalId,'pa_processing_creation_conflict');
  const ev=e.payload.evaluationId;need(!creation||creation.evaluationId===ev,'pa_processing_binding_conflict');
  need(!creation||await hash(serialize(creation.decision))===await hash(serialize(e.payload.decisionEvidence)),'pa_processing_creation_conflict');
  creation={id:r.id,digest:r.digest,evaluationId:ev,decision:e.payload.decisionEvidence};
 }
 if(direct)need(creation.evaluationId===direct.evaluation_id,'pa_processing_binding_conflict');
 binding=rows.find(b=>b.evaluation_id===creation.evaluationId&&b.signal_id===signalId);if(!binding)return pending('binding_unavailable');
 record=await readRecord(db,'decision:'+binding.evaluation_id,{archive});if(!record)return pending('decision_unavailable');d=parseEvidence(record.wire);
 need(d.kind==='OFFICIAL'&&d.evaluationId===binding.evaluation_id&&d.officialSignalId===signalId&&record.digest===binding.digest&&await hash(serialize(creation.decision))===await hash(serialize(d))&&record.cohort===binding.cohort&&binding.cohort===scope.cohort&&signalScope.cohort===scope.cohort,'pa_processing_binding_conflict');
 // Resolution I/O must not silently cross an authority boundary.
 await db.batch([processingContextFence(db,context),clearAssertions(db)]);
 const exact=selectedBinding(await processingContext(db,binding.evaluation_id,signalId,scope.cohort),binding);
 await db.batch([processingContextFence(db,context),processingContextFence(db,exact),clearAssertions(db)]);
 return {status:'RESOLVED',role:scopeId===signalId?'OFFICIAL_SIGNAL':'OFFICIAL_EVALUATION',scope,binding,record,decision:d,subjectId:signalId,creation,context:exact};
}
export function processingAuthority(resolution,snapshot){return {version:'pa-processing-binding-v1',context:resolution.context,decisionId:resolution.record.id,decisionDigest:resolution.record.digest,creation:resolution.creation?{id:resolution.creation.id,digest:resolution.creation.digest,evaluationId:resolution.creation.evaluationId}:null,projectionDigest:snapshot.projectionDigest,projectionRevision:snapshot.projectionRevision};}
// A returned success is never authority. Re-read and fence the exact durable checkpoint.
export async function completeEvaluationObligation(db,resolution,lease,{now,hook=null}={}){
 need(resolution.role==='OFFICIAL_EVALUATION'&&lease.id==='projection:'+resolution.binding.evaluation_id&&resolution.subjectId!==resolution.binding.evaluation_id,'pa_processing_lease_scope');
 const row=await db.first('SELECT * FROM pa_processing WHERE scope_id=?',resolution.subjectId);if(!row)return pending('canonical_processing_pending');
 const p=parseEvidence(row.payload),a=p.processingAuthority;if(p.subjectId!==resolution.subjectId||p.evaluationId!==resolution.binding.evaluation_id||p.decisionIdentity!==resolution.record.id||!p.completeForCapturedSnapshot||!a||a.version!=='pa-processing-binding-v1'||a.context.wire!==resolution.context.wire||a.decisionDigest!==resolution.record.digest||a.creation?.id!==resolution.creation.id||a.creation?.digest!==resolution.creation.digest)return pending('canonical_processing_stale');
 const resultId='result:'+await hash([resolution.subjectId,row.revision,p]),result=await db.first('SELECT id,digest FROM pa_records WHERE id=? AND ready=1',resultId);if(!result)return pending('canonical_result_unavailable');
 need(result.digest===await hash(row.payload),'pa_processing_result_conflict');
 await hook?.('EVALUATION_COMPLETION_PREPARED',{resolution,resultId});
 await db.batch([jobFence(db,lease,now),processingContextFence(db,resolution.context),
  assertion(db,"EXISTS(SELECT 1 FROM pa_scopes e,pa_scopes s,pa_projections p,pa_processing w,pa_records r,pa_meta m WHERE e.id=? AND s.id=? AND e.pending=0 AND e.corrections=0 AND e.conflict=0 AND e.quarantine=0 AND e.recovery_required=0 AND s.pending=0 AND s.corrections=0 AND s.conflict=0 AND s.quarantine=0 AND s.recovery_required=0 AND p.scope_id=s.id AND p.source_revision=s.revision AND p.epoch=m.epoch AND p.integrity='COMPLETE' AND p.digest=? AND p.available_at<=MAX(?,"+SQL_CLOCK+") AND w.scope_id=s.id AND w.revision=s.revision AND w.market_revision=COALESCE((SELECT revision FROM pa_cohort_state WHERE cohort=s.cohort),0) AND w.payload=? AND r.id=? AND r.ready=1 AND r.digest=? AND r.available_at<=MAX(?,"+SQL_CLOCK+") AND NOT EXISTS(SELECT 1 FROM pa_jobs j WHERE j.id='projection:'||s.id AND j.state IN('PENDING','RUNNING')))",resolution.binding.evaluation_id,resolution.subjectId,a.projectionDigest,now,row.payload,resultId,result.digest,now),
  db.statement("UPDATE pa_jobs SET state='DONE',owner=NULL,lease_until=0 WHERE id=?",[lease.id]),clearAssertions(db)]);
 return {status:'COMPLETED_AT_REVISION',subjectId:resolution.subjectId,id:resultId,jobFinished:true,delegated:true};
}
