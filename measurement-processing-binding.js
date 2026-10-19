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
// Pending scan state is not completion authority. Mask exactly the market
// revision, retaining its pending/conflict fields and every other dependency.
export function processingProgressFence(db,c){const p=JSON.parse(c.wire);if(p[3])p[3].splice(0,1);return assertion(db,`json_remove((${contextSQL.replace(' AS context','')}),'$[3][0]')=?4`,c.evaluationId,c.signalId,c.cohort,serialize(p));}
export async function resolveProcessingSubject(db,scopeId,{now,archive=null,lookup=null}={}){
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
 const signalScope=await db.first('SELECT * FROM pa_scopes WHERE id=?',signalId);if(!signalScope)return pending('signal_unavailable');
 let creation=null;
 if(lookup){
  creation=await lookup.creations(signalId);
  if(creation){
   // Enumeration progress is historical proof, not perpetual byte availability.
   const witness=await readRecord(db,creation.id,{archive});need(witness&&witness.digest===creation.digest,'pa_processing_creation_unavailable');
   const e=await validateMeasurementWire(witness.wire,{receivedAt:now});
   need(e.kind==='OFFICIAL_CREATION'&&e.payload.officialSignalId===signalId&&e.payload.evaluationId===creation.evaluationId&&await hash(serialize(e.payload.decisionEvidence))===creation.decisionDigest,'pa_processing_creation_conflict');
  }
 }
 else{
 let creations=await db.all("SELECT id FROM pa_records WHERE scope_id=? AND ready=1 AND kind='OFFICIAL_CREATION' ORDER BY id LIMIT 257",signalId);
 if(signalScope.archive_root){need(archive?.scopeRecords,'pa_processing_archive_required');const cold=await archive.scopeRecords(signalId,{rootKey:signalScope.archive_root});creations=[...new Map([...creations,...cold.filter(r=>r.kind==='OFFICIAL_CREATION')].map(r=>[r.id,r])).values()];}
 need(creations.length<=LIMITS.scopeRecords,'pa_processing_creation_bound');if(!creations.length)return pending('creation_unavailable');
 for(const row of creations){const r=await readRecord(db,row.id,{archive});need(r,'pa_processing_creation_unavailable');const e=await validateMeasurementWire(r.wire,{receivedAt:now});
  need(e.kind==='OFFICIAL_CREATION'&&e.payload.officialSignalId===signalId,'pa_processing_creation_conflict');
  const ev=e.payload.evaluationId;need(!creation||creation.evaluationId===ev,'pa_processing_binding_conflict');
  need(!creation||await hash(serialize(creation.decision))===await hash(serialize(e.payload.decisionEvidence)),'pa_processing_creation_conflict');
  creation={id:r.id,digest:r.digest,evaluationId:ev,decision:e.payload.decisionEvidence};
 }
 }
 if(!creation)return pending('creation_unavailable');
 if(direct)need(creation.evaluationId===direct.evaluation_id,'pa_processing_binding_conflict');
 binding=rows.find(b=>b.evaluation_id===creation.evaluationId&&b.signal_id===signalId);if(!binding)return pending('binding_unavailable');
 record=await readRecord(db,'decision:'+binding.evaluation_id,{archive});if(!record)return pending('decision_unavailable');d=parseEvidence(record.wire);
 need(d.kind==='OFFICIAL'&&d.evaluationId===binding.evaluation_id&&d.officialSignalId===signalId&&record.digest===binding.digest&&(creation.decisionDigest??await hash(serialize(creation.decision)))===await hash(serialize(d))&&record.cohort===binding.cohort&&binding.cohort===scope.cohort&&signalScope.cohort===scope.cohort,'pa_processing_binding_conflict');
 // Resolution I/O must not silently cross an authority boundary.
 await db.batch([processingContextFence(db,context),clearAssertions(db)]);
 const exact=selectedBinding(await processingContext(db,binding.evaluation_id,signalId,scope.cohort),binding);
 await db.batch([processingContextFence(db,context),processingContextFence(db,exact),clearAssertions(db)]);
 return {status:'RESOLVED',role:scopeId===signalId?'OFFICIAL_SIGNAL':'OFFICIAL_EVALUATION',scope,binding,record,decision:d,subjectId:signalId,creation,context:exact};
}
export function processingAuthority(resolution,snapshot){return {version:'pa-processing-binding-v1',context:resolution.context,decisionId:resolution.record.id,decisionDigest:resolution.record.digest,creation:resolution.creation?{id:resolution.creation.id,digest:resolution.creation.digest,evaluationId:resolution.creation.evaluationId}:null,projectionDigest:snapshot.projectionDigest,projectionRevision:snapshot.projectionRevision};}
// Prefix reuse is NOT completion authority. Only the market revision may advance;
// every other ownership/lifecycle/dependency field remains an exact match.
export function scanProgressIdentity(authority){
 const a=structuredClone(authority),parts=JSON.parse(a.context.wire);
 if(parts[3])parts[3][0]=null;
 a.context.wire=serialize(parts);
 return {version:'pa-sample-selection-v2',authority:a,market:'measurement-market-v1',definition:'engine-v1',codecs:['pa-identity-wire-v1','pa-gzip-wire-v1'],ordering:'receipt-end-source-session-sequence-id-v1',reducer:'pa-sample-fold-v2'};
}
const scanBoundarySQL=`SELECT json_array(epoch,root_key,catalogue_generation,archive_reference_revision,deletion_generation,
 (SELECT json_group_array(json(v)) FROM (SELECT json_array(id,scope_id,predecessor,predecessor_digest,state,result_id,expected_revision) v FROM pa_corrections ORDER BY id LIMIT 257))) AS boundary,revision FROM pa_meta`;
export async function marketScanBoundary(db){
 const b=await db.first(scanBoundarySQL);need(b&&bytes(b.boundary)<=LIMITS.rowBytes&&JSON.parse(b.boundary)[5].length<=256,'pa_processing_scan_bound');return b;
}
const marketSourceStateSQL="NOT EXISTS(SELECT 1 FROM pa_scopes WHERE cohort=? AND kind='MARKET' AND (pending!=0 OR corrections!=0 OR quarantine!=0 OR conflict!=0 OR recovery_required!=0))";
export function marketSourceStateFence(db,cohort){return assertion(db,marketSourceStateSQL,cohort);}
export function marketScanFence(db,b,cohort){return assertion(db,`(${scanBoundarySQL.slice(0,scanBoundarySQL.indexOf(' AS boundary'))+' FROM pa_meta'})=? AND ${marketSourceStateSQL}`,b.boundary,cohort);}
// Ready record metadata/chunks are immutable. Admission revisions, not wall-clock
// comparisons, detect even same-millisecond backfills. A deletion or archive change
// invalidates the boundary; corrections (including completed ones) do so as well.
export async function reusableMarketPrefix(db,previousRow,previous,identity,boundary,{now,archive=null}={}){
 const s=previous?.scanProgress;
 if(!s||s.version!=='pa-market-prefix-v1'||serialize(s.identity)!==serialize(identity)||s.boundary!==boundary.boundary)return false;
 need(s.cursor===previous.cursor&&s.foldDigest===await hash(previous.folded.state)&&Number.isSafeInteger(s.admissionRevision)&&s.admissionRevision>=0&&s.admissionRevision<=boundary.revision&&Number.isSafeInteger(s.records)&&s.records>=0,'pa_processing_scan_integrity');
 const receiptId='result:'+await hash([previous.subjectId,previousRow.revision,previous]);
 const receipt=await db.first('SELECT digest FROM pa_records WHERE id=? AND ready=1',receiptId);
 if(!receipt)return false;need(receipt.digest===await hash(previousRow.payload),'pa_processing_scan_integrity');
 if(!s.cursor)return true;
 const changed=await db.first("SELECT r.id FROM pa_records r LEFT JOIN pa_scopes s ON s.id=r.scope_id WHERE r.kind='MARKET_EXPORT' AND r.cohort=? AND r.ready=1 AND r.available_at<=? AND printf('%016x',r.created_at)||substr(r.order_key,1,48)<=? AND (r.revision>? OR r.available_at>? OR s.pending!=0 OR s.corrections!=0 OR s.quarantine!=0 OR s.conflict!=0 OR s.recovery_required!=0) LIMIT 1",identity.authority.context.cohort,now,s.cursor,s.admissionRevision,previous.preparedAt);
 if(changed)return false;
 const last=await readRecord(db,s.last.id,{archive});need(last&&last.digest===s.last.digest&&last.cohort===identity.authority.context.cohort&&last.kind==='MARKET_EXPORT'&&last.definition==='engine-v1'&&last.created_at.toString(16).padStart(16,'0')+last.order_key.slice(0,48)===s.cursor,'pa_processing_scan_integrity');
 return true;
}
export async function marketPrefixCheckpoint(previous,samples,{identity,boundary,cursor,folded,reused}){
 const old=reused?previous.scanProgress:null;
 const units=samples.map(r=>[r.id,r.digest,r.marketOrder,r.created_at,r.available_at,r.definition,r.codec]);
 return {version:'pa-market-prefix-v1',identity,boundary:boundary.boundary,admissionRevision:boundary.revision,cursor,records:(old?.records??0)+samples.length,prefixDigest:await hash([old?.prefixDigest??null,units]),foldDigest:await hash(folded.state),last:samples.length?{id:samples.at(-1).id,digest:samples.at(-1).digest}:old?.last??null};
}
// A returned success is never authority. Re-read and fence the exact durable checkpoint.
export async function completeEvaluationObligation(db,resolution,lease,{now,hook=null}={}){
 need(resolution.role==='OFFICIAL_EVALUATION'&&lease.id==='projection:'+resolution.binding.evaluation_id&&resolution.subjectId!==resolution.binding.evaluation_id,'pa_processing_lease_scope');
 const row=await db.first('SELECT * FROM pa_processing WHERE scope_id=?',resolution.subjectId);if(!row)return pending('canonical_processing_pending');
 const p=parseEvidence(row.payload),a=p.processingAuthority;if(p.version!=='measurement-result-samples-v2'||p.scanProgress?.version!=='pa-sample-selection-v2'||p.subjectId!==resolution.subjectId||p.evaluationId!==resolution.binding.evaluation_id||p.decisionIdentity!==resolution.record.id||!p.completeForCapturedSnapshot||!a||a.version!=='pa-processing-binding-v1'||a.context.wire!==resolution.context.wire||a.decisionDigest!==resolution.record.digest||a.creation?.id!==resolution.creation.id||a.creation?.digest!==resolution.creation.digest)return pending('canonical_processing_stale');
 const resultId='result:'+await hash([resolution.subjectId,row.revision,p]),result=await db.first('SELECT id,digest FROM pa_records WHERE id=? AND ready=1',resultId);if(!result)return pending('canonical_result_unavailable');
 need(result.digest===await hash(row.payload),'pa_processing_result_conflict');
 await hook?.('EVALUATION_COMPLETION_PREPARED',{resolution,resultId});
 await db.batch([jobFence(db,lease,now),processingContextFence(db,resolution.context),...(p.scanProgress?[marketSourceStateFence(db,resolution.context.cohort)]:[]),
  assertion(db,"EXISTS(SELECT 1 FROM pa_scopes e,pa_scopes s,pa_projections p,pa_processing w,pa_records r,pa_meta m WHERE e.id=? AND s.id=? AND e.pending=0 AND e.corrections=0 AND e.conflict=0 AND e.quarantine=0 AND e.recovery_required=0 AND s.pending=0 AND s.corrections=0 AND s.conflict=0 AND s.quarantine=0 AND s.recovery_required=0 AND p.scope_id=s.id AND p.source_revision=s.revision AND p.epoch=m.epoch AND p.integrity='COMPLETE' AND p.digest=? AND p.available_at<=MAX(?,"+SQL_CLOCK+") AND w.scope_id=s.id AND w.revision=s.revision AND w.market_revision=COALESCE((SELECT revision FROM pa_cohort_state WHERE cohort=s.cohort),0) AND w.payload=? AND r.id=? AND r.ready=1 AND r.digest=? AND r.available_at<=MAX(?,"+SQL_CLOCK+") AND NOT EXISTS(SELECT 1 FROM pa_jobs j WHERE j.id='projection:'||s.id AND j.state IN('PENDING','RUNNING')))",resolution.binding.evaluation_id,resolution.subjectId,a.projectionDigest,now,row.payload,resultId,result.digest,now),
  db.statement("UPDATE pa_jobs SET state='DONE',owner=NULL,lease_until=0 WHERE id=?",[lease.id]),clearAssertions(db)]);
 return {status:'COMPLETED_AT_REVISION',subjectId:resolution.subjectId,id:resultId,jobFinished:true,delegated:true};
}
// A legacy result stays historical evidence. Only its processing obligation is
// requeued; no old cursor/fold is credited to the sample-selection algorithm.
export async function requeueLegacyCanonicalProcessing(db,resolution,lease,{now}={}){
 need(resolution.role==='OFFICIAL_EVALUATION'&&lease.id==='projection:'+resolution.binding.evaluation_id,'pa_processing_lease_scope');
 const row=await db.first('SELECT payload FROM pa_processing WHERE scope_id=?',resolution.subjectId);if(!row||parseEvidence(row.payload).version!=='measurement-result-v1')return;
 await db.batch([jobFence(db,lease,now),processingContextFence(db,resolution.context),assertion(db,'EXISTS(SELECT 1 FROM pa_processing WHERE scope_id=? AND payload=?)',resolution.subjectId,row.payload),
  db.statement("UPDATE pa_jobs SET state='PENDING',next_at=MIN(next_at,?) WHERE id=? AND class='processing' AND scope_id=? AND state='DONE' AND owner IS NULL AND lease_until=0",[now,'projection:'+resolution.subjectId,resolution.subjectId]),clearAssertions(db)]);
}
