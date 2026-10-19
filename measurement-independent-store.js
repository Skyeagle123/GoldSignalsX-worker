import {validateDecodedMarket} from './measurement-processing-budget.js';
import {canonicalSerialize,parseEvidence,serialize,hash,bytes,need,time,identity,LIMITS,DAY,RESPONSE_VERSION,uid} from './measurement-independent-contract.js';
import {assertion,clearAssertions,requireD1,schemaReady,scopeFence,SQL_CLOCK,jobFence} from './measurement-d1-adapter.js';
import {reserve,estimateWrite,commitReservationStatements,releaseReservation,settleReservationStatements} from './measurement-reservations.js';
import {validateMeasurementWire} from './measurement-consumer.js';
import {projectLifecycle} from './measurement-lifecycle.js';
import {encodeWire,encodedChunks,decodeWire} from './measurement-wire-codec.js';
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true});
export const decisionRecords=e=>e.kind==='DECISION_CYCLE'?e.payload.records.filter(r=>r.type==='decision').map(r=>r.payload):['EVALUATION_CENSUS','OFFICIAL_CREATION'].includes(e.kind)?[e.payload.decisionEvidence]:[];
const signal=e=>[e.payload.signalId,e.payload.officialSignalId,e.payload.primarySignalId,e.payload.decisionEvidence?.officialSignalId].find(v=>typeof v==='string'&&v.length>0)??null;
const clock=v=>Number.isSafeInteger(v)&&v>=0?v:null;
const base64=b=>btoa(String.fromCharCode(...b));
export function chunkWire(wire){need(bytes(wire)<=LIMITS.eventBytes,'pa_wire_bound');const data=encoder.encode(wire),out=[];for(let i=0;i<data.length;i+=LIMITS.chunkBytes)out.push(base64(data.subarray(i,i+LIMITS.chunkBytes)));return out;}
export async function readRecord(db,id,{archive=null}={}){
 const r=await db.first('SELECT * FROM pa_records WHERE id=? AND ready=1',id);if(!r)return archive?.lookupRecord?archive.lookupRecord(id):null;
 if(r.location==='ARCHIVED'){need(archive,'pa_archive_unavailable');return archive.readRecord(r.archive_key,id);}
 const chunks=await db.all('SELECT ordinal,data FROM pa_chunks WHERE record_id=? ORDER BY ordinal LIMIT 51',id);
 need(chunks.length===r.chunk_count&&chunks.every((c,i)=>c.ordinal===i),'pa_chunks_missing');
 db.processingBudget?.reserve({records:1,market:1,decodedBytes:r.bytes});db.processingBudget?.record(r);
 const parts=chunks.map(c=>Uint8Array.from(atob(c.data),x=>x.charCodeAt(0))),wire=await decodeWire(parts,r);const dependencies=(await db.all('SELECT dependency FROM pa_dependencies WHERE owner_id=? ORDER BY dependency LIMIT 129',id)).map(x=>x.dependency);need(dependencies.length<=LIMITS.dependencies,'pa_dependencies_bound');return db.processingBudget?validateDecodedMarket({...r,wire,dependencies},db.processingBudget):{...r,wire,dependencies};
}
// Invisible staging records are guarded by durable ingress until commit.
export async function stageRecord(db,record,{guard=[]}={}){
 const wire=record.wire??serialize(record.payload,LIMITS.eventBytes),digest=await hash(wire),encoded=await encodeWire(wire),chunks=encodedChunks(encoded.data);
 const existing=await db.first('SELECT * FROM pa_records WHERE id=?',record.id);if(existing?.ready===1){need(existing.digest===digest&&existing.semantic_key===(record.semanticKey??record.id)&&existing.kind===record.kind&&existing.scope_id===(record.scopeId??null)&&existing.cohort===record.cohort&&existing.created_at===record.createdAt&&existing.definition===(record.definition??'engine-v1'),'pa_record_conflict');return {id:record.id,digest,chunks:existing.chunk_count};}
 need(Array.isArray(record.dependencies??[])&&(record.dependencies??[]).length<=LIMITS.dependencies,'pa_dependencies_bound');
 let retainedUntil=record.retention;if(retainedUntil==null){if(['CENSUS','DEFINITION'].includes(record.category))retainedUntil=Number.MAX_SAFE_INTEGER;else{const scope=record.scopeId?await db.first('SELECT kind,active,hold_until,corrections,pending,original_at FROM pa_scopes WHERE id=?',record.scopeId):null;retainedUntil=scope?.kind==='OFFICIAL'?scope.active||scope.corrections||scope.pending?Number.MAX_SAFE_INTEGER:Math.max(scope.original_at+455*DAY,scope.hold_until):record.createdAt+120*DAY;}}
 const metadata=[record.id,record.semanticKey??record.id,record.kind,record.scopeId??null,record.cohort,digest,bytes(wire),time(record.createdAt),record.availableAt??0,0,record.definition??'engine-v1',0,chunks.length,retainedUntil,record.category??'DETAIL',await hash(record.id),encoded.codec,encoded.data.length];
 await db.batch([...guard,assertion(db,'NOT EXISTS(SELECT 1 FROM pa_records WHERE id=? OR semantic_key=?) OR EXISTS(SELECT 1 FROM pa_records WHERE id=? AND semantic_key=? AND digest=?)',record.id,record.semanticKey??record.id,record.id,record.semanticKey??record.id,digest),
  assertion(db,'NOT EXISTS(SELECT 1 FROM pa_records WHERE id=?) OR EXISTS(SELECT 1 FROM pa_records WHERE id=? AND kind=? AND scope_id IS ? AND cohort=? AND created_at=? AND definition=?)',record.id,record.id,record.kind,record.scopeId??null,record.cohort,record.createdAt,record.definition??'engine-v1'),
  db.statement('INSERT INTO pa_records(id,semantic_key,kind,scope_id,cohort,digest,bytes,created_at,available_at,revision,definition,ready,chunk_count,retention,category,order_key,codec,encoded_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING',metadata),clearAssertions(db)]);
 for(let start=0;start<chunks.length;start+=16){const statements=[...guard];for(let i=start;i<Math.min(chunks.length,start+16);i++)statements.push(assertion(db,'NOT EXISTS(SELECT 1 FROM pa_chunks WHERE record_id=? AND ordinal=?) OR EXISTS(SELECT 1 FROM pa_chunks WHERE record_id=? AND ordinal=? AND data=?)',record.id,i,record.id,i,chunks[i]),db.statement('INSERT INTO pa_chunks VALUES(?,?,?) ON CONFLICT DO NOTHING',[record.id,i,chunks[i]]));statements.push(clearAssertions(db));await db.batch(statements);}
 for(const dep of record.dependencies??[])await db.batch([...guard,db.statement('INSERT INTO pa_dependencies VALUES(?,?) ON CONFLICT DO NOTHING',[record.id,identity(dep)]),clearAssertions(db)]);
 return {id:record.id,digest,chunks:chunks.length};
}
export function finishRecord(db,id,now){return [assertion(db,'EXISTS(SELECT 1 FROM pa_records r WHERE r.id=? AND (r.ready=1 OR r.chunk_count=(SELECT COUNT(*) FROM pa_chunks WHERE record_id=r.id)))',id),db.statement(`UPDATE pa_records SET ready=1,available_at=MAX(?,${SQL_CLOCK}),revision=(SELECT revision+1 FROM pa_meta) WHERE id=? AND ready=0`,[time(now),id])];}
export async function appendIndependentRecord(db,record,{now,workClass='ingestion',owner=record.id}={}){
 const old=await db.first('SELECT id,digest,ready FROM pa_records WHERE id=?',record.id),wire=record.wire??serialize(record.payload,LIMITS.eventBytes);
 if(old?.ready){need(old.digest===await hash(wire),'pa_record_conflict');return old;}
 const reservation=await reserve(db,{id:'record:'+record.id,workClass,owner,bytes:estimateWrite(bytes(wire),chunkWire(wire).length+3),now});
 if(reservation.state==='RESERVED')await db.batch([...commitReservationStatements(db,reservation,now),clearAssertions(db)]);
 if(!record.scopeId){record={...record,scopeId:'record-scope:'+record.id};await db.batch([db.statement('INSERT INTO pa_scopes(id,cohort,kind,original_at,revision) VALUES(?,?,?,?,(SELECT revision+1 FROM pa_meta)) ON CONFLICT DO NOTHING',[record.scopeId,record.cohort,record.kind,record.createdAt])]);}
 await stageRecord(db,{...record,wire});await db.batch([...finishRecord(db,record.id,now),...settleReservationStatements(db,reservation),clearAssertions(db)]);if(['INCIDENT','COVERAGE','OPERATIONAL_AUDIT'].includes(record.kind))await db.batch([db.statement("INSERT INTO pa_jobs(id,class,scope_id,next_at,deadline) VALUES(?,'archive',?,?,?) ON CONFLICT DO NOTHING",['archive-scope:'+record.scopeId,record.scopeId,now,now+DAY])]);return await db.first('SELECT * FROM pa_records WHERE id=?',record.id);
}
async function scopeRows(db,e){const decisions=decisionRecords(e),list=[],cohorts=new Map();
 for(const d of decisions){let cohort=e.kind==='DECISION_CYCLE'?e.payload.records.find(r=>r.type==='decision'&&r.payload.evaluationId===d.evaluationId)?.values?.cohort_id:e.payload.values?.cohort_id;
  if(!cohort)cohort=(await db.first('SELECT cohort FROM pa_bindings WHERE evaluation_id=?',d.evaluationId))?.cohort;
  if(!cohort&&d.sharedContextRef)cohort=(await db.first("SELECT cohort FROM pa_records WHERE semantic_key=? AND ready=1",canonicalSerialize(['goldsignalsx:b1','DECISION_CYCLE',d.sharedContextRef])))?.cohort;
  cohort=typeof cohort==='string'?cohort:'UNRESOLVED';cohorts.set(d.evaluationId,cohort);list.push({id:d.evaluationId,cohort,kind:d.kind,at:d.evaluatedAt});if(d.officialSignalId)list.push({id:d.officialSignalId,cohort,kind:'OFFICIAL',at:d.createdAt??d.evaluatedAt});
 }
 const s=signal(e);if(s&&!list.some(d=>d.id===s)){const known=await db.first('SELECT cohort FROM pa_scopes WHERE id=?',s);list.push({id:s,cohort:known?.cohort??'UNRESOLVED',kind:'OFFICIAL',at:clock(e.payload.createdAt)??clock(e.clocks.occurredAt)??0});}
 return {scopes:[...new Map(list.map(s=>[s.id,s])).values()],cohorts};
}
export async function registerIngress(db,e,now){
 const {scopes,cohorts}=await scopeRows(db,e),bindings=[];for(const d of decisionRecords(e))bindings.push({id:d.evaluationId,digest:await hash(canonicalSerialize(d,LIMITS.eventBytes)),signal:d.officialSignalId??null,cohort:cohorts.get(d.evaluationId),cycleKey:e.kind==='DECISION_CYCLE'?e.semanticKey:canonicalSerialize(['goldsignalsx:b1','DECISION_CYCLE',e.payload.values?.cycle_id??d.sharedContextRef])});
 need(scopes.length<=80&&bytes(serialize(scopes))<=LIMITS.rowBytes,'pa_scope_bound');
 await db.batch([assertion(db,`(SELECT COUNT(*) FROM pa_scopes)+(SELECT COUNT(*) FROM json_each(?) x WHERE NOT EXISTS(SELECT 1 FROM pa_scopes WHERE id=json_extract(x.value,'$.id')))<=${LIMITS.activeScopes}`,serialize(scopes)),
  db.statement("INSERT INTO pa_scopes(id,cohort,kind,original_at,revision) SELECT json_extract(value,'$.id'),json_extract(value,'$.cohort'),json_extract(value,'$.kind'),json_extract(value,'$.at'),(SELECT revision+1 FROM pa_meta) FROM json_each(?) WHERE true ON CONFLICT DO NOTHING",[serialize(scopes)]),
  db.statement("UPDATE pa_scopes SET cohort=(SELECT json_extract(value,'$.cohort') FROM json_each(?) WHERE json_extract(value,'$.id')=pa_scopes.id),revision=revision+1 WHERE cohort='UNRESOLVED' AND id IN(SELECT json_extract(value,'$.id') FROM json_each(?) WHERE json_extract(value,'$.cohort')!='UNRESOLVED')",[serialize(scopes),serialize(scopes)]),
  assertion(db,`EXISTS(SELECT 1 FROM pa_events WHERE id=? AND state IN('STAGED','PENDING') AND expires_at>MAX(?,${SQL_CLOCK}))`,e.eventId,now),
  db.statement("INSERT INTO pa_ingress VALUES(?,'PENDING',?) ON CONFLICT DO NOTHING",[e.eventId,now]),
  db.statement("INSERT INTO pa_semantic_ingress VALUES(?,?,'PENDING') ON CONFLICT DO NOTHING",[e.eventId,e.semanticKey]),
  db.statement("INSERT INTO pa_semantic_affected SELECT ?,id FROM pa_scopes WHERE id IN(SELECT evaluation_id FROM pa_bindings WHERE cycle_key=? UNION SELECT signal_id FROM pa_bindings WHERE cycle_key=?) AND (SELECT state FROM pa_semantic_ingress WHERE event_id=?)='PENDING' ON CONFLICT DO NOTHING",[e.eventId,e.semanticKey,e.semanticKey,e.eventId]),
  db.statement("INSERT INTO pa_affected SELECT ?,json_extract(value,'$.id') FROM json_each(?) WHERE (SELECT state FROM pa_ingress WHERE event_id=?)='PENDING' ON CONFLICT DO NOTHING",[e.eventId,serialize(scopes),e.eventId]),
  db.statement("UPDATE pa_events SET semantic_key=?,state='PENDING' WHERE id=? AND state IN('STAGED','PENDING')",[e.semanticKey,e.eventId]),clearAssertions(db)]);
 return {scopes,bindings,unresolvedCohort:scopes.some(s=>s.cohort==='UNRESOLVED')};
}
export async function reconcileBindings(db,e,{bindings},now){
 const manifest=serialize(bindings),semanticConflict="EXISTS(SELECT 1 FROM pa_events WHERE semantic_key=? AND id!=? AND state='COMMITTED')";
 const bindingConflict="EXISTS(SELECT 1 FROM json_each(?) x JOIN pa_bindings b ON b.evaluation_id=json_extract(x.value,'$.id') WHERE b.digest!=json_extract(x.value,'$.digest'))";
 await db.batch([
  db.statement(`UPDATE pa_scopes SET conflict=1,quarantine=1 WHERE id IN(SELECT b.evaluation_id FROM json_each(?) x JOIN pa_bindings b ON b.evaluation_id=json_extract(x.value,'$.id') WHERE b.digest!=json_extract(x.value,'$.digest') UNION SELECT b.signal_id FROM json_each(?) x JOIN pa_bindings b ON b.evaluation_id=json_extract(x.value,'$.id') WHERE b.digest!=json_extract(x.value,'$.digest'))`,[manifest,manifest]),
  db.statement(`UPDATE pa_scopes SET conflict=1,quarantine=1 WHERE id IN(SELECT scope_id FROM pa_affected WHERE event_id=? UNION SELECT scope_id FROM pa_semantic_affected WHERE event_id=?) AND (${semanticConflict})`,[e.eventId,e.eventId,e.semanticKey,e.eventId]),
  db.statement(`UPDATE pa_ingress SET state='CONFLICT' WHERE event_id=? AND state='PENDING' AND ((${bindingConflict}) OR (${semanticConflict}))`,[e.eventId,manifest,e.semanticKey,e.eventId]),
  db.statement("UPDATE pa_events SET state='CONFLICT',reason='canonical_conflict',classification='INTEGRITY_CONFLICT' WHERE id=? AND (SELECT state FROM pa_ingress WHERE event_id=?)='CONFLICT'",[e.eventId,e.eventId]),
  db.statement("UPDATE pa_semantic_ingress SET state='CONFLICT' WHERE event_id=? AND (SELECT state FROM pa_ingress WHERE event_id=?)='CONFLICT'",[e.eventId,e.eventId]),
  db.statement("INSERT INTO pa_bindings SELECT json_extract(value,'$.id'),json_extract(value,'$.digest'),json_extract(value,'$.signal'),?,json_extract(value,'$.cohort'),json_extract(value,'$.cycleKey') FROM json_each(?) WHERE (SELECT state FROM pa_ingress WHERE event_id=?)='PENDING' ON CONFLICT DO NOTHING",[e.eventId,manifest,e.eventId])]);
 return (await db.first('SELECT state FROM pa_ingress WHERE event_id=?',e.eventId)).state;
}
export function ingressFence(db,eventId){return assertion(db,`EXISTS(SELECT 1 FROM pa_ingress WHERE event_id=? AND state='PENDING') AND EXISTS(SELECT 1 FROM pa_events WHERE id=? AND state='PENDING' AND expires_at>${SQL_CLOCK})`,eventId,eventId);}
export async function commitEnvelope(db,wire,{now,archive=null,hook=null,lease=null}={}){
 requireD1(db);time(now);const e=await validateMeasurementWire(wire,{receivedAt:now});
 const event=await db.first('SELECT * FROM pa_events WHERE id=?',e.eventId);need(event,'pa_transport_registration_required');
 if(event.state==='COMMITTED')return d1Result(db,e,'DUPLICATE',true,{hook});
 const registration=await registerIngress(db,e,now);await hook?.('REGISTERED',e);
 if(archive?.lookupRecord){
  // Historical durability is not continuing workflow authority. Capture the exact
  // durable registration and repeat its predicates atomically after archive I/O.
  const authority=await db.first('SELECT e.replays,e.expires_at,m.epoch,j.owner,j.generation,j.lease_nonce,j.state AS job_state,j.lease_until FROM pa_events e CROSS JOIN pa_meta m LEFT JOIN pa_jobs j ON j.id=? WHERE e.id=?','event:'+e.eventId,e.eventId);
  const revisions=await db.all('SELECT s.id,s.revision FROM pa_scopes s WHERE id IN(SELECT scope_id FROM pa_affected WHERE event_id=? UNION SELECT scope_id FROM pa_semantic_affected WHERE event_id=?) ORDER BY s.id LIMIT 161',e.eventId,e.eventId);
  need(revisions.length<=160,'pa_duplicate_dependency_bound');const historical=await archive.lookupRecord('event:'+e.eventId);if(historical){need(historical.digest===await hash(wire),'pa_archived_event_conflict');let reconciliation='UNCHANGED_RECONCILED';
   try{await db.batch([assertion(db,`EXISTS(SELECT 1 FROM pa_events e,pa_meta m,pa_ingress i,pa_semantic_ingress si WHERE e.id=? AND i.event_id=e.id AND si.event_id=e.id AND e.state='PENDING' AND i.state='PENDING' AND si.state='PENDING' AND e.replays=? AND e.expires_at=? AND e.expires_at>MAX(?,${SQL_CLOCK}) AND m.epoch=?)`,e.eventId,authority.replays,authority.expires_at,now,authority.epoch),
    assertion(db,`EXISTS(SELECT 1 FROM pa_jobs WHERE id=? AND owner IS ? AND generation=? AND lease_nonce=? AND state=? AND lease_until=? AND (state!='RUNNING' OR lease_until>MAX(?,${SQL_CLOCK})))`,'event:'+e.eventId,authority.owner,authority.generation,authority.lease_nonce,authority.job_state,authority.lease_until,now),
    assertion(db,"NOT EXISTS(SELECT 1 FROM json_each(?) x LEFT JOIN pa_scopes s ON s.id=json_extract(x.value,'$.id') WHERE s.id IS NULL OR s.revision!=json_extract(x.value,'$.revision'))",serialize(revisions)),...(lease?[jobFence(db,lease,now)]:[]),
    db.statement("UPDATE pa_ingress SET state='COMMITTED' WHERE event_id=?",[e.eventId]),db.statement("UPDATE pa_semantic_ingress SET state='COMMITTED' WHERE event_id=?",[e.eventId]),db.statement("UPDATE pa_events SET state='COMMITTED',available_at=COALESCE(available_at,?) WHERE id=?",[historical.available_at,e.eventId]),db.statement("UPDATE pa_reservations SET state='SETTLED' WHERE id=? AND state='COMMITTED'",[event.reservation]),clearAssertions(db)]);
   }catch(error){if(!/pa_fence_failed/.test(error.message))throw error;reconciliation='STALE_PRESERVED';}
   return {responseVersion:RESPONSE_VERSION,ingestion:{status:'DUPLICATE',durable:true,eventId:e.eventId,firstDurableAvailability:historical.available_at,reconciliation},projection:{consistency:'NOT_VALIDATED',state:'PENDING',reason:'archived_snapshot_requires_validation'}};}

  let contradiction=false;const oldSemantic=archive.lookupSemantic?await archive.lookupSemantic(e.semanticKey):null;
  if(oldSemantic&&oldSemantic.digest!==await hash(wire))contradiction=true;
  for(const scope of registration.scopes){const historicalScope=archive.lookupScope?await archive.lookupScope(scope.id):null;if(historicalScope)await db.batch([ingressFence(db,e.eventId),db.statement('UPDATE pa_scopes SET quarantine=MAX(quarantine,?),conflict=MAX(conflict,?),recovery_required=MAX(recovery_required,?),first_available_at=MIN(COALESCE(first_available_at,?),?) WHERE id=?',[historicalScope.quarantine,historicalScope.conflict,historicalScope.recovery_required??0,historicalScope.first_available_at??now,historicalScope.first_available_at??now,scope.id]),clearAssertions(db)]);}
  for(const d of decisionRecords(e)){const canonical=await archive.lookupRecord('decision:'+d.evaluationId);if(canonical){if(canonical.digest!==await hash(canonicalSerialize(d,LIMITS.eventBytes)))contradiction=true;const b=[...(canonical.sourceSnapshot?.bindings??[]),...(canonical.sourceSnapshot?.censusBindings??[])].find(b=>b.evaluation_id===d.evaluationId);if(b)await db.batch([ingressFence(db,e.eventId),db.statement('INSERT INTO pa_bindings VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING',[b.evaluation_id,b.digest,b.signal_id,b.first_event,b.cohort,b.cycle_key??null]),clearAssertions(db)]);}}
  if(contradiction){await db.batch([db.statement('UPDATE pa_scopes SET quarantine=1,conflict=1 WHERE id IN(SELECT scope_id FROM pa_affected WHERE event_id=? UNION SELECT scope_id FROM pa_semantic_affected WHERE event_id=?)',[e.eventId,e.eventId]),db.statement("UPDATE pa_ingress SET state='CONFLICT' WHERE event_id=?",[e.eventId]),db.statement("UPDATE pa_semantic_ingress SET state='CONFLICT' WHERE event_id=?",[e.eventId]),db.statement("UPDATE pa_events SET state='CONFLICT',classification='INTEGRITY_CONFLICT',reason='archived_canonical_conflict' WHERE id=?",[e.eventId])]);for(const scope of registration.scopes)await appendIndependentRecord(db,{id:'conflict:'+await hash([e.eventId,scope.id]),kind:'CONFLICT',scopeId:scope.id,cohort:scope.cohort,payload:{eventId:e.eventId,payloadDigest:e.payloadDigest,rejectedWire:wire,scopeId:scope.id,reason:'archived_canonical_conflict'},createdAt:scope.at,category:'INCIDENT'},{now,workClass:'incidents'});return {responseVersion:RESPONSE_VERSION,ingestion:{status:'INTEGRITY_CONFLICT',durable:false},projection:{consistency:'NOT_VALIDATED',state:'CONFLICT'}};}
 }
 if(registration.unresolvedCohort)return {responseVersion:RESPONSE_VERSION,ingestion:{status:'DEPENDENCY_PENDING',durable:false},projection:{consistency:'NOT_VALIDATED',state:'PENDING',reason:'cohort_dependency_pending'}};
 if(await reconcileBindings(db,e,registration,now)==='CONFLICT'){
  for(const scope of registration.scopes)await appendIndependentRecord(db,{id:'conflict:'+await hash([e.eventId,scope.id]),kind:'CONFLICT',scopeId:scope.id,cohort:scope.cohort,payload:{eventId:e.eventId,payloadDigest:e.payloadDigest,rejectedWire:wire,scopeId:scope.id,reason:'canonical_conflict'},createdAt:scope.at,category:'INCIDENT'},{now,workClass:'incidents'});
  return {responseVersion:RESPONSE_VERSION,ingestion:{status:'INTEGRITY_CONFLICT',durable:false},projection:{consistency:'NOT_VALIDATED',state:'CONFLICT'}};
 }
 if(e.kind==='EVALUATION_CENSUS'&&!await db.first("SELECT id FROM pa_events WHERE kind='DECISION_CYCLE' AND semantic_key=? AND state='COMMITTED'",canonicalSerialize(['goldsignalsx:b1','DECISION_CYCLE',e.payload.values.cycle_id]))){
  const capture=await capturePendingCensus(db,e,wire,now,archive);return {responseVersion:RESPONSE_VERSION,ingestion:{status:'DEPENDENCY_PENDING',durable:false},capture,projection:{consistency:'NOT_VALIDATED',state:'PENDING',reason:'missing_cycle'}};
 }
 let records=[{id:'event:'+e.eventId,kind:e.kind,semanticKey:e.semanticKey,wire,scopeId:signal(e)??decisionRecords(e)[0]?.evaluationId??null,cohort:registration.scopes[0]?.cohort??e.payload.records?.find(r=>r.type==='cohort')?.values?.cohort_id??'independent',createdAt:clock(e.clocks.occurredAt)??now,category:e.kind==='EVALUATION_CENSUS'?'CENSUS':'RAW'}];
 if(e.kind==='DECISION_CYCLE')for(const r of e.payload.records){const v=r.values,d=r.payload;const id=r.type==='decision'?'decision:'+d.evaluationId:r.type+':'+(v.cycle_id??v.block_id??v.definition_id??v.cohort_id);records.push({id,semanticKey:id,kind:r.type==='decision'?'CENSUS':r.type.toUpperCase(),wire:canonicalSerialize(d,LIMITS.eventBytes),scopeId:r.type==='decision'?d.evaluationId:null,cohort:v.cohort_id??records[0].cohort,createdAt:clock(d.evaluatedAt??v.evaluated_at)??records[0].createdAt,category:r.type==='decision'?'CENSUS':r.type==='definition'||r.type==='cohort'?'DEFINITION':'DETAIL',definition:'engine-v1'});}
 if(e.kind==='EVALUATION_CENSUS')records.push({id:'decision:'+e.payload.decisionEvidence.evaluationId,kind:'CENSUS',wire:canonicalSerialize(e.payload.decisionEvidence,LIMITS.eventBytes),scopeId:e.payload.decisionEvidence.evaluationId,cohort:e.payload.values.cohort_id,createdAt:e.payload.decisionEvidence.evaluatedAt,category:'CENSUS'});
 need(records.length<=80,'pa_record_count');records[0].dependencies=records.slice(1).map(r=>r.id);
 if(archive?.lookupRecord){const retained=[];for(const record of records){const old=await archive.lookupRecord(record.id);if(old){need(old.digest===await hash(record.wire),'pa_archived_record_conflict');}else retained.push(record);}records=retained;}
 for(const record of records)await stageRecord(db,record,{guard:[ingressFence(db,e.eventId)]});
 await hook?.('EVIDENCE_STAGED',e);
 // Bounded staged chunks can span batches; a single visibility fence commits all metadata.
 const manifest=serialize(records.map(r=>r.id));
 await db.batch([ingressFence(db,e.eventId),
  assertion(db,"NOT EXISTS(SELECT 1 FROM pa_records r JOIN json_each(?) x ON r.id=x.value WHERE r.ready=0 AND r.chunk_count!=(SELECT COUNT(*) FROM pa_chunks WHERE record_id=r.id))",manifest),
  db.statement(`UPDATE pa_records SET ready=1,available_at=MAX(?,${SQL_CLOCK}),revision=(SELECT revision+1 FROM pa_meta) WHERE ready=0 AND id IN(SELECT value FROM json_each(?))`,[now,manifest]),
  db.statement("UPDATE pa_ingress SET state='COMMITTED' WHERE event_id=?",[e.eventId]),
  db.statement("UPDATE pa_scopes SET recovery_required=0 WHERE id IN(SELECT scope_id FROM pa_affected WHERE event_id=?) AND NOT EXISTS(SELECT 1 FROM pa_affected a JOIN pa_ingress i ON i.event_id=a.event_id WHERE a.scope_id=pa_scopes.id AND i.state='PENDING' AND i.event_id!=?)",[e.eventId,e.eventId]),
  db.statement("UPDATE pa_semantic_ingress SET state='COMMITTED' WHERE event_id=?",[e.eventId]),
  db.statement("UPDATE pa_reservations SET state='SETTLED' WHERE id=? AND state='COMMITTED'",[event.reservation]),
  db.statement(`UPDATE pa_events SET state='COMMITTED',available_at=COALESCE(available_at,MAX(?,${SQL_CLOCK})) WHERE id=?`,[now,e.eventId]),
  ...registration.scopes.filter(s=>decisionRecords(e).some(d=>(d.evaluationId===s.id||d.officialSignalId===s.id)&&['buy','sell'].includes(d.direction)&&d.outcome!=='SKIPPED')).map(s=>db.statement('UPDATE pa_scopes SET processing_eligible=1 WHERE id=?',[s.id])),
  ...(e.kind==='OFFICIAL_CREATION'&&e.payload.officialPersisted===true&&e.payload.performancePersistence?.performance==='SUCCEEDED'?[db.statement('UPDATE pa_scopes SET active=1 WHERE id=?',[signal(e)])]:[]),
  ...(e.kind==='LIFECYCLE_FACT'&&['tp2','sl','expired'].includes(e.payload.event)&&e.payload.performancePersistence?.ok===true?[db.statement('UPDATE pa_scopes SET active=0 WHERE id=?',[signal(e)])]:[]),
  ...(decisionRecords(e).length?[db.statement("INSERT INTO pa_jobs(id,class,scope_id,event_id,next_at,deadline) VALUES(?,'archive',?,'CENSUS',?,?) ON CONFLICT(id) DO UPDATE SET state='PENDING',next_at=MIN(next_at,excluded.next_at)",['archive-census:'+decisionRecords(e)[0].evaluationId,decisionRecords(e)[0].evaluationId,now,now+DAY])]:[]),
  db.statement("INSERT INTO pa_jobs(id,class,scope_id,event_id,next_at,deadline) SELECT 'projection:'||scope_id,'processing',scope_id,?, ?,? FROM (SELECT scope_id FROM pa_affected WHERE event_id=? UNION SELECT b.evaluation_id FROM pa_bindings b JOIN pa_affected a ON a.scope_id=b.signal_id WHERE a.event_id=?) WHERE true ON CONFLICT(id) DO UPDATE SET state='PENDING',next_at=MIN(next_at,excluded.next_at)",[e.eventId,now,now+DAY,e.eventId,e.eventId]),clearAssertions(db)]);
 return d1Result(db,e,'ACCEPTED',true,{hook});
}
async function capturePendingCensus(db,e,wire,now,archive){const d=e.payload.decisionEvidence,id='captured-census:'+d.evaluationId,scopeId='capture:'+d.evaluationId;
 const known=await db.first('SELECT available_at FROM pa_records WHERE id=? AND ready=1',id)??(archive?.lookupRecord?await archive.lookupRecord(id):null);if(known)return {status:'CAPTURED',durable:true,evaluationId:d.evaluationId,availableAt:known.available_at,interpretation:'DEPENDENCY_PENDING'};
 await db.batch([assertion(db,`EXISTS(SELECT 1 FROM pa_scopes WHERE id=?) OR (SELECT COUNT(*) FROM pa_scopes)<${LIMITS.activeScopes}`,scopeId),db.statement("INSERT INTO pa_scopes(id,cohort,revision,original_at,kind) VALUES(?,?,(SELECT revision+1 FROM pa_meta),?,'CENSUS_CAPTURE') ON CONFLICT DO NOTHING",[scopeId,e.payload.values.cohort_id,d.evaluatedAt]),clearAssertions(db)]);
 const stored=await appendIndependentRecord(db,{id,kind:'CENSUS_CAPTURE',scopeId,cohort:e.payload.values.cohort_id,createdAt:d.evaluatedAt,category:'CENSUS',payload:{version:'measurement-census-capture-v1',evaluationId:d.evaluationId,decisionDigest:await hash(canonicalSerialize(d,LIMITS.eventBytes)),decisionEvidence:d,originalWire:wire,parentCycle:e.payload.values.cycle_id,interpretation:'DEPENDENCY_PENDING',measurementOnly:true,decisionUse:false}},{now});
 await db.batch([db.statement("INSERT INTO pa_jobs(id,class,scope_id,next_at,deadline) VALUES(?,'archive',?,?,?) ON CONFLICT DO NOTHING",['archive-scope:'+scopeId,scopeId,now,now+14*DAY])]);return {status:'CAPTURED',durable:true,evaluationId:d.evaluationId,availableAt:stored.available_at,interpretation:'DEPENDENCY_PENDING'};
}
export async function rebuildD1Projection(db,scopeId,{now,archive=null,hook=null}={}){
 const m=await schemaReady(db),s=await db.first('SELECT * FROM pa_scopes WHERE id=?',scopeId);need(s,'pa_scope_missing');
 let rows=await db.all("SELECT r.id,r.digest,r.available_at FROM pa_records r WHERE r.ready=1 AND r.scope_id=? AND r.kind IN('OFFICIAL_CREATION','LIFECYCLE_FACT','CONFIRMATION_LINK') ORDER BY r.id LIMIT ?",scopeId,LIMITS.scopeRecords+1);
 if(s.archive_root){need(archive?.scopeRecords,'pa_projection_archive_required');const archived=await archive.scopeRecords(scopeId,{rootKey:s.archive_root}),all=new Map(archived.filter(r=>['OFFICIAL_CREATION','LIFECYCLE_FACT','CONFIRMATION_LINK'].includes(r.kind)).map(r=>[r.id,r]));for(const r of rows)all.set(r.id,r);rows=[...all.values()].sort((a,b)=>a.id.localeCompare(b.id));}
 need(rows.length<=LIMITS.scopeRecords,'pa_projection_budget');
 if(!rows.length)return null;
 const facts=[];for(const row of rows){const r=await readRecord(db,row.id,{archive});const e=await validateMeasurementWire(r.wire,{receivedAt:now});facts.push({event_id:e.eventId,fact_kind:e.kind,ingested_at:r.available_at,payload:parseEvidence(canonicalSerialize(e.payload,LIMITS.eventBytes))});}
 const p=projectLifecycle(scopeId,facts);if(s.conflict||s.quarantine){p.integrityStatus='CONFLICT';p.status=null;p.closedAt=null;}
 const wire=serialize(p),digest=await hash(wire),factIdentity=await hash(rows.map(r=>[r.id,r.digest,r.available_at]));await hook?.('PROJECTION_PREPARED');
 await db.batch([assertion(db,'EXISTS(SELECT 1 FROM pa_scopes s,pa_meta m WHERE s.id=? AND s.revision=? AND m.epoch=?)',scopeId,s.revision,m.epoch),
  db.statement('INSERT INTO pa_projections VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope_id) DO UPDATE SET epoch=excluded.epoch,source_revision=excluded.source_revision,digest=excluded.digest,payload=excluded.payload,available_at=excluded.available_at,integrity=excluded.integrity,fact_identity=excluded.fact_identity,fact_count=excluded.fact_count',[scopeId,m.epoch,s.revision,digest,wire,p.availableAt,p.integrityStatus,factIdentity,rows.length]),clearAssertions(db)]);return validateD1Snapshot(db,scopeId,{now,hook});
}
export async function validateD1Snapshot(db,scopeId,{now=Date.now(),hook=null}={}){
 time(now);
 const sql=`SELECT MAX(?,${SQL_CLOCK}) AS validated_at,m.epoch,m.revision AS live_revision,s.*,p.source_revision,p.digest,p.payload,p.integrity,p.fact_identity,p.fact_count,p.available_at AS projection_available,
 COALESCE((SELECT revision FROM pa_cohort_state WHERE cohort=s.cohort),0) AS market_revision,
 COALESCE((SELECT pending FROM pa_cohort_state WHERE cohort=s.cohort),0) AS market_pending,
 COALESCE((SELECT conflict FROM pa_cohort_state WHERE cohort=s.cohort),0) AS market_conflict,
 EXISTS(SELECT 1 FROM pa_bindings b JOIN pa_scopes dep ON dep.id=b.evaluation_id OR dep.id=b.signal_id WHERE (b.evaluation_id=s.id OR b.signal_id=s.id) AND (dep.pending!=0 OR dep.conflict!=0 OR dep.quarantine!=0 OR dep.corrections!=0 OR dep.recovery_required!=0)) AS binding_pending,
 p.epoch AS projection_epoch,
 (SELECT json_group_array(json(binding_json)) FROM(SELECT json_array(b.evaluation_id,b.digest,b.signal_id,es.revision,es.pending,es.quarantine,es.conflict,es.corrections,es.recovery_required,es.available_at,os.revision,os.pending,os.quarantine,os.conflict,os.corrections,os.recovery_required,os.available_at) AS binding_json FROM pa_bindings b LEFT JOIN pa_scopes es ON es.id=b.evaluation_id LEFT JOIN pa_scopes os ON os.id=b.signal_id WHERE b.signal_id=s.id OR b.evaluation_id=s.id ORDER BY b.evaluation_id LIMIT 81)) AS bindings,
 (SELECT json_group_array(json_array(id,digest,available_at)) FROM(SELECT id,digest,available_at FROM pa_records WHERE scope_id=s.id AND ready=1 AND kind!='RESULT' ORDER BY id LIMIT 257)) AS evidence
 FROM pa_scopes s CROSS JOIN pa_meta m LEFT JOIN pa_projections p ON p.scope_id=s.id WHERE s.id=?`;
 const result=await db.batch([db.statement(sql,[now,scopeId])]);const r=result[0].results?.[0];
 if(!r)return {responseVersion:RESPONSE_VERSION,consistency:'NOT_VALIDATED',state:'PENDING',reason:'missing_scope'};
 need(Number.isSafeInteger(r.revision)&&r.revision>=0&&Number.isSafeInteger(r.live_revision)&&r.live_revision>=0,'pa_revision_bound');
 need(bytes(r.bindings??'')+bytes(r.evidence??'')<=LIMITS.rowBytes&&JSON.parse(r.bindings).length<=80&&JSON.parse(r.evidence).length<=LIMITS.scopeRecords,'pa_snapshot_bound');
 const status=r.pending||r.market_pending||r.binding_pending?'pending_ingress':r.corrections?'unresolved_correction':r.recovery_required?'recovery_required':r.quarantine||r.conflict||r.market_conflict?'conflict':r.source_revision!==r.revision||r.projection_epoch!==r.epoch?'stale_projection':r.projection_available>r.validated_at?'availability_pending':null;
 const descriptor={responseVersion:RESPONSE_VERSION,scope:{id:scopeId,cohort:r.cohort},revisionEpoch:r.epoch,validatedRevision:r.revision,liveRevision:r.live_revision,
 projectionRevision:r.source_revision??null,projectionDigest:r.digest??null,stateAtRevision:status==='conflict'?'CONFLICT':status?'PENDING':r.integrity,marketRevision:r.market_revision,
 dependencyIdentity:await hash([r.recovery_required,r.pending,r.quarantine,r.conflict,r.corrections,r.bindings,r.available_at,r.source_revision,r.evidence,r.archive_root,r.market_revision,r.market_pending,r.market_conflict]),
 evidenceIdentity:await hash([r.evidence,r.archive_root,r.fact_identity]),availabilityBoundary:{ingressAdmission:'VALIDATED_REGISTERED_EVIDENCE_ONLY',transportStagingIsAnalyticalCompletion:false,asOf:r.validated_at,firstDurableAvailability:r.first_available_at,availableThrough:r.available_at,projectionAvailableAt:r.projection_available??null},
 definitionVersion:'engine-v1',factCount:r.fact_count??0,consistency:'VALIDATED_AT_REVISION',...(status?{reason:status}:{}),payload:r.payload?parseEvidence(r.payload):null};
 // Hashing/delivery may await. The descriptor describes the SQL read boundary, not delivery time.
 await hook?.('VALIDATED',descriptor);return descriptor;
}
async function d1Result(db,e,status,durable,{hook=null}={}){const signalId=signal(e),row=await db.first('SELECT available_at FROM pa_events WHERE id=?',e.eventId);return {responseVersion:RESPONSE_VERSION,
 ingestion:{status,durable,eventId:e.eventId,payloadDigest:e.payloadDigest,firstDurableAvailability:row?.available_at??null},
 projection:signalId?await validateD1Snapshot(db,signalId,{hook}):{consistency:'NOT_APPLICABLE'},processing:{state:'PENDING',reason:'durable_recovery_scheduled'}};}
