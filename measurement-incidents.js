import {identity,serialize,hash,time,need,LIMITS} from './measurement-independent-contract.js';
import {assertion,clearAssertions} from './measurement-d1-adapter.js';
import {reserve,estimateWrite,commitReservationStatements,settleReservationStatements} from './measurement-reservations.js';
import {appendIndependentRecord,readRecord} from './measurement-independent-store.js';
export const INCIDENT_CATEGORIES=Object.freeze(['PRODUCER_PREPARATION_FAILURE','HANDOFF_ACCEPTANCE_UNKNOWN','CAPTURE_GAP','MISSING_CYCLE','MISSING_CENSUS','MISSING_DETAIL','MULTIPART_EXPIRY','QUEUE_EXPIRY_RISK','DEPENDENCY_PENDING','CONFLICT_QUARANTINE','ARCHIVE_VERIFICATION_FAILURE','ARCHIVE_AVAILABILITY_FAILURE','RESOURCE_REJECTION','CORRECTION_EXPIRY','CORRECTION_UNREDUCIBLE','RESTORE_FAILURE','COHORT_CAPTURE_GAP','TRANSPORT_FAILURE']);
export async function recordIncident(db,input,{now}={}){
 time(now);need(INCIDENT_CATEGORIES.includes(input.category)&&typeof input.observed==='boolean'&&typeof input.unknownCount==='boolean','pa_incident_contract');
 if(input.knownMissingCount!=null)need(Number.isSafeInteger(input.knownMissingCount)&&input.knownMissingCount>=0,'pa_incident_count');
 const payload={...input,firstObservation:input.firstObservation??now,lastObservation:now,recoveryState:input.recoveryState??'OPEN'},wire=serialize(payload,8192),id='incident:'+await hash(wire);
 try{const prior=await db.first('SELECT id FROM pa_incidents WHERE id=?',id);if(prior)return {durable:true,id,status:'DUPLICATE'};const reservation=await reserve(db,{id,workClass:'incidents',bytes:estimateWrite(wire.length,4),owner:id,now});
  await db.batch([assertion(db,`(SELECT COUNT(*) FROM pa_incidents)<${LIMITS.incidents}`),db.statement('INSERT INTO pa_incidents VALUES(?,?,?,?,?,?,?,?)',[id,input.category,payload.firstObservation,now,'OPEN',wire,input.scopeId??null,reservation.id]),...commitReservationStatements(db,reservation,now),clearAssertions(db)]);
  await appendIndependentRecord(db,{id,kind:'INCIDENT',scopeId:input.scopeId??null,cohort:input.cohort??'independent',payload,createdAt:now,category:'INCIDENT'},{now,workClass:'incidents',owner:id});await db.batch([...settleReservationStatements(db,reservation),clearAssertions(db)]);return {durable:true,id};
 }catch(error){return {durable:null,status:'INCIDENT_DURABILITY_UNKNOWN',reason:error.message,coverage:'UNKNOWN_CAPTURE'};}
}
export async function recordCoverage(db,{cohort,from,to,status,knownMissing=null,evidenceIds=[],captureScope=null},{now,archive=null}={}){
 identity(cohort);time(from);time(to);need(from<=to&&['COMPLETE_FOR_SCOPE','KNOWN_GAPS','UNKNOWN_CAPTURE'].includes(status)&&evidenceIds.length<=128,'pa_coverage');
 if(knownMissing!==null)need(Number.isSafeInteger(knownMissing)&&knownMissing>=0,'pa_coverage_missing_count');
 if(status==='COMPLETE_FOR_SCOPE'){need(Array.isArray(captureScope)&&captureScope.length>0&&captureScope.length<=128&&new Set(captureScope).size===captureScope.length&&captureScope.every(id=>evidenceIds.includes(id)),'pa_coverage_requires_captured_scope');
  for(const id of captureScope){const record=await readRecord(db,id,{archive});need(record?.kind==='DECISION_CYCLE'&&record.cohort===cohort&&record.created_at>=from&&record.created_at<=to,'pa_coverage_cycle_proof');const e=JSON.parse(record.wire);for(const d of e.payload.records.filter(r=>r.type==='decision'))need(await readRecord(db,'decision:'+d.payload.evaluationId,{archive}),'pa_coverage_census_missing');}}

 const id='coverage:'+await hash([cohort,from,to,status,evidenceIds,captureScope]);
 await appendIndependentRecord(db,{id,kind:'COVERAGE',cohort,createdAt:from,category:'CENSUS',payload:{cohort,from,to,status,knownMissing,evidenceIds,captureScope,uncapturedIntervals:'UNKNOWN_CAPTURE'},dependencies:evidenceIds},{now});
 await db.batch([assertion(db,'NOT EXISTS(SELECT 1 FROM json_each(?) x LEFT JOIN pa_records r ON r.id=x.value WHERE r.id IS NULL OR r.ready!=1)',serialize(evidenceIds)),db.statement('INSERT INTO pa_coverage VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING',[id,cohort,from,to,status,knownMissing,id]),clearAssertions(db)]);return {id,status};
}
export async function measurementObservability(db,{now}={}){return {at:time(now),transport:await db.all('SELECT state,COUNT(*) count,MIN(first_at) oldest FROM pa_events GROUP BY state'),jobs:await db.all('SELECT class,state,COUNT(*) count,MIN(next_at) oldest FROM pa_jobs GROUP BY class,state'),archive:await db.first('SELECT catalogue_generation,root_key FROM pa_meta'),incidents:await db.all('SELECT category,state,COUNT(*) count FROM pa_incidents GROUP BY category,state'),identityLabels:'NONE',silenceWitness:'EXTERNAL_HEARTBEAT_CONTRACT_REQUIRED',cloudQuantitativeQualification:'REQUIRED'};}
