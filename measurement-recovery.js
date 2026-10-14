import {need,time,uid,LIMITS,DAY,parseEvidence,serialize,hash} from './measurement-independent-contract.js';
import {leaseJob,finishJob,jobFence,assertion,clearAssertions,scopeFence} from './measurement-d1-adapter.js';
import {recoverTransportEvent} from './measurement-queue-consumer.js';
import {rebuildD1Projection,validateD1Snapshot,readRecord,stageRecord,finishRecord} from './measurement-independent-store.js';
import {processCorrection} from './measurement-corrections.js';
import {reserve,estimateWrite,commitReservationStatements,settleReservationStatements,recoverReservations} from './measurement-reservations.js';
import {foldPostEntry} from './post-entry-evidence.js';
const classes=['census','official','incidents','catchup','processing','corrections','archive','restore'];
export async function runRecovery(db,{now,owner=uid(),archive=null,maxJobs=16,handlers={},hook=null}={}){
 time(now);need(maxJobs>=8&&maxJobs<=32,'pa_scheduler_bound');const outcomes=[],reservationsReleased=await recoverReservations(db,now,{limit:16});const turn=await db.batch([db.statement('UPDATE pa_meta SET scheduler_cursor=(scheduler_cursor+1)%8 RETURNING scheduler_cursor')]),offset=turn[0].results[0].scheduler_cursor,orderedClasses=[...classes.slice(offset),...classes.slice(0,offset)];
 // Each bounded pass reserves a turn for every class; urgent classes then get spare turns.
 for(let round=0;round<4&&outcomes.length<maxJobs;round++)for(const workClass of orderedClasses){if(outcomes.length>=maxJobs||db.remainingStatements()<300)break;
  const row=await db.first("SELECT * FROM pa_jobs WHERE class=? AND state IN('PENDING','RUNNING') AND next_at<=? AND lease_until<=? ORDER BY deadline,next_at,id LIMIT 1",workClass,now,now);if(!row)continue;
  if(['census','official'].includes(workClass)&&row.event_id){outcomes.push(await recoverTransportEvent(db,row.event_id,{now,owner,archive,hook}));continue;}
  if(workClass==='corrections'){outcomes.push(await processCorrection(db,row.event_id,{now,owner,archive,hook}));continue;}
  const lease=await leaseJob(db,row.id,owner,{now});if(!lease)continue;
  try{let result;if(workClass==='processing'){
    result=await rebuildD1Projection(db,row.scope_id,{now,archive,hook});
    const processing=await processCapturedOutcome(db,row.scope_id,{now,archive,lease,hook});result={projection:result,processing};
   }else{need(typeof handlers[workClass]==='function','pa_recovery_handler_pending');result=await handlers[workClass]({db,lease,row,now});}
   await finishJob(db,lease,now,{state:result?.processing?.status==='BACKLOG_PENDING'||result?.pending?'PENDING':'DONE',nextAt:now+1000});outcomes.push({job:row.id,status:'DONE',result});
  }catch(error){try{await finishJob(db,lease,now,{state:'PENDING',nextAt:now+1000});}catch{}outcomes.push({job:row.id,status:'PENDING',reason:error.message});}
 }
 return {outcomes,bounded:true,maxJobs,reservationsReleased};
}
export async function processCapturedOutcome(db,scopeId,{now,archive=null,lease=null,hook=null}={}){
 const binding=await db.first('SELECT * FROM pa_bindings WHERE evaluation_id=? OR signal_id=? ORDER BY evaluation_id LIMIT 1',scopeId,scopeId);if(!binding)return {status:'NOT_APPLICABLE'};
 const record=await readRecord(db,'decision:'+binding.evaluation_id,{archive});if(!record)return {status:'PENDING',reason:'decision_unavailable'};
 const d=parseEvidence(record.wire);if(d.kind==='CANDIDATE'&&(d.direction==='none'||d.outcome==='SKIPPED'))return {status:'CENSUS_ONLY'};
 const levels=d.engine?.levels;need(['buy','sell'].includes(d.direction)&&levels&&['entry','tp1','tp2','sl'].every(k=>Number.isFinite(levels[k])),'pa_processing_levels');
 let ownLease=false;if(!lease){await db.batch([db.statement("INSERT INTO pa_jobs(id,class,scope_id,next_at,deadline) VALUES(?,'processing',?,?,?) ON CONFLICT DO NOTHING",['projection:'+scopeId,scopeId,now,now+DAY])]);lease=await leaseJob(db,'projection:'+scopeId,uid(),{now});if(!lease)return {status:'LEASE_PENDING'};ownLease=true;}need(lease.id==='projection:'+scopeId,'pa_processing_lease_scope');
 const scope=await db.first('SELECT * FROM pa_scopes WHERE id=?',scopeId),snap=await validateD1Snapshot(db,scopeId,{now});
 if(d.kind==='OFFICIAL')need(snap.stateAtRevision==='COMPLETE','pa_processing_lifecycle_pending');
 need(scope.pending===0&&scope.quarantine===0&&scope.conflict===0&&scope.corrections===0,'pa_processing_fence_pending');
 const previousRow=await db.first('SELECT * FROM pa_processing WHERE scope_id=?',scopeId),previous=previousRow?parseEvidence(previousRow.payload):null;
 const market=await db.first('SELECT revision,pending,conflict FROM pa_cohort_state WHERE cohort=?',scope.cohort),marketRevision=market?.revision??0;need(!market?.pending&&!market?.conflict,'pa_processing_market_pending');
 let after=previous?.decisionDigest===record.digest?previous.cursor:'',prior=previous?.decisionDigest===record.digest?previous.folded.state:null;
 // Backfilled earlier evidence invalidates the incremental scan. Replay bounded pages
 // from the beginning; immutable old result versions remain historical information.
 if(after&&await db.first("SELECT id FROM pa_records WHERE kind='MARKET_EXPORT' AND cohort=? AND ready=1 AND available_at>? AND printf('%016x',created_at)||substr(order_key,1,48)<=? LIMIT 1",scope.cohort,previousRow.updated_at,after)){after='';prior=null;}
 const hot=await db.all("SELECT id,printf('%016x',created_at)||substr(order_key,1,48) AS marketOrder FROM pa_records WHERE kind='MARKET_EXPORT' AND ready=1 AND cohort=? AND available_at<=? AND printf('%016x',created_at)||substr(order_key,1,48)>? ORDER BY created_at,order_key LIMIT 9",scope.cohort,now,after);
 const cold=archive?.marketPage?await archive.marketPage(scope.cohort,{after,limit:9}):{records:[],complete:true,next:after};
 const frontier=!cold.complete?cold.next:null,all=new Map(cold.records.map(r=>[r.id,r]));for(const row of hot)if(!frontier||row.marketOrder<=frontier){const r=await readRecord(db,row.id,{archive});need(r,'pa_processing_market_missing');const old=all.get(row.id);need(!old||old.digest===r.digest,'pa_processing_market_conflict');all.set(row.id,{...r,marketOrder:row.marketOrder});}
 const ordered=[...all.values()].sort((a,b)=>a.marketOrder.localeCompare(b.marketOrder)),samples=ordered.slice(0,8),complete=ordered.length<=8&&hot.length<9&&cold.complete,cursor=samples.at(-1)?.marketOrder??after;
 need(complete||cursor>after,'pa_processing_scan_pending');const ticks=[],bars=[];
 for(const r of samples){const p=parseEvidence(r.wire);for(const s of p.samples){if(s.type==='TICK')ticks.push(s.value);else if(s.type==='CLOSED_BAR')bars.push(s.value);}}
 need(ticks.length<=2400&&bars.length<=2000,'pa_processing_input_bound');
 const subject={id:scopeId,side:d.direction,createdAt:d.createdAt??d.evaluatedAt,...levels,closedAt:snap.payload?.closedAt??null},folded=foldPostEntry(subject,prior,{ticks,bars,asOf:now,availableAt:now}),payload={version:'measurement-result-v1',subjectId:scopeId,evaluationId:d.evaluationId,decisionIdentity:record.id,decisionDigest:record.digest,sourceRevision:scope.revision,marketRevision,preparedAt:now,availabilitySemantics:'NO_EARLIER_THAN_CONTAINING_DURABLE_RECORD',folded,cursor,completeForCapturedSnapshot:complete,marketEvidence:samples.map(r=>[r.id,r.digest]),measurementOnly:true,decisionUse:false};
 const id='result:'+await hash([scopeId,scope.revision,payload]);await hook?.('OUTCOME_PREPARED',payload);
 const guards=()=>[...(lease?[jobFence(db,lease,now)]:[]),scopeFence(db,snap,{requireComplete:d.kind==='OFFICIAL',now}),assertion(db,'EXISTS(SELECT 1 FROM pa_scopes WHERE id=? AND revision=? AND pending=0 AND corrections=0 AND quarantine=0 AND conflict=0) AND COALESCE((SELECT revision FROM pa_cohort_state WHERE cohort=?),0)=? AND NOT EXISTS(SELECT 1 FROM pa_cohort_state WHERE cohort=? AND (pending!=0 OR conflict!=0))',scopeId,scope.revision,scope.cohort,marketRevision,scope.cohort)];
 const reservation=await reserve(db,{id:'processing:'+id,workClass:'processing',owner:id,bytes:estimateWrite(serialize(payload).length*2,12),now});if(reservation.state==='RESERVED')await db.batch([...guards(),...commitReservationStatements(db,reservation,now),clearAssertions(db)]);
 await stageRecord(db,{id,kind:'RESULT',scopeId,cohort:scope.cohort,payload,createdAt:subject.createdAt,category:'RESULT',dependencies:samples.map(r=>r.id)},{guard:guards()});
 await db.batch([...guards(),...finishRecord(db,id,now),...settleReservationStatements(db,reservation),db.statement('INSERT INTO pa_processing VALUES(?,?,?,?,?) ON CONFLICT(scope_id) DO UPDATE SET revision=excluded.revision,market_revision=excluded.market_revision,payload=excluded.payload,updated_at=excluded.updated_at',[scopeId,scope.revision,marketRevision,serialize(payload),now]),clearAssertions(db)]);if(ownLease)await finishJob(db,lease,now,{state:complete?'DONE':'PENDING',nextAt:now+1000});return {status:complete?'COMPLETED_AT_REVISION':'BACKLOG_PENDING',revision:scope.revision,marketRevision,id,cursor,ambiguity:'PRESERVED_BY_ACCEPTED_REDUCER'};
}
export function modeledCatchup({outageHours=48,arrivalsPerHour,servicePerHour}){need(outageHours===48&&Number.isFinite(arrivalsPerHour)&&arrivalsPerHour>=0&&Number.isFinite(servicePerHour)&&servicePerHour>arrivalsPerHour,'pa_catchup_model');const backlog=outageHours*arrivalsPerHour,hours=backlog/(servicePerHour-arrivalsPerHour);return {backlog,hours,withinSevenDays:hours<=168,continuingArrivals:true,modelOnly:true,realCloudQualification:'REQUIRED'};}
