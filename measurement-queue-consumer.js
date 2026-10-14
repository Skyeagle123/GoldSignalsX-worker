// Cloudflare Queue method contract, dependency injected. No Queue provisioning.
import {validateTransportFragment,reassembleMeasurementFragments,sameFragmentManifest,fragmentMeasurementPacket} from './measurement-transport-fragment.js';
import {serialize,hash,bytes,need,time,LIMITS,DAY,RESPONSE_VERSION,uid} from './measurement-independent-contract.js';
import {assertion,clearAssertions,leaseJob,jobFence,finishJob,SQL_CLOCK} from './measurement-d1-adapter.js';
import {reserve,estimateWrite,commitReservationStatements} from './measurement-reservations.js';
import {commitEnvelope,readRecord} from './measurement-independent-store.js';
export function classifyD1Response(r){
 need(r?.responseVersion===RESPONSE_VERSION,'pa_response_version');const status=r.ingestion?.status;
 if(status==='DURABILITY_UNKNOWN')return {classification:'DURABILITY_UNKNOWN',retry:true};
 if(status==='DEPENDENCY_PENDING')return {classification:'DEPENDENCY_PENDING',retry:true};
 if(status==='INTEGRITY_CONFLICT')return {classification:'INTEGRITY_CONFLICT',retry:false};
 if(status==='REJECTED')return {classification:'PERMANENT_MALFORMED',retry:false};
 if(['ACCEPTED','DUPLICATE'].includes(status)&&r.ingestion.durable===true)return {classification:'EVIDENCE_COMMITTED',retry:false,projection:r.projection,processing:r.processing};
 return {classification:'RETRYABLE',retry:true};
}
export async function stageQueueFragment(db,wire,{now,retentionMs=14*DAY,owner='transport',hook=null,archive=null,publishedAt=null}={}){
 time(now);if(publishedAt!==null){time(publishedAt);need(publishedAt<=now,'pa_transport_publication_clock');}const deadline=(publishedAt??now)+retentionMs;need(retentionMs>0&&retentionMs<=14*DAY,'pa_transport_expiry');const {manifest:f}=await validateTransportFragment(wire);
 const m=Object.fromEntries(['version','namespace','eventId','eventKind','payloadDigest','wireDigest','totalBytes','count'].map(k=>[k,f[k]]));
 if(archive?.lookupRecord){const terminal=await archive.lookupRecord('transport-terminal:'+f.eventId);if(terminal&&!await db.first("SELECT id FROM pa_events WHERE id=? AND (state='COMMITTED' OR (replays>0 AND state IN('PARTIAL','STAGED','PENDING')))",f.eventId)&&!await archive.lookupRecord('event:'+f.eventId)){const retained=JSON.parse(terminal.wire).event;need(retained.wire_digest===f.wireDigest&&retained.digest===f.payloadDigest,'pa_archived_terminal_conflict');return {status:retained.expires_at<=now?'EXPIRED':retained.state,durable:true,eventId:f.eventId,analyticalCompletion:false};}const historical=await archive.lookupRecord('event:'+f.eventId);if(historical){need(historical.digest===f.wireDigest,'pa_archived_transport_conflict');const frames=await fragmentMeasurementPacket({wire:historical.wire});need(frames[f.ordinal]===wire,'pa_archived_fragment_conflict');return {status:'ARCHIVED_DUPLICATE',durable:true,eventId:f.eventId,analyticalCompletion:false};}}
 const old=await db.first('SELECT * FROM pa_events WHERE id=?',f.eventId);
 if(old){need(sameFragmentManifest(JSON.parse(old.manifest),f),'pa_transport_manifest_conflict');
  if(old.state==='EXPIRED')return {status:'EXPIRED',durable:true,eventId:f.eventId,expiresAt:old.expires_at};
  if(['COMMITTED','CONFLICT','DLQ'].includes(old.state))return {status:old.state,durable:true,eventId:f.eventId};
 }
 let reservation=old?await db.first('SELECT * FROM pa_reservations WHERE id=?',old.reservation):await reserve(db,{id:'transport:'+f.eventId,workClass:'transport',owner,bytes:estimateWrite(f.totalBytes*6+f.count*60000,200),now,ttl:60000});
 const statements=[];
 if(!old&&reservation.state==='RESERVED')statements.push(...commitReservationStatements(db,reservation,now));
 statements.push(assertion(db,`EXISTS(SELECT 1 FROM pa_events WHERE id=?) OR (SELECT COUNT(*) FROM pa_events)<${LIMITS.activeEvents}`,f.eventId),
  db.statement('INSERT INTO pa_events(id,kind,digest,wire_digest,manifest,count,total_bytes,first_at,expires_at,reservation,published_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING',[f.eventId,f.eventKind,f.payloadDigest,f.wireDigest,serialize(m,4096),f.count,f.totalBytes,now,deadline,reservation.id,publishedAt]),
  db.statement('UPDATE pa_events SET expires_at=MIN(expires_at,?),published_at=CASE WHEN published_at IS NULL THEN ? WHEN ? IS NULL THEN published_at ELSE MIN(published_at,?) END WHERE id=?',[deadline,publishedAt,publishedAt,publishedAt,f.eventId]),
  assertion(db,'EXISTS(SELECT 1 FROM pa_events WHERE id=? AND digest=? AND wire_digest=? AND manifest=?)',f.eventId,f.payloadDigest,f.wireDigest,serialize(m,4096)),
  db.statement(`UPDATE pa_events SET state='EXPIRED',classification='PERMANENT_MALFORMED',reason='transport_expired' WHERE id=? AND state='PARTIAL' AND expires_at<=MAX(?,${SQL_CLOCK})`,[f.eventId,now]),
  assertion(db,'NOT EXISTS(SELECT 1 FROM pa_fragments WHERE event_id=? AND ordinal=?) OR EXISTS(SELECT 1 FROM pa_fragments WHERE event_id=? AND ordinal=? AND digest=? AND wire=?)',f.eventId,f.ordinal,f.eventId,f.ordinal,f.fragmentDigest,wire),
  db.statement(`INSERT INTO pa_fragments SELECT ?,?,?,? WHERE EXISTS(SELECT 1 FROM pa_events WHERE id=? AND state='PARTIAL' AND expires_at>MAX(?,${SQL_CLOCK})) ON CONFLICT DO NOTHING`,[f.eventId,f.ordinal,f.fragmentDigest,wire,f.eventId,now]),
  db.statement("UPDATE pa_events SET state='STAGED' WHERE id=? AND state='PARTIAL' AND count=(SELECT COUNT(*) FROM pa_fragments WHERE event_id=?)",[f.eventId,f.eventId]),
  db.statement("INSERT INTO pa_jobs(id,class,event_id,next_at,deadline) SELECT 'event:'||id,CASE WHEN kind IN('DECISION_CYCLE','EVALUATION_CENSUS') THEN 'census' ELSE 'official' END,id,?,expires_at FROM pa_events WHERE id=? AND state IN('PARTIAL','STAGED','PENDING') ON CONFLICT DO NOTHING",[now,f.eventId]),clearAssertions(db));
 await db.batch(statements);await hook?.('DURABLE_STAGED',f.eventId);
 const r=await db.first('SELECT state,expires_at,(SELECT COUNT(*) FROM pa_fragments WHERE event_id=pa_events.id) received FROM pa_events WHERE id=?',f.eventId);
 return {status:r.state,durable:true,eventId:f.eventId,received:r.received,declared:f.count,expiresAt:r.expires_at,analyticalCompletion:false};
}

function abandonExpiredIngress(db,id){return [db.statement("UPDATE pa_scopes SET recovery_required=1 WHERE id IN(SELECT scope_id FROM pa_affected WHERE event_id=? UNION SELECT scope_id FROM pa_semantic_affected WHERE event_id=?) AND EXISTS(SELECT 1 FROM pa_events WHERE id=? AND state='DLQ')",[id,id,id]),db.statement("UPDATE pa_ingress SET state='ABANDONED' WHERE event_id=? AND state='PENDING' AND EXISTS(SELECT 1 FROM pa_events WHERE id=? AND state='DLQ')",[id,id]),db.statement("UPDATE pa_semantic_ingress SET state='ABANDONED' WHERE event_id=? AND state='PENDING' AND EXISTS(SELECT 1 FROM pa_events WHERE id=? AND state='DLQ')",[id,id])];}
export async function markTransportFailure(db,id,classification,reason,{now,maxAttempts=5,lease=null}={}){
 need(['RETRYABLE','DEPENDENCY_PENDING','DURABILITY_UNKNOWN','INTEGRITY_CONFLICT','PERMANENT_MALFORMED'].includes(classification),'pa_failure_class');
 await db.batch([...(lease?[jobFence(db,lease,now)]:[]),db.statement("UPDATE pa_events SET attempts=MIN(attempts+1,32),first_failure=COALESCE(first_failure,?),last_failure=?,classification=?,reason=?,next_at=? WHERE id=? AND state NOT IN('COMMITTED','EXPIRED','CONFLICT')",[now,now,classification,String(reason).slice(0,512),now+Math.min(3600000,1000*2**Math.min(maxAttempts,12)),id]),
  db.statement("UPDATE pa_events SET state='DLQ' WHERE id=? AND state NOT IN('COMMITTED','EXPIRED','CONFLICT') AND ((attempts>=? AND classification!='DEPENDENCY_PENDING') OR classification IN('INTEGRITY_CONFLICT','PERMANENT_MALFORMED'))",[id,maxAttempts]),
  ...abandonExpiredIngress(db,id),
  db.statement("UPDATE pa_jobs SET state=CASE WHEN (SELECT state FROM pa_events WHERE id=?)='DLQ' THEN 'FAILED' ELSE 'PENDING' END,next_at=(SELECT next_at FROM pa_events WHERE id=?),lease_until=0 WHERE event_id=?",[id,id,id]),clearAssertions(db)]);
}
export async function recoverTransportEvent(db,id,{now,owner=uid(),archive=null,hook=null}={}){
 const prior=await db.first('SELECT state FROM pa_events WHERE id=?',id);
 if(!prior){const historical=archive?.lookupRecord?await archive.lookupRecord('event:'+id):null;if(historical)return {responseVersion:RESPONSE_VERSION,ingestion:{status:'DUPLICATE',durable:true,eventId:id,firstDurableAvailability:historical.available_at},projection:{consistency:'NOT_VALIDATED',state:'PENDING',reason:'archived_snapshot_requires_validation'}};return {responseVersion:RESPONSE_VERSION,ingestion:{status:'DEPENDENCY_PENDING',durable:false,eventId:id},projection:{consistency:'NOT_VALIDATED',state:'PENDING',reason:'transport_registration_missing'}};}
 if(prior?.state==='COMMITTED'){const r=await readRecord(db,'event:'+id,{archive});need(r,'pa_committed_event_unavailable');return commitEnvelope(db,r.wire,{now,archive,hook});}
 const lease=await leaseJob(db,'event:'+id,owner,{now});if(!lease)return {status:'LEASE_PENDING'};
 try{
  const event=await db.first('SELECT * FROM pa_events WHERE id=?',id);need(event,'pa_event_missing');
  if(event.state==='PARTIAL'&&event.expires_at<=now){await db.batch([jobFence(db,lease,now),db.statement("UPDATE pa_events SET state='EXPIRED',reason='transport_expired' WHERE id=? AND state='PARTIAL'",[id]),clearAssertions(db)]);await finishJob(db,lease,now);return {status:'EXPIRED'};}
  if(['STAGED','PENDING'].includes(event.state)&&event.expires_at<=now){await db.batch([jobFence(db,lease,now),db.statement("UPDATE pa_events SET state='DLQ',reason='complete_application_expired',classification='PERMANENT_EXPIRED' WHERE id=? AND state IN('STAGED','PENDING')",[id]),...abandonExpiredIngress(db,id),clearAssertions(db)]);await finishJob(db,lease,now,{state:'FAILED'});return {status:'DLQ',reason:'complete_application_expired',replayable:false};}
  if(event.state==='PARTIAL'){await finishJob(db,lease,now,{state:'PENDING',nextAt:now+1000});return {status:'PENDING',reason:'missing_fragments'};}
  if(['EXPIRED','CONFLICT','DLQ'].includes(event.state)){await finishJob(db,lease,now,{state:'FAILED'});return {status:event.state};}
  const pieces=new Map((await db.all('SELECT ordinal,wire FROM pa_fragments WHERE event_id=? ORDER BY ordinal LIMIT 51',id)).map(r=>[r.ordinal,r.wire]));
  const manifest=JSON.parse(event.manifest),wire=await reassembleMeasurementFragments(manifest,pieces,{receivedAt:now});await hook?.('ASSEMBLED',id);
  // The lease is checked in SQL before beginning recoverable ingress. Individual
  // ingress/commit transactions remain independently idempotent across lease loss.
  await db.batch([jobFence(db,lease,now),clearAssertions(db)]);
  const result=await commitEnvelope(db,wire,{now,archive,hook});const classification=classifyD1Response(result);
  if(classification.retry){await markTransportFailure(db,id,classification.classification,result.ingestion.status,{now,lease});return result;}
  await finishJob(db,lease,now);return result;
 }catch(error){const classification=/transport_|measurement_.*invalid|measurement_.*mismatch/.test(error.message)?'PERMANENT_MALFORMED':'DURABILITY_UNKNOWN';
  if(classification==='DURABILITY_UNKNOWN'){try{const durable=await db.first("SELECT id FROM pa_events WHERE id=? AND state='COMMITTED'",id);if(durable){const r=await readRecord(db,'event:'+id,{archive});if(r)return commitEnvelope(db,r.wire,{now,archive});}}catch{}}
  try{await markTransportFailure(db,id,classification,error.message,{now,lease});}catch{}return {responseVersion:RESPONSE_VERSION,ingestion:{status:classification==='PERMANENT_MALFORMED'?'REJECTED':'DURABILITY_UNKNOWN',durable:classification==='PERMANENT_MALFORMED'?false:null},projection:{consistency:'NOT_VALIDATED',state:'PENDING'},error:error.message};}
}
export async function consumeQueueBatch(db,batch,{now,hook=null,onIncident=null,archive=null}={}){
 need(batch&&Array.isArray(batch.messages)&&batch.messages.length<=64,'pa_queue_batch_bound');const outcomes=[];
 // Explicit individual ack; never ackAll. One failure cannot acknowledge siblings.
 for(const message of batch.messages){try{
  need(typeof message.ack==='function'&&typeof message.retry==='function','pa_queue_message');
  need(typeof message.body==='string','pa_queue_body_type');const wire=message.body;
  const outcome=await stageQueueFragment(db,wire,{now,hook,archive,publishedAt:message.timestamp instanceof Date?message.timestamp.getTime():null});message.ack();outcomes.push(outcome);
 }catch(error){message.retry({delaySeconds:30});outcomes.push({status:'RETRY',durable:null,error:error.message});try{await onIncident?.({category:'TRANSPORT_FAILURE',observed:true,unknownCount:true,reason:error.message});}catch{}}}
 return outcomes;
}
export async function authorizeReplay(db,id,{authorization,environment,now}={}){
 need(authorization?.operation==='replay'&&authorization.environment===environment&&authorization.expiresAt>now,'pa_replay_unauthorized');
 const event=await db.first('SELECT * FROM pa_events WHERE id=?',id);need(event&&!['EXPIRED','CONFLICT'].includes(event.state),'pa_replay_terminal');const prior=await db.first('SELECT bytes FROM pa_reservations WHERE id=?',event.reservation);need(prior,'pa_replay_reservation');
 const {settleReservationStatements,releaseReservation}=await import('./measurement-reservations.js'),reservation=await reserve(db,{id:'transport-replay:'+id+':'+(event.replays+1),workClass:'transport',owner:'replay:'+id,bytes:event.state==='COMMITTED'?estimateWrite(4096,8):prior.bytes,now});
 try{await db.batch([assertion(db,`? > MAX(?,${SQL_CLOCK}) AND EXISTS(SELECT 1 FROM pa_events WHERE id=? AND replays=? AND replays<4 AND state NOT IN('EXPIRED','CONFLICT') AND (state='COMMITTED' OR expires_at>MAX(?,${SQL_CLOCK})))`,authorization.expiresAt,now,id,event.replays,now),
 ...(reservation.state==='RESERVED'?commitReservationStatements(db,reservation,now):[]),
 db.statement("UPDATE pa_reservations SET state='SETTLED' WHERE id=? AND state='COMMITTED' AND EXISTS(SELECT 1 FROM pa_events WHERE id=? AND state='DLQ' AND reservation=?)",[event.reservation,id,event.reservation]),
 db.statement("DELETE FROM pa_ingress WHERE event_id=? AND state='ABANDONED'",[id]),db.statement("DELETE FROM pa_semantic_ingress WHERE event_id=? AND state='ABANDONED'",[id]),
 db.statement("UPDATE pa_events SET replays=replays+1,reservation=CASE WHEN state='COMMITTED' THEN reservation ELSE ? END,state=CASE WHEN state='DLQ' THEN CASE WHEN count=(SELECT COUNT(*) FROM pa_fragments WHERE event_id=?) THEN 'STAGED' ELSE 'PARTIAL' END ELSE state END,next_at=?,reason=NULL WHERE id=?",[reservation.id,id,now,id]),
 db.statement("UPDATE pa_jobs SET state='PENDING',next_at=?,lease_until=0,generation=generation+1,lease_nonce=lower(hex(randomblob(16))) WHERE event_id=?",[now,id]),
 ...(event.state==='COMMITTED'?settleReservationStatements(db,reservation):[]),clearAssertions(db)]);}catch(error){try{await releaseReservation(db,reservation,{now});}catch{}throw error;}return {status:'REPLAY_SCHEDULED',id};
}
