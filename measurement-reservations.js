import {need,identity,time,LIMITS} from './measurement-independent-contract.js';
import {assertion,clearAssertions,requireD1,SQL_CLOCK} from './measurement-d1-adapter.js';
export const BUDGET=Object.freeze({normal:4000000000,hard:5500000000,reserve:1500000000,
 partitions:Object.freeze({incidents:200000000,catchup:600000000,archive:400000000,restore:300000000})});
const classes={ingestion:'normal',transport:'normal',processing:'normal',corrections:'normal',reporting:'normal',incidents:'incidents',catchup:'catchup',archive:'archive',restore:'restore'};
// Conservative model: 16x serialized payload + per-row/index/temp allowances.
// This is an admission ceiling model; physical Cloud-D1 amplification is a Package-B gate.
export function estimateWrite(payloadBytes,rows=1){need(Number.isSafeInteger(payloadBytes)&&payloadBytes>=0&&Number.isSafeInteger(rows)&&rows>=1&&rows<=8192,'pa_estimate');return payloadBytes*16+rows*16384;}
export async function reserve(db,{id,workClass,bytes,owner,generation=1,now,ttl=60000}){
 requireD1(db);identity(id);identity(owner);time(now);need(classes[workClass]&&Number.isSafeInteger(bytes)&&bytes>0&&Number.isSafeInteger(generation)&&generation>0&&ttl>0&&ttl<=86400000,'pa_reservation');
 const pool=classes[workClass],capacity=pool==='normal'?BUDGET.normal:BUDGET.partitions[pool];
 let result;try{result=await db.batch([
  assertion(db,`NOT EXISTS(SELECT 1 FROM pa_reservations WHERE id=?) OR EXISTS(SELECT 1 FROM pa_reservations WHERE id=? AND class=? AND bytes=? AND owner=? AND generation=? AND state!='RELEASED')`,id,id,workClass,bytes,owner,generation),
  assertion(db,`EXISTS(SELECT 1 FROM pa_reservations WHERE id=? AND state!='SETTLED') OR (((SELECT COUNT(*) FROM pa_reservations)<${LIMITS.reservations} OR EXISTS(SELECT 1 FROM pa_reservations WHERE id=?)) AND (SELECT physical_charged FROM pa_meta)+(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE state='RESERVED')+?<=? AND (SELECT charged FROM pa_pool_use WHERE pool=?)+(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE pool=? AND state='RESERVED')+?<=?)`,id,id,bytes,BUDGET.hard,pool,pool,bytes,capacity),
  db.statement("INSERT INTO pa_reservations(id,class,pool,bytes,owner,generation,expires_at,state) VALUES(?,?,?,?,?,?,?,'RESERVED') ON CONFLICT(id) DO UPDATE SET state='RESERVED',token=lower(hex(randomblob(16))),expires_at=excluded.expires_at WHERE pa_reservations.state='SETTLED'",[id,workClass,pool,bytes,owner,generation,now+ttl]),
  clearAssertions(db),db.statement('SELECT r.*,m.epoch FROM pa_reservations r CROSS JOIN pa_meta m WHERE r.id=?',[id])]);
 }catch(error){if(workClass!=='incidents'){try{const {recordIncident}=await import('./measurement-incidents.js');error.incident=await recordIncident(db,{category:'RESOURCE_REJECTION',observed:true,unknownCount:true,reason:'admission_failed',workClass,requestedModelBytes:bytes},{now});}catch{error.incident={durable:null,reason:'incident_writer_unavailable'};}}throw error;}
 return result.at(-1).results[0];
}
export function reservationFence(db,r,now){return assertion(db,`EXISTS(SELECT 1 FROM pa_reservations r CROSS JOIN pa_meta m WHERE r.id=? AND r.owner=? AND r.generation=? AND r.token=? AND m.epoch=? AND r.state='RESERVED' AND r.expires_at>MAX(?,${SQL_CLOCK}))`,r.id,r.owner,r.generation,r.token,r.epoch,time(now));}
export function commitReservationStatements(db,r,now){return [reservationFence(db,r,now),db.statement('UPDATE pa_meta SET physical_charged=physical_charged+(SELECT bytes FROM pa_reservations WHERE id=?)',[r.id]),db.statement('UPDATE pa_pool_use SET charged=charged+(SELECT bytes FROM pa_reservations WHERE id=?) WHERE pool=?',[r.id,r.pool]),db.statement("UPDATE pa_reservations SET state='COMMITTED' WHERE id=?",[r.id])];}
export async function releaseReservation(db,r,{now,recovery=false}={}){
 const condition=recovery?' AND expires_at<=?':'';
 await db.batch([assertion(db,`EXISTS(SELECT 1 FROM pa_reservations r CROSS JOIN pa_meta m WHERE r.id=? AND r.owner=? AND r.generation=? AND r.token=? AND m.epoch=?${condition})`,r.id,r.owner,r.generation,r.token,r.epoch,...(recovery?[time(now)]:[])),db.statement("UPDATE pa_reservations SET state='RELEASED' WHERE id=? AND state='RESERVED'",[r.id]),clearAssertions(db)]);
}
export async function recoverReservations(db,now,{limit=64}={}){need(limit>=1&&limit<=64,'pa_recovery_bound');const rows=await db.all("SELECT r.*,m.epoch FROM pa_reservations r CROSS JOIN pa_meta m WHERE r.state='RESERVED' AND r.expires_at<=? LIMIT ?",time(now),limit);for(const r of rows)await releaseReservation(db,r,{now,recovery:true});return rows.length;}
export async function resourceSnapshot(db){return {model:BUDGET,meta:await db.first('SELECT physical_charged,physical_observed,resource_revision,deletion_generation,epoch FROM pa_meta'),pools:await db.all("SELECT pool,state,SUM(bytes) bytes,COUNT(*) count FROM pa_reservations GROUP BY pool,state"),physicalReclamationCredit:0,cloudQualification:'REQUIRED'};}

// Settlement ends future liability, not physical allocation. No byte credit is
// granted here or by DELETE. A later, separately qualified observation may retire
// unused estimates while retaining every unfinished COMMITTED liability.
export function settleReservationStatements(db,r){return [assertion(db,"EXISTS(SELECT 1 FROM pa_reservations r,pa_meta m WHERE r.id=? AND r.token=? AND r.owner=? AND r.generation=? AND m.epoch=? AND r.state IN('COMMITTED','SETTLED'))",r.id,r.token,r.owner,r.generation,r.epoch),db.statement("UPDATE pa_reservations SET state='SETTLED' WHERE id=? AND state='COMMITTED'",[r.id])];}
export async function capturePhysicalObservation(db){const result=await db.batch([db.statement('SELECT epoch,resource_revision,deletion_generation FROM pa_meta')]),row=result[0].results[0],physicalBytes=result[0].meta?.size_after;need(Number.isSafeInteger(physicalBytes)&&physicalBytes>=0,'pa_physical_measurement_unavailable');return {...row,physicalBytes,basis:'QUALIFICATION_REQUIRED_D1_SIZE_AFTER'};}
export async function reconcileQualifiedPhysicalObservation(db,observation,{qualificationDigest,now}={}){
 need(Number.isSafeInteger(observation.physicalBytes)&&observation.physicalBytes>=0&&typeof qualificationDigest==='string'&&qualificationDigest.length===64,'pa_physical_observation');time(now);
 const liability="(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE state='COMMITTED')",reserved="(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE state='RESERVED')";
 await db.batch([assertion(db,`EXISTS(SELECT 1 FROM pa_meta m,pa_resource_qualification q,pa_environment e WHERE m.epoch=? AND m.resource_revision=? AND m.deletion_generation=? AND q.resource_id=e.resource_id AND q.qualification_digest=? AND q.size_after_conservative=1 AND (m.deletion_generation=0 OR q.reuse_verified=1))`,observation.epoch,observation.resource_revision,observation.deletion_generation,qualificationDigest),
 assertion(db,`?+${liability}+${reserved}<=?`,observation.physicalBytes,BUDGET.hard),
 db.statement(`UPDATE pa_meta SET physical_observed=?,physical_charged=?+${liability},resource_revision=resource_revision+1`,[observation.physicalBytes,observation.physicalBytes]),
 db.statement("UPDATE pa_pool_use SET charged=CASE WHEN pool='normal' THEN ?+(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE state='COMMITTED' AND pool='normal') ELSE (SELECT COALESCE(SUM(bytes),0) FROM pa_reservations r WHERE r.state='COMMITTED' AND r.pool=pa_pool_use.pool) END",[observation.physicalBytes]),clearAssertions(db)]);
 return {status:'QUALIFIED_ALLOCATION_RECONCILED',physicalBytes:observation.physicalBytes,unfinishedLiabilityPreserved:true,physicalClassAttribution:'UNATTRIBUTED_CONSERVATIVE_NORMAL_BASE',qualificationDigest};
}
