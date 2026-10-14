import {LIMITS,need,bytes,identity,serialize,uid,time,hash} from './measurement-independent-contract.js';
const registered=new WeakMap();
const prepared=new WeakMap();
export const SQL_CLOCK="CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)";
// Bindings are acquired only by the dedicated Worker composition root.
export function createMeasurementD1Adapter(binding,{diagnostics=false,diagnosticLimit=64,statementBudget=null}={}){
 need(binding&&typeof binding.prepare==='function'&&typeof binding.batch==='function'&&typeof binding.getBookmark!=='function','pa_d1_binding_primary_required');
 need(statementBudget===null||Number.isInteger(statementBudget)&&statementBudget>=16&&statementBudget<=900,'pa_statement_budget');need(Number.isInteger(diagnosticLimit)&&diagnosticLimit>=0&&diagnosticLimit<=256,'pa_diagnostics_bound');
 const owner=Symbol('d1-statement-owner'),calls=[],metrics={statements:0,reads:0,batches:0,rowsRead:0,rowsWritten:0};
 const statement=(sql,values=[])=>{
  need(bytes(sql)<=LIMITS.statementBytes&&values.length<=LIMITS.parameters,'pa_sql_bound');
  for(const v of values)if(typeof v==='string')need(bytes(v)<=LIMITS.rowBytes,'pa_parameter_bound');
  need(statementBudget===null||metrics.statements<statementBudget,'pa_invocation_statement_budget');metrics.statements++;if(diagnostics&&diagnosticLimit){calls.push(sql);if(calls.length>diagnosticLimit)calls.shift();}
  const stmt=binding.prepare(sql).bind(...values);prepared.set(stmt,{owner,bytes:bytes(sql)+values.reduce((n,v)=>n+(typeof v==='string'?bytes(v):ArrayBuffer.isView(v)?v.byteLength:16),0)});return stmt;
 };
 const api=Object.freeze({statement,
  async first(sql,...v){metrics.reads++;const r=await statement(sql,v).first();metrics.rowsRead+=r?1:0;return r;},
  async all(sql,...v){metrics.reads++;const r=await statement(sql,v).all();metrics.rowsRead+=r.results?.length||0;return r.results||[];},
  async batch(statements){need(Array.isArray(statements)&&statements.length>0&&statements.length<=LIMITS.batch,'pa_batch_bound');need(statements.every(s=>prepared.get(s)?.owner===owner),'pa_statement_owner');need(statements.reduce((n,s)=>n+prepared.get(s).bytes,0)<=2000000,'pa_batch_byte_bound');metrics.batches++;const r=await binding.batch(statements);for(const x of r)metrics.rowsWritten+=x.meta?.rows_written??x.meta?.changes??0;return r;},
  remainingStatements:()=>statementBudget===null?Infinity:statementBudget-metrics.statements,metrics:()=>({...metrics,diagnosticCalls:calls.length}),diagnostics:()=>[...calls]});
 registered.set(api,true);return api;
}
export function requireD1(db){need(registered.has(db),'pa_adapter_required');}
export function assertion(db,sql,...values){requireD1(db);return db.statement(`INSERT INTO pa_assertions SELECT CASE WHEN (${sql}) THEN 1 ELSE 0 END`,values);}
export const clearAssertions=db=>db.statement('DELETE FROM pa_assertions');
export async function schemaReady(db){requireD1(db);const row=await db.first('SELECT schema_version,epoch FROM pa_meta WHERE singleton=1');need(row?.schema_version===7&&row.epoch,'pa_schema_not_ready');return row;}
export async function leaseJob(db,id,owner,{now,leaseMs=30000}={}){
 identity(id);identity(owner);time(now);need(Number.isInteger(leaseMs)&&leaseMs>=1&&leaseMs<=60000,'pa_lease_bound');
 const result=await db.batch([db.statement(`UPDATE pa_jobs SET owner=?,generation=generation+1,lease_nonce=lower(hex(randomblob(16))),lease_until=MAX(?,${SQL_CLOCK})+?,state='RUNNING',attempts=MIN(attempts+1,1000000) WHERE id=? AND state IN('PENDING','RUNNING') AND next_at<=MAX(?,${SQL_CLOCK}) AND lease_until<=MAX(?,${SQL_CLOCK}) RETURNING *,(SELECT epoch FROM pa_meta) AS epoch`,[owner,now,leaseMs,id,now,now])]);
 const row=result[0].results?.[0];return row?Object.freeze({id,owner,generation:row.generation,nonce:row.lease_nonce,epoch:row.epoch,expiresAt:row.lease_until}):null;
}
export function jobFence(db,lease,now){time(now);return assertion(db,`EXISTS(SELECT 1 FROM pa_jobs j,pa_meta m WHERE j.id=? AND j.owner=? AND j.generation=? AND j.lease_nonce=? AND j.state='RUNNING' AND j.lease_until>MAX(?,${SQL_CLOCK}) AND m.epoch=?)`,lease.id,lease.owner,lease.generation,lease.nonce,now,lease.epoch);}
export async function finishJob(db,lease,now,{state='DONE',nextAt=now}={}){need(['DONE','PENDING','FAILED'].includes(state),'pa_job_state');await db.batch([jobFence(db,lease,now),db.statement('UPDATE pa_jobs SET state=?,next_at=?,lease_until=0,owner=NULL WHERE id=?',[state,nextAt,lease.id]),clearAssertions(db)]);}
export function scopeFence(db,snapshot,{requireComplete=true,now=Date.now()}={}){
 const s=snapshot.scope;need(s&&snapshot.revisionEpoch&&Number.isSafeInteger(snapshot.validatedRevision),'pa_snapshot_required');
 const clauses=requireComplete?"AND p.source_revision=s.revision AND p.epoch=m.epoch AND p.digest=? AND p.integrity='COMPLETE'":"";
 return assertion(db,`EXISTS(SELECT 1 FROM pa_scopes s,pa_meta m LEFT JOIN pa_projections p ON p.scope_id=? WHERE s.id=? AND s.cohort=? AND m.epoch=? AND s.revision=? AND s.pending=0 AND s.quarantine=0 AND s.conflict=0 AND s.corrections=0 AND s.recovery_required=0 AND NOT EXISTS(SELECT 1 FROM pa_bindings b JOIN pa_scopes dep ON dep.id=b.evaluation_id OR dep.id=b.signal_id WHERE (b.evaluation_id=s.id OR b.signal_id=s.id) AND (dep.pending!=0 OR dep.conflict!=0 OR dep.quarantine!=0 OR dep.corrections!=0 OR dep.recovery_required!=0)) AND COALESCE(s.available_at,0)<=? AND COALESCE((SELECT revision FROM pa_cohort_state WHERE cohort=s.cohort),0)=? AND NOT EXISTS(SELECT 1 FROM pa_cohort_state WHERE cohort=s.cohort AND (pending!=0 OR conflict!=0)) ${clauses})`,s.id,s.id,s.cohort,snapshot.revisionEpoch,snapshot.validatedRevision,time(now),snapshot.marketRevision??0,...(requireComplete?[snapshot.projectionDigest]:[]));
}
export async function protectedEffect(db,snapshot,id,payload,{now=Date.now(),lease=null}={}){
 identity(id);need(snapshot.responseVersion==='measurement-d1-response-v1'&&snapshot.definitionVersion==='engine-v1'&&snapshot.consistency==='VALIDATED_AT_REVISION','pa_snapshot_contract');need(snapshot.payload&&await hash(serialize(snapshot.payload))===snapshot.projectionDigest,'pa_snapshot_payload_digest');
 // Early rejection saves capacity; the mutation still repeats every SQL fence.
 await db.batch([scopeFence(db,snapshot,{now}),...(lease?[jobFence(db,lease,now)]:[]),clearAssertions(db)]);
 const {reserve,estimateWrite,commitReservationStatements,settleReservationStatements,releaseReservation}=await import('./measurement-reservations.js'),reservation=await reserve(db,{id:'effect:'+id,workClass:'processing',owner:id,bytes:estimateWrite(serialize(payload).length,4),now});
 const statements=[scopeFence(db,snapshot,{now})];if(lease)statements.push(jobFence(db,lease,now));if(reservation.state==='RESERVED')statements.push(...commitReservationStatements(db,reservation,now));
 statements.push(assertion(db,'NOT EXISTS(SELECT 1 FROM pa_effects WHERE id=?) OR EXISTS(SELECT 1 FROM pa_effects WHERE id=? AND scope_id=? AND payload=?)',id,id,snapshot.scope.id,serialize(payload)),db.statement('INSERT INTO pa_effects(id,scope_id,epoch,revision,payload,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING',[id,snapshot.scope.id,snapshot.revisionEpoch,snapshot.validatedRevision,serialize(payload),now]),...settleReservationStatements(db,reservation),clearAssertions(db));
 try{await db.batch(statements);}catch(error){try{await releaseReservation(db,reservation,{now});}catch{}throw error;}return {status:'COMMITTED',id};
}
