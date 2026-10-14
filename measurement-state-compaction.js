// Portable immutable audit precedes removal of bounded operational rows.
import {appendIndependentRecord} from './measurement-independent-store.js';
import {serialize,hash,need} from './measurement-independent-contract.js';
import {assertion,clearAssertions} from './measurement-d1-adapter.js';
export async function compactOperationalRows(db,{now,limit=16}={}){
 need(Number.isSafeInteger(limit)&&limit>=1&&limit<=32,'pa_operational_cleanup_bound');let count=0;
 for(const [table,predicate] of [['pa_publications',"state='STALE' AND NOT EXISTS(SELECT 1 FROM pa_jobs WHERE id='root-checkpoint:'||pa_publications.id AND state IN('PENDING','RUNNING'))"],['pa_restores',"state='COMPLETE'"],['pa_format_work',"state='COMPLETE'"]]){
  const rows=await db.all(`SELECT * FROM ${table} WHERE ${predicate} ORDER BY id LIMIT ?`,limit);
  for(const row of rows){const recordId='operational:'+await hash([table,row]),wire=serialize({version:'measurement-operational-v1',table,identity:row.id,state:row});
   await appendIndependentRecord(db,{id:recordId,kind:'OPERATIONAL_AUDIT',cohort:'maintenance',wire,createdAt:now,category:'INCIDENT'},{now,workClass:'incidents'});
   const guard=table==='pa_format_work'?'id=? AND state=?':'id=? AND state=? AND generation=? AND lease_nonce=?';const values=table==='pa_format_work'?[row.id,row.state]:[row.id,row.state,row.generation,row.lease_nonce];
   await db.batch([assertion(db,'EXISTS(SELECT 1 FROM pa_records WHERE id=? AND ready=1)',recordId),...(table==='pa_publications'?[db.statement("UPDATE pa_reservations SET state='SETTLED' WHERE id=? AND state='COMMITTED' AND EXISTS(SELECT 1 FROM pa_publications WHERE id=? AND state='STALE' AND generation=? AND lease_nonce=?)",[row.reservation,row.id,row.generation,row.lease_nonce])]:[]),db.statement(`DELETE FROM ${table} WHERE ${guard}`,values),clearAssertions(db)]);count++;
  }
 }
 // Protected SQL effects are not discarded until included in a verified archive.
 const effects=await db.all("SELECT * FROM pa_effects WHERE NOT EXISTS(SELECT 1 FROM pa_records WHERE id='effect:'||pa_effects.id AND ready=1) LIMIT ?",limit);
 for(const effect of effects)await appendIndependentRecord(db,{id:'effect:'+effect.id,kind:'PROTECTED_EFFECT',scopeId:effect.scope_id,cohort:(await db.first('SELECT cohort FROM pa_scopes WHERE id=?',effect.scope_id))?.cohort??'maintenance',payload:effect,createdAt:effect.created_at,category:'INCIDENT'},{now,workClass:'incidents'});
 return {captured:count,effectsCaptured:effects.length};
}
