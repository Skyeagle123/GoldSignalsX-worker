// Portable immutable audit precedes removal of bounded operational rows.
import {appendIndependentRecord} from './measurement-independent-store.js';
import {serialize,hash,need,LIMITS} from './measurement-independent-contract.js';
import {assertion,clearAssertions,SQL_CLOCK} from './measurement-d1-adapter.js';
import {verifyRoot} from './measurement-archive.js';
import {settleReservationStatements} from './measurement-reservations.js';
export async function compactOperationalRows(db,{now,limit=16,archive=null,hook=null}={}){
 need(Number.isSafeInteger(limit)&&limit>=1&&limit<=32,'pa_operational_cleanup_bound');let count=0;const publications=await retireStalePublications(db,{now,limit,archive,hook});count+=publications.retired;
 for(const [table,predicate] of [['pa_restores',"state='COMPLETE' AND NOT EXISTS(SELECT 1 FROM pa_archive_lineage WHERE restore_id=pa_restores.id)"],['pa_format_work',"state='COMPLETE'"]]){
  const rows=await db.all(`SELECT * FROM ${table} WHERE ${predicate} ORDER BY id LIMIT ?`,limit);
  for(const row of rows){if(db.remainingStatements()<100)break;const recordId='operational:'+await hash([table,row]),wire=serialize({version:'measurement-operational-v1',table,identity:row.id,state:row});
   await appendIndependentRecord(db,{id:recordId,kind:'OPERATIONAL_AUDIT',cohort:'maintenance',wire,createdAt:now,category:'INCIDENT'},{now,workClass:'incidents'});
   const guard=table==='pa_format_work'?'id=? AND state=?':'id=? AND state=? AND generation=? AND lease_nonce=?';const values=table==='pa_format_work'?[row.id,row.state]:[row.id,row.state,row.generation,row.lease_nonce];
   await db.batch([assertion(db,'EXISTS(SELECT 1 FROM pa_records WHERE id=? AND ready=1)',recordId),db.statement(`DELETE FROM ${table} WHERE ${guard}`,values),clearAssertions(db)]);count++;
  }
 }
 // Protected SQL effects are not discarded until included in a verified archive.
 const effects=await db.all("SELECT * FROM pa_effects WHERE NOT EXISTS(SELECT 1 FROM pa_records WHERE id='effect:'||pa_effects.id AND ready=1) LIMIT ?",limit);
 for(const effect of effects)await appendIndependentRecord(db,{id:'effect:'+effect.id,kind:'PROTECTED_EFFECT',scopeId:effect.scope_id,cohort:(await db.first('SELECT cohort FROM pa_scopes WHERE id=?',effect.scope_id))?.cohort??'maintenance',payload:effect,createdAt:effect.created_at,category:'INCIDENT'},{now,workClass:'incidents'});
 return {captured:count,effectsCaptured:effects.length,publications};
}

const registeredRoots=`SELECT root_key FROM pa_meta WHERE root_key IS NOT NULL
 UNION SELECT archive_root FROM pa_scopes WHERE archive_root IS NOT NULL
 UNION SELECT root_key FROM pa_publications WHERE root_key IS NOT NULL
 UNION SELECT root_key FROM pa_restores WHERE root_key IS NOT NULL
 UNION SELECT source_root FROM pa_archive_lineage
 UNION SELECT old_root FROM pa_format_work WHERE old_root IS NOT NULL
 UNION SELECT new_root FROM pa_format_work WHERE new_root IS NOT NULL`;
const scopes=`(s.id=p.scope_id OR s.id IN(SELECT json_extract(value,'$.id') FROM json_each(json_extract(p.snapshot,'$.dependencyScopes'))))`;
// Repeat this entire eligibility predicate inside the deletion transaction.
// Expiry alone is insufficient: a valid unchanged publication is resumable.
const retirementEligible=`p.state='STALE' AND p.root_key IS NULL AND p.lease_until<=clock.at AND p.epoch=m.epoch
 AND (NOT EXISTS(SELECT 1 FROM pa_scopes s WHERE s.id=p.scope_id AND s.revision=p.input_revision AND s.archive_revision=p.revision)
 OR EXISTS(SELECT 1 FROM json_each(json_extract(p.snapshot,'$.dependencyScopes')) x LEFT JOIN pa_scopes s ON s.id=json_extract(x.value,'$.id') WHERE s.id IS NULL OR s.revision!=json_extract(x.value,'$.revision') OR s.archive_revision!=json_extract(x.value,'$.archive_revision')))
 AND NOT EXISTS(SELECT 1 FROM pa_scopes s WHERE ${scopes} AND (s.pending!=0 OR s.corrections!=0 OR s.quarantine!=0 OR s.conflict!=0 OR s.recovery_required!=0))
 AND NOT EXISTS(SELECT 1 FROM pa_pins pin JOIN pa_scopes s ON s.id=pin.scope_id WHERE ${scopes} AND pin.until_at>clock.at)
 AND NOT EXISTS(SELECT 1 FROM pa_corrections c WHERE c.state IN('PENDING','CONFLICT','EVIDENCE_UNAVAILABLE','UNREDUCIBLE') AND (c.scope_id IN(SELECT s.id FROM pa_scopes s WHERE ${scopes}) OR c.predecessor IN(SELECT json_extract(value,'$.id') FROM json_each(json_extract(p.snapshot,'$.membership'))) OR EXISTS(SELECT 1 FROM json_each(c.dependencies) d JOIN json_each(json_extract(p.snapshot,'$.membership')) r ON d.value=json_extract(r.value,'$.id'))))
 AND NOT EXISTS(SELECT 1 FROM pa_jobs j WHERE j.state IN('PENDING','RUNNING') AND (j.event_id=p.id OR j.id='root-checkpoint:'||p.id OR (j.class!='archive' AND j.scope_id IN(SELECT s.id FROM pa_scopes s WHERE ${scopes}))))
 AND NOT EXISTS(SELECT 1 FROM pa_incidents i WHERE i.state!='CLOSED' AND (i.scope_id IN(SELECT s.id FROM pa_scopes s WHERE ${scopes}) OR instr(i.payload,p.id)>0 OR instr(i.payload,p.manifest_key)>0 OR instr(i.payload,p.receipt_key)>0))
 AND NOT EXISTS(SELECT 1 FROM pa_restores WHERE state!='COMPLETE' OR root_key=p.manifest_key OR root_key=p.receipt_key)
 AND NOT EXISTS(SELECT 1 FROM pa_format_work WHERE state!='COMPLETE' OR new_manifest=p.manifest_key OR new_receipt=p.receipt_key OR old_root=p.manifest_key OR new_root=p.manifest_key)
 AND NOT EXISTS(SELECT 1 FROM pa_records WHERE archive_key=p.manifest_key OR archive_key=p.receipt_key)
 AND NOT EXISTS(SELECT 1 FROM pa_publications other WHERE other.id!=p.id AND (other.reservation=p.reservation OR other.manifest_key=p.manifest_key OR other.receipt_key=p.receipt_key OR instr(COALESCE(other.snapshot,''),p.id)>0))`;
const eligibleFrom=`FROM pa_publications p CROSS JOIN pa_meta m CROSS JOIN (SELECT MAX(?,${SQL_CLOCK}) AS at) clock WHERE ${retirementEligible}`;
export async function retireStalePublications(db,{now,limit=16,archive=null,hook=null}={}){
 need(Number.isInteger(limit)&&limit>=1&&limit<=32,'pa_publication_retirement_bound');
 const candidates=await db.all(`SELECT p.* ${eligibleFrom} ORDER BY p.id LIMIT ?`,now,limit);let retired=0,blocked=0,referenceReads=0;const proofArchive=archive?{...archive,async read(key,options){need(++referenceReads<=512,'pa_retirement_reference_bound');return archive.read(key,options);}}:null;
 for(const p of candidates){
  if(db.remainingStatements()<150||referenceReads>=512)break;
  const roots=(await db.all(`SELECT root_key FROM (${registeredRoots}) ORDER BY root_key LIMIT 129`)).map(r=>r.root_key);
  if(roots.length>128||roots.length&&!archive){blocked++;continue;}
  // Every registered root and its complete predecessor chain is retained. Only
  // metadata is retired; immutable R2 objects are never deleted here.
  let referenced=false;const visited=new Set();try{for(const key of roots){let current=key;for(let depth=0;current;depth++){need(depth<64&&visited.size<256,'pa_retirement_reference_bound');if(visited.has(current))break;visited.add(current);const r=await verifyRoot(proofArchive,current);if(r.publication===p.id||r.manifestKey===p.manifest_key||r.receiptKey===p.receipt_key){referenced=true;break;}current=r.previous;}if(referenced)break;}}catch{blocked++;continue;}
  if(referenced){blocked++;continue;}
  const reservation=await db.first('SELECT r.*,m.epoch FROM pa_reservations r CROSS JOIN pa_meta m WHERE r.id=?',p.reservation);
  if(!reservation||reservation.owner!==p.id||reservation.generation!==1||reservation.class!=='archive'||!['COMMITTED','SETTLED'].includes(reservation.state)){blocked++;continue;}
  const wire=serialize({version:'measurement-operational-v1',table:'pa_publications',identity:p.id,state:p,meaning:'TERMINAL_METADATA_RETIREMENT_CANDIDATE'},LIMITS.eventBytes),recordId='operational:'+await hash(wire);
  try{await appendIndependentRecord(db,{id:recordId,kind:'OPERATIONAL_AUDIT',cohort:'maintenance',wire,createdAt:p.lease_until,category:'INCIDENT'},{now,workClass:'incidents'});}catch(error){if(!/pa_fence_failed/.test(error.message))throw error;blocked++;continue;}
  await hook?.('PUBLICATION_RETIREMENT_PREPARED',p);
  try{const result=await db.batch([
   assertion(db,`EXISTS(SELECT 1 ${eligibleFrom} AND p.id=? AND p.owner=? AND p.generation=? AND p.lease_nonce=? AND p.lease_until=? AND p.snapshot IS ? AND p.manifest_key IS ? AND p.receipt_key IS ? AND p.reservation=? AND p.scope_id=? AND p.revision=? AND p.input_revision=?)`,now,p.id,p.owner,p.generation,p.lease_nonce,p.lease_until,p.snapshot,p.manifest_key,p.receipt_key,p.reservation,p.scope_id,p.revision,p.input_revision),
   assertion(db,`NOT EXISTS(SELECT root_key FROM (${registeredRoots}) WHERE root_key NOT IN(SELECT value FROM json_each(?)))`,serialize(roots)),
   assertion(db,'EXISTS(SELECT 1 FROM pa_records WHERE id=? AND ready=1)',recordId),
   ...settleReservationStatements(db,reservation),
   db.statement("DELETE FROM pa_publications WHERE id=? AND state='STALE' AND generation=? AND lease_nonce=? RETURNING id",[p.id,p.generation,p.lease_nonce]),clearAssertions(db)]);
   retired+=result.at(-2).results.length;
  }catch(error){if(!/pa_fence_failed/.test(error.message))throw error;blocked++;}
 }
 return {scanned:candidates.length,retired,blocked,bounded:true,physicalReclamationCredit:0};
}
