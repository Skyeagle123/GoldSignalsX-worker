// Portable immutable audit precedes removal of bounded operational rows.
import {appendIndependentRecord} from './measurement-independent-store.js';
import {serialize,hash,need,LIMITS} from './measurement-independent-contract.js';
import {assertion,clearAssertions,SQL_CLOCK} from './measurement-d1-adapter.js';
import {verifyRoot} from './measurement-archive.js';
import {verifyArchiveEdge} from './measurement-archive-lineage.js';
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
const candidateIdentity=p=>Object.fromEntries(Object.entries(p).filter(([k])=>!['retirement_proof','retirement_proof_generation'].includes(k)));
function candidateFence(db,p,context,now,boundary=null){return assertion(db,`EXISTS(SELECT 1 ${eligibleFrom} AND p.id=? AND p.owner=? AND p.generation=? AND p.lease_nonce=? AND p.lease_until=? AND p.snapshot IS ? AND p.manifest_key IS ? AND p.receipt_key IS ? AND p.reservation=? AND p.scope_id=? AND p.revision=? AND p.input_revision=? AND p.retirement_proof_generation=? AND m.epoch=? ${boundary?'AND m.archive_reference_revision=? AND m.catalogue_generation=? AND m.reference_sequence=?':''})`,now,p.id,p.owner,p.generation,p.lease_nonce,p.lease_until,p.snapshot,p.manifest_key,p.receipt_key,p.reservation,p.scope_id,p.revision,p.input_revision,p.retirement_proof_generation,context.epoch,...(boundary?[boundary.referenceRevision,boundary.catalogueGeneration,boundary.sequence]:[]));}
export async function retireStalePublications(db,{now,limit=16,archive=null,hook=null}={}){
 need(Number.isInteger(limit)&&limit>=1&&limit<=32,'pa_publication_retirement_bound');
 // Discard obsolete operational cursors in bounded batches, never evidence or
 // reservation liability. Each update tests the current durable boundary.
 await db.batch([db.statement(`UPDATE pa_publications SET retirement_proof=NULL,retirement_proof_generation=retirement_proof_generation+1 WHERE id IN(SELECT p.id FROM pa_publications p CROSS JOIN pa_meta m WHERE p.retirement_proof IS NOT NULL AND (p.state!='STALE' OR p.epoch!=m.epoch OR json_extract(p.retirement_proof,'$.context.format')!='measurement-retirement-proof-v2') LIMIT ?)`,[limit])]);
 const cursor=(await db.first('SELECT retirement_cursor FROM pa_meta')).retirement_cursor;
 const candidates=await db.all(`SELECT p.* ${eligibleFrom} ORDER BY CASE WHEN p.retirement_proof IS NULL THEN 1 ELSE 0 END,(p.id>?) DESC,p.id LIMIT ?`,now,cursor,limit);let retired=0,blocked=0,referenceReads=0,pending=0,referenceBacklog=0,attempted=0;
 const proofArchive=archive?{...archive,async read(key,options){need(referenceReads<512,'pa_retirement_reference_bound');referenceReads++;return archive.read(key,options);}}:null;
 for(const p of candidates){
  if(db.remainingStatements()<150||referenceReads>=512)break;
  attempted++;await db.batch([db.statement('UPDATE pa_meta SET retirement_cursor=?',[p.id])]);
  const meta=await db.first('SELECT epoch,catalogue_generation,archive_reference_revision,reference_sequence FROM pa_meta');
  const reservation=await db.first('SELECT r.*,m.epoch FROM pa_reservations r CROSS JOIN pa_meta m WHERE r.id=?',p.reservation);
  if(!reservation||reservation.owner!==p.id||reservation.generation!==1||reservation.class!=='archive'||!['COMMITTED','SETTLED'].includes(reservation.state)){blocked++;continue;}
  const context={format:'measurement-retirement-proof-v2',candidate:await hash(candidateIdentity(p)),epoch:meta.epoch,reservation:await hash([reservation.id,reservation.token,reservation.owner,reservation.generation,reservation.epoch,reservation.state,reservation.class]),namespace:archive?.namespace??null};
  const boundary={referenceRevision:meta.archive_reference_revision,catalogueGeneration:meta.catalogue_generation,sequence:meta.reference_sequence};
  const contextDigest=await hash(context);let proof=p.retirement_proof?JSON.parse(p.retirement_proof):null;
  if(proof){const {progressDigest,...payload}=proof;need(progressDigest===await hash(payload),'pa_retirement_progress_integrity');}
  if(!proof||proof.contextDigest!==contextDigest)proof={contextDigest,context,afterSequence:0,current:null,roots:0,anchors:[],complete:false};
  // An exact immutable prefix remains an absence proof for this candidate.
  // New registrations are obligations with increasing identities, not a reason
  // to discard the prefix. No generation number alone proves ancestry.
  proof.complete=false;proof.boundary=boundary;
  let referenced=false,unavailable=false;
  const header=r=>({key:r.key,epoch:r.epoch,generation:r.generation,previous:r.previous??null,lineageTransition:r.lineageTransition??null});
  const finishReference=()=>{
   if(proof.tip)proof.anchors=[proof.tip,...proof.anchors.filter(a=>a.key!==proof.tip.key&&a.key!==proof.matchedAnchor)].sort((a,b)=>b.generation-a.generation).slice(0,8);
   proof.afterSequence=proof.reference.sequence;proof.lastReference=proof.reference;proof.current=null;proof.child=null;proof.tip=null;proof.matchedAnchor=null;
  };
  try{while(!proof.complete&&referenceReads<512&&db.remainingStatements()>150){
   if(!proof.current){const next=await db.first('SELECT sequence,root_key FROM pa_archive_reference_work WHERE sequence>? AND sequence<=? ORDER BY sequence LIMIT 1',proof.afterSequence,boundary.sequence);
    if(!next){proof.afterSequence=boundary.sequence;proof.complete=true;break;}
    proof.reference={sequence:next.sequence,key:next.root_key};proof.current=next.root_key;proof.child=null;proof.tip=null;
   }
   if(!proofArchive){unavailable=true;break;}
   const anchor=proof.anchors.find(a=>a.key===proof.current);
   if(anchor){if(proof.child)await verifyArchiveEdge(proofArchive,proof.child,anchor);proof.matchedAnchor=anchor.key;finishReference();continue;}
   let root;try{root=await verifyRoot(proofArchive,proof.current);if(proof.child)await verifyArchiveEdge(proofArchive,proof.child,root);}catch(error){if(error.message==='pa_retirement_reference_bound')break;throw error;}
   const dependencies=[p.id,p.manifest_key,p.receipt_key].filter(Boolean);
   if(root.publication===p.id||root.manifestKey===p.manifest_key||root.receiptKey===p.receipt_key||dependencies.includes(root.supersedes)||dependencies.includes(root.verified.manifest.supersedes)||root.verified.records.some(r=>dependencies.includes(r.id)||(r.dependencies??[]).some(d=>dependencies.includes(d)))){referenced=true;break;}
   proof.tip??=header(root);proof.roots++;proof.child=header(root);proof.current=root.previous??null;
   if(!proof.current)finishReference();
  }}catch{unavailable=true;}
  if(referenced||unavailable){if(referenced)await db.batch([db.statement('UPDATE pa_publications SET retirement_proof=NULL,retirement_proof_generation=retirement_proof_generation+1 WHERE id=? AND retirement_proof_generation=?',[p.id,p.retirement_proof_generation])]);blocked++;continue;}
  // The row is the authenticated progress authority. A CAS generation plus exact
  // candidate identity rejects stale Workers and ABA. Saving historical progress
  // need not freeze registration; the final transaction checks the fresh boundary.
  {const {progressDigest,...payload}=proof;proof.progressDigest=await hash(payload);}
  try{await db.batch([candidateFence(db,p,context,now),db.statement('UPDATE pa_publications SET retirement_proof=?,retirement_proof_generation=retirement_proof_generation+1 WHERE id=?',[serialize(proof,8192),p.id]),clearAssertions(db)]);p.retirement_proof_generation++;p.retirement_proof=serialize(proof,8192);}catch(error){if(!/pa_fence_failed/.test(error.message))throw error;blocked++;continue;}
  await hook?.('PUBLICATION_PROOF_SAVED',{id:p.id,roots:proof.roots,complete:proof.complete});
  if(!proof.complete){pending++;referenceBacklog+=(await db.first('SELECT COUNT(*) count FROM pa_archive_reference_work WHERE sequence>?',proof.afterSequence)).count;continue;}
  await hook?.('PUBLICATION_PROOF_COMPLETE',p);
  // Progress fields do not change the immutable audit identity across retries.
  const wire=serialize({version:'measurement-operational-v1',table:'pa_publications',identity:p.id,state:candidateIdentity(p),meaning:'TERMINAL_METADATA_RETIREMENT_CANDIDATE'},LIMITS.eventBytes),recordId='operational:'+await hash(wire);
  try{await appendIndependentRecord(db,{id:recordId,kind:'OPERATIONAL_AUDIT',cohort:'maintenance',wire,createdAt:p.lease_until,category:'INCIDENT'},{now,workClass:'incidents'});}catch(error){if(!/pa_fence_failed/.test(error.message))throw error;blocked++;continue;}
  await hook?.('PUBLICATION_RETIREMENT_PREPARED',p);
  try{const result=await db.batch([
   candidateFence(db,p,context,now,boundary),
   assertion(db,"EXISTS(SELECT 1 FROM pa_publications WHERE id=? AND retirement_proof=? AND json_extract(retirement_proof,'$.complete')=1)",p.id,p.retirement_proof),
   assertion(db,'EXISTS(SELECT 1 FROM pa_records WHERE id=? AND ready=1)',recordId),
   assertion(db,"EXISTS(SELECT 1 FROM pa_reservations WHERE id=? AND class='archive' AND pool=? AND bytes=?)",reservation.id,reservation.pool,reservation.bytes),
   ...settleReservationStatements(db,reservation),
   db.statement("DELETE FROM pa_publications WHERE id=? AND state='STALE' AND generation=? AND lease_nonce=? RETURNING id",[p.id,p.generation,p.lease_nonce]),clearAssertions(db)]);
   retired+=result.at(-2).results.length;
  }catch(error){if(!/pa_fence_failed/.test(error.message))throw error;blocked++;}
 }
 pending+=candidates.length-attempted;
 return {scanned:attempted,selected:candidates.length,retired,blocked,pending,referenceBacklog,referenceBacklogScope:'PROCESSED_CANDIDATES',referenceReads,status:pending||blocked?'PENDING':'COMPLETE',reason:pending?'reference_proof_backlog':blocked?'reference_or_eligibility_pending':null,bounded:true,physicalReclamationCredit:0};
}
