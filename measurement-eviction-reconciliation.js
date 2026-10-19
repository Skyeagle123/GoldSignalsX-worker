// Historical archive proof is not deletion authority. Only eviction's final SQL
// transaction may consume READY, against the exact still-current durable state.
import {need,hash,serialize,identity,time,uid} from './measurement-independent-contract.js';
import {assertion,clearAssertions,SQL_CLOCK,schemaReady} from './measurement-d1-adapter.js';
import {verifyRoot,verifyArchiveLineage} from './measurement-archive.js';
import {verifyArchiveEdge} from './measurement-archive-lineage.js';
import {catalogueLookup} from './measurement-archive-catalog.js';
const FORMAT='measurement-eviction-reconciliation-v1',TTL=60000;
const header=r=>({key:r.key,previous:r.previous,epoch:r.epoch,generation:r.generation,lineageTransition:r.lineageTransition??null});
export async function decodeEvictionProgress(wire){
 need(typeof wire==='string','pa_eviction_reconciliation_required');const e=JSON.parse(wire);
 need(e.state?.format===FORMAT&&e.digest===await hash(serialize(e.state,8192)),'pa_eviction_progress_integrity');return e.state;
}
export const encodeEvictionProgress=async state=>serialize({state,digest:await hash(serialize(state,8192))},8192);
export const evictionToken=p=>({publicationId:p.id,generation:p.eviction_generation,nonce:p.eviction_nonce});
function match(p,t){return t?.publicationId===p.id&&t.generation===p.eviction_generation&&t.nonce===p.eviction_nonce;}
export function reconciliationFence(db,p,now){return assertion(db,`EXISTS(SELECT 1 FROM pa_publications p,pa_meta m WHERE p.id=? AND p.state='EVICTION_ELIGIBLE' AND p.epoch=m.epoch AND p.eviction_generation=? AND p.eviction_owner=? AND p.eviction_nonce=? AND p.eviction_progress=? AND p.eviction_until>MAX(?,${SQL_CLOCK}))`,p.id,p.eviction_generation,p.eviction_owner,p.eviction_nonce,p.eviction_progress,time(now));}
export function reconciliationBoundaryFence(db,p,s){const c=s.context;return assertion(db,`EXISTS(SELECT 1 FROM pa_publications p,pa_meta m WHERE p.id=? AND p.scope_id=? AND p.domain=? AND p.catalogue_generation=? AND m.schema_version=12 AND p.epoch=? AND p.epoch=m.epoch AND p.root_key=? AND p.manifest_key=? AND p.receipt_key=? AND p.revision=? AND p.input_revision=? AND p.generation=? AND p.owner=? AND p.lease_nonce=? AND p.snapshot=? AND m.root_key=? AND m.catalogue_generation=? AND m.archive_reference_revision=?)`,p.id,c.scopeId,c.domain,c.publicationCatalogueGeneration,c.epoch,c.publicationRoot,c.manifest,c.receipt,c.archiveRevision,c.sourceRevision,c.publicationGeneration,c.publicationOwner,c.publicationNonce,p.snapshot,c.root,c.catalogueGeneration,c.referenceRevision);}
export async function loadEvictionReconciliation(db,p,token,{now,completed=false}={}){
 const s=await decodeEvictionProgress(p.eviction_progress);
 if(completed&&s.phase==='EVICTED'){
  need(token?.publicationId===p.id&&token.nonce===p.eviction_nonce&&token.generation===s.completedFrom,'pa_eviction_reconciliation_token');return {state:s,result:s.result};
 }
 need(match(p,token)&&s.phase==='READY','pa_eviction_reconciliation_token');
 await db.batch([reconciliationFence(db,p,now),reconciliationBoundaryFence(db,p,s),clearAssertions(db)]);return {state:s};
}
function memberMatches(entry,member,p){
 need(entry&&!entry.lookupType&&entry.packageKey===p.manifest_key&&entry.receiptKey===p.receipt_key,'pa_eviction_membership');
 for(const k of ['id','digest','bytes','created_at','available_at','kind','cohort','definition','scope_id','semantic_key','codec','chunk_count','encoded_bytes'])need(entry[k]===member[k],'pa_eviction_membership');
 need(serialize(entry.dependencies??[])===serialize(member.dependencies??[]),'pa_eviction_membership');
}
export function verifyPublicationRootIdentity(p,root){
 need(root.key===p.root_key&&root.manifestKey===p.manifest_key&&root.receiptKey===p.receipt_key&&root.epoch===p.epoch&&root.sourceRevision===p.revision&&root.scopeId===p.scope_id&&serialize(root.verified.snapshot)===p.snapshot,'pa_eviction_publication_identity');
}
async function retainedMember(archive,indexRoot,member,p){
 memberMatches(await catalogueLookup(archive,indexRoot,member.id),member,p);
 const semantic=await catalogueLookup(archive,indexRoot,'semantic:'+member.semantic_key);
 need(semantic?.lookupType==='SEMANTIC'&&semantic.recordId===member.id,'pa_eviction_semantic_membership');
 memberMatches({...semantic,id:member.id,lookupType:undefined},member,p);
}
export async function recheckRetainedPublication(archive,p,old,current){
 verifyPublicationRootIdentity(p,old);
 // 128 members x two paths x at most 32 nodes, plus two packages (<=550
 // reads in the caller): at most 8,742 reads, within the accepted 9,000 limit.
 for(const member of old.verified.snapshot.membership)await retainedMember(archive,current.indexRoot,member,p);
}
// At most 1,024 proof reads per segment. Membership needs two atomic packages
// (each <=272 reads plus root metadata) and at most six catalogue paths (<=34).
export async function reconcilePublicationEvictionAuthority(db,archive,publicationId,{now,owner=uid(),continuation=null,hook=null}={}){
 time(now);identity(owner);identity(publicationId);need((await schemaReady(db)).schema_version===12,'pa_eviction_schema_required');
 let p=await db.first('SELECT * FROM pa_publications WHERE id=?',publicationId);need(p?.state==='EVICTION_ELIGIBLE'&&p.root_key&&p.parity_verified===1,'pa_eviction_not_verified');
 let saved=p.eviction_progress?await decodeEvictionProgress(p.eviction_progress):null;
 if(saved?.phase==='EVICTED')return {status:'EVICTED',...saved.result};
 const m=await db.first('SELECT epoch,root_key,catalogue_generation,archive_reference_revision FROM pa_meta');
 need(m.epoch===p.epoch&&m.root_key,'pa_eviction_epoch');
 const context={namespace:archive.namespace,publication:p.id,scopeId:p.scope_id,domain:p.domain,publicationCatalogueGeneration:p.catalogue_generation,publicationRoot:p.root_key,manifest:p.manifest_key,receipt:p.receipt_key,snapshotDigest:await hash(p.snapshot),epoch:m.epoch,root:m.root_key,catalogueGeneration:m.catalogue_generation,referenceRevision:m.archive_reference_revision,sourceRevision:p.input_revision,archiveRevision:p.revision,publicationGeneration:p.generation,publicationOwner:p.owner,publicationNonce:p.lease_nonce};
 if(continuation){need(match(p,continuation)&&owner===p.eviction_owner,'pa_eviction_reconciliation_token');need(saved&&serialize(saved.context)===serialize(context),'pa_eviction_boundary_changed');}
 else{
  const available=await db.first(`SELECT id FROM pa_publications WHERE id=? AND eviction_until<=MAX(?,${SQL_CLOCK})`,p.id,now);
  if(!available)return {status:'PENDING',reason:'eviction_reconciliation_owned',complete:false};
  const state=saved&&serialize(saved.context)===serialize(context)?saved:{format:FORMAT,context,phase:'LINEAGE',lineage:null,current:m.root_key,child:null,member:0,sample:0};
  const wire=await encodeEvictionProgress(state),claim=await db.batch([assertion(db,`EXISTS(SELECT 1 FROM pa_publications WHERE id=? AND eviction_generation=? AND eviction_until<=MAX(?,${SQL_CLOCK}))`,p.id,p.eviction_generation,now),reconciliationBoundaryFence(db,p,{context}),db.statement(`UPDATE pa_publications SET eviction_owner=?,eviction_nonce=lower(hex(randomblob(16))),eviction_generation=eviction_generation+1,eviction_until=MAX(?,${SQL_CLOCK})+?,eviction_progress=? WHERE id=? RETURNING *`,[owner,now,TTL,wire,p.id]),clearAssertions(db)]);p=claim[2].results[0];saved=state;
 }
 await db.batch([reconciliationFence(db,p,now),reconciliationBoundaryFence(db,p,saved),clearAssertions(db)]);
 let reads=0;const bounded={...archive,async read(...args){need(reads<1024,'pa_eviction_segment_budget');reads++;return archive.read(...args);}};
 let state={...saved};
 if(state.phase==='LINEAGE'){
  const result=await verifyArchiveLineage(bounded,context.root,{continuation:state.lineage});
  if(result.status==='PENDING')state.lineage=result.continuation;else{need(result.epoch===context.epoch&&result.generation===context.catalogueGeneration,'pa_eviction_boundary_identity');state.phase='ANCESTRY';state.lineage=null;}
 }else if(state.phase==='ANCESTRY'){
  for(let units=0;state.current&&units<16;units++){
   let root;try{root=await verifyRoot(bounded,state.current);if(state.child)await verifyArchiveEdge(bounded,state.child,root);}catch(e){if(e.message==='pa_eviction_segment_budget')break;throw e;}
   if(root.key===p.root_key){verifyPublicationRootIdentity(p,root);state.phase='MEMBERSHIP';state.current=null;state.child=null;break;}
   need(root.previous,'pa_eviction_not_descendant');state.child=header(root);state.current=root.previous;
  }
 }else if(state.phase==='MEMBERSHIP'){
  const old=await verifyRoot(bounded,p.root_key),current=await verifyRoot(bounded,context.root);
  verifyPublicationRootIdentity(p,old);
  need(await hash(p.snapshot)===context.snapshotDigest,'pa_eviction_snapshot_changed');
  const member=old.verified.snapshot.membership[state.member];
  if(member){await retainedMember(bounded,current.indexRoot,member,p);
   if(member.kind==='MARKET_EXPORT'){
    const record=old.verified.records.find(r=>r.id===member.id),samples=JSON.parse(record.wire).samples;
    // A fixed four aliases per turn; no permanent per-sample progress rows.
    for(let n=0;state.sample<samples.length&&n<4;n++,state.sample++){
     const sample=samples[state.sample],alias=await catalogueLookup(bounded,current.indexRoot,'sample:'+sample.id);
     need(alias?.recordId===member.id&&alias.packageKey===p.manifest_key&&alias.recordDigest===member.digest&&alias.sampleIndex===state.sample&&alias.sampleDigest===await hash({type:sample.type,value:sample.value,occurredAt:sample.occurredAt}),'pa_eviction_sample_membership');
    }
    if(state.sample===samples.length){state.member++;state.sample=0;}
   }else state.member++;
  }
  if(state.member===old.verified.snapshot.membership.length)state.phase='READY';
 }else need(state.phase==='READY','pa_eviction_progress_phase');
 await hook?.('VERIFIED_SEGMENT',{publication:p,progress:state});
 const wire=await encodeEvictionProgress(state),r=await db.batch([reconciliationFence(db,p,now),reconciliationBoundaryFence(db,p,state),db.statement(`UPDATE pa_publications SET eviction_progress=?,eviction_generation=eviction_generation+1,eviction_until=MAX(?,${SQL_CLOCK})+? WHERE id=? RETURNING *`,[wire,now,TTL,p.id]),clearAssertions(db)]);p=r[2].results[0];
 return {status:state.phase==='READY'?'READY':'PENDING',complete:state.phase==='READY',continuation:evictionToken(p),progress:{phase:state.phase,predecessor:state.current,member:state.member,sample:state.sample},reads,rootKey:context.root,catalogueGeneration:context.catalogueGeneration};
}
