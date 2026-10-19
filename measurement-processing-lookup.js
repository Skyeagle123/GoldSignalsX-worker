// Job-owned evidence enumeration. This is not a canonical-subject receipt.
import {need,hash,serialize,parseEvidence,bytes,LIMITS} from './measurement-independent-contract.js';
import {readRecord,validateD1Snapshot} from './measurement-independent-store.js';
import {validateMeasurementWire} from './measurement-consumer.js';
import {projectLifecycle} from './measurement-lifecycle.js';
import {assertion,clearAssertions,jobFence,schemaReady} from './measurement-d1-adapter.js';
import {ProcessingYield} from './measurement-processing-budget.js';
const VERSION='pa-processing-lookup-v2';
const boundarySQL=`SELECT json_array(m.epoch,m.root_key,m.catalogue_generation,m.archive_reference_revision,
 (SELECT json_group_array(json(v)) FROM (SELECT json_array(id,revision,archive_revision,archive_root,pending,corrections,quarantine,conflict,recovery_required) v FROM pa_scopes WHERE id=? OR id=? OR id IN(SELECT evaluation_id FROM pa_bindings WHERE signal_id=? OR evaluation_id=?) ORDER BY id)),
 (SELECT json_group_array(json(v)) FROM (SELECT json_array(evaluation_id,digest,signal_id,first_event,cohort,cycle_key) v FROM pa_bindings WHERE evaluation_id=? OR signal_id=? ORDER BY evaluation_id LIMIT 81))) AS wire FROM pa_meta m`;
async function context(db,requested,scope,purpose){const r=await db.first(boundarySQL,requested,scope,scope,requested,requested,scope);need(r&&bytes(r.wire)<=60000,'pa_processing_lookup_bound');return {operation:VERSION,requested,scope,purpose,boundary:r.wire};}
const fence=(db,c)=>assertion(db,`(${boundarySQL.replace(' AS wire','')})=?`,c.requested,c.scope,c.scope,c.requested,c.requested,c.scope,c.boundary);
// Lossless projection-input tuples omit repeated JSON field names. Only fields
// actually read by the unchanged projectLifecycle reducer are represented.
function compactFact(e){const p=e.payload;
 if(e.kind==='OFFICIAL_CREATION'){const d=p.decisionEvidence;return [d.createdAt,d.timeframe,d.direction,d.engine?.levels,p.officialPersisted,p.performancePersistence?.performance];}
 if(e.kind==='CONFIRMATION_LINK')return [p.primarySignalId,p.confirmationSignalId,p.link,p.linkPersistence];
 return [p.createdAt,p.timeframe,p.direction,p.occurredAt,p.event,p.performancePersistence,p.status,p.closedAt];
}
function expandFact(f){const p=f[4];let payload;
 if(f[3]==='OFFICIAL_CREATION')payload={decisionEvidence:{createdAt:p[0],timeframe:p[1],direction:p[2],engine:{levels:p[3]}},officialPersisted:p[4],performancePersistence:{performance:p[5]}};
 else if(f[3]==='CONFIRMATION_LINK')payload={primarySignalId:p[0],confirmationSignalId:p[1],link:p[2],linkPersistence:p[3]};
 else payload=Object.fromEntries(['createdAt','timeframe','direction','occurredAt','event','performancePersistence','status','closedAt'].map((key,i)=>[key,p[i]]));
 return {event_id:f[0],ingested_at:f[2],fact_kind:f[3],payload};
}
export function processingLookup(db,archive,budget,requested,lease,now){
 async function enumerate(scope,purpose){
  budget.phase('LOOKUP_'+purpose);const c=await context(db,requested,scope,purpose),identity=await hash(c),store=archive?.raw?.progress;
  budget.reserve({objects:68,bytes:4080000});let loaded=store?await store.acquire(c,{claim:true}):null;
  let s=loaded?.state??{version:VERSION,identity,generation:0,source:'HOT',after:'',coldAfter:'',facts:[],creation:null,complete:false};
  need(s.version===VERSION&&s.identity===identity&&Number.isSafeInteger(s.generation)&&s.generation>=0&&Array.isArray(s.facts)&&s.facts.length<=256,'pa_processing_lookup_integrity');s=structuredClone(s);
  async function save(){
   if(!store)throw Error('pa_processing_lookup_archive_required');
   await db.batch([jobFence(db,lease,now),fence(db,c),clearAssertions(db)]);
   s.owner={jobId:lease.id,owner:lease.owner,generation:lease.generation,nonce:lease.lease_nonce,leaseUntil:lease.lease_until};s.generation++;need(bytes(serialize(s))<=58000,'pa_processing_lookup_bound');
   budget.beforeCheckpoint();await store.save(c,s,loaded);budget.committed();
   await db.batch([jobFence(db,lease,now),fence(db,c),clearAssertions(db)]);
  }
  if(!s.complete)try{while(!s.complete){
   budget.reserve({objects:80,bytes:4800000,records:1});let row,entry,page;
   if(s.source==='HOT'){
    row=await db.first("SELECT id FROM pa_records WHERE scope_id=? AND ready=1 AND kind IN('OFFICIAL_CREATION','CONFIRMATION_LINK','LIFECYCLE_FACT') AND id>? ORDER BY id LIMIT 1",scope,s.after);
    if(!row){s.source='COLD';continue;}
   }else{
    const owner=await db.first('SELECT archive_root FROM pa_scopes WHERE id=?',scope);
    if(!owner?.archive_root){s.complete=true;break;}
    need(archive?.scopePage,'pa_processing_archive_required');page=await archive.scopePage(scope,{rootKey:owner.archive_root,after:s.coldAfter,limit:1});
    if(!page.entries.length){need(page.complete||page.next>s.coldAfter,'pa_archive_scope_pending');s.coldAfter=page.next;if(page.complete)s.complete=true;continue;}
    entry=page.entries[0];row={id:entry.recordId??entry.id};
    const exists=await db.first('SELECT id FROM pa_records WHERE id=? AND ready=1',row.id);
    if(exists){s.coldAfter=page.next;if(page.complete)s.complete=true;continue;}
   }
   const relevant=purpose==='CREATIONS'?(!entry||entry.kind==='OFFICIAL_CREATION'):(!entry||['OFFICIAL_CREATION','CONFIRMATION_LINK','LIFECYCLE_FACT'].includes(entry.kind));
   if(relevant){const r=entry?await archive.entryRecord(entry):await readRecord(db,row.id,{archive});need(r,'pa_processing_creation_unavailable');
    if(purpose!=='CREATIONS'||r.kind==='OFFICIAL_CREATION'){
     const e=await validateMeasurementWire(r.wire,{receivedAt:now});
     if(purpose==='CREATIONS'){
      need(e.kind==='OFFICIAL_CREATION'&&e.payload.officialSignalId===scope,'pa_processing_creation_conflict');
      const v={id:r.id,digest:r.digest,evaluationId:e.payload.evaluationId,decisionDigest:await hash(serialize(e.payload.decisionEvidence))};
      need(!s.creation||s.creation.evaluationId===v.evaluationId&&s.creation.decisionDigest===v.decisionDigest,'pa_processing_binding_conflict');
      if(!s.creation||v.id>s.creation.id)s.creation=v;
     }else{need(r.id==='event:'+e.eventId,'pa_archive_semantic_identity');s.facts.push([e.eventId,r.digest,r.available_at,e.kind,compactFact(e)]);need(s.facts.length<=256,'pa_projection_budget');}
    }
   }
   if(s.source==='HOT')s.after=row.id;else{s.coldAfter=page.next;if(page.complete)s.complete=true;}
  }}catch(e){if(!(e instanceof ProcessingYield))throw e;await save();throw e;}
  await budget.notify?.('PROCESSING_LOOKUP_VERIFIED',{context:c});
  await db.batch([jobFence(db,lease,now),fence(db,c),clearAssertions(db)]);
  // Completed lookup state is a resumable enumeration, never ownership authority.
  if(store&&!loaded?.state?.complete)await save();
  return {state:s,context:c};
 }
 return {
  async creations(scope){return (await enumerate(scope,'CREATIONS')).state.creation;},
  async rebuild(scope,{hook=null}={}){
   const {state:s,context:c}=await enumerate(scope,'LIFECYCLE');if(!s.facts.length)return null;
   const m=await schemaReady(db),owner=await db.first('SELECT * FROM pa_scopes WHERE id=?',scope),p=projectLifecycle(scope,s.facts.map(expandFact));
   if(owner.conflict||owner.quarantine){p.integrityStatus='CONFLICT';p.status=null;p.closedAt=null;}
   const rows=s.facts.map(f=>({id:'event:'+f[0],digest:f[1],available_at:f[2]})).sort((a,b)=>a.id.localeCompare(b.id)),wire=serialize(p),digest=await hash(wire),factIdentity=await hash(rows.map(r=>[r.id,r.digest,r.available_at]));await hook?.('PROJECTION_PREPARED');
   await db.batch([jobFence(db,lease,now),fence(db,c),assertion(db,'EXISTS(SELECT 1 FROM pa_scopes s,pa_meta m WHERE s.id=? AND s.revision=? AND m.epoch=?)',scope,owner.revision,m.epoch),
    db.statement('INSERT INTO pa_projections VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope_id) DO UPDATE SET epoch=excluded.epoch,source_revision=excluded.source_revision,digest=excluded.digest,payload=excluded.payload,available_at=excluded.available_at,integrity=excluded.integrity,fact_identity=excluded.fact_identity,fact_count=excluded.fact_count',[scope,m.epoch,owner.revision,digest,wire,p.availableAt,p.integrityStatus,factIdentity,rows.length]),clearAssertions(db)]);
   return validateD1Snapshot(db,scope,{now,hook});
  }
 };
}
