import {serialize,canonicalSerialize,parseEvidence,hash,bytes,need,time,identity,LIMITS,uid,DAY} from './measurement-independent-contract.js';
import {assertion,clearAssertions,schemaReady,SQL_CLOCK,leaseJob,jobFence,finishJob} from './measurement-d1-adapter.js';
import {readRecord,chunkWire} from './measurement-independent-store.js';
import {catalogueInsert,catalogueLookup,cataloguePage} from './measurement-archive-catalog.js';
import {DEFINITION_PACK} from './measurement-definition-pack.js';
import {decodeWire} from './measurement-wire-codec.js';
import {reserve,estimateWrite,commitReservationStatements} from './measurement-reservations.js';
async function validatedDefinitions(wire,engine){const d=JSON.parse(wire);need(d.version==='measurement-definitions-v1'&&d.engineSemanticsVersion===engine&&d.files&&d.sourceDigests&&Object.keys(d.files).length<=32,'pa_archive_definition_unsupported');
 for(const file of ['signal-engine.js','evidence-codec.js','forward-validation.js','post-entry-evidence.js','measurement-lifecycle.js','measurement-wire-codec.js','measurement-market-envelope.js'])need(typeof d.files[file]==='string'&&d.sourceDigests[file]===await hash(d.files[file])&&d.sourceDigests[file]===DEFINITION_PACK.sourceDigests[file],'pa_archive_definition_unsupported');return d;}
export const ARCHIVE_FORMAT='measurement-archive-v1';
export const ARCHIVE_LIMITS=Object.freeze({objects:256,bytes:16000000,records:128});
const transitions=['PLANNED','SNAPSHOT_PINNED','OBJECTS_WRITTEN','VERIFIED','CATALOGUED','EVICTION_ELIGIBLE'];
const prefixPattern=/^synthetic-b1-[a-z0-9-]{1,48}\/$/;
export function createPrivateArchiveAdapter(bucket,{namespace,operationBudget=null}={}){
 need(bucket&&typeof bucket.get==='function'&&typeof bucket.put==='function'&&typeof bucket.list==='function'&&prefixPattern.test(namespace),'pa_archive_binding');
 need(operationBudget===null||Number.isSafeInteger(operationBudget)&&operationBudget>=512&&operationBudget<=9000,'pa_archive_operation_budget');let operations=0;const admit=()=>{need(operationBudget===null||operations<operationBudget,'pa_archive_invocation_budget');operations++;};
 const metrics={reads:0,writes:0,bytesRead:0,bytesWritten:0};
 async function read(key,{maxBytes=LIMITS.rowBytes}={}){need(typeof key==='string'&&key.startsWith(namespace)&&!key.includes('..'),'pa_archive_namespace');
  admit();const object=await bucket.get(key);need(object,'pa_archive_object_missing');need(Number.isSafeInteger(object.size)&&object.size<=maxBytes,'pa_archive_object_bound');
  const data=new Uint8Array(await object.arrayBuffer());need(data.length===object.size&&data.length<=maxBytes,'pa_archive_short_read');const wire=new TextDecoder('utf-8',{fatal:true}).decode(data);const digest=await hash(wire);
  need(key.endsWith('/'+digest),'pa_archive_digest');metrics.reads++;metrics.bytesRead+=data.length;return wire;
 }
 async function write(type,value){const wire=typeof value==='string'?value:serialize(value),digest=await hash(wire),key=namespace+type+'/'+digest;
  need(bytes(wire)<=LIMITS.rowBytes,'pa_archive_object_bound');admit();await bucket.put(key,wire,{onlyIf:{etagDoesNotMatch:'*'}});metrics.writes++;metrics.bytesWritten+=bytes(wire);
  // Upload ack is not proof: every write requires complete readback.
  need(await read(key)===wire,'pa_archive_readback');return {key,digest,bytes:bytes(wire)};
 }
 async function listRoots({cursor,limit=64}={}){need(limit>=1&&limit<=64,'pa_root_list_bound');admit();return bucket.list({prefix:namespace+'roots/',cursor,limit});}
 return Object.freeze({namespace,read,write,listRoots,remaining:()=>operationBudget===null?Infinity:operationBudget-operations,metrics:()=>({...metrics,operations})});
}
function pubFence(db,p,now,{revision=true}={}){
 return assertion(db,`EXISTS(SELECT 1 FROM pa_publications p,pa_meta m,pa_scopes s WHERE p.id=? AND p.owner=? AND p.generation=? AND p.lease_nonce=? AND p.lease_until>MAX(?,${SQL_CLOCK}) AND p.epoch=m.epoch AND s.id=p.scope_id ${revision?'AND s.archive_revision=p.revision AND s.revision=p.input_revision AND s.pending=0 AND s.corrections=0':''})`,p.id,p.owner,p.generation,p.lease_nonce,now);
}
async function advance(db,p,from,to,now,assignments='',values=[]){need(transitions.indexOf(to)===transitions.indexOf(from)+1,'pa_archive_transition');await db.batch([pubFence(db,p,now),assertion(db,'EXISTS(SELECT 1 FROM pa_publications WHERE id=? AND state=?)',p.id,from),db.statement(`UPDATE pa_publications SET state=?${assignments} WHERE id=?`,[to,...values,p.id]),clearAssertions(db)]);}
export async function planPublication(db,scopeId,{now,owner=uid(),leaseMs=60000,domain='EVIDENCE'}={}){
 need(['EVIDENCE','CENSUS'].includes(domain),'pa_archive_domain');const s=await db.first('SELECT * FROM pa_scopes WHERE id=?',identity(scopeId)),m=await schemaReady(db);need(s,'pa_scope_missing');need(leaseMs>0&&leaseMs<=60000,'pa_archive_lease');
 const id='archive:'+await hash([m.epoch,scopeId,s.revision,s.archive_revision,domain]),old=await db.first('SELECT * FROM pa_publications WHERE id=?',id);
 if(old){if(old.state==='EVICTION_ELIGIBLE')return old;if(old.owner!==owner&&old.lease_until>now)return {...old,pending:true,reason:'publication_lease_pending'};await db.batch([assertion(db,'EXISTS(SELECT 1 FROM pa_publications WHERE id=? AND (lease_until<=? OR owner=?))',id,now,owner),db.statement("UPDATE pa_publications SET owner=?,generation=generation+1,lease_nonce=lower(hex(randomblob(16))),lease_until=?,catalogue_offset=CASE WHEN state='STALE' THEN 0 ELSE catalogue_offset END,sample_offset=CASE WHEN state='STALE' THEN 0 ELSE sample_offset END,catalogue_index_root=CASE WHEN state='STALE' THEN NULL ELSE catalogue_index_root END,state=CASE WHEN state='STALE' THEN 'PLANNED' ELSE state END WHERE id=?",[owner,now+leaseMs,id]),clearAssertions(db)]);return db.first('SELECT * FROM pa_publications WHERE id=?',id);}
 const reservation=await reserve(db,{id:'publication:'+id,workClass:'archive',owner:id,bytes:estimateWrite(LIMITS.rowBytes*3,64),now});
 try{await db.batch([assertion(db,`(SELECT COUNT(*) FROM pa_publications)<${LIMITS.publications} AND EXISTS(SELECT 1 FROM pa_scopes WHERE id=? AND revision=? AND archive_revision=? AND pending=0 AND corrections=0 )`,scopeId,s.revision,s.archive_revision),
  db.statement("INSERT INTO pa_publications(id,scope_id,epoch,revision,state,owner,generation,lease_until,reservation,input_revision,domain) VALUES(?,?,?,?,'PLANNED',?,1,?,?,?,?)",[id,scopeId,m.epoch,s.archive_revision,owner,now+leaseMs,reservation.id,s.revision,domain]),...commitReservationStatements(db,reservation,now),clearAssertions(db)]);}catch(error){const raced=await db.first('SELECT * FROM pa_publications WHERE id=?',id);if(raced)return {...raced,pending:true,reason:'publication_lease_pending'};throw error;}return db.first('SELECT * FROM pa_publications WHERE id=?',id);
}
async function snapshotClosure(db,p,archive){
 const bindings=await db.all('SELECT * FROM pa_bindings WHERE signal_id=? OR evaluation_id=? ORDER BY evaluation_id LIMIT 81',p.scope_id,p.scope_id);need(bindings.length<=80,'pa_archive_bindings_bound');
 const ids=new Set((await db.all("SELECT id FROM pa_records WHERE ready=1 AND scope_id=? ORDER BY id LIMIT ?",p.scope_id,8)).map(r=>r.id));
 if(p.domain==='CENSUS'){ids.clear();for(const b of bindings)ids.add('decision:'+b.evaluation_id);const captures=await db.all("SELECT id FROM pa_records WHERE scope_id=? AND ready=1 AND kind IN('CENSUS','CENSUS_CAPTURE','COVERAGE','CONFLICT') LIMIT 81",p.scope_id);for(const r of captures)ids.add(r.id);need(ids.size>0,'pa_census_archive_empty');}else for(const b of bindings){ids.add('decision:'+b.evaluation_id);ids.add('event:'+b.first_event);}
 // Each raw cycle is self-contained accepted M1 evidence, including market manifests,
 // cohort definitions and exact rejection/census evidence. No portable SQL row IDs.
 const reader=archiveReader(archive,{getRoot:async()=> (await db.first('SELECT root_key FROM pa_meta')).root_key});
 for(const b of (p.domain==='CENSUS'?[]:bindings))if(b.cycle_key){const parent=await db.first('SELECT id FROM pa_records WHERE semantic_key=? AND ready=1',b.cycle_key)??(reader.lookupSemantic?await reader.lookupSemantic(b.cycle_key):null);if(parent)ids.add(parent.id);}
 const records=[];let total=0;const scopeIds=new Set([p.scope_id]);for(const id of ids){const r=await readRecord(db,id,{archive:reader});need(r,'pa_archive_dependency_missing');if(r.kind==='TRANSPORT_TERMINAL'){const terminal=parseEvidence(r.wire).event;r.transportEventId=terminal.id;r.transportGeneration=terminal.replays;}records.push(r);for(const dependency of r.dependencies??[])ids.add(dependency);need(ids.size<=ARCHIVE_LIMITS.records,'pa_archive_dependency_bound');total+=r.bytes;if(r.scope_id)scopeIds.add(r.scope_id);
  if(r.kind==='DECISION_CYCLE'){const e=JSON.parse(r.wire);for(const record of e.payload.records.filter(r=>r.type==='decision')){scopeIds.add(record.payload.evaluationId);if(typeof record.payload.officialSignalId==='string')scopeIds.add(record.payload.officialSignalId);}}
 }
 need(records.length<=ARCHIVE_LIMITS.records&&total<=ARCHIVE_LIMITS.bytes,'pa_archive_closure_bound');
 need(scopeIds.size<=80,'pa_archive_dependency_scope_bound');const dependencyScopes=await db.all('SELECT * FROM pa_scopes WHERE id IN(SELECT value FROM json_each(?)) ORDER BY id LIMIT 81',serialize([...scopeIds]));
 const snapshot={membershipBoundary:'EXPLICIT_MEMBERSHIP_ONLY',format:ARCHIVE_FORMAT,epoch:p.epoch,scopeId:p.scope_id,sourceRevision:p.revision,inputRevision:p.input_revision,membership:records.map(({id,digest,bytes,available_at,created_at,kind,cohort,definition,retention,category,scope_id,semantic_key,chunk_count,order_key,codec,encoded_bytes,dependencies,transportEventId,transportGeneration})=>({id,digest,bytes,available_at,created_at,kind,cohort,definition,retention,category,scope_id,semantic_key,chunk_count,order_key,codec,encoded_bytes,dependencies:dependencies??[],...(transportEventId?{transportEventId,transportGeneration}:{})})),
  bindings:p.domain==='CENSUS'?[]:bindings,censusBindings:p.domain==='CENSUS'?bindings:[],domain:p.domain,dependencyScopes,policy:'b1-independent-retention-v1',availableThrough:Math.max(...records.map(r=>r.available_at)),expectedCount:records.length,dependencyClosure:p.domain==='CENSUS'?'CENSUS_SELF_CONTAINED':'SELF_CONTAINED_RAW_CYCLES',definitions:{codec:'b1-canonical-v1',numericTags:['-0','NaN','Infinity','-Infinity'],engine:'6721f84-engine-v1',reducer:'tp1-first-v1'},integrity:'PINNED'};
 return {snapshot,records};
}
function closureFence(db,snapshot){return assertion(db,'NOT EXISTS(SELECT 1 FROM json_each(?) x LEFT JOIN pa_scopes s ON s.id=json_extract(x.value,\'$.id\') WHERE s.id IS NULL OR s.revision!=json_extract(x.value,\'$.revision\') OR s.archive_revision!=json_extract(x.value,\'$.archive_revision\') OR s.pending!=0 OR s.corrections!=0 OR s.quarantine!=json_extract(x.value,\'$.quarantine\') OR s.conflict!=json_extract(x.value,\'$.conflict\'))',serialize(snapshot.dependencyScopes));}
export async function publishArchive(db,archive,scopeId,{now,owner=uid(),hook=null,domain='EVIDENCE',maxSteps=64}={}){
 need(Number.isSafeInteger(maxSteps)&&maxSteps>=1&&maxSteps<=64,'pa_archive_step_bound');let p=await planPublication(db,scopeId,{now,owner,domain});if(p.pending)return p;
 for(let step=0;step<maxSteps&&p.state!=='EVICTION_ELIGIBLE';step++){if(archive.remaining&&archive.remaining()<4096||db.remainingStatements()<250)return {...p,pending:true,reason:'publication_invocation_budget'};
  try{
   if(p.state==='PLANNED'){const {snapshot}=await snapshotClosure(db,p,archive);await db.batch([pubFence(db,p,now),closureFence(db,snapshot),db.statement("UPDATE pa_publications SET state='SNAPSHOT_PINNED',snapshot=? WHERE id=? AND state='PLANNED'",[serialize(snapshot),p.id]),clearAssertions(db)]);}
   else if(p.state==='SNAPSHOT_PINNED'){
    const snapshot=JSON.parse(p.snapshot),entries=[];let objectCount=0,objectBytes=0;
    const reader=archiveReader(archive,{getRoot:async()=> (await db.first('SELECT root_key FROM pa_meta')).root_key});
    for(const member of snapshot.membership){const record=await readRecord(db,member.id,{archive:reader});need(record&&record.digest===member.digest,'pa_archive_source_changed');let chunks=await db.all('SELECT ordinal,data FROM pa_chunks WHERE record_id=? ORDER BY ordinal LIMIT 51',member.id);if(!chunks.length&&record.objects){chunks=[];for(const o of record.objects)chunks.push({ordinal:o.ordinal,data:await archive.read(o.key)});}const objects=[];
     for(const c of chunks){const object=await archive.write('chunks',c.data);objects.push({...object,ordinal:c.ordinal});objectCount++;objectBytes+=object.bytes;need(objectCount<=ARCHIVE_LIMITS.objects&&objectBytes<=ARCHIVE_LIMITS.bytes*2,'pa_archive_object_budget');}
     entries.push({...member,objects});
    }
    // Bounded manifest pages avoid a single giant manifest/row.
    const pages=[];let group=[];for(const entry of entries){if(bytes(canonicalSerialize([...group,entry],LIMITS.eventBytes))>48000){pages.push(await archive.write('manifests',{format:ARCHIVE_FORMAT,records:group}));group=[];}group.push(entry);}if(group.length)pages.push(await archive.write('manifests',{format:ARCHIVE_FORMAT,records:group}));
    const definitionWire=serialize(DEFINITION_PACK,LIMITS.eventBytes),definitionChunks=[];for(const chunk of chunkWire(definitionWire))definitionChunks.push(await archive.write('definitions',chunk));
    const manifest=await archive.write('packages',{format:ARCHIVE_FORMAT,snapshot,pages,definitionPack:{version:'measurement-definitions-v1',digest:await hash(definitionWire),chunks:definitionChunks}});await advance(db,p,'SNAPSHOT_PINNED','OBJECTS_WRITTEN',now,',manifest_key=?',[manifest.key]);
   }
   else if(p.state==='OBJECTS_WRITTEN'){
    const verified=await verifyArchivePackage(archive,p.manifest_key);need(verified.snapshot.epoch===p.epoch&&verified.snapshot.sourceRevision===p.revision,'pa_archive_snapshot_mismatch');
    const receipt=await archive.write('receipts',{format:'measurement-verification-v1',publication:p.id,epoch:p.epoch,sourceRevision:p.revision,manifestKey:p.manifest_key,membershipDigest:await hash(verified.snapshot.membership),expectedCount:verified.snapshot.expectedCount,verifiedAt:now,readback:true});
    await db.batch([pubFence(db,p,now),closureFence(db,verified.snapshot),assertion(db,"EXISTS(SELECT 1 FROM pa_publications WHERE id=? AND state='OBJECTS_WRITTEN')",p.id),db.statement("UPDATE pa_publications SET state='VERIFIED',receipt_key=? WHERE id=?",[receipt.key,p.id]),clearAssertions(db)]);
   }
   else if(p.state==='VERIFIED'){
    if(await db.first("SELECT id FROM pa_publications WHERE state='CATALOGUED' AND id!=? LIMIT 1",p.id))return {...p,pending:true,reason:'catalogue_publication_pending'};
    const meta=await db.first('SELECT * FROM pa_meta');
    // SQL publication authority precedes the recoverable immutable root.
    await db.batch([pubFence(db,p,now),closureFence(db,JSON.parse(p.snapshot)),assertion(db,"EXISTS(SELECT 1 FROM pa_meta WHERE catalogue_generation=?) AND NOT EXISTS(SELECT 1 FROM pa_publications WHERE state='CATALOGUED' AND id!=?)",meta.catalogue_generation,p.id),
     db.statement('UPDATE pa_meta SET catalogue_generation=catalogue_generation+1'),db.statement("UPDATE pa_publications SET state='CATALOGUED',catalogue_generation=? WHERE id=? AND state='VERIFIED'",[meta.catalogue_generation+1,p.id]),clearAssertions(db)]);
   }
   else if(p.state==='CATALOGUED'){
    let rootKey=p.root_key;
    if(!rootKey){const meta=await db.first('SELECT * FROM pa_meta');need(meta.catalogue_generation===p.catalogue_generation,'pa_catalogue_raced');
     const snapshot=JSON.parse(p.snapshot),members=snapshot.membership;
     let indexRoot=p.catalogue_index_root??(meta.root_key?(await verifyRoot(archive,meta.root_key)).indexRoot:null);
     if(p.catalogue_offset<members.length){const member=members[p.catalogue_offset],scopeState=snapshot.dependencyScopes.find(s=>s.id===member.scope_id)??null,entry={...member,scopeState,packageKey:p.manifest_key,receiptKey:p.receipt_key};indexRoot=await catalogueInsert(archive,indexRoot,entry);
      if(member.scope_id){const scopeAlias='scope:'+member.scope_id,previous=await catalogueLookup(archive,indexRoot,scopeAlias),scopeIndex=await catalogueInsert(archive,previous?.scopeIndex??null,entry),factIndex=['OFFICIAL_CREATION','LIFECYCLE_FACT','CONFIRMATION_LINK'].includes(member.kind)?await catalogueInsert(archive,previous?.factIndex??null,entry):previous?.factIndex??null;indexRoot=await catalogueInsert(archive,indexRoot,{id:scopeAlias,lookupType:'SCOPE',scopeIndex,factIndex,firstConflictAt:scopeState?.first_conflict_at??previous?.firstConflictAt??null,conflictIndex:member.kind==='CONFLICT'?await catalogueInsert(archive,previous?.conflictIndex??null,entry):previous?.conflictIndex??null,digest:await hash(scopeAlias),order_key:await hash(scopeAlias),scopeState});}
      const aliasId='semantic:'+member.semantic_key;indexRoot=await catalogueInsert(archive,indexRoot,{...entry,id:aliasId,recordId:member.id,lookupType:'SEMANTIC',order_key:await hash(aliasId)});
      let nextSample=0,memberComplete=true;
      if(member.kind==='MARKET_EXPORT'){const reader=archiveReader(archive,{rootKey:meta.root_key}),r=await readRecord(db,member.id,{archive:reader}),samples=parseEvidence(r.wire).samples;need(samples.length<=2400,'pa_archive_samples_bound');
       const marketAlias='cohort-market:'+member.cohort,prior=await catalogueLookup(archive,indexRoot,marketAlias),marketOrder=member.created_at.toString(16).padStart(16,'0')+member.order_key.slice(0,48),marketIndex=await catalogueInsert(archive,prior?.marketIndex??null,{...entry,order_key:marketOrder});indexRoot=await catalogueInsert(archive,indexRoot,{id:marketAlias,lookupType:'MARKET_INDEX',marketIndex,digest:await hash(marketAlias),order_key:await hash(marketAlias)});
       nextSample=Math.min(samples.length,p.sample_offset+32);for(let i=p.sample_offset;i<nextSample;i++){const id='sample:'+samples[i].id;indexRoot=await catalogueInsert(archive,indexRoot,{...entry,id,recordId:member.id,lookupType:'SAMPLE',sampleIndex:i,sampleDigest:await hash({type:samples[i].type,value:samples[i].value,occurredAt:samples[i].occurredAt}),digest:await hash({type:samples[i].type,value:samples[i].value,occurredAt:samples[i].occurredAt}),recordDigest:member.digest,order_key:await hash(id)});}memberComplete=nextSample===samples.length;
      }
      await db.batch([pubFence(db,p,now),closureFence(db,snapshot),db.statement('UPDATE pa_publications SET catalogue_index_root=?,catalogue_offset=?,sample_offset=? WHERE id=?',[indexRoot,p.catalogue_offset+(memberComplete?1:0),memberComplete?0:nextSample,p.id]),clearAssertions(db)]);p=await db.first('SELECT * FROM pa_publications WHERE id=?',p.id);await hook?.('CATALOGUE_PROGRESS',p);continue;
     }
     const root=await archive.write('roots',{format:'measurement-root-v1',generation:p.catalogue_generation,epoch:p.epoch,publication:p.id,manifestKey:p.manifest_key,receiptKey:p.receipt_key,previous:meta.root_key,indexRoot,sourceRevision:p.revision,scopeId:p.scope_id,publishedAt:now});rootKey=root.key;
     await db.batch([pubFence(db,p,now),closureFence(db,JSON.parse(p.snapshot)),assertion(db,'EXISTS(SELECT 1 FROM pa_meta WHERE catalogue_generation=? AND root_key IS ?)',p.catalogue_generation,meta.root_key),db.statement('UPDATE pa_meta SET root_key=?',[rootKey]),db.statement('UPDATE pa_publications SET root_key=? WHERE id=?',[rootKey,p.id]),db.statement("INSERT INTO pa_jobs(id,class,event_id,next_at,deadline) VALUES(?,'archive',?,?,?) ON CONFLICT DO NOTHING",['root-checkpoint:'+p.id,p.id,now,now]),clearAssertions(db)]);
     await hook?.('ROOT_REGISTERED',{...p,root_key:rootKey});
    }
    const root=JSON.parse(await archive.read(rootKey));await archive.write('checkpoints',rootCheckpoint(root,rootKey));
    await db.batch([pubFence(db,p,now),closureFence(db,JSON.parse(p.snapshot)),db.statement("UPDATE pa_publications SET state='EVICTION_ELIGIBLE' WHERE id=? AND state='CATALOGUED' AND root_key=?",[p.id,rootKey]),db.statement("UPDATE pa_reservations SET state='SETTLED' WHERE id=? AND state='COMMITTED'",[p.reservation]),clearAssertions(db)]);
   }else throw Error('pa_archive_state');
   p=await db.first('SELECT * FROM pa_publications WHERE id=?',p.id);await hook?.(p.state,p);
  }catch(error){if(/fence_failed|source_changed|catalogue_raced/.test(error.message))await db.batch([db.statement("UPDATE pa_publications SET state='STALE' WHERE id=? AND owner=? AND generation=?",[p.id,p.owner,p.generation])]);throw error;}
 }
 return {...p,...(p.state!=='EVICTION_ELIGIBLE'?{pending:true,reason:'publication_step_budget'}:{})};
}
export async function verifyArchivePackage(archive,key,{budget={objects:ARCHIVE_LIMITS.objects+16,bytes:ARCHIVE_LIMITS.bytes*2}}={}){
 let objects=0,total=0;const read=async k=>{need(++objects<=budget.objects,'pa_archive_read_budget');const w=await archive.read(k);total+=bytes(w);need(total<=budget.bytes,'pa_archive_read_budget');return w;};
 const manifest=JSON.parse(await read(key));need(['measurement-archive-v1','measurement-archive-v2'].includes(manifest.format)&&manifest.pages?.length<=16,'pa_archive_format');
 const snapshot=manifest.snapshot;need(snapshot?.definitions?.codec==='b1-canonical-v1'&&snapshot.definitions.engine==='6721f84-engine-v1'&&snapshot.membership?.length<=ARCHIVE_LIMITS.records&&['SELF_CONTAINED_RAW_CYCLES','CENSUS_SELF_CONTAINED'].includes(snapshot.dependencyClosure),'pa_archive_definition');
 need(manifest.definitionPack?.version==='measurement-definitions-v1'&&manifest.definitionPack.chunks.length>0&&manifest.definitionPack.chunks.length<=50,'pa_archive_definition_missing');
 const definitionParts=[];for(const c of manifest.definitionPack.chunks)definitionParts.push(Uint8Array.from(atob(await read(c.key)),x=>x.charCodeAt(0)));const definitionBytes=new Uint8Array(definitionParts.reduce((n,p)=>n+p.length,0));let di=0;for(const p of definitionParts){definitionBytes.set(p,di);di+=p.length;}
 const definitionWire=new TextDecoder('utf-8',{fatal:true}).decode(definitionBytes);need(await hash(definitionWire)===manifest.definitionPack.digest,'pa_archive_definition_digest');await validatedDefinitions(definitionWire,snapshot.definitions.engine);
 const records=[];for(const page of manifest.pages){const p=JSON.parse(await read(page.key));need(p.format===manifest.format&&Array.isArray(p.records),'pa_archive_manifest');for(const r of p.records){need(r.objects.length===r.chunk_count&&r.objects.length<=50,'pa_archive_geometry');
   const chunks=[];for(let i=0;i<r.objects.length;i++){need(r.objects[i].ordinal===i,'pa_archive_geometry');const wire=await read(r.objects[i].key);chunks.push(Uint8Array.from(atob(wire),c=>c.charCodeAt(0)));}
   const wire=await decodeWire(chunks,r);parseEvidence(wire);records.push({...r,wire});
  }}
 need(records.length===snapshot.expectedCount&&records.length===snapshot.membership.length&&new Set(records.map(r=>r.id)).size===records.length,'pa_archive_counts');
 for(const member of snapshot.membership){const r=records.find(r=>r.id===member.id);need(r&&r.digest===member.digest&&r.available_at===member.available_at&&r.created_at===member.created_at,'pa_archive_identity');}
 // Revalidate self-contained accepted raw envelopes, including their M1 manifests.
 const {validateMeasurementWire}=await import('./measurement-consumer.js');for(const r of records)if(r.id.startsWith('event:')){const e=await validateMeasurementWire(r.wire,{receivedAt:Math.max(r.available_at,r.created_at)});need(r.id==='event:'+e.eventId,'pa_archive_semantic_identity');}
 for(const r of records)for(const dep of r.dependencies??[])need(records.some(x=>x.id===dep),'pa_archive_dependency_missing');
 for(const b of snapshot.censusBindings??[]){const r=records.find(r=>r.id==='decision:'+b.evaluation_id);need(r&&r.digest===b.digest&&parseEvidence(r.wire).evaluationId===b.evaluation_id,'pa_archive_census_binding');}
 for(const b of snapshot.bindings){need(records.some(r=>r.id==='decision:'+b.evaluation_id)&&records.some(r=>r.id==='event:'+b.first_event),'pa_archive_dependency_missing');}
 return {snapshot,records,objects,bytes:total,manifest};
}
const rootCheckpoint=(root,key)=>({format:'measurement-root-commit-v1',rootKey:key,publication:root.publication,epoch:root.epoch,generation:root.generation,sourceRevision:root.sourceRevision,sqlCatalogueAuthorized:true});
export async function verifyRootCheckpoint(archive,root,key,{read=k=>archive.read(k)}={}){const expected=rootCheckpoint(root,key),digest=await hash(serialize(expected));need(await read(archive.namespace+'checkpoints/'+digest)===serialize(expected),'pa_root_checkpoint');}
export async function verifyRoot(archive,key,{budget}={}){const root=JSON.parse(await archive.read(key));need(root.format==='measurement-root-v1'&&Number.isSafeInteger(root.generation)&&root.generation>0,'pa_root_format');await verifyRootCheckpoint(archive,root,key);
 const receipt=JSON.parse(await archive.read(root.receiptKey));need(receipt.format==='measurement-verification-v1'&&receipt.publication===root.publication&&receipt.manifestKey===root.manifestKey&&receipt.epoch===root.epoch&&receipt.sourceRevision===root.sourceRevision&&receipt.readback===true,'pa_root_receipt');
 const verified=await verifyArchivePackage(archive,root.manifestKey,{budget});need(receipt.membershipDigest===await hash(verified.snapshot.membership),'pa_root_membership');return {...root,key,verified};}
export async function readArchiveEntry(archive,entry,{read=key=>archive.read(key)}={}){
 const receipt=JSON.parse(await read(entry.receiptKey));need(receipt.format==='measurement-verification-v1'&&receipt.manifestKey===entry.packageKey&&receipt.readback===true,'pa_archive_receipt');
 const manifest=JSON.parse(await read(entry.packageKey));need(['measurement-archive-v1','measurement-archive-v2'].includes(manifest.format)&&manifest.pages.length<=16&&manifest.snapshot.definitions.codec==='b1-canonical-v1'&&manifest.definitionPack?.version==='measurement-definitions-v1','pa_archive_format');
 need(receipt.membershipDigest===await hash(manifest.snapshot.membership),'pa_archive_membership');
 need(manifest.definitionPack.chunks?.length>0&&manifest.definitionPack.chunks.length>0&&manifest.definitionPack.chunks.length<=50,'pa_archive_definition_missing');
 const definitionParts=[];for(const c of manifest.definitionPack.chunks)definitionParts.push(Uint8Array.from(atob(await read(c.key)),x=>x.charCodeAt(0)));const definitionData=new Uint8Array(definitionParts.reduce((n,p)=>n+p.length,0));let offset=0;for(const p of definitionParts){definitionData.set(p,offset);offset+=p.length;}const definitionWire=new TextDecoder('utf-8',{fatal:true}).decode(definitionData);need(await hash(definitionWire)===manifest.definitionPack.digest,'pa_archive_definition_digest');await validatedDefinitions(definitionWire,manifest.snapshot.definitions.engine);let record;
 for(const page of manifest.pages){const decoded=JSON.parse(await read(page.key));need(decoded.format===manifest.format,'pa_archive_manifest');record=decoded.records.find(r=>r.id===(entry.recordId??entry.id));if(record)break;}
 need(record&&record.digest===(entry.recordDigest??entry.digest)&&record.available_at===entry.available_at&&record.created_at===entry.created_at&&record.cohort===entry.cohort&&record.definition===entry.definition&&record.objects.length===record.chunk_count&&record.chunk_count<=50,'pa_archive_entry');
 const parts=[];for(let i=0;i<record.objects.length;i++){need(record.objects[i].ordinal===i,'pa_archive_geometry');parts.push(Uint8Array.from(atob(await read(record.objects[i].key)),c=>c.charCodeAt(0)));}
 const wire=await decodeWire(parts,record);const payload=parseEvidence(wire);if(entry.lookupType==='SAMPLE'){const sample=payload.samples?.[entry.sampleIndex];need(sample&&'sample:'+sample.id===entry.id&&await hash({type:sample.type,value:sample.value,occurredAt:sample.occurredAt})===entry.sampleDigest,'pa_archive_sample_identity');return {...record,id:entry.id,sampleDigest:entry.sampleDigest,wire:serialize(sample),scopeState:entry.scopeState};}return {...record,wire,scopeState:entry.scopeState,sourceSnapshot:manifest.snapshot,sourcePackageKey:entry.packageKey};
}
export async function discoverArchiveRoot(archive,{fallback='FAIL_CLOSED',cursor=null,limit=64,continuation=null}={}){
 need(['FAIL_CLOSED','VERIFIED_PREDECESSOR'].includes(fallback),'pa_root_fallback_policy');
 let best=continuation?.bestRoot?await verifyRoot(archive,continuation.bestRoot):null,invalidCount=continuation?.invalidCount??0,scanned=continuation?.scanned??0;
 need(Number.isSafeInteger(invalidCount)&&invalidCount>=0&&Number.isSafeInteger(scanned)&&scanned>=0,'pa_root_discovery_token');
 const listing=await archive.listRoots({cursor:continuation?.cursor??cursor,limit}),invalid=[];
 for(const o of listing.objects){scanned++;try{const root=await verifyRoot(archive,o.key);if(best){need(root.epoch===best.epoch,'pa_root_epoch_ambiguity');need(root.generation!==best.generation||root.key===best.key,'pa_root_generation_conflict');}if(!best||root.generation>best.generation)best=root;}catch(error){if(/epoch_ambiguity|generation_conflict/.test(error.message))throw error;invalidCount++;if(invalid.length<8)invalid.push({key:o.key,error:error.message});}}
 if(listing.truncated)return {status:'PENDING',reason:'root_discovery_budget',continuation:{cursor:listing.cursor,bestRoot:best?.key??null,invalidCount,scanned},fallback};
 need(best,'pa_root_unavailable');need(fallback==='VERIFIED_PREDECESSOR'||invalidCount===0,'pa_corrupt_root_requires_policy');
 if(best.previous){const previous=await verifyRoot(archive,best.previous);need(previous.generation<best.generation&&previous.epoch===best.epoch,'pa_root_chain');}
 return {...best,status:'COMPLETE',discovery:{invalid,invalidCount,scanned,fallback}};
}
export function archiveReader(archive,{rootKey=null,getRoot=null}={}){let cachedKey=null,cachedRoot=null;const rootAt=async key=>{if(key!==cachedKey){cachedKey=key;cachedRoot=verifyRoot(archive,key);try{await cachedRoot;}catch(error){cachedKey=null;cachedRoot=null;throw error;}}return cachedRoot;};return Object.freeze({async readRecord(packageKey,id){const verified=await verifyArchivePackage(archive,packageKey);return verified.records.find(r=>r.id===id)??null;},
 async lookupRecord(id,{rootKey:requestedRoot,maxObjects=64}={}){const key=requestedRoot??(getRoot?await getRoot():rootKey);if(!key)return null;const root=await rootAt(key);const entry=await catalogueLookup(archive,root.indexRoot,id,{maxObjects});if(!entry)return null;const retained=root.verified.records.find(r=>r.id===(entry.recordId??entry.id));if(retained&&entry.lookupType!=='SAMPLE'){need(retained.digest===entry.digest,'pa_archive_entry');return {...retained,sourceSnapshot:root.verified.snapshot,scopeState:entry.scopeState,sourcePackageKey:entry.packageKey};}return readArchiveEntry(archive,entry);},
 async lookupSemantic(semanticKey){const key=getRoot?await getRoot():rootKey;if(!key)return null;const root=await rootAt(key),entry=await catalogueLookup(archive,root.indexRoot,'semantic:'+semanticKey);return entry?readArchiveEntry(archive,entry):null;},
 async marketPage(cohort,{after='',limit=8}={}){const key=getRoot?await getRoot():rootKey;if(!key)return {records:[],complete:true,next:after};const root=await rootAt(key),alias=await catalogueLookup(archive,root.indexRoot,'cohort-market:'+cohort);if(!alias)return {records:[],complete:true,next:after};const page=await cataloguePage(archive,alias.marketIndex,{after,limit,maxObjects:64});const records=[];for(const entry of page.entries)records.push({...await readArchiveEntry(archive,entry),marketOrder:entry.order_key});return {...page,records};},
 async lookupScope(scopeId){const key=getRoot?await getRoot():rootKey;if(!key)return null;const root=await rootAt(key),entry=await catalogueLookup(archive,root.indexRoot,'scope:'+scopeId);return entry?.scopeState?{...entry.scopeState,archive_root:key}:null;},
 async scopeRecords(scopeId,{rootKey:requestedRoot,factsOnly=true}={}){const key=requestedRoot??(getRoot?await getRoot():rootKey);if(!key)return [];const root=await rootAt(key),scope=await catalogueLookup(archive,root.indexRoot,'scope:'+scopeId);if(!scope)return [];
  const records=[];let after='',complete=false;for(let page=0;page<4&&!complete;page++){const result=await cataloguePage(archive,factsOnly?scope.factIndex:scope.scopeIndex,{after,limit:Math.min(129,LIMITS.scopeRecords+1-records.length),maxObjects:64});for(const entry of result.entries)records.push(await readArchiveEntry(archive,entry));need(records.length<=LIMITS.scopeRecords,'pa_archive_scope_bound');need(result.complete||result.next>after,'pa_archive_scope_pending');after=result.next;complete=result.complete;}need(complete,'pa_archive_scope_pending');return records;}});}
export async function archiveOrphanEligibility(db,key,{archive,rootKey,now}={}){
 if(key.includes('/catalogue/'))return {eligible:false,reason:'catalogue_tree_proof_required'};
 const active=await db.first("SELECT id FROM pa_publications WHERE manifest_key=? OR receipt_key=? OR root_key=? OR state IN('PLANNED','SNAPSHOT_PINNED','OBJECTS_WRITTEN','VERIFIED','CATALOGUED') LIMIT 1",key,key,key);
 if(active)return {eligible:false,reason:'publication_reference_or_pending'};
 if(await db.first("SELECT id FROM pa_pins WHERE until_at>? LIMIT 1",now)||await db.first("SELECT id FROM pa_corrections WHERE state='PENDING' LIMIT 1"))return {eligible:false,reason:'retention_or_recovery_hold'};
 // No generic age deletion: retained root chains can own arbitrary chunk dependencies.
 if(rootKey){let root=rootKey;for(let i=0;root&&i<64;i++){const r=await verifyRoot(archive,root);if([root,r.manifestKey,r.receiptKey,...r.verified.manifest.pages.map(p=>p.key),...r.verified.manifest.definitionPack.chunks.map(p=>p.key)].includes(key)||r.verified.records.some(x=>x.objects.some(o=>o.key===key)))return {eligible:false,reason:'retained_archive_reference'};root=r.previous;}if(root)return {eligible:false,reason:'proof_budget_exhausted'};}
 return {eligible:true,destructiveOperationEnabled:false,proof:'bounded_complete_reference_walk'};
}
export async function migrateArchiveFormat(db,archive,oldRoot,{now,owner=uid(),hook=null}={}){
 const source=await verifyRoot(archive,oldRoot),id='format:'+await hash([oldRoot,'measurement-archive-v2']);
 await db.batch([assertion(db,'(SELECT COUNT(*) FROM pa_format_work)<64 OR EXISTS(SELECT 1 FROM pa_format_work WHERE id=?)',id),db.statement("INSERT INTO pa_format_work(id,old_root,state) VALUES(?,?,'PLANNED') ON CONFLICT DO NOTHING",[id,oldRoot]),db.statement("INSERT INTO pa_jobs(id,class,event_id,next_at,deadline) VALUES(?,'archive',?,?,?) ON CONFLICT DO NOTHING",[id,id,now,now+DAY]),clearAssertions(db)]);
 let work=await db.first('SELECT * FROM pa_format_work WHERE id=?',id);if(work.state==='COMPLETE')return work;
 const lease=await leaseJob(db,id,owner,{now});need(lease,'pa_format_lease_pending');
 const update=async(state,fields='',values=[])=>{await db.batch([jobFence(db,lease,now),db.statement(`UPDATE pa_format_work SET state=?${fields} WHERE id=?`,[state,...values,id]),clearAssertions(db)]);await hook?.(state);};
 if(work.state==='PLANNED'){await update('SNAPSHOT_PINNED');work=await db.first('SELECT * FROM pa_format_work WHERE id=?',id);}
 if(work.state==='SNAPSHOT_PINNED'){
  const pages=[];for(const old of source.verified.manifest.pages){const page=JSON.parse(await archive.read(old.key));page.format='measurement-archive-v2';pages.push(await archive.write('manifests',page));}
  const manifest=await archive.write('packages',{...source.verified.manifest,format:'measurement-archive-v2',pages,supersedes:source.manifestKey});await update('OBJECTS_WRITTEN',',new_manifest=?',[manifest.key]);work=await db.first('SELECT * FROM pa_format_work WHERE id=?',id);
 }
 if(work.state==='OBJECTS_WRITTEN'){const target=await verifyArchivePackage(archive,work.new_manifest);need(await hash(target.records.map(r=>[r.id,r.digest,r.available_at]))===await hash(source.verified.records.map(r=>[r.id,r.digest,r.available_at])),'pa_format_semantics_changed');
  const receipt=await archive.write('receipts',{format:'measurement-verification-v1',publication:id,epoch:source.epoch,sourceRevision:source.sourceRevision,manifestKey:work.new_manifest,membershipDigest:await hash(target.snapshot.membership),expectedCount:target.snapshot.expectedCount,verifiedAt:now,readback:true,supersedes:source.receiptKey});await update('VERIFIED',',new_receipt=?',[receipt.key]);work=await db.first('SELECT * FROM pa_format_work WHERE id=?',id);
 }
 if(work.state==='VERIFIED'){const meta=await db.first('SELECT * FROM pa_meta');need(meta.root_key===oldRoot,'pa_format_catalogue_changed');let indexRoot=source.indexRoot;for(const member of source.verified.snapshot.membership)indexRoot=await catalogueInsert(archive,indexRoot,{...member,packageKey:work.new_manifest,receiptKey:work.new_receipt});
  const root=await archive.write('roots',{format:'measurement-root-v1',epoch:source.epoch,sourceRevision:source.sourceRevision,scopeId:source.scopeId,generation:meta.catalogue_generation+1,publication:id,manifestKey:work.new_manifest,receiptKey:work.new_receipt,indexRoot,previous:oldRoot,supersedes:oldRoot,publishedAt:now});
  await db.batch([jobFence(db,lease,now),assertion(db,'EXISTS(SELECT 1 FROM pa_meta WHERE root_key=? AND catalogue_generation=?)',oldRoot,meta.catalogue_generation),db.statement('UPDATE pa_meta SET root_key=?,catalogue_generation=catalogue_generation+1',[root.key]),db.statement("UPDATE pa_format_work SET state='CATALOGUED',new_root=? WHERE id=?",[root.key,id]),clearAssertions(db)]);work=await db.first('SELECT * FROM pa_format_work WHERE id=?',id);
 }
 if(work.state==='CATALOGUED'){const root=JSON.parse(await archive.read(work.new_root));await archive.write('checkpoints',rootCheckpoint(root,work.new_root));await update('COMPLETE');await finishJob(db,lease,now);work=await db.first('SELECT * FROM pa_format_work WHERE id=?',id);}
 return work;
}
// Root commit recovery is independent of later source revisions. The checkpoint
// proves the earlier SQL catalogue authorization; it never grants fresh eviction.
export async function recoverRootCheckpoint(db,archive,publicationId,{now,lease}={}){
 const p=await db.first('SELECT p.*,m.epoch AS current_epoch FROM pa_publications p CROSS JOIN pa_meta m WHERE p.id=?',publicationId);need(p?.root_key&&p.epoch===p.current_epoch,'pa_root_recovery_authority');
 const guard=()=>[jobFence(db,lease,now),assertion(db,'EXISTS(SELECT 1 FROM pa_publications p,pa_meta m WHERE p.id=? AND p.root_key=? AND p.manifest_key=? AND p.receipt_key=? AND p.epoch=m.epoch AND p.catalogue_generation<=m.catalogue_generation)',p.id,p.root_key,p.manifest_key,p.receipt_key)];
 await db.batch([...guard(),clearAssertions(db)]);const root=JSON.parse(await archive.read(p.root_key));need(root.publication===p.id&&root.manifestKey===p.manifest_key&&root.receiptKey===p.receipt_key&&root.epoch===p.epoch&&root.generation===p.catalogue_generation&&root.sourceRevision===p.revision,'pa_root_recovery_identity');
 const receipt=JSON.parse(await archive.read(p.receipt_key)),verified=await verifyArchivePackage(archive,p.manifest_key);need(receipt.readback===true&&receipt.membershipDigest===await hash(verified.snapshot.membership),'pa_root_recovery_verification');
 await archive.write('checkpoints',rootCheckpoint(root,p.root_key));await db.batch([...guard(),db.statement("UPDATE pa_publications SET state='STALE' WHERE id=? AND state='CATALOGUED' AND EXISTS(SELECT 1 FROM pa_scopes WHERE id=pa_publications.scope_id AND (revision!=pa_publications.input_revision OR archive_revision!=pa_publications.revision))",[p.id]),clearAssertions(db)]);return {status:'ROOT_COMMIT_RECOVERED',rootKey:p.root_key,evictionAuthority:false};
}
