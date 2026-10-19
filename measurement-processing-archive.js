// Verification-only package facts. No processing/binding/completion transitions.
import {need,hash,serialize,parseEvidence,bytes,LIMITS} from './measurement-independent-contract.js';
import {decodeWire} from './measurement-wire-codec.js';
import {validateMeasurementWire} from './measurement-consumer.js';
import {validatedDefinitions,ARCHIVE_LIMITS,verifyRootCheckpoint} from './measurement-archive.js';
import {catalogueLookup,cataloguePage} from './measurement-archive-catalog.js';
import {ProcessingYield,validateDecodedMarket} from './measurement-processing-budget.js';
export const PACKAGE_PROOF_VERSION='pa-package-verification-v1';
const decodedArchiveBytes=Symbol('verified-archive-read-bytes');
const budgetError=e=>['pa_archive_progress_capacity','pa_archive_progress_raced'].includes(e.message);
export function boundedProcessingArchive(raw,budget,{rootKey=null,getRoot=null}={}){
 budget.archiveRemaining=()=>raw.remaining();
 const archive=raw.processingSession(budget),heads=new Map(),roots=new Map(),decoded=new Map(),verified=new Set();let cacheBytes=0;
 const nodes=new Map(),catalogue={...archive,async read(key){if(nodes.has(key))return nodes.get(key);budget.reserve({objects:1,bytes:60000,catalogue:1});const wire=await archive.read(key);nodes.set(key,wire);need(nodes.size<=64,'pa_processing_catalogue_reads');return wire;}};
 async function packageHead(packageKey,receiptKey){
  const key=packageKey+'|'+receiptKey;if(heads.has(key))return heads.get(key);
  budget.reserve({objects:72,bytes:4320000});
  let objects=0,total=0;const read=async key=>{const wire=await archive.read(key);objects++;total+=bytes(wire);need(objects<=ARCHIVE_LIMITS.objects+16&&total<=ARCHIVE_LIMITS.bytes*2,'pa_archive_read_budget');return wire;};
  const manifest=JSON.parse(await read(packageKey)),snapshot=manifest.snapshot;
  need(['measurement-archive-v1','measurement-archive-v2'].includes(manifest.format)&&Array.isArray(manifest.pages)&&manifest.pages.length<=16,'pa_archive_format');
  need(snapshot?.definitions?.codec==='b1-canonical-v1'&&snapshot.definitions.engine==='6721f84-engine-v1'&&snapshot.membership?.length<=128&&['SELF_CONTAINED_RAW_CYCLES','CENSUS_SELF_CONTAINED'].includes(snapshot.dependencyClosure),'pa_archive_definition');
  const receipt=receiptKey?JSON.parse(await archive.read(receiptKey)):{membershipDigest:await hash(snapshot.membership)};
  if(receiptKey)need(receipt.format==='measurement-verification-v1'&&receipt.manifestKey===packageKey&&receipt.readback===true&&receipt.membershipDigest===await hash(snapshot.membership),'pa_archive_receipt');
  need(manifest.definitionPack?.version==='measurement-definitions-v1'&&manifest.definitionPack.chunks.length>0&&manifest.definitionPack.chunks.length<=50,'pa_archive_definition_missing');
  const chunks=[];for(const c of manifest.definitionPack.chunks)chunks.push(Uint8Array.from(atob(await read(c.key)),x=>x.charCodeAt(0)));
  const data=new Uint8Array(chunks.reduce((n,c)=>n+c.length,0));let at=0;for(const c of chunks){data.set(c,at);at+=c.length;}
  const definitionWire=new TextDecoder('utf-8',{fatal:true}).decode(data);need(await hash(definitionWire)===manifest.definitionPack.digest,'pa_archive_definition_digest');await validatedDefinitions(definitionWire,snapshot.definitions.engine);
  const records=[];for(const page of manifest.pages){const p=JSON.parse(await read(page.key));need(p.format===manifest.format&&Array.isArray(p.records),'pa_archive_manifest');records.push(...p.records);need(records.length<=128,'pa_archive_counts');}
  need(records.length===snapshot.expectedCount&&records.length===snapshot.membership.length&&new Set(records.map(r=>r.id)).size===records.length,'pa_archive_counts');
  for(const m of snapshot.membership){const r=records.find(r=>r.id===m.id);need(r&&r.digest===m.digest&&r.available_at===m.available_at&&r.created_at===m.created_at,'pa_archive_identity');}
  for(const r of records){need(Array.isArray(r.objects)&&r.objects.length===r.chunk_count&&r.chunk_count<=50&&r.chunk_count>=1,'pa_archive_geometry');for(let i=0;i<r.objects.length;i++)need(r.objects[i].ordinal===i,'pa_archive_geometry');for(const dep of r.dependencies??[])need(records.some(x=>x.id===dep),'pa_archive_dependency_missing');}
  for(const b of snapshot.bindings)need(records.some(r=>r.id==='decision:'+b.evaluation_id)&&records.some(r=>r.id==='event:'+b.first_event),'pa_archive_dependency_missing');
  for(const b of snapshot.censusBindings??[])need(records.some(r=>r.id==='decision:'+b.evaluation_id&&r.digest===b.digest),'pa_archive_census_binding');
  const context={operation:PACKAGE_PROOF_VERSION,namespace:archive.namespace,packageKey,receiptKey,format:manifest.format,definition:manifest.definitionPack.digest,membership:receipt.membershipDigest};
  const h={manifest,snapshot,receipt,records,context,identity:await hash(context),objects,bytes:total};heads.set(key,h);return h;
 }
 async function decode(r){
  const key=await hash(r);if(decoded.has(key))return decoded.get(key);
  budget.reserve({objects:r.objects.length,bytes:r.objects.length*60000,records:1,market:1,decodedBytes:r.bytes});budget.record(r);
  let actualBytes=0;const chunks=[];for(const o of r.objects){const encoded=await archive.read(o.key);actualBytes+=bytes(encoded);chunks.push(Uint8Array.from(atob(encoded),x=>x.charCodeAt(0)));}
  const wire=await decodeWire(chunks,r),p=parseEvidence(wire);
  if(r.id.startsWith('event:')){const e=await validateMeasurementWire(wire,{receivedAt:Math.max(r.available_at,r.created_at)});need(r.id==='event:'+e.eventId,'pa_archive_semantic_identity');}
  const result=await validateDecodedMarket({...r,wire,[decodedArchiveBytes]:actualBytes},budget,p);while(cacheBytes+bytes(wire)>4000000&&decoded.size){const [id,v]=decoded.entries().next().value;cacheBytes-=bytes(v.wire);decoded.delete(id);}decoded.set(key,result);cacheBytes+=bytes(wire);return result;
 }
 async function verifyPackage(packageKey,receiptKey){
  budget.phase('ARCHIVE_VERIFY');const h=await packageHead(packageKey,receiptKey);if(verified.has(h.identity))return h;budget.reserve({objects:68,bytes:4080000});
  let loaded;try{loaded=await archive.progress.acquire(h.context,{claim:true});}catch(e){if(budgetError(e))throw new ProcessingYield('ARCHIVE_BACKPRESSURE');throw e;}
  let s=loaded?.state??{version:PACKAGE_PROOF_VERSION,identity:h.identity,phase:'RECORDS',record:0,objects:h.objects,bytes:h.bytes,generation:0,complete:false};
  need(s.version===PACKAGE_PROOF_VERSION&&s.identity===h.identity&&Number.isSafeInteger(s.generation)&&s.generation>=0&&Number.isSafeInteger(s.record)&&s.record>=0&&s.record<=h.records.length&&Number.isSafeInteger(s.objects)&&s.objects>=h.objects&&s.objects<=ARCHIVE_LIMITS.objects+16&&Number.isSafeInteger(s.bytes)&&s.bytes>=h.bytes&&s.bytes<=ARCHIVE_LIMITS.bytes*2&&s.complete===(s.phase==='COMPLETE')&&(!s.complete||s.record===h.records.length),'pa_package_progress_integrity');
  if(s.complete){verified.add(h.identity);return h;}
  s=structuredClone(s);
  while(s.record<h.records.length){const r=h.records[s.record];
   if(!budget.can({objects:r.objects.length+4,bytes:r.objects.length*60000+240000,records:1,market:1,decodedBytes:r.bytes}))break;
   const record=await decode(r);
   for(const b of h.snapshot.censusBindings??[])if(record.id==='decision:'+b.evaluation_id)need(record.digest===b.digest&&parseEvidence(record.wire).evaluationId===b.evaluation_id,'pa_archive_census_binding');
   s.record++;s.objects+=r.objects.length;s.bytes+=record[decodedArchiveBytes];
   need(s.objects<=ARCHIVE_LIMITS.objects+16&&Number.isSafeInteger(s.bytes)&&s.bytes<=ARCHIVE_LIMITS.bytes*2,'pa_archive_read_budget');
  }
  s.generation++;if(s.record===h.records.length){s.complete=true;s.phase='COMPLETE';}
  try{budget.beforeCheckpoint();await budget.notify?.('ARCHIVE_PROGRESS_PREPARED',{context:h.context,state:s});await archive.progress.save(h.context,s,loaded);budget.committed();await budget.notify?.('ARCHIVE_PROGRESS_COMMITTED',{context:h.context,state:s});}catch(e){if(budgetError(e))throw new ProcessingYield('ARCHIVE_BACKPRESSURE');throw e;}
  if(!s.complete)throw new ProcessingYield('ARCHIVE_VERIFY');verified.add(h.identity);return h;
 }
 async function root(key){
  if(roots.has(key))return roots.get(key);budget.reserve({objects:3,bytes:180000});
  need(typeof key==='string'&&key.startsWith(archive.namespace+'roots/')&&/^[a-f0-9]{64}$/.test(key.slice((archive.namespace+'roots/').length)),'pa_root_namespace');
  const r=JSON.parse(await archive.read(key));need(r.format==='measurement-root-v1'&&Number.isSafeInteger(r.generation)&&r.generation>0,'pa_root_format');await verifyRootCheckpoint(archive,r,key);
  const h=await verifyPackage(r.manifestKey,r.receiptKey);need(h.receipt.publication===r.publication&&h.receipt.epoch===r.epoch&&h.receipt.sourceRevision===r.sourceRevision,'pa_root_receipt');
  const result={...r,key,verified:h};roots.set(key,result);return result;
 }
 async function selectedRoot(key){const selected=key??(getRoot?await getRoot():rootKey);return selected?root(selected):null;}
 async function entryRecord(entry){
  const h=await verifyPackage(entry.packageKey,entry.receiptKey),r=h.records.find(r=>r.id===(entry.recordId??entry.id));
  need(r&&r.digest===(entry.recordDigest??entry.digest)&&r.available_at===entry.available_at&&r.created_at===entry.created_at&&r.cohort===entry.cohort&&r.definition===entry.definition,'pa_archive_entry');
  const result={...await decode(r),sourceSnapshot:h.snapshot,scopeState:entry.scopeState,sourcePackageKey:entry.packageKey};
  if(entry.lookupType==='SAMPLE'){const s=parseEvidence(result.wire).samples?.[entry.sampleIndex];need(s&&'sample:'+s.id===entry.id&&await hash({type:s.type,value:s.value,occurredAt:s.occurredAt})===entry.sampleDigest,'pa_archive_sample_identity');return {...result,id:entry.id,sampleDigest:entry.sampleDigest,wire:serialize(s)};}
  return result;
 }
 async function lookup(id,key){const r=await selectedRoot(key);if(!r)return null;const e=await catalogueLookup(catalogue,r.indexRoot,id);return e?entryRecord(e):null;}
 const api={
  budget,raw:archive,verifyPackage,root,decode,
  async readRecord(packageKey,id){
   // Direct archived rows carry only the package key. The immutable package's
   // verification receipt is discovered from an authenticated catalogue entry.
   const r=await selectedRoot();need(r,'pa_processing_archive_required');const e=await catalogueLookup(catalogue,r.indexRoot,id);
   need(e,'pa_processing_archive_membership');
   if(e.packageKey===packageKey)return entryRecord(e);
   // The live row's original container remains a required witness. A newer
   // catalogue entry authenticates current membership, not container rebinding.
   await verifyPackage(e.packageKey,e.receiptKey);
   const old=await verifyPackage(packageKey,null),record=old.records.find(r=>r.id===id);
   need(record&&record.digest===e.digest&&record.available_at===e.available_at&&record.created_at===e.created_at&&record.cohort===e.cohort,'pa_processing_archive_membership');
   return {...await decode(record),sourceSnapshot:old.snapshot,sourcePackageKey:packageKey};
  },
  lookupRecord:(id,{rootKey:key}={})=>lookup(id,key),
  async lookupScope(id){const r=await selectedRoot();if(!r)return null;const e=await catalogueLookup(catalogue,r.indexRoot,'scope:'+id);return e?.scopeState?{...e.scopeState,archive_root:r.key}:null;},
  async scopePage(id,{after='',rootKey:key,factsOnly=true,limit=8}={}){const r=await selectedRoot(key);const scope=await catalogueLookup(catalogue,r.indexRoot,'scope:'+id);if(!scope)return {entries:[],complete:true,next:after};return cataloguePage(catalogue,factsOnly?scope.factIndex:scope.scopeIndex,{after,limit,maxObjects:64});},
  entryRecord,
  async processingRootStep(key){await root(key);return {version:PACKAGE_PROOF_VERSION,key,complete:true};},
  async marketEntriesPage(cohort,{after='',rootKey:key,limit=9}={}){const r=await selectedRoot(key);if(!r)return {entries:[],complete:true,next:after};const e=await catalogueLookup(catalogue,r.indexRoot,'cohort-market:'+cohort);if(!e)return {entries:[],complete:true,next:after};return cataloguePage(catalogue,e.marketIndex,{after,limit,maxObjects:64});},
  async processingMarketPage(cohort,{after='',rootKey:key,limit=1}={}){const r=await selectedRoot(key);const e=await catalogueLookup(catalogue,r.indexRoot,'cohort-market:'+cohort);if(!e)return {records:[],complete:true,next:after};const page=await cataloguePage(catalogue,e.marketIndex,{after,limit,maxObjects:64}),records=[];for(const x of page.entries)records.push({...await entryRecord(x),marketOrder:x.order_key});return {...page,records};},
  processingRecord:(id,{rootKey:key}={})=>lookup(id,key),
  async processingAlias(id,{rootKey:key}={}){const r=await selectedRoot(key);return catalogueLookup(catalogue,r.indexRoot,id);},
  marketPage:(...args)=>api.processingMarketPage(...args),
  remaining:()=>Math.min(archive.remaining(),1024-budget.trace().archiveOperations)
 };
 return api;
}
