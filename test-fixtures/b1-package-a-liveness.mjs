import assert from 'node:assert/strict';
import {packageDatabase,fakeBucket} from './b1-package-a.mjs';
import {appendIndependentRecord} from '../measurement-independent-store.js';
import {createPrivateArchiveAdapter,publishArchive,planPublication,verifyRoot} from '../measurement-archive.js';
import {hydrateArchive} from '../measurement-restore.js';
import {compactIndependentState} from '../measurement-independent-retention.js';
import worker from '../measurement-worker.js';

export const namespace='synthetic-b1-liveness/';
export function emptyArchive(){const b=fakeBucket();return {...b,archive:createPrivateArchiveAdapter(b.bucket,{namespace})};}
export async function checkpoint(a,root){const o=await a.archive.write('roots',root);await a.archive.write('checkpoints',{format:'measurement-root-commit-v1',rootKey:o.key,publication:root.publication,epoch:root.epoch,generation:root.generation,sourceRevision:root.sourceRevision,sqlCatalogueAuthorized:true});return o.key;}
const blueprints=new Map();
export async function history(count=129,{records=1,payloadBytes=0}={}){
 const signature=records+':'+payloadBytes;
 if(!blueprints.has(signature))blueprints.set(signature,(async()=>{
  const x=await packageDatabase(),a=emptyArchive();try{
   const ids=Array.from({length:records},(_,i)=>'seed:'+String(i).padStart(3,'0'));let scope;
   for(let i=records-1;i>=0;i--){const r=await appendIndependentRecord(x.db,{id:ids[i],kind:'LOCAL_TEST',scopeId:scope,cohort:'review',createdAt:1,payload:{measurementOnly:true,body:'x'.repeat(payloadBytes)},dependencies:i===0?ids.slice(1):[],category:'DETAIL'},{now:100});scope=r.scope_id;}
   let p;for(let turn=0;turn<10;turn++){p=await publishArchive(x.db,a.archive,scope,{now:100,owner:'fixture'});if(!p.pending)break;}assert.equal(p.state,'EVICTION_ELIGIBLE');assert.equal((await verifyRoot(a.archive,p.root_key)).verified.records.length,records);
   return {objects:[...a.objects],root:p.root_key};
  }finally{x.close();}
 })());
 const source=await blueprints.get(signature),a=emptyArchive();for(const [k,v] of source.objects)a.objects.set(k,v);
 a.root=source.root;a.roots=[a.root];while(a.roots.length<count)await successor(a);return a;
}
export async function successor(a,{epoch=null,lineageTransition=null,publication=null,manifestKey=null,receiptKey=null,generation=null,extra={}}={}){
 const old=JSON.parse(await a.archive.read(a.root)),proof=JSON.parse(await a.archive.read(old.receiptKey));
 const id=publication??'history:'+a.roots.length,targetEpoch=epoch??old.epoch,manifest=manifestKey??old.manifestKey;
 const receipt=receiptKey??(await a.archive.write('receipts',{...proof,publication:id,epoch:targetEpoch,manifestKey:manifest})).key;
 const root={...old,...extra,epoch:targetEpoch,publication:id,generation:generation??old.generation+1,manifestKey:manifest,receiptKey:receipt,previous:a.root};
 delete root.lineageTransition;if(lineageTransition)root.lineageTransition=lineageTransition;
 a.root=await checkpoint(a,root);a.roots.push(a.root);return a.root;
}
export async function finish(operation,{continuation=null,max=1000}={}){
 const steps=[];let r;for(let i=0;i<max;i++){r=await operation(continuation);steps.push(r);if(r.status!=='PENDING')return {result:r,steps};assert(r.continuation);continuation=r.continuation;}throw Error('test_no_progress');
}
export async function restore(x,a){return finish(c=>hydrateArchive(x.db,a.archive,a.root,{now:1000,owner:'restore',environment:'synthetic-b1-package-a',authorization:{operation:'restore',environment:'synthetic-b1-package-a',expiresAt:121000},lineageContinuation:c}));}
export async function seed(x,id){return (await appendIndependentRecord(x.db,{id,kind:'LOCAL_TEST',cohort:'review',createdAt:1,payload:{measurementOnly:true},category:'DETAIL'},{now:100})).scope_id;}
export async function obsolete(x,a,{id='obsolete',now=2000}={}){
 const scope=await seed(x,id),p=await publishArchive(x.db,a.archive,scope,{now,maxSteps:4,owner:'old'});assert.equal(p.state,'CATALOGUED');x.raw.prepare('UPDATE pa_scopes SET revision=revision+1 WHERE id=?').run(scope);now+=60001;await planPublication(x.db,scope,{now,owner:'new'});assert.equal(x.raw.prepare('SELECT state FROM pa_publications WHERE id=?').get(p.id).state,'STALE');return {p,scope,now};
}
export async function maintain(x,a,now,hook=null){return compactIndependentState(x.db,{now,retirementArchive:a.archive,hook});}
export async function scheduled(x,a,now){
 const saved=Date.now,tasks=[];Date.now=()=>now;try{
  await worker.scheduled({}, {B1_MEASUREMENT_CONSUMER_ENABLED:'1',B1_ARCHIVE_WRITE_ENABLED:'1',MEASUREMENT_DB:x.binding,MEASUREMENT_ARCHIVE:a.bucket,MEASUREMENT_ARCHIVE_NAMESPACE:namespace,MEASUREMENT_ENVIRONMENT:'synthetic-b1-package-a',MEASUREMENT_EXPECTED_D1_ID:'LOCAL_ONLY_NO_CLOUD_ID'}, {waitUntil(p){tasks.push(p);}});await Promise.all(tasks);
 }finally{Date.now=saved;}
}
export async function register(x,a,options={}){
 const meta=x.raw.prepare('SELECT catalogue_generation,epoch FROM pa_meta').get(),old=JSON.parse(await a.archive.read(a.root)),epoch=options.epoch??meta.epoch;let lineageTransition=options.lineageTransition;
 if(old.epoch!==epoch&&epoch===meta.epoch){const l=x.raw.prepare('SELECT * FROM pa_archive_lineage WHERE target_epoch=?').get(epoch);assert(l&&l.source_root===a.root);const t=await a.archive.write('transitions',l.transition_wire);assert.equal(t.key,l.transition_key);lineageTransition=t.key;}
 const key=await successor(a,{...options,epoch,lineageTransition,generation:Math.max(meta.catalogue_generation,old.generation)+1});const r=JSON.parse(await a.archive.read(key));x.raw.prepare('UPDATE pa_meta SET root_key=?,catalogue_generation=?').run(key,r.generation);return key;
}
