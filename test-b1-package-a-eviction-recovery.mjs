import {test} from 'node:test';
import assert from 'node:assert/strict';
import {packageDatabase,fakeBucket,gate} from './test-fixtures/b1-package-a.mjs';
import {appendIndependentRecord} from './measurement-independent-store.js';
import {createPrivateArchiveAdapter,publishArchive,verifyRoot,verifyArchiveLineage} from './measurement-archive.js';
import {verifyArchiveParity,evictPublication} from './measurement-independent-retention.js';
import {reconcilePublicationEvictionAuthority,decodeEvictionProgress} from './measurement-eviction-reconciliation.js';
import {createMeasurementD1Adapter} from './measurement-d1-adapter.js';
import {admitCorrection} from './measurement-corrections.js';
import {reserve} from './measurement-reservations.js';

const namespace='synthetic-b1-eviction-recovery/';
async function fixture(){
 const systemNow=Date.now;let clock=systemNow();Date.now=()=>clock;
 const x=await packageDatabase({clock:()=>clock}),b=fakeBucket(),archive=createPrivateArchiveAdapter(b.bucket,{namespace});
 try{
  async function add(id){const r=await appendIndependentRecord(x.db,{id,kind:'LOCAL_TEST',cohort:'review',createdAt:1,payload:{measurementOnly:true},category:'DETAIL'},{now:clock});return publishArchive(x.db,archive,r.scope_id,{now:clock});}
  const a=await add('older'),next=await add('later');await verifyArchiveParity(x.db,archive,a.id);
  const original=JSON.parse(await archive.read(next.root_key)),receipt=JSON.parse(await archive.read(original.receiptKey));let previous=next.root_key;
  for(let i=3;i<=32;i++){
   const publication='recovery-history:'+i,r=await archive.write('receipts',{...receipt,publication});
   const root={...original,publication,receiptKey:r.key,generation:i,previous},o=await archive.write('roots',root);
   await archive.write('checkpoints',{format:'measurement-root-commit-v1',rootKey:o.key,publication,epoch:root.epoch,generation:i,sourceRevision:root.sourceRevision,sqlCatalogueAuthorized:true});previous=o.key;
  }
  x.raw.prepare('UPDATE pa_meta SET root_key=?,catalogue_generation=32').run(previous);
  return {...x,...b,archive,a,next,add,root:previous,get now(){return clock;},advance(ms){clock+=ms;},close(){x.close();Date.now=systemNow;}};
 }catch(error){x.close();Date.now=systemNow;throw error;}
}
const call=(x,continuation=null,options={})=>reconcilePublicationEvictionAuthority(x.db,x.archive,x.a.id,{now:x.now,owner:'reconciler',continuation,...options});
const saved=async x=>decodeEvictionProgress(x.raw.prepare('SELECT eviction_progress FROM pa_publications WHERE id=?').get(x.a.id).eviction_progress);
const count=x=>x.raw.prepare("SELECT COUNT(*) n FROM pa_records WHERE id='older'").get().n;
async function ready(x,result){let r=result;for(let n=0;n<100;n++){if(r?.status==='READY')return r;r=await call(x,r?.continuation);assert.equal(r.status==='READY'||r.status==='PENDING',true);assert(r.reads<=1024);assert(r.continuation);}throw Error('recovery_not_progressing');}
async function expire(x){const initial=await call(x);assert.equal(initial.progress.phase,'LINEAGE');assert((await saved(x)).lineage);x.advance(3600001);return initial;}
async function evict(x,r){return evictPublication(x.db,x.archive,x.a.id,{now:x.now,reconciliation:r.continuation});}

test('PA-EVICT-REVIEW-01 control: actual 32-root reconciliation reaches READY in six invocations',async()=>{const x=await fixture();try{let r,turns=0;do{r=await call(x,r?.continuation);turns++;assert(r.reads<=1024);}while(r.status==='PENDING');assert.equal(turns,6);assert.equal(r.status,'READY');assert.equal((await evict(x,r)).status,'EVICTED');}finally{x.close();}});
test('PA-EVICT-REVIEW-01 A: expired archive continuation is fenced/reset, then native proof and eviction complete',async()=>{const x=await fixture();try{const old=await expire(x),reset=await call(x);assert.equal(reset.status,'PENDING');assert.equal(reset.reason,'lineage_progress_restarted');assert.equal(reset.progress.phase,'LINEAGE');assert.equal((await saved(x)).lineage,null);assert(reset.continuation.generation>old.continuation.generation);assert.equal(count(x),1);const r=await ready(x,reset);assert.equal((await evict(x,r)).status,'EVICTED');assert.equal(count(x),0);}finally{x.close();}});
test('PA-EVICT-REVIEW-01 B: shared verifier beyond replay window cannot strand reconciliation',async()=>{const x=await fixture();try{const initial=await call(x);let token=(await saved(x)).lineage;for(let n=0;n<2;n++){const other=await verifyArchiveLineage(x.archive,x.root,{continuation:token});token=other.continuation;}
 const reset=await call(x,initial.continuation);assert.equal(reset.reason,'lineage_progress_restarted');assert.equal((await saved(x)).lineage,null);const r=await ready(x,reset);assert.equal((await evict(x,r)).status,'EVICTED');assert.equal(count(x),0);}finally{x.close();}});
test('PA-EVICT-REVIEW-01 C: simultaneous reset has one CAS winner; loser rereads without authority',async()=>{const x=await fixture();try{await expire(x);const g=gate(),release=gate();const slow=call(x,null,{hook:async phase=>{if(phase==='LINEAGE_RESET_PREPARED'){g.resolve();await release.promise;}}});await g.promise;
 const row=x.raw.prepare('SELECT eviction_generation,eviction_nonce FROM pa_publications WHERE id=?').get(x.a.id),token={publicationId:x.a.id,generation:row.eviction_generation,nonce:row.eviction_nonce};
 const winner=await call(x,token);release.resolve();const loser=await slow;assert.equal(winner.reason,'lineage_progress_restarted');assert.equal(loser.reason,'eviction_reconciliation_raced');assert.equal(loser.continuation,undefined);assert.equal((await saved(x)).lineage,null);assert.equal((await evict(x,await ready(x,winner))).status,'EVICTED');}finally{x.close();}});

for(const [label,change] of [
 ['new root',async x=>x.add('successor')],
 ['catalogue boundary',async x=>x.raw.exec('UPDATE pa_meta SET catalogue_generation=catalogue_generation+1')],
 ['reference boundary',async x=>x.raw.exec('UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1')],
 ['source revision',async x=>x.raw.prepare('UPDATE pa_scopes SET revision=revision+1 WHERE id=?').run(x.a.scope_id)],
 ['archive revision',async x=>x.raw.prepare('UPDATE pa_scopes SET archive_revision=archive_revision+1 WHERE id=?').run(x.a.scope_id)],
 ['pending ingress',async x=>x.raw.prepare('UPDATE pa_scopes SET pending=1 WHERE id=?').run(x.a.scope_id)],
 ['correction',async x=>admitCorrection(x.db,{id:'racing-correction',scopeId:x.a.scope_id,predecessor:'older',reason:'review'},{now:x.now})],
 ['recovery dependency',async x=>x.raw.prepare('UPDATE pa_scopes SET recovery_required=1 WHERE id=?').run(x.a.scope_id)],
 ['epoch replacement',async x=>x.raw.exec("UPDATE pa_meta SET epoch='replacement'")],
 ['epoch ABA',async x=>{const {epoch}=x.raw.prepare('SELECT epoch FROM pa_meta').get();x.raw.exec("UPDATE pa_meta SET epoch='replacement'");x.raw.prepare('UPDATE pa_meta SET epoch=?').run(epoch);}],
 ['restore authority',async x=>{const r=await reserve(x.db,{id:'restore-reservation',workClass:'restore',bytes:1000,owner:'restorer',now:x.now});x.raw.prepare("INSERT INTO pa_restores(id,root_key,state,owner,generation,lease_nonce,lease_until,restored_at,source_epoch,reservation) VALUES('restore',?,'RUNNING','restorer',1,'nonce',?,?,?,?)").run(x.root,x.now+60000,x.now,x.a.epoch,r.id);}],
 ['publication state',async x=>x.raw.prepare("UPDATE pa_publications SET state='STALE' WHERE id=?").run(x.a.id)],
 ['publication identity',async x=>x.raw.prepare("UPDATE pa_publications SET owner='changed' WHERE id=?").run(x.a.id)],
 ['lease replacement',async x=>x.raw.prepare("UPDATE pa_publications SET eviction_owner='other',eviction_nonce='replacement' WHERE id=?").run(x.a.id)],
 ['lease expiry',async x=>x.advance(60001)]
])test('PA-EVICT-REVIEW-01 D: fresh reset rejects '+label+' during recovery with zero deletion',async()=>{const x=await fixture();try{await expire(x);const before=(await saved(x)).lineage;await assert.rejects(call(x,null,{hook:async phase=>{if(phase==='LINEAGE_RESET_PREPARED')await change(x);}}),/pa_fence_failed/);assert.deepEqual((await saved(x)).lineage,before);assert.equal(count(x),1);assert.notEqual((await saved(x)).phase,'READY');}finally{x.close();}});

test('PA-EVICT-REVIEW-01 crash before reset commit retains old proof and retry safely resets',async()=>{const x=await fixture();try{await expire(x);const before=(await saved(x)).lineage;await assert.rejects(call(x,null,{hook:phase=>{if(phase==='LINEAGE_RESET_PREPARED')throw Error('CRASH_BEFORE_RESET');}}),/CRASH_BEFORE_RESET/);assert.deepEqual((await saved(x)).lineage,before);x.advance(60001);const reset=await call(x);assert.equal(reset.reason,'lineage_progress_restarted');assert.equal((await evict(x,await ready(x,reset))).status,'EVICTED');}finally{x.close();}});
test('PA-EVICT-REVIEW-01 lost reset response and adapter restart resume durable null frontier',async()=>{const x=await fixture();try{await expire(x);await assert.rejects(call(x,null,{hook:phase=>{if(phase==='LINEAGE_RESET_COMMITTED')throw Error('LOST_RESET_RESPONSE');}}),/LOST_RESET_RESPONSE/);assert.equal((await saved(x)).lineage,null);assert.equal((await call(x)).reason,'eviction_reconciliation_owned');x.advance(60001);x.db=createMeasurementD1Adapter(x.binding);x.archive=createPrivateArchiveAdapter(x.bucket,{namespace});const restarted=await call(x);assert.equal(restarted.status,'PENDING');assert((await saved(x)).lineage);assert.equal((await evict(x,await ready(x,restarted))).status,'EVICTED');}finally{x.close();}});
test('PA-EVICT-REVIEW-01 crash after fresh archive proof starts safely repeats uncredited work',async()=>{const x=await fixture();try{await expire(x);const reset=await call(x);await assert.rejects(call(x,reset.continuation,{hook:phase=>{if(phase==='VERIFIED_SEGMENT')throw Error('CRASH_AFTER_ARCHIVE_PROGRESS');}}),/CRASH_AFTER_ARCHIVE_PROGRESS/);assert.equal((await saved(x)).lineage,null);const retry=await call(x,reset.continuation);assert.notEqual(retry.status,'READY');assert.equal((await evict(x,await ready(x,retry))).status,'EVICTED');}finally{x.close();}});
test('PA-EVICT-REVIEW-01 lost fresh-progress response resumes committed proof after lease expiry',async()=>{const x=await fixture();try{await expire(x);const reset=await call(x),batch=x.binding.batch;let drop=true;x.binding.batch=async statements=>{const r=await batch(statements);if(drop&&statements.some(s=>s.sql.startsWith('UPDATE pa_publications SET eviction_progress='))){drop=false;throw Error('LOST_FRESH_PROGRESS_RESPONSE');}return r;};await assert.rejects(call(x,reset.continuation),/LOST_FRESH_PROGRESS_RESPONSE/);assert((await saved(x)).lineage);assert.equal((await saved(x)).phase,'LINEAGE');x.advance(60001);x.db=createMeasurementD1Adapter(x.binding);const resumed=await call(x);assert.equal((await evict(x,await ready(x,resumed))).status,'EVICTED');}finally{x.close();}});
test('PA-EVICT-REVIEW-01 stale owner after recovery cannot resume or consume new proof',async()=>{const x=await fixture();try{const old=await expire(x),reset=await call(x);await assert.rejects(call(x,old.continuation),/pa_eviction_reconciliation_token/);await assert.rejects(evictPublication(x.db,x.archive,x.a.id,{now:x.now,reconciliation:old.continuation}),/pa_eviction_reconciliation_token/);assert.equal(count(x),1);assert.equal((await evict(x,await ready(x,reset))).status,'EVICTED');}finally{x.close();}});
test('PA-EVICT-REVIEW-01 archive progress advances again after reset without granting partial authority',async()=>{const x=await fixture();try{const initial=await call(x);let token=(await saved(x)).lineage;for(let i=0;i<2;i++){const r=await verifyArchiveLineage(x.archive,x.root,{continuation:token});token=r.continuation;}const reset=await call(x,initial.continuation);const r=await ready(x,reset);assert.equal((await evict(x,r)).status,'EVICTED');assert.equal((await evict(x,r)).replayed,true);}finally{x.close();}});
for(const [label,damage] of [
 ['progress digest corruption',async(x,token)=>{const key=namespace+'progress/'+String(token.slot).padStart(2,'0'),entry=JSON.parse(x.objects.get(key));entry.digest='corrupt';x.objects.set(key,JSON.stringify(entry));}],
 ['missing archive root',async x=>x.objects.delete(x.root)],
 ['corrupt archive root',async x=>x.objects.set(x.root,x.objects.get(x.root)+'corrupt')]
])test('PA-EVICT-REVIEW-01 never converts '+label+' into restart success',async()=>{const x=await fixture();try{const initial=await call(x),state=await saved(x);await damage(x,state.lineage);let reset=false;await assert.rejects(call(x,initial.continuation,{hook:phase=>{if(phase==='LINEAGE_RESET_PREPARED')reset=true;}}),/pa_archive_(progress_integrity|object_missing|digest)/);assert.equal(reset,false);assert.deepEqual((await saved(x)).lineage,state.lineage);assert.equal(count(x),1);}finally{x.close();}});
test('PA-EVICT-REVIEW-01 corruption discovered after expiry reset still fails closed on fresh verification',async()=>{const x=await fixture();try{await expire(x);x.objects.delete(x.root);const reset=await call(x);assert.equal(reset.status,'PENDING');await assert.rejects(call(x,reset.continuation),/pa_archive_object_missing/);assert.equal(count(x),1);}finally{x.close();}});
test('PA-EVICT-REVIEW-01 recovery retains byte bound, fixed accounting and zero physical credit',async()=>{const x=await fixture();try{await expire(x);const before=x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged,reset=await call(x),r=await ready(x,reset);assert(Buffer.byteLength(x.raw.prepare('SELECT eviction_progress FROM pa_publications WHERE id=?').get(x.a.id).eviction_progress)<=8192);assert.equal(x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged,before);assert.equal((await evict(x,r)).physicalReclamationCredit,0);assert.equal((await evict(x,r)).physicalReclamationCredit,0);}finally{x.close();}});
