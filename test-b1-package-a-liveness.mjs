import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import {packageDatabase,gate} from './test-fixtures/b1-package-a.mjs';
import {history,finish,restore,seed,obsolete,maintain,scheduled,register,successor,namespace,checkpoint} from './test-fixtures/b1-package-a-liveness.mjs';
import {createPrivateArchiveAdapter,discoverArchiveRoot,verifyArchiveLineage,verifyRoot,publishArchive,planPublication} from './measurement-archive.js';
import {retireStalePublications} from './measurement-state-compaction.js';
import {createMeasurementD1Adapter,schemaReady} from './measurement-d1-adapter.js';
import {rollbackPlan} from './measurement-qualification.js';

const progress=(a,token)=>JSON.parse(a.objects.get(namespace+'progress/'+String(token.slot).padStart(2,'0'))).payload;
async function discovery(a,{continuation=null}={}){
 let partialPackages=0;
 const done=await finish(async c=>{const before=a.archive.metrics().reads,r=await discoverArchiveRoot(a.archive,{continuation:c});const reads=a.archive.metrics().reads-before;assert(reads<=516);if(r.status==='PENDING'&&reads>=512)partialPackages++;return r;},{continuation});
 assert.equal(done.result.key,a.root);return {...done,partialPackages};
}
for(const [roots,records,payloadBytes] of [[8,1,0],[8,90,0],[8,90,43000],[2,90,43000]])test(`PA-LH-REVIEW-01 discovery ${roots} roots x ${records} records x ${payloadBytes} extra bytes completes`,async()=>{
 const a=await history(roots,{records,payloadBytes}),r=await discovery(a);assert.equal(r.result.discovery.lineage.roots,roots);if(records>1){assert(r.steps.length>1);assert(r.partialPackages>0);}console.log('LIVENESS_DISCOVERY',JSON.stringify({roots,records,payloadBytes,invocations:r.steps.length,partialPackages:r.partialPackages}));
});
test('PA-LH-REVIEW-01 mid-package exhaustion saves last complete confirmation unit, not a skipped record',async()=>{
 const a=await history(8,{records:90});let c=null,seen=false,last=0;
 await finish(async token=>{const r=await discoverArchiveRoot(a.archive,{continuation:token});if(r.status==='PENDING'){const s=progress(a,r.continuation);if(s.phase==='CONFIRM'){seen=true;assert(s.fingerprint.count>=last);last=s.fingerprint.count;assert(s.queue.length>0||s.truncated);assert(s.best.key===a.root);}}c=r.continuation;return r;});assert(seen);assert(c===undefined);
});
test('PA-LH-REVIEW-01 confirmation replay and adapter restart preserve exact frontier',async()=>{
 const a=await history(8,{records:90});let first;await finish(async c=>{const r=await discoverArchiveRoot(a.archive,{continuation:c});if(r.status==='PENDING'&&r.progress.phase==='CONFIRM'){first=r;return {status:'STOP'};}return r;});assert(first);
 const next=await discoverArchiveRoot(a.archive,{continuation:first.continuation}),replayed=await discoverArchiveRoot(a.archive,{continuation:first.continuation});assert.deepEqual(replayed.continuation,next.continuation);
 const restarted={...a,archive:createPrivateArchiveAdapter(a.bucket,{namespace})};assert.equal((await discovery(restarted,{continuation:next.continuation})).result.key,a.root);
});
for(const tamper of ['token','persisted'])test('PA-LH-REVIEW-01 confirmation '+tamper+' tampering rejects',async()=>{
 const a=await history(8,{records:90}),r=await discoverArchiveRoot(a.archive);let token=r.continuation;
 if(tamper==='token')token={...token,digest:'0'.repeat(64)};else{const key=namespace+'progress/'+String(token.slot).padStart(2,'0'),e=JSON.parse(a.objects.get(key));e.payload.phase='CONFIRM';e.payload.queue=[];a.objects.set(key,JSON.stringify(e));}
 await assert.rejects(discoverArchiveRoot(a.archive,{continuation:token}),/pa_archive_progress_token|pa_archive_progress_integrity/);
});
for(const damage of ['package','earlier-record'])test('PA-LH-REVIEW-01 partial confirmation detects changed '+damage,async()=>{
 const a=await history(8,{records:90});let token=null,r;do{r=await discoverArchiveRoot(a.archive,{continuation:token});token=r.continuation;}while(r.progress?.phase!=='CONFIRM');
 const root=await verifyRoot(a.archive,a.root),key=damage==='package'?root.manifestKey:root.verified.records[0].objects[0].key;a.objects.set(key,'corrupt');await assert.rejects(discovery(a,{continuation:token}),/pa_corrupt_root_requires_policy|pa_archive_digest/);
});
test('PA-LH-REVIEW-01 two discovery Workers cannot skip large confirmation work',async()=>{
 const a=await history(8,{records:90}),r=await discoverArchiveRoot(a.archive),other=createPrivateArchiveAdapter(a.bucket,{namespace});
 const outcomes=await Promise.allSettled([a.archive,other].map(archive=>discoverArchiveRoot(archive,{continuation:r.continuation})));assert(outcomes.some(x=>x.status==='fulfilled'));
 for(const o of outcomes)if(o.status==='rejected')assert.match(o.reason.message,/pa_archive_progress_raced|pa_archive_progress_token/);
 const next=outcomes.find(x=>x.status==='fulfilled').value;assert.equal((await discovery(a,{continuation:next.continuation})).result.key,a.root);
});
test('PA-LH-REVIEW-01 actual large restore, fresh epoch, target publish and archive-only discovery',async()=>{
 const a=await history(8,{records:90}),x=await packageDatabase();let closed=false;try{
  const sourceEpoch=(await verifyRoot(a.archive,a.root)).epoch,targetEpoch=x.raw.prepare('SELECT epoch FROM pa_meta').get().epoch;assert.notEqual(sourceEpoch,targetEpoch);
  await restore(x,a);const p=await publishArchive(x.db,a.archive,await seed(x,'target'),{now:2000});assert.equal(p.state,'EVICTION_ELIGIBLE');a.root=p.root_key;a.roots.push(p.root_key);assert.equal(x.raw.prepare('SELECT epoch FROM pa_meta').get().epoch,targetEpoch);x.close();closed=true;
  const r=await discovery(a);assert.equal(r.result.discovery.lineage.transitions,1);assert.equal(r.result.discovery.lineage.roots,9);
 }finally{if(!closed)x.close();}
});

const row=(x,p)=>x.raw.prepare('SELECT * FROM pa_publications WHERE id=?').get(p.id);
const proof=(x,p)=>{const r=row(x,p);return r?.retirement_proof?JSON.parse(r.retirement_proof):null;};
for(const length of [129,640])test('PA-LH-REVIEW-02 actual scheduled continuing publication preserves '+length+'-root progress',async()=>{
 const a=await history(length),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);let last=0,retiredAt=null;const observed=[];
  for(let pass=1;pass<=24;pass++){await scheduled(x,a,v.now);const s=proof(x,v.p);if(s){assert(s.roots>=last);last=s.roots;observed.push(s.roots);assert(s.context.format==='measurement-retirement-proof-v2');}else retiredAt??=pass;
   await register(x,a);if(pass>=12&&retiredAt)break;
  }
  assert(retiredAt);if(length===640)assert(retiredAt>10);assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation)?.state??'SETTLED','SETTLED');console.log('LIVENESS_RETIREMENT',JSON.stringify({length,retiredAt,observed}));
 }finally{x.close();}
});
test('PA-LH-REVIEW-02 real publishArchive registration adds only the new authenticated suffix',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);let before=0;
  for(let turn=0;turn<8&&row(x,v.p);turn++){await scheduled(x,a,v.now);const s=proof(x,v.p);if(s){assert(s.roots>=before);before=s.roots;}const p=await publishArchive(x.db,a.archive,await seed(x,'arrival:'+turn),{now:v.now});assert.equal(p.state,'EVICTION_ELIGIBLE');a.root=p.root_key;a.roots.push(p.root_key);}
  assert(!row(x,v.p));
 }finally{x.close();}
});
for(const reference of ['direct','superseded-package'])test('PA-LH-REVIEW-02 new '+reference+' dependency blocks retirement',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);await maintain(x,a,v.now);
  if(reference==='direct')await register(x,a,{publication:v.p.id,manifestKey:v.p.manifest_key,receiptKey:v.p.receipt_key});else{const root=await verifyRoot(a.archive,a.root),manifest=await a.archive.write('packages',{...root.verified.manifest,supersedes:v.p.manifest_key});await register(x,a,{manifestKey:manifest.key});}
  for(let i=0;i<5;i++)await maintain(x,a,v.now);assert(row(x,v.p));assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation).state,'COMMITTED');
 }finally{x.close();}
});
for(const change of ['state','revision','owner','reservation','epoch'])test('PA-LH-REVIEW-02 candidate '+change+' change invalidates old authority',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);await maintain(x,a,v.now);const old=proof(x,v.p);
  if(change==='state')x.raw.prepare("UPDATE pa_publications SET state='PLANNED' WHERE id=?").run(v.p.id);
  if(change==='revision')x.raw.prepare('UPDATE pa_publications SET revision=revision+1 WHERE id=?').run(v.p.id);
  if(change==='owner')x.raw.prepare('UPDATE pa_publications SET owner=?,generation=generation+1 WHERE id=?').run('new-owner',v.p.id);
  if(change==='reservation')x.raw.prepare('UPDATE pa_reservations SET token=? WHERE id=?').run('new-token',v.p.reservation);
  if(change==='epoch')x.raw.prepare("UPDATE pa_meta SET epoch='new-epoch'").run();
  await maintain(x,a,v.now);assert(row(x,v.p));assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation).state,'COMMITTED');const next=proof(x,v.p);if(next)assert.notEqual(next.contextDigest,old.contextDigest);
 }finally{x.close();}
});
test('PA-LH-REVIEW-02 crash after incremental progress resumes with new references',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);await assert.rejects(maintain(x,a,v.now,phase=>{if(phase==='PUBLICATION_PROOF_SAVED')throw Error('CRASH');}),/CRASH/);const n=proof(x,v.p).roots;await register(x,a);await maintain(x,a,v.now);assert(proof(x,v.p).roots>n);for(let i=0;i<5&&row(x,v.p);i++)await maintain(x,a,v.now);assert(!row(x,v.p));}finally{x.close();}
});
test('PA-LH-REVIEW-02 new publication racing final fence preserves proof but cannot delete',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);let raced=false;
  for(let i=0;i<8&&!raced;i++)await maintain(x,a,v.now,async phase=>{if(phase==='PUBLICATION_RETIREMENT_PREPARED'){raced=true;await register(x,a);}});
  assert(raced);assert(row(x,v.p));const n=proof(x,v.p).roots;assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation).state,'COMMITTED');await maintain(x,a,v.now);assert(!row(x,v.p));assert(n>=129);
 }finally{x.close();}
});
test('PA-LH-REVIEW-02 two maintenance Workers settle once after incremental new publication',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);await maintain(x,a,v.now);await register(x,a);await maintain(x,a,v.now);const results=await Promise.all([x.db,createMeasurementD1Adapter(x.binding)].map(db=>retireStalePublications(db,{now:v.now,archive:a.archive})));assert.equal(results.reduce((n,r)=>n+r.retired,0),1);assert(!row(x,v.p));assert.equal((await maintain(x,a,v.now)).operational.publications.retired,0);}finally{x.close();}
});
test('PA-LH-REVIEW-02 overload preserves a growing frontier and exposes bounded PENDING backlog',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);let n=0;
  for(let pass=0;pass<5;pass++){for(let i=0;i<70;i++){await register(x,a);x.raw.prepare('INSERT INTO pa_scopes(id,cohort,original_at,archive_root) VALUES(?,?,1,?)').run('hold:'+pass+':'+i,'review',a.root);}const r=(await maintain(x,a,v.now)).operational.publications;assert.equal(r.status,'PENDING');assert(r.referenceReads<=512);assert(r.referenceBacklog>0);assert(proof(x,v.p).roots>n);n=proof(x,v.p).roots;}
  assert(row(x,v.p));assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation).state,'COMMITTED');
 }finally{x.close();}
});
test('PA-LH-REVIEW-02 unauthenticated new epoch edge blocks incremental retirement',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);await maintain(x,a,v.now);await register(x,a,{epoch:'unauthorized'});for(let i=0;i<5;i++)await maintain(x,a,v.now);assert(row(x,v.p));assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation).state,'COMMITTED');}finally{x.close();}
});
test('PA-LH-REVIEW-02 long history and 72 invalidations recover capacity during continuing publication',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const scope=await seed(x,'repeat'),arrivalScope=await seed(x,'arrivals');let now=2000;
  for(let i=0;i<72;i++){const p=await publishArchive(x.db,a.archive,scope,{now,maxSteps:4,owner:'worker'});assert.equal(p.state,'CATALOGUED');x.raw.prepare('UPDATE pa_scopes SET revision=revision+1 WHERE id=?').run(scope);now+=60001;
   if(i<63)await planPublication(x.db,scope,{now,owner:'worker'});else{let rejected=false;try{await planPublication(x.db,scope,{now,owner:'worker'});}catch(error){assert.match(error.message,/pa_fence_failed/);rejected=true;}if(i===63)assert(rejected);assert.equal(x.raw.prepare('SELECT state FROM pa_publications WHERE id=?').get(p.id).state,'STALE');let freed=0;for(let turn=0;turn<10&&!freed;turn++){freed+=(await maintain(x,a,now)).operational.publications.retired;await register(x,a);}assert(freed>0);await planPublication(x.db,scope,{now,owner:'worker'});}
  }
  assert(x.raw.prepare('SELECT COUNT(*) n FROM pa_publications').get().n<=64);assert.equal((await publishArchive(x.db,a.archive,scope,{now,owner:'worker'})).state,'EVICTION_ELIGIBLE');assert(arrivalScope);
 }finally{x.close();}
});

test('PA-LH schema 0010 -> 0011 preserves populated state, bounds registry and rejects ABA reuse',async()=>{
 const x=await packageDatabase({migrations:false});try{
  for(const name of (await fs.readdir('migrations')).filter(n=>/^00(0[1-9]|10)_/.test(n)).sort()){assert.deepEqual(await fs.readFile('migrations/'+name),execFileSync('git',['show','b57ceb5980bcd01afd68a3ba23344237c8009494:migrations/'+name]));x.raw.exec(await fs.readFile('migrations/'+name,'utf8'));}
  x.raw.exec("INSERT INTO pa_scopes(id,cohort,original_at,archive_root) VALUES('old','review',1,'old-root');UPDATE pa_meta SET root_key='old-root'");await assert.rejects(schemaReady(x.db),/pa_schema_not_ready/);
  x.raw.exec(await fs.readFile('migrations/0011_package_a_incremental_references.sql','utf8'));assert.equal((await schemaReady(x.db)).schema_version,11);assert.equal(x.raw.prepare('SELECT archive_root FROM pa_scopes').get().archive_root,'old-root');assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_archive_reference_work').get().n,1);
  const first=x.raw.prepare('SELECT sequence FROM pa_archive_reference_work').get().sequence,charged=x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged;
  x.raw.exec("UPDATE pa_meta SET root_key=NULL;UPDATE pa_scopes SET archive_root=NULL");assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_archive_reference_work').get().n,0);assert.equal(x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged,charged);
  x.raw.exec("UPDATE pa_meta SET root_key='old-root'");assert(x.raw.prepare('SELECT sequence FROM pa_archive_reference_work').get().sequence>first);assert.throws(()=>x.raw.exec("UPDATE pa_archive_reference_work SET root_key='wrong'"),/pa_reference_immutable/);assert.throws(()=>x.raw.prepare('INSERT INTO pa_archive_reference_work(root_key) VALUES(?)').run('x'.repeat(1025)),/CHECK/);
  assert.deepEqual(x.raw.prepare('PRAGMA foreign_key_check').all(),[]);assert.throws(()=>rollbackPlan('VERSION',{artifact:'old',compatibleSchema:10,compatibleArchive:'measurement-archive-v1'}),/pa_rollback_incompatible/);assert(rollbackPlan('VERSION',{artifact:'new',compatibleSchema:11,compatibleArchive:'measurement-archive-v1'}));
 }finally{x.close();}
});
test('PA-LH reference registry admission respects archive reserve/hard ceiling and cleanup gives no credit',async()=>{
 const x=await packageDatabase();try{const before=x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged;x.raw.exec("UPDATE pa_meta SET root_key='root';UPDATE pa_meta SET root_key=NULL");assert.equal(x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged,before+81920);x.raw.exec("UPDATE pa_pool_use SET charged=400000000 WHERE pool='archive'");assert.throws(()=>x.raw.exec("UPDATE pa_meta SET root_key='blocked'"),/pa_reference_resource_capacity/);assert.equal(x.raw.prepare('SELECT root_key FROM pa_meta').get().root_key,null);x.raw.exec("UPDATE pa_pool_use SET charged=0 WHERE pool='archive';UPDATE pa_meta SET physical_charged=5500000000");assert.throws(()=>x.raw.exec("UPDATE pa_meta SET root_key='blocked'"),/pa_reference_resource_capacity/);}finally{x.close();}
});
test('PA-LH registry hard capacity, uniqueness, immutable identity and monotonic sequence survive cleanup',async()=>{
 const x=await packageDatabase();try{
  x.raw.exec("UPDATE pa_meta SET root_key='registered'");assert.throws(()=>x.raw.exec("DELETE FROM pa_archive_reference_work WHERE root_key='registered'"),/pa_reference_still_registered/);assert.throws(()=>x.raw.exec('UPDATE pa_meta SET reference_sequence=0'),/pa_reference_sequence_aba/);assert.throws(()=>x.raw.exec("INSERT INTO pa_archive_reference_work(root_key) VALUES('registered')"),/UNIQUE/);
  x.raw.exec("UPDATE pa_meta SET root_key=NULL;WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4353) INSERT INTO pa_archive_reference_work(root_key) SELECT 'capacity-'||x FROM n");assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_archive_reference_work').get().n,4353);assert.throws(()=>x.raw.exec("INSERT INTO pa_archive_reference_work(root_key) VALUES('over-capacity')"),/pa_reference_capacity/);
  const charged=x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged,sequence=x.raw.prepare('SELECT reference_sequence FROM pa_meta').get().reference_sequence;x.raw.exec('DELETE FROM pa_archive_reference_work');assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_archive_reference_work').get().n,0);assert.equal(x.raw.prepare('SELECT physical_charged FROM pa_meta').get().physical_charged,charged);assert.equal(x.raw.prepare('SELECT reference_sequence FROM pa_meta').get().reference_sequence,sequence);
 }finally{x.close();}
});
test('PA-LH-REVIEW-02 modified durable proof cannot skip history or settle liability',async()=>{
 const a=await history(),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);await maintain(x,a,v.now);const s=proof(x,v.p);s.current=null;s.afterSequence=9000;s.complete=true;x.raw.prepare('UPDATE pa_publications SET retirement_proof=? WHERE id=?').run(JSON.stringify(s),v.p.id);await assert.rejects(maintain(x,a,v.now),/pa_retirement_progress_integrity/);assert(row(x,v.p));assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation).state,'COMMITTED');}finally{x.close();}
});
test('PA-LH-REVIEW-02 exhausted SQL turn reports unprocessed candidates as PENDING',async()=>{
 const a=await history(1),x=await packageDatabase();try{await restore(x,a);const v=await obsolete(x,a);const db=createMeasurementD1Adapter(x.binding,{statementBudget:16}),r=await retireStalePublications(db,{now:v.now,archive:a.archive});assert.equal(r.status,'PENDING');assert.equal(r.scanned,0);assert.equal(r.pending,1);assert(row(x,v.p));assert.equal(x.raw.prepare('SELECT state FROM pa_reservations WHERE id=?').get(v.p.reservation).state,'COMMITTED');}finally{x.close();}
});
