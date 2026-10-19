import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {gunzipSync} from 'node:zlib';
import {createHash} from 'node:crypto';
import {seedOfficial,packageDatabase,fakeBucket} from './test-fixtures/b1-package-a.mjs';
import {marketPackets,tick} from './test-fixtures/b1-processing-ordering.mjs';
import {createPrivateArchiveAdapter,archiveReader,publishArchive,verifyArchivePackage} from './measurement-archive.js';
import {ingestMarketEvidence} from './measurement-market-store.js';
import {processCapturedOutcome,runRecovery} from './measurement-recovery.js';
import {createMeasurementD1Adapter} from './measurement-d1-adapter.js';
import {hydrateArchive} from './measurement-restore.js';
import {processingBudget,validateDecodedMarket} from './measurement-processing-budget.js';
import {hash,serialize} from './measurement-independent-contract.js';
import {confirmation} from './test-fixtures/b1-m2.mjs';
import {deliver} from './test-fixtures/b1-package-a.mjs';
import measurement from './measurement-worker.js';
import {verifyArchiveParity,evictPublication} from './measurement-independent-retention.js';

function bounded(t){assert(t.marketDecodes<=8);assert(t.recordDecodes<=128);assert(t.archiveOperations<=1024);assert(t.archiveBytes<=32000000);assert(t.catalogueReads<=64);assert(t.sqlStatements<=900);}
async function fixture(){
 let clock=0;const x=await seedOfficial({clock:()=>clock}),b=fakeBucket(),raw=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-processing-archive/'});clock=x.at+20000;
 // Preserve a supported price-basis annotation in one captured tick. The
 // regenerated control exercises the reported 59/12/81 and 2,225,978-byte
 // geometry; it is not asserted byte-identical to the lost in-memory bucket.
 for(let i=0;i<12;i++){const observed=tick(i,4100+i,{at:x.at+i,providerAt:x.at+i,session:'archive-bound',sequence:i});if(i===10)observed.measurement.priceBasis='MT5 observed quote value for bounded archive verification; occurrence and receipt kept separate';const p=await marketPackets([observed],{cohort:x.snapshot.scope.cohort,observedAt:clock,from:x.at,to:x.at+10000});for(const packet of p)await ingestMarketEvidence(x.db,packet,{now:clock});}
 for(let i=0;i<12;i++){clock+=2000;await runRecovery(createMeasurementD1Adapter(x.binding),{now:clock});if(x.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId)?.state==='DONE')break;}
 // An expired synthetic operational hold is preserved in the portable scope
 // snapshot. This is not an availability clock or a processing-completion edit.
 x.raw.prepare('UPDATE pa_scopes SET hold_until=1000 WHERE id=?').run(x.scopeId);
 const p=await publishArchive(x.db,raw,x.scopeId,{now:clock}),verified=await verifyArchivePackage(raw,p.manifest_key);
 return {...x,b,raw,p,verified,clock:()=>clock,advance:()=>clock+=40000};
}
// Freeze the reviewed parent's exact geometry independently of future RESULT
// checkpoint metadata. All other fixtures below still use current production code.
async function parentControl(){
 const wire=await readFile(new URL('./test-fixtures/b1-processing-archive-parent-control.json.gz',import.meta.url));
 assert.equal(createHash('sha256').update(wire).digest('hex'),'f0629e027831226191fa34809aeeae88d65df363aaab448745433e6a15dd17c7');
 const f=JSON.parse(gunzipSync(wire));assert.equal(f.sourceCandidate,'8e898c8cdabb500c19560b0c44a483bcfaeda0de');
 const b=fakeBucket();for(const [key,value] of f.objects)b.objects.set(key,value);
 const raw=createPrivateArchiveAdapter(b.bucket,{namespace:f.namespace}),verified=await verifyArchivePackage(raw,f.manifestKey);let clock=f.clock;
 assert.equal(verified.snapshot.bindings.length,1);const binding=verified.snapshot.bindings[0];
 return {b,raw,verified,evaluationId:binding.evaluation_id,scopeId:binding.signal_id,p:{root_key:f.rootKey},clock:()=>clock,advance:()=>clock+=40000,close(){}};
}
test('Bounded archive: genuine package and restore-backed outer opportunities share all counters',async()=>{
 const x=await parentControl(),target=await packageDatabase({clock:x.clock});
 try{
  assert.equal(x.verified.records.length,59);assert.equal(x.verified.records.filter(r=>r.kind==='MARKET_EXPORT').length,12);assert.equal(x.verified.objects,81);assert.equal(x.verified.bytes,2225978);
  console.log(JSON.stringify({archiveControl:{records:x.verified.records.length,market:x.verified.records.filter(r=>r.kind==='MARKET_EXPORT').length,objects:x.verified.objects,bytes:x.verified.bytes}}));
  await hydrateArchive(target.db,x.raw,x.p.root_key,{now:x.clock(),environment:'synthetic-b1-package-a',authorization:{operation:'restore',environment:'synthetic-b1-package-a',expiresAt:x.clock()+100000}});
  const traces=[];
  for(let i=0;i<100;i++){
   x.advance();const db=createMeasurementD1Adapter(target.binding,{statementBudget:900}),r=await processCapturedOutcome(db,x.evaluationId,{now:x.clock(),archive:archiveReader(x.raw,{rootKey:x.p.root_key}),rebuild:true});
   traces.push({status:r.status,reason:r.reason,...r.budget});bounded(r.budget);
   if(r.status==='COMPLETED_AT_REVISION'){console.log(JSON.stringify({archiveOuterTrace:traces}));assert(traces.length>1);return;}
  }
  assert.fail(JSON.stringify(traces));
 }finally{x.close();target.close();}
});

test('Bounded archive: exact-context reacquisition preserves accepted one-generation replay',async()=>{
 const b=fakeBucket(),raw=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-progress-reacquire/'}),c={operation:'test-verify-v1',package:'exact'};
 const a=await raw.progress.save(c,{complete:false,generation:1}),loaded=await raw.progress.acquire(c);assert.equal(loaded.token.nonce,a.nonce);
 const second=await raw.progress.save(c,{complete:false,generation:2},loaded);assert.equal((await raw.progress.load(c,a)).state.generation,2);
 const l2=await raw.progress.acquire(c);await raw.progress.save(c,{complete:true,generation:3},l2);
 await assert.rejects(raw.progress.load(c,a),/pa_archive_progress_token/);assert.equal((await raw.progress.load(c,second)).state.generation,3);assert.equal((await raw.progress.acquire(c)).state.generation,3);
});

test('Bounded archive: meter charges repeated actual decodes and reserves commit capacity',async()=>{
 const x=await packageDatabase();try{const m=processingBudget(x.db);for(let i=0;i<120;i++)m.record({kind:'OFFICIAL_CREATION'});for(let i=0;i<8;i++)m.record({kind:'MARKET_EXPORT',id:'same'});assert.equal(m.trace().marketDecodes,8);assert.throws(()=>m.record({kind:'MARKET_EXPORT'}),/pa_processing_budget_pending/);assert.equal(m.trace().recordDecodes,128);assert.throws(()=>m.record({kind:'OFFICIAL_CREATION'}),/pa_processing_budget_pending/);assert.equal(m.can({objects:1017}),false);}finally{x.close();}
});

test('Bounded archive: two CAS writers have one winner and loser reacquires current context',async()=>{
 const b=fakeBucket(),a=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-progress-race/'}),c={operation:'verification-only',package:'same'};
 const [x,y]=await Promise.all([a.progress.acquire(c,{claim:true}),a.progress.acquire(c,{claim:true})]);
 const out=await Promise.allSettled([a.progress.save(c,{complete:false,generation:1},x),a.progress.save(c,{complete:false,generation:1},y)]);
 assert.equal(out.filter(x=>x.status==='fulfilled').length,1);assert.equal((await a.progress.acquire(c,{claim:true})).state.generation,1);
});
test('Bounded archive: full active slot population gives backpressure without overwrite',async()=>{
 const b=fakeBucket(),a=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-progress-capacity/'}),contexts=new Map();
 for(let i=0;contexts.size<64;i++){const c={operation:'busy',id:i},slot=parseInt((await hash(c)).slice(0,8),16)%64;if(!contexts.has(slot))contexts.set(slot,c);}
 for(const c of contexts.values())await a.progress.save(c,{complete:false,generation:1});
 const before=new Map(b.objects);await assert.rejects(a.progress.acquire({operation:'other'},{claim:true}),/pa_archive_progress_capacity/);assert.deepEqual(b.objects,before);
 const c=contexts.values().next().value,loaded=await a.progress.acquire(c,{claim:true});await a.progress.save(c,{complete:true,generation:2},loaded);
 const free=await a.progress.acquire({operation:'other'},{claim:true});assert(free.vacant);await a.progress.save({operation:'other'},{complete:false,generation:1},free);
});
test('Bounded archive: expiry safely restarts continuation without accepting expired token',async()=>{
 const b=fakeBucket(),a=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-progress-expiry/'}),c={operation:'verify',package:'one'};
 const token=await a.progress.save(c,{complete:false,generation:1}),key=[...b.objects.keys()][0],entry=JSON.parse(b.objects.get(key));entry.expiresAt=0;b.objects.set(key,serialize(entry));
 await assert.rejects(a.progress.load(c,token),/pa_archive_progress_expired/);const empty=await a.progress.acquire(c,{claim:true});assert(empty.vacant);
 await a.progress.save(c,{complete:false,generation:1},empty);assert.equal((await a.progress.acquire(c,{claim:true})).state.complete,false);
});
for(const field of ['payload','namespace','format'])test('Bounded archive: corrupt progress '+field+' fails closed',async()=>{
 const b=fakeBucket(),a=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-progress-integrity/'}),c={operation:'verify'};await a.progress.save(c,{complete:false,generation:1});
 const key=[...b.objects.keys()][0],entry=JSON.parse(b.objects.get(key));if(field==='payload')entry.payload.generation=999;else entry[field]='wrong';b.objects.set(key,JSON.stringify(entry));
 await assert.rejects(a.progress.acquire(c,{claim:true}),/pa_archive_progress_integrity/);
});
test('Bounded archive: incompatible operation/context cannot reuse another active continuation',async()=>{
 const b=fakeBucket(),a=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-progress-context/'}),c={operation:'verify-v1',package:'one'},t=await a.progress.save(c,{complete:false,generation:1});
 for(const wrong of [{...c,operation:'verify-v2'},{...c,package:'two'},{...c,root:'wrong'},{...c,receipt:'wrong'}])await assert.rejects(a.progress.load(wrong,t),/pa_archive_progress_token/);
 await assert.rejects(a.progress.load(c,{...t,nonce:'stale'}),/pa_archive_progress_token/);
});
test('Bounded archive: response loss after slot commit is reacquired without a local token',async()=>{
 const b=fakeBucket(),base=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-progress-response/'}),c={operation:'verify'},fault={...b.bucket,async put(...args){await b.bucket.put(...args);throw Error('response_lost');}},a=createPrivateArchiveAdapter(fault,{namespace:base.namespace});
 await assert.rejects(a.progress.save(c,{complete:false,generation:1}),/response_lost/);assert.equal((await base.progress.acquire(c,{claim:true})).state.generation,1);
});
for(const kind of ['archive operations','archive bytes','catalogue','SQL'])test('Bounded archive: '+kind+' reserve refuses optional work before checkpoint capacity is spent',async()=>{
 const x=await packageDatabase();try{const db=createMeasurementD1Adapter(x.binding,{statementBudget:900}),m=processingBudget(db);
 if(kind==='archive operations')for(let i=0;i<1016;i++)m.operation();
 if(kind==='archive bytes')m.readBytes(31820000);
 if(kind==='catalogue')for(let i=0;i<64;i++)m.operation('synthetic-b1/catalogue/one');
 if(kind==='SQL')for(let i=0;i<773;i++)await db.first('SELECT 1');
 assert.equal(m.can({objects:1,bytes:1,catalogue:1}),false);if(kind!=='SQL')m.beforeCheckpoint();bounded(m.trace());
 }finally{x.close();}
});
for(const count of [130,180])test('Bounded archive: non-market lifecycle/creation lookup '+count+' resumes beyond 128 decodes',async()=>{
 let now=0;const x=await seedOfficial({clock:()=>now}),b=fakeBucket(),raw=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-nonmarket-lookup/'});now=x.at+1000;
 try{
  for(let i=0;i<count;i++)await deliver(x.db,await confirmation(x.scopeId,'nonmarket-child-'+i),now);
  const traces=[];let completed=false;
  for(let i=0;i<20;i++){now+=40000;const r=await processCapturedOutcome(createMeasurementD1Adapter(x.binding,{statementBudget:900}),x.evaluationId,{now,archive:archiveReader(raw,{getRoot:async()=>null}),rebuild:true});bounded(r.budget);traces.push(r.budget);if(r.status==='COMPLETED_AT_REVISION'){completed=true;break;}}
  assert(completed);assert(traces.some(t=>t.recordDecodes===128));assert(traces.length>1);assert.equal(x.raw.prepare('SELECT fact_count FROM pa_projections WHERE scope_id=?').get(x.scopeId).fact_count,count+1);console.log(JSON.stringify({nonmarketTrace:traces}));
 }finally{x.close();}
});

async function restored(x){const target=await packageDatabase({clock:x.clock});await hydrateArchive(target.db,x.raw,x.p.root_key,{now:x.clock(),environment:'synthetic-b1-package-a',authorization:{operation:'restore',environment:'synthetic-b1-package-a',expiresAt:x.clock()+100000}});target.op=async hook=>{x.advance();const db=createMeasurementD1Adapter(target.binding,{statementBudget:900}),meter=processingBudget(db);meter.notify=hook;let result,error;
 try{result=await processCapturedOutcome(db,x.evaluationId,{now:x.clock(),archive:archiveReader(x.raw,{rootKey:x.p.root_key}),rebuild:true,hook,budget:meter});return result;}catch(e){error=e.message;throw e;}finally{bounded(meter.trace());console.log(JSON.stringify({outerBudgetTrace:{status:result?.status??'REJECTED',error:error??null,...meter.trace()}}));}};return target;}
for(const phase of ['ARCHIVE_PROGRESS_PREPARED','ARCHIVE_PROGRESS_COMMITTED'])test('Bounded archive: crash at '+phase+' restarts without verification becoming a protected effect',async()=>{
 const x=await fixture(),t=await restored(x);try{let crashed=false;await assert.rejects(t.op(p=>{if(p===phase&&!crashed){crashed=true;throw Error('archive_crash');}}),/archive_crash/);assert(crashed);assert.equal(t.raw.prepare('SELECT COUNT(*) n FROM pa_processing').get().n,0);
  const trace=[];for(let i=0;i<30;i++){const r=await t.op();bounded(r.budget);const p=t.raw.prepare('SELECT payload FROM pa_processing WHERE scope_id=?').get(x.scopeId);trace.push([r.status,r.phase,r.budget.marketDecodes,r.budget.catalogueReads,p?JSON.parse(p.payload).scanProgress.state.phase:null,p?JSON.parse(p.payload).scanProgress.state.cursor:null]);if(r.status==='COMPLETED_AT_REVISION')return;}assert.fail('restart did not resume '+JSON.stringify(trace));
 }finally{x.close();t.close();}
});
for(const change of ['binding','root','catalogue','reference','epoch','correction'])test('Bounded archive: '+change+' change after archive verification rejects stale subject lookup',async()=>{
 const x=await fixture(),t=await restored(x);try{let changed=false,rejected=false;
  for(let i=0;i<10&&!rejected;i++)try{await t.op(p=>{if(p!=='PROCESSING_LOOKUP_VERIFIED'||changed)return;changed=true;
   if(change==='binding')t.raw.prepare('DELETE FROM pa_bindings WHERE evaluation_id=?').run(x.evaluationId);
   if(change==='root')t.raw.exec("UPDATE pa_meta SET root_key='changed-root'");
   if(change==='catalogue')t.raw.exec('UPDATE pa_meta SET catalogue_generation=catalogue_generation+1');
   if(change==='reference')t.raw.exec('UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1');
   if(change==='epoch')t.raw.exec("UPDATE pa_meta SET epoch='new-target-epoch'");
   if(change==='correction')t.raw.prepare('UPDATE pa_scopes SET corrections=corrections+1 WHERE id=?').run(x.evaluationId);
  });}catch(e){assert.match(e.message,/pa_fence_failed/);rejected=true;}
  assert(changed&&rejected);assert.equal(t.raw.prepare('SELECT COUNT(*) n FROM pa_processing').get().n,0);
 }finally{x.close();t.close();}
});
for(const missing of [true,false])test('Bounded archive: '+(missing?'missing':'corrupt')+' required object is an integrity failure, never obsolete continuation',async()=>{
 const x=await fixture(),t=await restored(x);try{const object=x.verified.records[0].objects[0].key;if(missing)x.b.objects.delete(object);else x.b.objects.set(object,x.b.objects.get(object)+'corrupt');
  await assert.rejects(t.op(),/pa_archive_object_missing|pa_archive_digest/);assert.equal(t.raw.prepare('SELECT COUNT(*) n FROM pa_processing').get().n,0);
 }finally{x.close();t.close();}
});
test('Bounded archive: two outer processors share package facts but keep independent job leases',async()=>{
 const x=await fixture(),a=await restored(x),b=await restored(x);try{
  const ra=await a.op();bounded(ra.budget);assert.equal(ra.budget.marketDecodes,8);assert.equal(a.raw.prepare('SELECT COUNT(*) n FROM pa_processing').get().n,0);
  const rb=await b.op();bounded(rb.budget);assert.notEqual(a.raw.prepare('SELECT epoch FROM pa_meta').get().epoch,b.raw.prepare('SELECT epoch FROM pa_meta').get().epoch);
  assert(rb.budget.marketDecodes<=8);assert.notEqual(b.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'DONE');
  for(const t of [a,b]){let done=false;const trace=[];for(let i=0;i<30;i++){const r=await t.op();bounded(r.budget);const p=t.raw.prepare('SELECT payload FROM pa_processing WHERE scope_id=?').get(x.scopeId);trace.push([r.status,r.phase,r.budget.marketDecodes,r.budget.catalogueReads,p?JSON.parse(p.payload).scanProgress.state.phase:null]);if(r.status==='COMPLETED_AT_REVISION'){done=true;break;}}assert(done,JSON.stringify(trace));}
 }finally{x.close();a.close();b.close();}
});

for(const lost of ['expired','removed'])test('Bounded archive: '+lost+' package progress safely restarts and reaches canonical completion',async()=>{
 const x=await fixture(),t=await restored(x);try{const first=await t.op();assert.equal(first.budget.marketDecodes,8);assert.equal(t.raw.prepare('SELECT COUNT(*) n FROM pa_processing').get().n,0);
  for(const [key,wire] of [...x.b.objects])if(key.includes('/progress/')){const v=JSON.parse(wire);if(v.payload.version!=='pa-package-verification-v1')continue;if(lost==='removed')x.b.objects.delete(key);else{v.expiresAt=0;x.b.objects.set(key,serialize(v));}}
  let done=false;for(let i=0;i<30;i++){const r=await t.op();bounded(r.budget);if(r.status==='COMPLETED_AT_REVISION'){done=true;break;}}assert(done);
 }finally{x.close();t.close();}
});
for(const field of ['generation','identity','version'])test('Bounded archive: authenticated slot with invalid package progress '+field+' is rejected',async()=>{
 const x=await fixture(),t=await restored(x);try{await t.op();let changed=false;
  for(const [key,wire] of [...x.b.objects])if(key.includes('/progress/')){const v=JSON.parse(wire);if(v.payload.version!=='pa-package-verification-v1')continue;v.payload[field]=field==='generation'?-1:'wrong';v.digest=await hash(serialize(v.payload));x.b.objects.set(key,serialize(v));changed=true;}
  assert(changed);await assert.rejects(t.op(),/pa_package_progress_integrity/);assert.equal(t.raw.prepare('SELECT COUNT(*) n FROM pa_processing').get().n,0);
 }finally{x.close();t.close();}
});
test('Bounded archive: scanner and delegated signal consume only the remaining outer allowance',async()=>{
 const x=await fixture(),t=await restored(x);try{const first=await t.op();bounded(first.budget);assert.equal(first.budget.marketDecodes,8);assert.equal(t.raw.prepare('SELECT COUNT(*) n FROM pa_processing').get().n,0);
  const second=await t.op();bounded(second.budget);assert.equal(second.budget.marketDecodes,8);assert(second.budget.checkpoints.some(c=>c.phase==='ARCHIVE_VERIFY'));assert(second.budget.checkpoints.some(c=>c.phase.startsWith('SAMPLE_')));const p=t.raw.prepare('SELECT payload FROM pa_processing WHERE scope_id=?').get(x.scopeId);assert(p);assert.equal(JSON.parse(p.payload).completeForCapturedSnapshot,false);assert.equal(t.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'PENDING');
 }finally{x.close();t.close();}
});
test('Bounded archive: checkpoint reservation includes the parent archive adapter remaining budget',async()=>{const x=await packageDatabase();try{const b=processingBudget(x.db);b.archiveRemaining=()=>8;assert.equal(b.can({objects:1}),false);assert.equal(b.can({objects:0}),true);}finally{x.close();}});
test('Bounded archive: record decode admission never consumes another unit on rejection',async()=>{const x=await packageDatabase();try{const b=processingBudget(x.db);for(let i=0;i<8;i++)b.record({kind:'MARKET_EXPORT',bytes:60000});const before=b.trace();assert.throws(()=>b.record({kind:'MARKET_EXPORT',bytes:60000}),/pa_processing_budget_pending/);assert.equal(b.trace().decodedBytes,before.decodedBytes);assert.equal(b.trace().recordDecodes,before.recordDecodes);}finally{x.close();}});

test('Bounded archive: actual scheduled Worker restores, verifies and catches continuing Queue arrivals',async()=>{
 const x=await fixture(),t=await restored(x),oldNow=Date.now;Date.now=x.clock;
 try{
  const env={MEASUREMENT_ENVIRONMENT:'synthetic-b1-package-a',MEASUREMENT_EXPECTED_D1_ID:'LOCAL_ONLY_NO_CLOUD_ID',MEASUREMENT_DB:t.binding,MEASUREMENT_ARCHIVE:x.b.bucket,MEASUREMENT_ARCHIVE_NAMESPACE:x.raw.namespace,MEASUREMENT_QUEUE_NAME:'synthetic-b1-queue',MEASUREMENT_DLQ_NAME:'synthetic-b1-dlq',B1_MEASUREMENT_CONSUMER_ENABLED:'1',B1_ARCHIVE_WRITE_ENABLED:'0',B1_ARCHIVE_EVICTION_ENABLED:'0'},fronts=[];let done=false,total=12;
  for(let pass=0;pass<120;pass++){
   x.advance();const tasks=[];await measurement.scheduled({},env,{waitUntil:p=>tasks.push(p)});await Promise.all(tasks);
   const row=t.raw.prepare('SELECT payload FROM pa_processing WHERE scope_id=?').get(x.scopeId),p=row?JSON.parse(row.payload):null;fronts.push(p?.scanProgress.records??0);
   if(p?.completeForCapturedSnapshot&&t.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId)?.state==='DONE'){assert.equal(p.scanProgress.records,total);done=true;break;}
   const packets=await marketPackets([tick(total,4100+total,{at:x.at+total,providerAt:x.at+total,session:'archive-bound',sequence:total})],{cohort:x.snapshot.scope.cohort,observedAt:x.clock(),from:x.at,to:x.at+10000});let ack=0;
   await measurement.queue({queue:env.MEASUREMENT_QUEUE_NAME,messages:packets.map(p=>({body:JSON.stringify({version:'measurement-market-queue-v1',...p}),ack(){ack++;},retry(){assert.fail('valid continuing packet retried');}}))},env);assert.equal(ack,packets.length);total++;
  }
  assert(done,JSON.stringify(fronts));assert(fronts.length>1);assert(total>12);console.log(JSON.stringify({archiveScheduledContinuing:{fronts,initial:12,arrivals:total-12,total}}));
 }finally{Date.now=oldNow;x.close();t.close();}
});

test('Bounded archive: genuine pending processing prevents a concurrent destructive hot-to-archive transition',async()=>{
 const x=await fixture();try{await verifyArchiveParity(x.db,x.raw,x.p.id);await deliver(x.db,await confirmation(x.scopeId,'movement-arrival'),x.clock());
  const before=(await x.db.first('SELECT COUNT(*) n FROM pa_chunks')).n;let attempted=false;
  for(let i=0;i<5&&!attempted;i++){x.advance();await processCapturedOutcome(createMeasurementD1Adapter(x.binding,{statementBudget:900}),x.evaluationId,{now:x.clock(),archive:archiveReader(x.raw,{rootKey:x.p.root_key}),rebuild:true,hook:async phase=>{if(phase==='PROCESSING_BINDING_RESOLVED'&&!attempted){attempted=true;await assert.rejects(evictPublication(x.db,x.raw,x.p.id,{now:x.clock()}),/pa_fence_failed/);}}});}
  assert(attempted);assert.equal((await x.db.first('SELECT COUNT(*) n FROM pa_chunks')).n>=before,true);
 }finally{x.close();}
});

test('Bounded archive: a legacy completed checkpoint cannot acknowledge a new-format evaluation obligation',async()=>{
 const x=await seedOfficial();try{
  await processCapturedOutcome(x.db,x.scopeId,{now:x.at,rebuild:true});
  const row=x.raw.prepare('SELECT payload FROM pa_processing WHERE scope_id=?').get(x.scopeId),legacy=JSON.parse(row.payload);legacy.version='measurement-result-v1';delete legacy.scanProgress;
  // Old-format checkpoint fixture only. No test SQL completes any job/result.
  x.raw.prepare('UPDATE pa_processing SET payload=? WHERE scope_id=?').run(serialize(legacy),x.scopeId);
  const before=x.raw.prepare("SELECT COUNT(*) n FROM pa_records WHERE kind='RESULT'").get().n;
  const r=await processCapturedOutcome(createMeasurementD1Adapter(x.binding),x.evaluationId,{now:x.at+40000,rebuild:true});
  assert.equal(r.status,'COMPLETED_AT_REVISION');assert.equal(x.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'DONE');
  const next=JSON.parse(x.raw.prepare('SELECT payload FROM pa_processing WHERE scope_id=?').get(x.scopeId).payload);assert.equal(next.version,'measurement-result-samples-v2');assert.equal(next.scanProgress.version,'pa-sample-selection-v2');assert(x.raw.prepare("SELECT COUNT(*) n FROM pa_records WHERE kind='RESULT'").get().n>before);
 }finally{x.close();}
});

test('Bounded archive: a non-market metadata label cannot bypass the market allowance',async()=>{const x=await packageDatabase();try{const b=processingBudget(x.db),[packet]=await marketPackets([tick(1)]);b.record({kind:'RESULT',bytes:packet.wire.length});await assert.rejects(validateDecodedMarket({kind:'RESULT',wire:packet.wire},b),/pa_processing_market_identity/);assert.equal(b.trace().marketDecodes,1);for(let i=0;i<7;i++)b.record({kind:'MARKET_EXPORT'});assert.throws(()=>b.record({kind:'RESULT'}),/pa_processing_budget_pending/);}finally{x.close();}});

test('Bounded archive: completed lookup proof cannot replace a missing creation witness',async()=>{
 const x=await fixture(),t=await restored(x);try{
  const creation=x.verified.records.find(r=>r.kind==='OFFICIAL_CREATION');assert(creation);let removed=false,rejected=false;
  // Storage-location fixture: force the restored row to use its exact archive
  // container. No processing state or obligation is marked complete.
  t.raw.prepare("UPDATE pa_records SET location='ARCHIVED',archive_key=? WHERE id=?").run(x.p.manifest_key,creation.id);
  // Reach a durably completed creation enumeration, then interrupt the later
  // lifecycle lookup. Fixed invocation counts do not identify that frontier.
  for(let i=0;i<30&&!removed;i++)try{await t.op((phase,detail)=>{if(phase==='PROCESSING_LOOKUP_VERIFIED'&&detail.context.purpose==='LIFECYCLE'){removed=true;x.b.objects.delete(creation.objects[0].key);throw Error('creation_response_lost');}});}catch(e){assert.match(e.message,/creation_response_lost/);}
  assert(removed);
  for(let i=0;i<30&&!rejected;i++)try{const r=await t.op();assert.notEqual(r.status,'COMPLETED_AT_REVISION');}catch(e){assert.match(e.message,/pa_archive_object_missing/);rejected=true;}
  assert(rejected);assert.notEqual(t.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'DONE');
 }finally{x.close();t.close();}
});
