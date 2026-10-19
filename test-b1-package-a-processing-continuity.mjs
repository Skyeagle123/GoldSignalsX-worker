import {test} from 'node:test';
import assert from 'node:assert/strict';
import {AT,subject,tick,marketPackets,orderedModel} from './test-fixtures/b1-processing-ordering.mjs';
import {packageDatabase,seedOfficial,fakeBucket} from './test-fixtures/b1-package-a.mjs';
import {foldPostEntry} from './post-entry-evidence.js';
import {sampleDigest,ingestMarketEvidence} from './measurement-market-store.js';
import {CONTINUITY_VERSION,emptySampleFold,foldSelectedSample,sampleObservations} from './measurement-processing-fold.js';
import {validateMarketEvidence} from './measurement-market-envelope.js';
import {sampleSelectionKey} from './measurement-processing-samples.js';
import {processCapturedOutcome,runRecovery} from './measurement-recovery.js';
import {ProcessingYield} from './measurement-processing-budget.js';
import {createMeasurementD1Adapter,leaseJob} from './measurement-d1-adapter.js';
import {resolveProcessingSubject,completeEvaluationObligation} from './measurement-processing-binding.js';
import {createPrivateArchiveAdapter,archiveReader,publishArchive,recoverRootCheckpoint} from './measurement-archive.js';
import {verifyArchiveParity,evictPublication} from './measurement-independent-retention.js';
import {reconcilePublicationEvictionAuthority} from './measurement-eviction-reconciliation.js';
import {advanceSampleScan} from './measurement-processing-scan.js';
import {hash,serialize,bytes} from './measurement-independent-contract.js';

const end=AT+1000;
const pair=(price=100)=>[tick(1,price,{at:AT-1000}),tick(2,111,{at:end})];
function expected(ticks,bars=[]){return foldPostEntry(subject,null,{ticks,bars,asOf:end,availableAt:end}).state;}
function check(r,old){assert.deepEqual(r.fold.covered,old.covered);assert.equal(r.fold.outcome.directionalOutcome,old.outcome.directionalOutcome);assert.equal(r.fold.outcome.coverage,old.outcome.coverage);assert.equal(r.fold.global.mfe,old.global.mfe);assert.equal(r.fold.global.mae,old.global.mae);}

test('Continuity: exact Astra entry-straddling case preserves COMPLETE coverage and TP1 SUCCESS',async()=>{
 const ticks=pair(),old=expected(ticks),r=await orderedModel(await marketPackets(ticks),{now:end});check(r,old);assert.equal(r.fold.outcome.coverage,'COMPLETE');assert.equal(r.fold.outcome.directionalOutcome,'SUCCESS');assert.equal(r.fold.global.mfe,11);assert.equal(r.fold.global.mae,0);console.log(JSON.stringify({continuityControl:{accepted:old.outcome,candidate:r.fold.outcome,covered:r.fold.covered}}));
});
for(const price of [10000,1])test('Continuity: extreme pre-entry price '+price+' contributes no analytical price or barrier',async()=>{
 const ticks=pair(price),r=await orderedModel(await marketPackets(ticks),{now:end,page:1});check(r,expected(ticks));assert.deepEqual(r.fold.counts,{TP1:1,TP2:0,SL:0});for(const w of Object.values(r.fold.quality.windows)){assert.equal(w.observedCount,1);assert.equal(w.mfe,11);assert.equal(w.mae,0);}
});
for(const [group,page,decode,reverse] of [[2,64,8,false],[1,64,8,true],[1,1,1,false],[2,1,1,true]])test(`Continuity: partition invariance group=${group} page=${page} decode=${decode} reverse=${reverse}`,async()=>{
 const ticks=pair(),p=await marketPackets(ticks,{group}),r=await orderedModel(reverse?p.reverse():p,{now:end,page,decode});check(r,expected(ticks));assert.equal(r.count,2);assert(r.stats.maxCheckpointBytes<60000);
});
test('Continuity: predecessor identity survives serialized selection/invocation boundary without its price',async()=>{
 const ticks=pair(10000),p=await marketPackets(ticks,{group:1}),original=(await validateMarketEvidence(p[0].wire)).samples[0];let observed=false;
 const r=await orderedModel(p,{now:end,page:1,decode:1,async onPass(_x,state){if(state.count===1){observed=true;const f=state.fold;assert.equal(f.continuityVersion,CONTINUITY_VERSION);assert.equal(f.lastTick.id,original.id);assert.equal(f.lastTick.digest,await sampleDigest(original));}}});
 assert(observed);check(r,expected(ticks));
});

test('Continuity: only one compact authenticated predecessor retained for a long pre-entry prefix',async()=>{
 const ticks=Array.from({length:130},(_,i)=>tick(i,9999,{at:AT-130+i})),p=await marketPackets(ticks),f=emptySampleFold(subject);let final;
 for(const packet of p){const data=await validateMarketEvidence(packet.wire);for(const s of data.samples){await foldSelectedSample(subject,f,s,await sampleSelectionKey(data,s),{asOf:end,availableAt:end});final=s;}}
 assert.equal(f.lastTick.id,final.id);assert.equal(f.lastTick.digest,await sampleDigest(final));assert.equal(f.lastTick.source,'mt5');assert.equal(f.lastTick.price,undefined);assert(bytes(serialize(f.lastTick))<1024);assert.equal(f.global.mfe,null);assert.equal(f.global.mae,null);assert.equal(f.processedThrough,AT);assert.deepEqual(f.counts,{TP1:0,TP2:0,SL:0});for(const w of Object.values(f.quality.windows))assert.equal(w.observedCount,0);
});
for(const kind of ['session','source'])test('Continuity: unrelated '+kind+' cannot bridge entry coverage',async()=>{
 const ticks=pair(),p=await marketPackets(ticks.slice(0,1));
 if(kind==='session')p.push(...await marketPackets([tick(2,111,{at:end,session:'unrelated'})]));
 else{const {prepareMarketEvidence}=await import('./measurement-market-envelope.js');const t=structuredClone(ticks[1]);t.measurement.provider='other';p.push(...await prepareMarketEvidence({cohort:'synthetic-b1-ordering',source:'other',session:'ordering',from:AT-1000,to:end,observedAt:end,ticks:[t]}));}
 const r=await orderedModel(p,{now:end});assert.equal(r.fold.outcome.coverage,'INSUFFICIENT');assert.notEqual(r.fold.outcome.directionalOutcome,'SUCCESS');
});
for(const kind of ['cadence','sequence','provider'])test('Continuity: invalid '+kind+' relationship remains insufficient',async()=>{
 const ticks=pair();if(kind==='cadence')ticks[0]=tick(1,100,{at:AT-21000});if(kind==='sequence')ticks[1]=tick(4,111,{at:end});if(kind==='provider')ticks[1].measurement.providerTimestamp=AT-2000;
 const r=await orderedModel(await marketPackets(ticks),{now:end});check(r,expected(ticks));assert.equal(r.fold.outcome.coverage,'INSUFFICIENT');assert.notEqual(r.fold.outcome.directionalOutcome,'SUCCESS');
});
test('Continuity: exact-entry equal-time sequence follows accepted ordering without pre-entry price',async()=>{
 const ticks=[tick(1,10000,{at:AT-1000}),tick(2,111,{at:AT}),tick(3,89,{at:AT})],old=foldPostEntry(subject,null,{ticks,asOf:AT,availableAt:AT}).state,r=await orderedModel(await marketPackets(ticks),{now:AT,page:1,decode:1});check(r,old);assert.equal(r.fold.outcome.directionalOutcome,'SUCCESS');assert.deepEqual(r.fold.counts,{TP1:1,TP2:0,SL:1});
});
for(const start of [AT-30000,AT])test('Continuity: bar/tick interval at '+(start-AT)+' preserves accepted bar eligibility and ambiguity',async()=>{
 const ticks=pair(),bars=[{t:start,o:100,h:130,l:80,c:100,provider:'mt5',sessionId:'ordering'}],now=AT+60000,old=foldPostEntry(subject,null,{ticks,bars,asOf:now,availableAt:now}).state,r=await orderedModel(await marketPackets(ticks,{bars}),{now,page:1});check(r,old);assert.equal(r.fold.global.mfe,start<AT?11:30);assert.equal(r.fold.global.mae,start<AT?0:20);
});

async function actual(){let clock=0;const x=await seedOfficial({clock:()=>clock});clock=x.at+10000;const resolved=await resolveProcessingSubject(x.db,x.scopeId,{now:clock}),d=resolved.decision,entry=d.createdAt??d.evaluatedAt,levels=d.engine.levels,hit=d.direction==='buy'?levels.tp1+1:levels.tp1-1;
 const ticks=[tick(1,levels.entry,{at:entry-1000}),tick(2,hit,{at:entry+1000})],packets=await marketPackets(ticks,{cohort:x.snapshot.scope.cohort,observedAt:clock,group:1});for(const p of packets)await ingestMarketEvidence(x.db,p,{now:clock});
 const post=(await validateMarketEvidence(packets[1].wire)).samples[0].id;
 return {...x,ticks,entry,packets,post,now:()=>clock,advance:()=>clock+=60001,checkpoint:()=>{const r=x.raw.prepare('SELECT payload FROM pa_processing WHERE scope_id=?').get(x.scopeId);return r?JSON.parse(r.payload):null;},scan:(options={})=>processCapturedOutcome(createMeasurementD1Adapter(x.binding),x.scopeId,{now:clock,rebuild:true,...options}),drain:async()=>{for(let n=0;n<30;n++){clock+=60001;await runRecovery(createMeasurementD1Adapter(x.binding),{now:clock});if(x.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId)?.state==='DONE')return;}assert.fail('continuity recovery stayed pending');}};
}
function predecessorYield(x){return (phase,detail)=>{if(phase==='SAMPLE_BEFORE_FOLD'&&detail.id===x.post)throw new ProcessingYield('TEST_PREDECESSOR_CHECKPOINT');};}
function assertPrefix(x){const p=x.checkpoint();assert(p);assert.equal(p.completeForCapturedSnapshot,false);assert.equal(p.scanProgress.records,1);assert(p.folded.state.lastTick.ts<x.entry);assert.equal(p.folded.state.global.mfe,null);assert.equal(p.folded.state.lastTick.digest.length,64);assert.notEqual(x.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'DONE');return p;}
function assertComplete(x){const p=x.checkpoint();assert(p.completeForCapturedSnapshot);assert.equal(p.scanProgress.records,2);assert.equal(p.folded.state.outcome.directionalOutcome,'SUCCESS');assert.equal(p.folded.state.lastTick.id,x.post);assert.equal(x.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'DONE');}
test('Continuity: actual scheduled recovery resumes durable predecessor with fresh adapter and separate evaluation completion',async()=>{const x=await actual();try{await x.scan({hook:predecessorYield(x)});assertPrefix(x);x.advance();await x.drain();assertComplete(x);}finally{x.close();}});
for(const loss of ['before commit','after commit'])test('Continuity: crash/response loss '+loss+' preserves exactly-once predecessor recovery',async()=>{const x=await actual();try{
 if(loss==='before commit')await assert.rejects(x.scan({hook:(phase,detail)=>{predecessorYield(x)(phase,detail);if(phase==='OUTCOME_PREPARED')throw Error('continuity_crash');}}),/continuity_crash/);
 else{let lost=false;const db=createMeasurementD1Adapter({...x.binding,async batch(s){const r=await x.binding.batch(s);if(!lost&&s.some(q=>q.sql.includes('INSERT INTO pa_processing'))){lost=true;throw Error('continuity_response_lost');}return r;}});await assert.rejects(processCapturedOutcome(db,x.scopeId,{now:x.now(),rebuild:true,hook:predecessorYield(x)}),/continuity_response_lost/);assertPrefix(x);}
 x.advance();await x.drain();assertComplete(x);
 }finally{x.close();}});

async function coldMarket(x,archive,id,now){const scope=x.raw.prepare('SELECT scope_id FROM pa_records WHERE id=?').get(id).scope_id,p=await publishArchive(x.db,archive,scope,{now});await verifyArchiveParity(x.db,archive,p.id);for(let i=0;i<4;i++)await runRecovery(createMeasurementD1Adapter(x.binding),{now:now+i*2000,handlers:{archive:({row,lease,now})=>row.id.startsWith('root-checkpoint:')?recoverRootCheckpoint(x.db,archive,row.event_id,{now,lease}):publishArchive(x.db,archive,row.scope_id,{now})}});let r;for(let n=0;n<40;n++){r=await reconcilePublicationEvictionAuthority(x.db,archive,p.id,{now:now+10000,owner:'continuity-cold',continuation:r?.continuation??null});if(r.status==='READY')break;}assert.equal(r.status,'READY');assert.equal((await evictPublication(x.db,archive,p.id,{now:now+10000,reconciliation:r.continuation})).status,'EVICTED');}
for(const cold of [0,1])test('Continuity: '+(cold===0?'predecessor':'successor')+' archived across hot/cold boundary preserves exact sample identity',async()=>{const x=await packageDatabase(),b=fakeBucket(),archive=createPrivateArchiveAdapter(b.bucket,{namespace:'synthetic-b1-continuity-'+cold+'/'});try{
 const ticks=pair(),packets=await marketPackets(ticks,{group:1}),now=AT+7200000;for(const p of packets)await ingestMarketEvidence(x.db,p,{now});await coldMarket(x,archive,packets[cold].id,now);const root=x.raw.prepare('SELECT root_key FROM pa_meta').get().root_key;
 const r=await orderedModel(packets,{database:x,ingest:false,archive:archiveReader(archive,{rootKey:root}),now,subject:{...subject,closedAt:end},page:1});check(r,expected(ticks));assert.equal(r.count,2);assert(r.stats.maxDecoded<=8);
 }finally{x.close();}});

test('Continuity: old sample checkpoint restarts instead of reinterpreting its missing witness',async()=>{
 const identity={authority:{context:{cohort:'synthetic-b1-continuity-old'}}},boundary={boundary:serialize(['epoch',null,0,0,0]),revision:0},old={version:'pa-sample-selection-v2',identity,boundary:boundary.boundary,pin:{},fold:{global:{mfe:9999}},generation:7},previous={subjectId:subject.id,folded:{state:old.fold},scanProgress:{version:'pa-sample-selection-v2',state:old,stateDigest:await hash(old),foldDigest:await hash(old.fold)}},row={revision:1,payload:serialize(previous)},db={async first(){return {digest:await hash(row.payload)};}};
 const r=await advanceSampleScan(db,null,{previous,previousRow:row,identity,boundary,marketRevision:0,now:end,subject,budget:{remaining:0}});assert.equal(r.state.fold.continuityVersion,CONTINUITY_VERSION);assert.equal(r.state.fold.global.mfe,null);assert.equal(r.state.count,0);assert.equal(r.complete,false);
});
test('Continuity: pre-fix canonical result cannot acknowledge evaluation under the current witness contract',async()=>{const x=await actual();try{
 for(let n=0;n<20&&!x.checkpoint()?.completeForCapturedSnapshot;n++){x.advance();await x.scan();}const old=x.checkpoint();assert(old.completeForCapturedSnapshot);delete old.folded.state.continuityVersion;delete old.scanProgress.state.fold.continuityVersion;x.raw.prepare('UPDATE pa_processing SET payload=? WHERE scope_id=?').run(serialize(old),x.scopeId);
 const resolution=await resolveProcessingSubject(createMeasurementD1Adapter(x.binding),x.evaluationId,{now:x.now()}),lease=await leaseJob(createMeasurementD1Adapter(x.binding),'projection:'+x.evaluationId,'old-fold-review',{now:x.now()});assert(lease);const r=await completeEvaluationObligation(createMeasurementD1Adapter(x.binding),resolution,lease,{now:x.now()});assert.equal(r.status,'PENDING');assert.notEqual(x.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'DONE');
 // Historical-format fixture only: no test SQL completes a job or fabricates a new result.
 }finally{x.close();}});
test('Continuity: tampered persisted predecessor cannot become coverage authority',async()=>{const x=await actual();try{
 await x.scan({hook:predecessorYield(x)});const p=assertPrefix(x);p.scanProgress.state.fold.lastTick.digest='0'.repeat(64);x.raw.prepare('UPDATE pa_processing SET payload=? WHERE scope_id=?').run(serialize(p),x.scopeId);x.advance();await assert.rejects(x.scan(),/pa_processing_scan_integrity/);assert.notEqual(x.raw.prepare('SELECT state FROM pa_jobs WHERE id=?').get('projection:'+x.evaluationId).state,'DONE');
 }finally{x.close();}});
