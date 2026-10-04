import assert from 'node:assert/strict';
import {test} from 'node:test';
import {localMeasurementDatabase,outcomeProfile} from './test-fixtures/b1-outcome-profiles.mjs';
import {transactionalBinding,sqliteBinding} from './test-fixtures/b1-sqlite.mjs';
import {beginDecisionCycle,observeAttempt,persistDecisionCycle} from './signal-measurement.js';
import {computeServerSignal,SIGNAL_TF_MS,HIGHER_SIGNAL_TIMEFRAMES,evaluateCandleQuality} from './signal-engine.js';
import {decodeStoredEvidence,decodeStoredOutcome,restoreCensus} from './evidence-codec.js';
import {replayMarketManifest} from './market-evidence.js';
import {foldPostEntry} from './post-entry-evidence.js';
import {reduceOutcome} from './forward-validation.js';
import {measurementWriter} from './signal-evidence-store.js';
import {maintainMeasurementRetention} from './measurement-retention.js';
import {createForwardReadCapability,readForwardValidation,parseForwardQuery} from './forward-validation-reader.js';
const now=Date.UTC(2026,8,28,15,10),DAY=86400000;
function fullCycle(){
 const frames={};for(const [tf,n]of Object.entries({'1m':2000,'5m':600,'15m':300,'30m':200,'60m':120,'240m':80,'1d':60})){
  const step=SIGNAL_TF_MS[tf],end=Math.floor(now/step)*step;
  frames[tf]={tf,bars:Array.from({length:n},(_,i)=>{const c=4100+(i-n+1)*.35;return{t:end-(n-i)*step,o:c-.3,h:c+.1,l:c-.4,c,v:1,provider:'mt5'};})};frames[tf].quality=evaluateCandleQuality(frames[tf].bars,tf);
 }
 const live={price:4100,ts:now,receivedAt:now,source:'mt5'},filters={nyFilterOn:false,pivotFilterOn:false},j=beginDecisionCycle(now,frames,live,null,filters);
 j.capturedAt=now;j.candidates=[];j.decisions=new Map();j.officialIds=new Set();j.confirmationIds=new Set();j.evaluations=new Map();
 for(const [tf,f]of Object.entries(frames)){
  const trace={},includedMtf=HIGHER_SIGNAL_TIMEFRAMES[tf]||[],result=computeServerSignal(f.bars,{tf,mtf:includedMtf.map(tf=>frames[tf]),live,barsSource:'d1',evaluationAt:now,filters},trace);
  observeAttempt(j,{tf,trace,result,requestedMtf:includedMtf,includedMtf});j.evaluations.set(tf,result);
  if(['buy','sell'].includes(result.side)){const id=`${tf}:${f.bars.at(-1).t}:${result.side}`;j.candidates.push({id,tf,createdAt:now,conf:result.conf});j.decisions.set(id,{decision:j.officialIds.size?'blocked_opposite':'accepted'});if(!j.officialIds.size)j.officialIds.add(id);}
 }
 return j;
}
const provenance={codeCommit:'a'.repeat(40),measurementEffectiveAt:now};
test('F1 full-sized seven-attempt cycle persists exact replayable raw and immutable retry; low budget preserves census',async()=>{
 for(const maxWrites of [160,20]){
  const db=localMeasurementDatabase(),j=fullCycle(),binding=transactionalBinding(db);
  const result=await persistDecisionCycle(binding,j,provenance,{maxWrites});assert(result.ok,result.error);
  const rows=db.prepare('SELECT * FROM signal_decision_evidence').all();assert.equal(rows.length,7);assert.equal(db.prepare("SELECT COUNT(*) n FROM signal_decision_evidence WHERE kind='OFFICIAL'").get().n,1);
  const counts=db.prepare('SELECT COUNT(*) n FROM measurement_rich_evidence').get().n;
  if(maxWrites===160){assert.equal(result.persistence.richCaptureGap,null);const raw=await Promise.all(db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='market'").all().map(async r=>({blockId:r.owner_id,payload:JSON.stringify(await decodeStoredEvidence(r))})));
   const cycle=await decodeStoredEvidence(db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='cycle'").get());
   for(const [tf,f]of Object.entries(j.frames))assert.deepEqual(replayMarketManifest(cycle.manifests[tf],raw).map(b=>[b.t,b.o,b.h,b.l,b.c]),f.bars.map(b=>[b.t,b.o,b.h,b.l,b.c]));
   assert(result.persistence.records>80,JSON.stringify(result));assert(result.persistence.records<=160);
  }else {assert.equal(counts,0);for(const row of rows)assert.equal(restoreCensus(JSON.parse(row.payload_json),row).captureGap,'RICH_CAPTURE_WRITE_BUDGET_EXHAUSTED');}
  const retry=await persistDecisionCycle(binding,j,provenance,{maxWrites});assert(retry.ok,retry.error);assert.equal(db.prepare('SELECT COUNT(*) n FROM signal_decision_evidence').get().n,7);assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_rich_evidence').get().n,counts);db.close();
 }
});
const subject={id:'late',createdAt:60000,entry:100,tp1:110,tp2:120,sl:90,side:'buy',status:'active'};
const tick=(ts,price,seq)=>({ts,price,measurement:{provider:'mt5',providerTimestamp:ts,sessionId:'s',sequence:seq}});
test('F2 late overlapping opposite bar downgrades prior SUCCESS and FAILURE; harmless later data preserves proof',()=>{
 for(const [price,outcome,h,l]of [[111,'SUCCESS',115,89],[89,'FAILURE',111,85]]){
  const first=foldPostEntry(subject,null,{asOf:65000,ticks:[tick(60000,100,1),tick(65000,price,2)]});assert.equal(first.state.outcome.directionalOutcome,outcome);
  const late=foldPostEntry(subject,first.state,{asOf:180000,bars:[{t:60000,o:100,h,l,c:100,provider:'mt5'}]});assert.equal(late.state.outcome.directionalOutcome,'INSUFFICIENT_DATA');assert.equal(late.state.outcome.timeToTp1,null);assert.equal(late.state.outcome.timeToSl,null);
  const retried=foldPostEntry(subject,late.state,{asOf:180000,bars:[{t:60000,o:100,h,l,c:100,provider:'mt5'}]});assert.equal(retried.events.length,0);assert.deepEqual(retried.state.outcome,late.state.outcome);
 }
 const a=foldPostEntry(subject,null,{asOf:65000,ticks:[tick(60000,100,1),tick(65000,111,2)]}),b=foldPostEntry(subject,a.state,{asOf:70000,ticks:[tick(65000,111,2),tick(70000,112,3)]});assert.equal(b.state.outcome.directionalOutcome,'SUCCESS');
});
test('F3 coverage proof requires exact same-subject nonfuture at-discovery checkpoint, never later accumulator',()=>{
 const event={subjectId:'late',eventId:'w',evidenceType:'BARRIER_OBSERVATION',eventType:'TP1',eligible:true,occurredAt:65000,availableAt:66000,coverageRef:'c'};
 const c={subjectId:'late',eventId:'c',eventType:'COVERAGE_CHECKPOINT',evidenceType:'COVERAGE_CHECKPOINT',checkpointRole:'BARRIER_COVERAGE',availableAt:66000,covered:[{from:60000,to:65000}],gaps:[]};
 const opts={asOf:70000,coverageIntervals:[{from:60000,to:70000}],requireCoverageReferences:true};
 assert.equal(reduceOutcome(subject,[event,c],opts).directionalOutcome,'SUCCESS');
 for(const bad of [null,{...c,subjectId:'other'},{...c,availableAt:66001},{...c,eventType:'TP1'},{...c,covered:[]}])assert.equal(reduceOutcome(subject,[event,...(bad?[bad]:[])],opts).directionalOutcome,'INSUFFICIENT_DATA');
});
async function officialProfile(){
 const p=await outcomeProfile('complete');p.db.exec('CREATE TABLE production_signals(signal_id TEXT PRIMARY KEY,source TEXT,created_at INTEGER,timeframe TEXT,direction TEXT,entry REAL,tp1 REAL,tp2 REAL,sl REAL,status TEXT,closed_at INTEGER,tp1_at INTEGER,sl_at INTEGER)');
 p.db.prepare('INSERT INTO production_signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(p.subject.id,'production',p.at,'5m','buy',4100,4100.4,4100.8,4099.4,'active',null,null,null);return p;
}
async function report(p,asOf,path='/forward-validation'){
 const calls=[],read=createForwardReadCapability(sqliteBinding(p.db,{selectOnly:true,calls}));const body=await readForwardValidation(read,new URL('https://local.invalid'+path),path,asOf);assert(calls.every(s=>/^SELECT/.test(s)));return body;
}
test('F3 SELECT-only record and population summaries reject missing coverage dependency',async()=>{
 const p=await officialProfile(),w=p.events.find(e=>e.evidenceType==='BARRIER_OBSERVATION');
 // Deliberately corrupt the local synthetic checkpoint through physical deletion.
 p.db.exec('DROP TRIGGER measurement_compact_dependency_retained');const target=p.db.prepare('SELECT rowid FROM signal_outcome_evidence WHERE event_id=?').get(w.coverageRef);p.db.prepare('DELETE FROM measurement_outcome_records WHERE record_id=?').run(target.rowid);
 const body=await report(p,p.end+1);assert.equal(body.records[0].outcome.directionalOutcome,'INSUFFICIENT_DATA');assert(body.records[0].correctionStatus.dependencyIssues.length);assert.equal(body.summary.provenDirectional,0);p.db.close();
});
test('F4 later retained correction pins older barrier and checkpoint through bounded cleanup; dependencies ultimately expire',async()=>{
 const p=await outcomeProfile('complete'),binding=transactionalBinding(p.db),w=measurementWriter(binding),barrier=p.events.find(e=>e.evidenceType==='BARRIER_OBSERVATION');
 // Candidate census controls retention eligibility.
 p.db.prepare('INSERT INTO measurement_cohorts(cohort_id,schema_version,effective_at,payload_json,payload_digest,recorded_at) VALUES(?,1,?,?,?,?)').run('c',p.at,'{}','d',p.at);
 p.db.prepare('INSERT INTO decision_cycle_evidence(cycle_id,cohort_id,evaluated_at,payload_json,block_ids_json,payload_digest,recorded_at) VALUES(?,?,?,?,?,?,?)').run('y','c',p.at,'{}','[]','d',p.at);
 p.db.prepare("INSERT INTO signal_decision_evidence(evaluation_id,kind,cycle_id,cohort_id,timeframe,evaluated_at,measurement_only,decision_use,payload_json,payload_digest,recorded_at) VALUES(?,'CANDIDATE','y','c','5m',?,1,0,'{}','d',?)").run(p.subject.id,p.at,p.at);
 const at=p.end+100*DAY,correction={subjectId:p.subject.id,eventId:p.subject.id+':late-correction',eventType:'CORRECTION',evidenceType:'CORRECTION',supersedesEventId:barrier.eventId,reason:'SOURCE_BAR_REVISION',availableAt:at,measurementOnly:true,decisionUse:false};
 await w.immutable('outcome',{event_id:correction.eventId,subject_id:p.subject.id,event_type:'CORRECTION',available_at:at,recorded_at:at},correction);
 for(let i=0;i<5;i++){const r=await maintainMeasurementRetention(binding,at+DAY,{limit:1});assert(r.ok,r.error);}
 assert(p.db.prepare('SELECT event_id FROM signal_outcome_evidence WHERE event_id=?').get(barrier.eventId));assert(p.db.prepare('SELECT event_id FROM signal_outcome_evidence WHERE event_id=?').get(barrier.coverageRef));
 for(let i=0;i<20;i++){const r=await maintainMeasurementRetention(binding,at+91*DAY);assert(r.ok,r.error);}
 assert.equal(p.db.prepare('SELECT COUNT(*) n FROM measurement_outcome_records').get().n,0);assert.equal(p.db.prepare('SELECT COUNT(*) n FROM measurement_outcome_dependencies').get().n,0);p.db.close();
});
test('F5 append-only correction exposes superseded evidence, pending reduction and stale result without GET writes',async()=>{
 const p=await officialProfile(),w=measurementWriter(transactionalBinding(p.db)),barrier=p.events.find(e=>e.evidenceType==='BARRIER_OBSERVATION'),at=p.end+1000;
 const c={subjectId:p.subject.id,eventId:p.subject.id+':pending',eventType:'CORRECTION',evidenceType:'CORRECTION',supersedesEventId:barrier.eventId,reason:'SOURCE_BAR_REVISION',availableAt:at,measurementOnly:true,decisionUse:false};
 await w.immutable('outcome',{event_id:c.eventId,subject_id:c.subjectId,event_type:'CORRECTION',available_at:at,recorded_at:at},c);
 const body=await report(p,at+1);assert.equal(body.records[0].correctionStatus.pendingCount,1);assert.equal(body.records[0].correctionStatus.stale,true);assert.equal(body.summary.correctionPending,1);assert.equal(body.summary.provenDirectional,0);assert(p.db.prepare('SELECT event_id FROM signal_outcome_evidence WHERE event_id=?').get(barrier.eventId));
 const old=await report(p,p.end);assert.equal(old.records[0].correctionStatus.pendingCount,0);p.db.close();
});
test('F6 cohort validation and cursor binding reject empty/duplicate/malformed cohort; filtering accepted',()=>{
 assert.equal(parseForwardQuery(new URL('https://local.invalid?cohort=c_1'),now).cohort,'c_1');
 for(const query of ['cohort=','cohort=a&cohort=b','cohort=a%20b','cohort='+ 'a'.repeat(181)])assert.throws(()=>parseForwardQuery(new URL('https://local.invalid?'+query),now),/query_invalid/);
});
test('F6 actual Official/candidate cohort filtering and pagination bind cursors to the cohort',async()=>{
 const db=localMeasurementDatabase(),binding=transactionalBinding(db);db.exec('CREATE TABLE production_signals(signal_id TEXT PRIMARY KEY,source TEXT,created_at INTEGER,timeframe TEXT,direction TEXT,entry REAL,tp1 REAL,tp2 REAL,sl REAL,status TEXT,closed_at INTEGER,tp1_at INTEGER,sl_at INTEGER)');
 const a=fullCycle(),b=fullCycle();b.cycleId+=':other';b.candidates=[];b.officialIds.clear();
 assert((await persistDecisionCycle(binding,a,provenance)).ok);assert((await persistDecisionCycle(binding,b,{...provenance,codeCommit:'b'.repeat(40)})).ok);
 const cohort=db.prepare('SELECT cohort_id FROM decision_cycle_evidence WHERE cycle_id=?').get(b.cycleId).cohort_id,read=createForwardReadCapability(sqliteBinding(db,{selectOnly:true}));
 const path='/forward-validation/candidates',url=new URL(`https://local.invalid${path}?cohort=${cohort}&limit=1`),first=await readForwardValidation(read,url,path,now+1);
 assert.equal(first.records.length,1);assert.equal(first.records[0].cohortRef,cohort);assert(first.pagination.nextCursor);
 const next=await readForwardValidation(read,new URL(url.href+'&cursor='+first.pagination.nextCursor),path,now+2);assert.equal(next.records[0].cohortRef,cohort);assert.notEqual(first.records[0].evaluationId,next.records[0].evaluationId);
 await assert.rejects(readForwardValidation(read,new URL(`https://local.invalid${path}?cohort=wrong&limit=1&cursor=${first.pagination.nextCursor}`),path,now+2),/cursor_invalid/);
 const filtered=await readForwardValidation(read,new URL('https://local.invalid/forward-validation?cohort='+cohort),'/forward-validation',now+1);assert.equal(filtered.summary.totalOfficial,0);db.close();
});
test('F4 migration and atomic guards preserve unindexed old evidence and reject removal of referenced dependencies',async()=>{
 const p=await outcomeProfile('complete'),w=p.events.find(e=>e.evidenceType==='BARRIER_OBSERVATION'),id=p.db.prepare('SELECT rowid FROM signal_outcome_evidence WHERE event_id=?').get(w.coverageRef).rowid;
 assert.throws(()=>p.db.prepare('DELETE FROM measurement_outcome_records WHERE record_id=?').run(id),/dependency_retained/);
 assert.throws(()=>p.db.prepare('INSERT INTO measurement_outcome_dependencies VALUES(?,999999999,1)').run(id),/reference_missing/);
 const plan=p.db.prepare('EXPLAIN QUERY PLAN SELECT owner_ref FROM measurement_outcome_dependencies WHERE target_ref=?').all(id).map(r=>r.detail).join('\n');assert(plan.includes('measurement_outcome_dependency_target'));p.db.close();
});
test('F4 pending correction cycles cannot become undeletable retention graphs',async()=>{
 const db=localMeasurementDatabase(),writer=measurementWriter(transactionalBinding(db));
 const events=['a','b'].map((id,i)=>({subjectId:'cycle',eventId:id,eventType:'CORRECTION',evidenceType:'CORRECTION',supersedesEventId:i?'a':'b',reason:'SOURCE_BAR_REVISION',availableAt:now,measurementOnly:true,decisionUse:false}));
 await assert.rejects(writer.immutableBatch(events.map(payload=>({type:'outcome',values:{event_id:payload.eventId,subject_id:'cycle',event_type:'CORRECTION',available_at:now,recorded_at:now},payload}))),/reference_cycle/);
 assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_outcome_records').get().n,0);db.close();
});
test('F2 earlier same-source conflicting witness is append-only and a duplicate retry does not recount it',()=>{
 const later={t:120000,o:100,h:105,l:89,c:100,provider:'mt5'},earlier={...later,t:60000};
 const first=foldPostEntry(subject,null,{asOf:180000,ticks:[tick(60000,100,1),tick(65000,111,2)],bars:[later]});assert.equal(first.state.outcome.directionalOutcome,'SUCCESS');
 const challenged=foldPostEntry(subject,first.state,{asOf:240000,bars:[earlier]});assert.equal(challenged.state.outcome.directionalOutcome,'INSUFFICIENT_DATA');assert(challenged.events.some(e=>e.eventType==='SL'));
 const retry=foldPostEntry(subject,challenged.state,{asOf:240000,bars:[earlier]});assert.equal(retry.events.length,0);assert.deepEqual(retry.state.quality,challenged.state.quality);
});
