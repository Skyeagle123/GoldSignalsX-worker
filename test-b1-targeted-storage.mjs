import assert from 'node:assert/strict';
import {test} from 'node:test';
import {outcomeProfile,localMeasurementDatabase} from './test-fixtures/b1-outcome-profiles.mjs';
import {transactionalBinding} from './test-fixtures/b1-sqlite.mjs';
import {measurementWriter} from './signal-evidence-store.js';
import {maintainMeasurementRetention,RETENTION_LIMIT} from './measurement-retention.js';
import {canonicalSerialize} from './signal-evidence.js';
import {decodeStoredOutcome,encodeOutcomeEvidence} from './evidence-codec.js';
import {outcomeProfileBudget,sqliteRecordPayloadBytes} from './measurement-resource-budget.js';
import {foldPostEntry} from './post-entry-evidence.js';
const sessionId='01c4fef2-36fb-47df-82e1-10288f2e329a',DAY=86400000;
test('all six profiles preserve distinct witnesses, five windows and resolvable at-discovery coverage',async()=>{
 const samples=[];
 for(const name of ['complete','gaps','incremental','session-transition','fragmented','correction']){
  const p=await outcomeProfile(name,{sessionId,realistic:true});samples.push({bytes:p.bytes});assert(p.bytes<=8192);
  assert.equal(p.events.filter(e=>e.evidenceType==='WINDOW_FINALIZED').length,5);
  for(const e of p.events.filter(e=>e.coverageRef)){const coverage=p.events.find(c=>c.eventId===e.coverageRef);assert(coverage);assert(coverage.availableAt<=e.availableAt);assert.equal(coverage.checkpointRole,'BARRIER_COVERAGE');}
  const witnesses=p.events.filter(e=>e.evidenceType==='BARRIER_OBSERVATION');assert.equal(witnesses.length,name==='gaps'?3:6);assert.equal(new Set(witnesses.map(w=>w.eventId)).size,witnesses.length);
  if(name==='correction'){const c=p.events.find(e=>e.eventType==='CORRECTION');assert(p.events.some(e=>e.eventId===c.supersedesEventId));}
  assert(!p.events.find(e=>e.evidenceType==='FINAL_MEASUREMENT').processedBoundaryTicks);
  const physical=p.db.prepare('SELECT * FROM measurement_outcome_records').all();const subject=p.db.prepare('SELECT * FROM measurement_subjects').get();
  assert.equal(sqliteRecordPayloadBytes([null,subject.subject_id,subject.recorded_at,subject.persisted_at])+physical.reduce((n,r)=>n+sqliteRecordPayloadBytes(Object.entries(r).map(([k,v])=>k==='record_id'?null:v)),0),p.bytes);
  p.db.close();
 }
 assert(outcomeProfileBudget(samples).planningAveragePass);assert(!outcomeProfileBudget(samples).reviewAlarm);
});
test('UUID codec is lossless and digest covers original precision/order/unavailable values',async()=>{
 for(const value of [sessionId,sessionId.toUpperCase(),'not-a-uuid']){
  const p={subjectId:'s',eventId:'s:e',eventType:'TP1',evidenceType:'BARRIER_OBSERVATION',availableAt:1790608200000,sessionId:value,values:[4100.001,-0,NaN,null,undefined],measurementOnly:true,decisionUse:false};
  const e=await encodeOutcomeEvidence(p);const restored=await decodeStoredOutcome({subject_id:p.subjectId,event_id:p.eventId,event_type:p.eventType,available_at:p.availableAt,codec:e.codec,payload_blob:e.data,payload_digest:e.digest,uncompressed_length:e.length});assert.equal(canonicalSerialize(restored),canonicalSerialize(p));
 }
});
test('coverage integrity rejects dangling/future/cross-subject refs; frozen retry is idempotent',async()=>{
 const p=await outcomeProfile('complete',{sessionId,realistic:true}),w=measurementWriter(transactionalBinding(p.db));
 const b=p.events.find(e=>e.evidenceType==='BARRIER_OBSERVATION');
 await assert.rejects(w.immutable('outcome',{event_id:'bad',subject_id:p.subject.id,event_type:b.eventType,available_at:p.end,recorded_at:p.end,block_ids_json:'[]'},{...b,eventId:'bad',coverageRef:'missing'}),/reference_missing/);
 const original=canonicalSerialize(p.events.find(e=>e.eventId===b.coverageRef));
 await w.immutable('outcome',{event_id:b.eventId,subject_id:p.subject.id,event_type:b.eventType,available_at:b.availableAt,recorded_at:p.end,block_ids_json:'[]'},b);
 assert.equal(canonicalSerialize(await decodeStoredOutcome(p.db.prepare('SELECT * FROM signal_outcome_evidence WHERE event_id=?').get(b.coverageRef))),original);
 const corrupt={...b,eventId:'correction-bad',eventType:'CORRECTION',supersedesEventId:'missing',reason:'SOURCE_BAR_REVISION'};await assert.rejects(w.immutable('outcome',{event_id:corrupt.eventId,subject_id:p.subject.id,event_type:corrupt.eventType,available_at:p.end,recorded_at:p.end,block_ids_json:'[]'},corrupt),/reference_missing/);p.db.close();
});
test('same observation clock with a new sequence produces a new immutable coverage reference',()=>{
 const s={id:'s',createdAt:60000,entry:100,tp1:110,tp2:120,sl:90,side:'buy',status:'active'};
 const tick=(price,sequence)=>({ts:65000,price,measurement:{provider:'mt5',sessionId:'s',providerTimestamp:65000,sequence}});
 const a=foldPostEntry(s,null,{asOf:65000,ticks:[tick(111,1)]}),b=foldPostEntry(s,a.state,{asOf:65000,ticks:[tick(121,2)]});
 assert.notEqual(a.events[0].eventId,b.events[0].eventId);assert.equal(b.events.filter(e=>e.eventType==='TP2').length,1);
});
test('review ceiling alarms without truncating append-only corrections',async()=>{
 const p=await outcomeProfile('complete',{sessionId,realistic:true}),w=measurementWriter(transactionalBinding(p.db),{maxWrites:160});let review=[];
 for(let i=0;i<48;i++){
  const c={subjectId:p.subject.id,eventId:`${p.subject.id}:correction:${i}`,eventType:'CORRECTION',evidenceType:'CORRECTION',supersedesEventId:p.events.find(e=>e.evidenceType==='BARRIER_OBSERVATION').eventId,reason:'SOURCE_BAR_REVISION',price:4100.001+i*.001,availableAt:p.end+i,observedAt:p.end+i,measurementOnly:true,decisionUse:false};
  const result=await w.immutable('outcome',{event_id:c.eventId,subject_id:c.subjectId,event_type:c.eventType,available_at:c.availableAt,recorded_at:c.availableAt,block_ids_json:'[]'},c);review.push(...result.resourceReviews);
 }
 assert(review.some(r=>r.alarm&&r.bytes>8192));assert.equal(p.db.prepare("SELECT COUNT(*) n FROM signal_outcome_evidence WHERE event_type='CORRECTION'").get().n,48);p.db.close();
});
// A 1/48-scale generated workload preserves the exact generation/cleanup ratio:
// 42 subjects/day x 20 outcome rows; six cycles/day x 256 cleanup slots.
test('cleanup sustainably catches up after 48h outage; no census/result/raw/reference orphans',async()=>{
 const db=localMeasurementDatabase();db.exec('PRAGMA foreign_keys=ON');const binding=transactionalBinding(db),start=1790608200000;
 db.prepare('INSERT INTO measurement_cohorts(cohort_id,schema_version,effective_at,payload_json,payload_digest,recorded_at) VALUES(?,1,?,?,?,?)').run('cohort',start,'{}','d',start);
 const cycle=db.prepare('INSERT INTO decision_cycle_evidence(cycle_id,cohort_id,evaluated_at,payload_json,block_ids_json,payload_digest,recorded_at) VALUES(?,?,?,?,?,?,?)');
 const census=db.prepare("INSERT INTO signal_decision_evidence(evaluation_id,kind,cycle_id,cohort_id,timeframe,evaluated_at,measurement_only,decision_use,payload_json,payload_digest,recorded_at) VALUES(?,'CANDIDATE',?,'cohort','5m',?,1,0,'{}','d',?)");
 const sub=db.prepare('INSERT INTO measurement_subjects(subject_id,recorded_at) VALUES(?,?)');
 const event=db.prepare('INSERT INTO measurement_outcome_records(subject_ref,event_suffix,event_kind,available_offset,payload_digest,payload_blob,uncompressed_length,recorded_offset,retention_bucket,persisted_offset) VALUES(?,?,0,0,?, ?,1,0,?,0)');
 const raw=db.prepare('INSERT INTO market_evidence_blocks(block_id,timeframe,payload_json,payload_digest,recorded_at) VALUES(?,\'1m\',\'{}\',\'d\',?)');
 const rich=db.prepare("INSERT INTO measurement_rich_evidence(evidence_id,owner_type,owner_id,payload_json,payload_digest,payload_blob,codec,uncompressed_length,recorded_at,retain_until) VALUES(?,'decision',?,'{}','d',?,'test',1,?,?)");
 const final=db.prepare('INSERT INTO measurement_final_results(subject_id,directional,extended,terminal,coverage,ordering,numeric_flags,reducer_version,payload_digest,recorded_at,retain_until) VALUES(?,0,0,0,0,0,0,1,?, ?,?)');
 let lastBacklog=0,clearedDay=null;
 for(let day=0;day<7;day++){
  const now=start+day*DAY,old=now-400*DAY;db.exec('BEGIN');
  for(let i=0;i<42;i++){const id=`${day}:${i}`;cycle.run(id,'cohort',old,'{}','[]','d',old);census.run(id,id,old,old);sub.run(id,old);const ref=db.prepare('SELECT subject_ref FROM measurement_subjects WHERE subject_id=?').get(id).subject_ref;for(let k=0;k<20;k++)event.run(ref,String(k),new Uint8Array(32),new Uint8Array(10),Math.floor(old/DAY));rich.run('r:'+id,id,new Uint8Array(10),old,old+90*DAY);final.run(id,new Uint8Array(32),old,old+365*DAY);raw.run('b:'+id,old);}
  db.exec('COMMIT');
  if(day>=2)for(let cycle=0;cycle<6;cycle++){const r=await maintainMeasurementRetention(binding,now);assert(r.ok,JSON.stringify(r));}
  const backlog=db.prepare('SELECT COUNT(*) n FROM measurement_outcome_records').get().n;
  if(day>=2)assert(backlog<=lastBacklog);lastBacklog=backlog;if(day>=2&&backlog===0&&clearedDay===null)clearedDay=day;
 }
 assert(clearedDay<=4,`backlog cleared on day ${clearedDay}`);
 for(const table of ['measurement_outcome_records','measurement_final_results','signal_decision_evidence','measurement_subjects','market_evidence_blocks','measurement_rich_evidence'])assert.equal(db.prepare(`SELECT COUNT(*) n FROM ${table}`).get().n,0,table);
 assert.equal(RETENTION_LIMIT,256);db.close();
});
test('physical outcome retention query uses bucket index; final-result orphan expiry is independent',async()=>{
 const p=await outcomeProfile('complete',{sessionId,realistic:true}),db=p.db;
 const plan=db.prepare('EXPLAIN QUERY PLAN SELECT r.record_id FROM measurement_outcome_records r JOIN measurement_subjects s ON s.subject_ref=r.subject_ref WHERE r.retention_bucket<=? AND s.recorded_at+r.recorded_offset<=? ORDER BY r.retention_bucket,r.record_id LIMIT ?').all(Math.floor(p.end/DAY),p.end,256).map(r=>r.detail).join('\n');assert(plan.includes('measurement_outcome_record_retention'));assert(!plan.includes('TEMP B-TREE'),plan);
 // No census exists in this direct writer fixture: independent expiry must still
 // delete the result. Keep Official results (retain_until NULL) outside the path.
 const r=await maintainMeasurementRetention(transactionalBinding(db),p.end+366*DAY);assert(r.ok,JSON.stringify(r));assert.equal(db.prepare('SELECT COUNT(*) n FROM measurement_final_results').get().n,0);db.close();
});
test('delayed discovery keeps occurrence horizon and actual availability separate',()=>{
 const at=1790608200000,horizon=at+60000,availableAt=at+120000;
 const s={id:'delayed',createdAt:at,entry:100,tp1:110,tp2:120,sl:90,side:'buy',status:'active'};
 const folded=foldPostEntry(s,null,{asOf:horizon,availableAt,bars:[{t:at,o:100,h:111,l:99,c:110,provider:'mt5'}]});
 const tp1=folded.events.find(e=>e.eventType==='TP1');assert.equal(tp1.availableAt,availableAt);assert.equal(tp1.occurredTo,horizon);assert.equal(folded.state.outcome.evidenceAsOf,availableAt);
 const coverage=folded.events.find(e=>e.eventId===tp1.coverageRef);assert.equal(coverage.availableAt,availableAt);assert.equal(coverage.occurredTo,horizon);
});
