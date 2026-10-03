import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {DatabaseSync} from 'node:sqlite';
import {test} from 'node:test';
import {canonicalSerialize,parseEvidence,cohortManifest} from './signal-evidence.js';
import {measurementWriter} from './signal-evidence-store.js';
import {buildMarketManifest,replayMarketManifest} from './market-evidence.js';
const migration=await fs.readFile(new URL('./migrations/0001_measurement_evidence.sql',import.meta.url),'utf8');
export function sqliteBinding(db){return {prepare(sql){return {bind(...values){return {async run(){const r=db.prepare(sql).run(...values);return {meta:{rows_written:Number(r.changes)}};},async first(){return db.prepare(sql).get(...values)??null;},async all(){return {results:db.prepare(sql).all(...values)};}};},async all(){return {results:db.prepare(sql).all()};},async first(){return db.prepare(sql).get()??null;}};}};}
test('explicit migration: clean and representative existing schema; five immutable guards',async()=>{
 for(const existing of [false,true]){
  const db=new DatabaseSync(':memory:');if(existing)db.exec(await fs.readFile(new URL('./schema.sql',import.meta.url),'utf8'));
  db.exec(migration);assert.equal(db.prepare("SELECT count(*) n FROM sqlite_schema WHERE type='table' AND name LIKE '%evidence%'").get().n,5);
  const writer=measurementWriter(sqliteBinding(db));const manifest=cohortManifest({codeCommit:'a'.repeat(40),measurementEffectiveAt:1000,configFingerprint:'public-filter-hash'});
  await writer.immutable('cohort',{cohort_id:manifest.cohortId,schema_version:1,effective_at:1000,recorded_at:1000},manifest);
  await writer.immutable('cohort',{cohort_id:manifest.cohortId,schema_version:1,effective_at:1000,recorded_at:2000},manifest);
  assert.equal(db.prepare('SELECT count(*) n FROM measurement_cohorts').get().n,1);
  await assert.rejects(writer.immutable('cohort',{cohort_id:manifest.cohortId,schema_version:1,effective_at:1000,recorded_at:2000},{...manifest,captureVersion:'changed'}),/integrity_conflict/);
  assert.throws(()=>db.prepare('UPDATE measurement_cohorts SET payload_json=?').run('{}'),/immutable/);
  assert.equal(db.prepare("SELECT count(*) n FROM sqlite_schema WHERE type='trigger' AND name LIKE 'measurement_%_immutable'").get().n,5);db.close();
 }
});
test('serialization: stable digest inputs; cycles, secrets and bounds fail safely',()=>{
 assert.equal(canonicalSerialize({b:2,a:1}),canonicalSerialize({a:1,b:2}));
 const nums={v:[NaN,-0,Infinity,-Infinity]};assert.deepEqual(parseEvidence(canonicalSerialize(nums)),nums);
 const circular={};circular.self=circular;assert.throws(()=>canonicalSerialize(circular),/serialization/);
 assert.throws(()=>canonicalSerialize({GSX_WRITE_TOKEN:'fixture-never-secret'}),/sensitive/);
 assert.throws(()=>canonicalSerialize({data:'x'.repeat(100)},20),/payload_exceeded/);
});
test('raw market manifests: exact ordering, deduplication and source revision isolation',async()=>{
 const bars=Array.from({length:150},(_,i)=>({t:1000+i*60000,o:100+i,h:102+i,l:99+i,c:101+i,v:10,provider:'mt5'}));
 const first=await buildMarketManifest('1m',bars,10000),retry=await buildMarketManifest('1m',bars,20000);
 assert.deepEqual(first.blocks.map(x=>x.blockId),retry.blocks.map(x=>x.blockId));
 const replay=replayMarketManifest(first.manifest,first.blocks);
 assert.deepEqual(replay.map(({sessionId,sourceAvailableAt,...b})=>b),bars);
 bars[0].c=999;assert.equal(replayMarketManifest(first.manifest,first.blocks)[0].c,101);
 const revised=await buildMarketManifest('1m',bars,20000);assert.notEqual(revised.blocks[0].blockId,first.blocks[0].blockId);
 assert.throws(()=>replayMarketManifest(first.manifest,[]),/market_missing/);
});
test('mutable state cannot alter immutable payload; corrections are new events',async()=>{
 const db=new DatabaseSync(':memory:');db.exec(migration);const writer=measurementWriter(sqliteBinding(db));
 const record={eventId:'original',measurementOnly:true,decisionUse:false,level:10};
 const values={event_id:'original',subject_id:'candidate',event_type:'BARRIER_OBSERVATION',occurred_at:100,available_at:200,block_ids_json:'[]',recorded_at:200};
 await writer.immutable('outcome',values,record);await writer.state('candidate',null,{cursor:1},200);await writer.state('candidate',null,{cursor:2},300);
 await writer.immutable('outcome',{...values,event_id:'correction'},{...record,eventId:'correction',supersedes:'original'});
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence').get().n,2);
 assert.equal(parseEvidence(db.prepare('SELECT payload_json FROM signal_outcome_evidence WHERE event_id=?').get('original').payload_json).level,10);
 assert.throws(()=>db.exec("UPDATE signal_outcome_evidence SET payload_json='{}'"));
 db.close();
});
test('measurement writes fail closed to measurement, with fixed bound',async()=>{
 const db=new DatabaseSync(':memory:');db.exec(migration);const writer=measurementWriter(sqliteBinding(db),{maxWrites:0});
 await assert.rejects(writer.state('candidate',null,{},1),/budget/);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_measurement_state').get().n,0);db.close();
});
test('writer rejects decision-use contamination before persisting immutable measurement',async()=>{
 const db=new DatabaseSync(':memory:');db.exec(migration);const writer=measurementWriter(sqliteBinding(db));
 await assert.rejects(writer.immutable('outcome',{event_id:'bad',subject_id:'candidate',event_type:'TP1',available_at:1,recorded_at:1,block_ids_json:'[]'},{measurementOnly:true,decisionUse:true}),/decision_use_forbidden/);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_outcome_evidence').get().n,0);db.close();
});
