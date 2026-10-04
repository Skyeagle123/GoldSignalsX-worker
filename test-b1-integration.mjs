import assert from 'node:assert/strict';
import {test} from 'node:test';
import fs from 'node:fs/promises';
import {DatabaseSync} from 'node:sqlite';
import {transactionalBinding} from './test-fixtures/b1-sqlite.mjs';
import {beginDecisionCycle,observeAttempt,finishDecisionCycle,takeDecisionCycle,persistDecisionCycle} from './signal-measurement.js';
import {computeServerSignal} from './signal-engine.js';
import {collectMeasurement} from './measurement-collector.js';
import {maintainMeasurementRetention} from './measurement-retention.js';
const migration=(await fs.readFile(new URL('./migrations/0001_measurement_evidence.sql',import.meta.url),'utf8'))+(await fs.readFile(new URL('./migrations/0002_measurement_storage_tiers.sql',import.meta.url),'utf8'));
const now=Date.UTC(2026,8,28,15,10),base='6721f84b961f6afe3d52513a2ed56e138a7990d5';
function setup(){const db=new DatabaseSync(':memory:');db.exec('PRAGMA foreign_keys=ON');db.exec(migration);db.exec('CREATE TABLE production_signals(signal_id TEXT PRIMARY KEY,status TEXT,closed_at INTEGER)');return {db,binding:transactionalBinding(db)};}
function journal({official=false,cycleId='integration',value=4100}={}){
 const bars=Array.from({length:100},(_,i)=>{const c=value+(i-99)*.35;return{t:now-(100-i)*300000,o:c-.3,h:c+.1,l:c-.4,c,v:1,provider:'mt5'};});
 const frames={'5m':{bars},'1m':{bars:[]}},live={price:value,ts:now,receivedAt:now,source:'mt5'},trace={};
 // No-MTF isolated 5m intentionally rejects. Use 240m engine gate requiring no higher confirmation.
 const result=computeServerSignal(bars,{tf:'240m',live,evaluationAt:now,barsSource:'d1',filters:{nyFilterOn:false,pivotFilterOn:false}},trace);
 frames['240m']={bars};assert(trace.levels);const id=`240m:${bars.at(-1).t}:${result.side}`;
 const j=beginDecisionCycle(now,frames,live,{admissionContext:{cycle:{cycleId}}},{nyFilterOn:false,pivotFilterOn:false});
 observeAttempt(j,{tf:'240m',trace,result,requestedMtf:[],includedMtf:[]});
 const output=finishDecisionCycle(j,{}, {decisions:new Map([[id,{decision:official?'accepted':'blocked_opposite',primarySignalId:'old-primary'}]]),candidates:[{id,tf:'240m',createdAt:now,conf:result.conf}],officialIds:new Set(official?[id]:[]),confirmationIds:new Set(),exposureResult:{state:{primarySignalId:'old-primary'}}});
 return {j:takeDecisionCycle(output),id,result};
}
test('real capture wiring persists exact immutable attempt/raw inputs; identical retry and conflict',async()=>{
 const {db,binding}=setup(),input=journal();
 const provenance={codeCommit:base,measurementEffectiveAt:now};
 assert.equal((await persistDecisionCycle(binding,input.j,provenance)).ok,true);
 assert.equal((await persistDecisionCycle(binding,input.j,provenance)).ok,true);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,1);
 assert.equal(db.prepare('SELECT count(*) n FROM production_signals').get().n,0);
 assert(db.prepare('SELECT count(*) n FROM market_evidence_references').get().n>0);
 assert.equal((await persistDecisionCycle(binding,journal({value:4200}).j,provenance)).error,'measurement_integrity_conflict');
 db.close();
});
test('candidate collector cannot create Official; explicit no-data coverage, bounded cursor and isolated retention',async()=>{
 const {db,binding}=setup(),input=journal();await persistDecisionCycle(binding,input.j,{codeCommit:base,measurementEffectiveAt:now});
 const result=await collectMeasurement(binding,{asOf:now+300000,ticks:[],bars:[]});assert.equal(result.ok,true);assert.equal(result.updated,1);
 assert.equal(db.prepare('SELECT count(*) n FROM production_signals').get().n,0);
 const state=JSON.parse(db.prepare('SELECT payload_json FROM signal_measurement_state').get().payload_json);assert.equal(state.global.mfe,null);assert.equal(state.global.coverage,'INSUFFICIENT');assert(state.gaps.length>0);
 const retained=await maintainMeasurementRetention(binding,now+91*86400000);assert.equal(retained.ok,true);assert.equal(retained.candidateAttemptsRemoved,0);
 assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,1);db.close();
});
test('Official evidence and referenced market blocks survive candidate retention',async()=>{
 const {db,binding}=setup(),input=journal({official:true});await persistDecisionCycle(binding,input.j,{codeCommit:base,measurementEffectiveAt:now});
 await maintainMeasurementRetention(binding,Date.now()+366*86400000);
 assert.equal(db.prepare("SELECT count(*) n FROM signal_decision_evidence WHERE kind='OFFICIAL'").get().n,1);
 assert(db.prepare('SELECT count(*) n FROM market_evidence_blocks').get().n>0);db.close();
});
test('capture budgets and serialization fail to measurement only, without external I/O',async()=>{
 const {db,binding}=setup(),input=journal();
 for(const options of [{maxWrites:0},{maxBytes:10}]){const result=await persistDecisionCycle(binding,input.j,{codeCommit:base,measurementEffectiveAt:now},options);assert.equal(result.ok,false);}
 assert.equal(db.prepare('SELECT count(*) n FROM production_signals').get().n,0);assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,0);db.close();
});
test('Official accumulator is not starved by new research attempts; candidate horizon bounded',async()=>{
 const {db,binding}=setup();
 const candidate=journal({cycleId:'older'}),official=journal({official:true,cycleId:'later',value:4200});
 for(const j of [candidate,official])assert.equal((await persistDecisionCycle(binding,j.j,{codeCommit:base,measurementEffectiveAt:now})).ok,true);
 db.prepare('INSERT INTO production_signals VALUES(?,?,?)').run(official.id,'active',null);
 const r=await collectMeasurement(binding,{asOf:now+300000,ticks:[],bars:[],maxSubjects:1});assert.equal(r.ok,true);assert.equal(r.budgetLimitReached,true);
 assert.equal(db.prepare('SELECT updated_at FROM signal_measurement_state WHERE subject_id=?').get(official.id).updated_at,now+300000);
 assert.equal(db.prepare('SELECT updated_at FROM signal_measurement_state WHERE subject_id=?').get('older:0').updated_at,now);
 assert.equal(db.prepare('SELECT count(*) n FROM production_signals').get().n,1);
 const end=await collectMeasurement(binding,{asOf:now+7200000,ticks:[],bars:[],maxSubjects:8});assert.equal(end.ok,true,JSON.stringify(end));
 assert.equal(db.prepare('SELECT payload_json FROM signal_measurement_state WHERE subject_id=?').get('older:0'),undefined);
 const final=await import('./evidence-codec.js');const state=await final.decodeEvidence(JSON.parse(db.prepare('SELECT payload_json FROM measurement_rich_evidence WHERE evidence_id=?').get('outcome:older:0:final').payload_json));
 assert.equal(state.measurementHorizonAt,now+3600000);assert.equal(state.outcome.evidenceAsOf,now+3600000);assert(state.measurementStatus.startsWith('HORIZON_ATTEMPT_COMPLETE'));
 db.close();
});
test('later News/MTF/indicator context cannot rewrite frozen attempt or entry',async()=>{
 const {db,binding}=setup(),input=journal({official:true});const provenance={codeCommit:base,measurementEffectiveAt:now};
 assert.equal((await persistDecisionCycle(binding,input.j,provenance)).ok,true);
 const before=db.prepare('SELECT payload_json FROM signal_decision_evidence').get().payload_json;
 input.j.newsContext={calendar:{stale:false,matchedEvent:'later revision'}};input.j.attempts[0].trace.indicators.rsi=999;
 const result=await persistDecisionCycle(binding,input.j,provenance);assert.equal(result.error,'measurement_integrity_conflict');
 assert.equal(db.prepare('SELECT payload_json FROM signal_decision_evidence').get().payload_json,before);assert.equal(db.prepare('SELECT count(*) n FROM signal_decision_evidence').get().n,1);db.close();
});
test('collector chooses due rows using indexed cursor without scanning decision JSON',async()=>{
 const {db,binding}=setup(),input=journal();await persistDecisionCycle(binding,input.j,{codeCommit:base,measurementEffectiveAt:now});
 const sql=`SELECT d.evaluation_id FROM signal_measurement_state s JOIN signal_decision_evidence d ON d.evaluation_id=s.evaluation_id WHERE s.kind=? AND s.next_observe_at<=? ORDER BY s.next_observe_at,s.subject_id LIMIT ?`;
 const plan=db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all('CANDIDATE',now+300000,8).map(p=>p.detail).join('\n');
 assert(plan.includes('measurement_collection_due'));assert(!plan.includes('SCAN d'));assert(!plan.includes('TEMP B-TREE'));
 db.close();
});
