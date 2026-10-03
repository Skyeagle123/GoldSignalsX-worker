import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
import {test} from 'node:test';
import {SIGNAL_TF_MS} from './signal-engine.js';
import {takeDecisionCycle,persistDecisionCycle} from './signal-measurement.js';
const base='6721f84b961f6afe3d52513a2ed56e138a7990d5';
async function load(source,name,old=false){const url=new URL(`./.b1-${name}-worker.mjs`,import.meta.url);await fs.writeFile(url,source.replace("import { DurableObject } from 'cloudflare:workers';",'class DurableObject {}').replace("'./signal-engine.js'",old?"'./test-fixtures/signal-engine-pre-b1.js'":"'./signal-engine.js'"));try{return await import(url.href);}finally{await fs.unlink(url);}}
const before=await load(execFileSync('git',['show',`${base}:goldsignalsx-worker.js`],{encoding:'utf8'}),'old',true);
const after=await load(await fs.readFile(new URL('./goldsignalsx-worker.js',import.meta.url),'utf8'),'new');
class Binding{
 constructor(){this.database=new DatabaseSync(':memory:');}
 async exec(sql){this.database.exec(sql);return {};}
 prepare(sql){const db=this.database;function statement(values=[]){return {bind(...v){return statement(v);},async all(){return {results:db.prepare(sql).all(...values)};},async first(){return db.prepare(sql).get(...values)||null;},async run(){const result=db.prepare(sql).run(...values);return {meta:{changes:Number(result.changes)}};},runSync(){return db.prepare(sql).run(...values);}};}return statement();}
 async batch(statements){this.database.exec('BEGIN');try{const rows=statements.map(x=>x.runSync());this.database.exec('COMMIT');return rows;}catch(e){this.database.exec('ROLLBACK');throw e;}}
}
const now=Date.UTC(2026,8,28,15,10);const realClock=Date.now;const realFetch=globalThis.fetch;
function setup(module,{exposure=null,stale=false,news=null}={}){
 const db=new Binding();db.database.exec("CREATE TABLE bars_v2(tf INTEGER,t INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,provider TEXT,PRIMARY KEY(tf,t));");
 for(const [tf,step] of Object.entries(SIGNAL_TF_MS)){
  const end=Math.floor(now/step)*step;
  const stmt=db.database.prepare('INSERT INTO bars_v2 VALUES(?,?,?,?,?,?,?,?)');
  for(let i=0;i<100;i++){const c=4100+(i-99)*.35;stmt.run(step/60000,end-(100-i)*step,c-.3,c+.1,c-.4,c,1,'mt5');}
 }
 const values=new Map();const writes=[];let state=exposure;const decisions=[];
 const kv={async get(key,type){const value=values.get(key);return type==='json'&&typeof value==='string'?JSON.parse(value):value??null;},async put(key,value){values.set(key,value);writes.push([key,value]);}};
 const feed={async status(){return {latestQuote:{event:'price',price:4100,ts:now,receivedAt:stale?now-21000:now,source:'mt5',sessionId:'synthetic',sequence:11}};},async manageGoldExposure(input){const trace={};const result=module.decideGoldExposure(state,input,trace);decisions.push(result);state=result.state;return {...result,measurementBefore:input.measurementEvidence?exposure:null,...(input.measurementEvidence?{measurementTrace:trace}:{})};},async cancelGoldExposureReservation(){return null;}};
 return {env:{GSX_DB:db,GSX_KV:kv,MT5_INGEST_TOKEN:'synthetic-fixture',SIGNAL_ALERTS_ENABLED:'0',GOLD_FEED:{getByName:()=>feed}},db,values,writes,decisions,news};
}
test('cycle-level exact parity: candidate ordering, winner, confirmations, blocks, Official/KV/performance identities',async()=>{
 Date.now=()=>now;globalThis.fetch=()=>{throw new Error('NO_EXTERNAL_FETCH');};
 try{for(const options of [{},{stale:true},{news:{ok:false,stale:true,safety:{calendarBlockTechnicalSignal:true,reason:'fixture-high'}}},{exposure:{symbol:'XAUUSD',status:'active',side:'sell',primarySignalId:'old',primaryTf:'60m',openedAt:now-10000,maxPositions:1,cooldownUntil:0,confirmations:[],blocked:[]}}]){
  const a=setup(before,options),b=setup(after,options);
  const filters={nyFilterOn:false,pivotFilterOn:false};
  const original=await before.runSignalCycle(a.env,a.news,filters),actual=await after.runSignalCycle(b.env,b.news,filters);
  assert.deepEqual(actual,original);assert.deepEqual(b.writes,a.writes);
  assert.deepEqual(b.decisions,a.decisions); // policy result, not the passive input envelope
  const tables=a.db.database.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'production_%' ORDER BY name").all();
  for(const {name} of tables)assert.deepEqual(b.db.database.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all(),a.db.database.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all());
  if(!options.stale&&!options.news&&!options.exposure){a.db.database.exec(await fs.readFile(new URL('./migrations/0001_measurement_evidence.sql',import.meta.url),'utf8'));for(const {name} of tables)assert.deepEqual(b.db.database.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all(),a.db.database.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all());}
  const journal=takeDecisionCycle(actual);assert(journal);if(!options.stale&&!options.news&&!options.exposure){assert(journal.candidates.length>0);assert.equal(journal.officialIds.size,1);}assert.equal(journal.attempts.length,7);
  const frozenWrites=structuredClone(b.writes);
  const failure=await persistDecisionCycle({prepare(){throw new Error('synthetic measurement failure');}},journal,{codeCommit:base,measurementEffectiveAt:now});
  assert.equal(failure.ok,false);assert.deepEqual(b.writes,frozenWrites);
  a.db.database.close();b.db.database.close();
 }}finally{Date.now=realClock;globalThis.fetch=realFetch;}
});
test('Official persistence failure/cancellation and performance-storage failure preserve cycle parity',async()=>{
 Date.now=()=>now;globalThis.fetch=()=>{throw new Error('NO_EXTERNAL_FETCH');};
 try{for(const mode of ['kv-failure','performance-failure']){
  const a=setup(before),b=setup(after),cancellations=[];
  for(const [index,x]of [a,b].entries()){
   const feed=x.env.GOLD_FEED.getByName();feed.cancelGoldExposureReservation=async input=>{(cancellations[index]||= []).push(input);};
   if(mode==='kv-failure'){const put=x.env.GSX_KV.put;x.env.GSX_KV.put=async(k,v)=>{if(k.startsWith('signal:state:'))throw new Error('synthetic Official KV failure');return put(k,v);};}
   else {const prepare=x.db.prepare.bind(x.db);x.db.prepare=sql=>{if(/INSERT[\s\S]*production_signals/.test(sql))throw new Error('synthetic performance unavailable');return prepare(sql);};}
  }
  const filters={nyFilterOn:false,pivotFilterOn:false};let oldError,newError,oldResult,newResult;
  try{oldResult=await before.runSignalCycle(a.env,null,filters);}catch(e){oldError=e;}
  try{newResult=await after.runSignalCycle(b.env,null,filters);}catch(e){newError=e;}
  assert.equal(newError?.message,oldError?.message);assert.deepEqual(newResult,oldResult);assert.deepEqual(b.writes,a.writes);assert.deepEqual(b.decisions,a.decisions);assert.deepEqual(cancellations[1],cancellations[0]);
  const journal=takeDecisionCycle(newError||newResult);assert(journal,`${mode}: ${newError?.stack||JSON.stringify(newResult)}`);
  if(mode==='kv-failure'){assert(journal.failedOfficialId);assert.equal(journal.officialIds.size,0);}
  else {assert.equal(journal.officialIds.size,1);b.db.database.exec(await fs.readFile(new URL('./migrations/0001_measurement_evidence.sql',import.meta.url),'utf8'));const result=await persistDecisionCycle(b.db,journal,{codeCommit:base,measurementEffectiveAt:now});assert.equal(result.ok,true);const row=b.db.database.prepare("SELECT payload_json FROM signal_decision_evidence WHERE kind='OFFICIAL'").get();assert.equal(JSON.parse(row.payload_json).officialPersistence.performance,'FAILED');}
  a.db.database.close();b.db.database.close();
 }}finally{Date.now=realClock;globalThis.fetch=realFetch;}
});
test('passive Exposure observer cannot change ranking, ownership or clock semantics',()=>{
 const input={now,officialSignals:[],candidates:[{id:'one',tf:'5m',side:'buy',conf:80,signalBarTs:now-300000,createdAt:now},{id:'two',tf:'15m',side:'sell',conf:88,signalBarTs:now-900000,createdAt:now}]};
 const trace={};const actual=after.decideGoldExposure(null,input,trace);assert.deepEqual(actual,before.decideGoldExposure(null,input));assert.equal(trace.eligibleComparatorOrder[0].id,'two');
 const broken=new Proxy({},{set(){throw new Error('measurement-only failure');}});assert.deepEqual(after.decideGoldExposure(null,input,broken),actual);
});
