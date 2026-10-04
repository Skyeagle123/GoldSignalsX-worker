import assert from 'node:assert/strict';
import {test} from 'node:test';
import fs from 'node:fs/promises';
import {DatabaseSync} from 'node:sqlite';
import {sqliteBinding} from './test-fixtures/b1-sqlite.mjs';
import {createForwardReadCapability,readForwardValidation} from './forward-validation-reader.js';
const migration=(await fs.readFile(new URL('./migrations/0001_measurement_evidence.sql',import.meta.url),'utf8'))+(await fs.readFile(new URL('./migrations/0002_measurement_storage_tiers.sql',import.meta.url),'utf8'));
const source=await fs.readFile(new URL('./goldsignalsx-worker.js',import.meta.url),'utf8');
const generated=new URL('./.b1-reporting-worker.mjs',import.meta.url);
await fs.writeFile(generated,source.replace("import { DurableObject } from 'cloudflare:workers';",'class DurableObject {}'));
const {default:worker}=await import(generated.href);await fs.unlink(generated);
if(!crypto.subtle.timingSafeEqual)Object.defineProperty(crypto.subtle,'timingSafeEqual',{value:(a,b)=>Buffer.from(a).equals(Buffer.from(b))});
const paths=['/forward-validation','/forward-validation/candidates','/forward-validation/signals/5m:1789992000000:buy'];
const origin='https://skyeagle123.github.io';
const trap=new Proxy({},{get(){throw new Error('CAPABILITY_MUTATION_TRAP');}});
const now=Date.now();
function env(db){return {ALLOW_ORIGINS:JSON.stringify([origin]),GSX_WRITE_TOKEN:'synthetic-test-credential',GSX_DB:db,GSX_KV:trap,GOLD_FEED:trap};}
const headers={Origin:origin,'x-gsx-write-token':'synthetic-test-credential'};
const request=(path,auth=headers)=>new Request(`https://local.invalid${path}`,{headers:auth});
const context={waitUntil(){throw new Error('BACKGROUND_MUTATION_TRAP');}};
function setup(){const db=new DatabaseSync(':memory:');db.exec(migration);db.exec(`CREATE TABLE production_signals(signal_id TEXT PRIMARY KEY,source TEXT,created_at INTEGER,timeframe TEXT,direction TEXT,entry REAL,tp1 REAL,tp2 REAL,sl REAL,status TEXT,closed_at INTEGER,tp1_at INTEGER,sl_at INTEGER);`);return db;}
test('all report paths authenticate before ANY database/capability access',async()=>{
 for(const path of paths)for(const [h,status]of [[{},403],[{Origin:origin},401],[{...headers,'x-gsx-write-token':'wrong'},401],[{...headers,Origin:'https://invalid'},403]]){
  const response=await worker.fetch(request(path,h),env(trap),context);assert.equal(response.status,status);
 }
});
test('cold and warm missing tables return error WITHOUT schema repair',async()=>{
 const db=new DatabaseSync(':memory:'),calls=[];for(const path of paths)for(let i=0;i<2;i++){
  const response=await worker.fetch(request(path),env(sqliteBinding(db,{selectOnly:true,calls})),context);
  assert.equal(response.status,503);assert.equal((await response.json()).error,'measurement_schema_not_ready');
 }assert(calls.every(sql=>/^SELECT/i.test(sql)));assert.equal(db.prepare("SELECT count(*) n FROM sqlite_schema WHERE type='table'").get().n,0);db.close();
});
test('empty and incompatible schema, invalid inputs; GET remains SELECT-only',async()=>{
 const db=setup(),calls=[],binding=sqliteBinding(db,{selectOnly:true,calls});
 for(const path of paths){const response=await worker.fetch(request(path),env(binding),context);assert.equal(response.status,200);const body=await response.json();assert.equal(body.records.length,0);if(body.summary)assert.equal(body.summary.directionalAccuracy,null);}
 for(const query of ['limit=0','limit=101','cursor=bad!','bad=1','limit=1&limit=2']){
  const response=await worker.fetch(request(`/forward-validation?${query}`),env(binding),context);assert.equal(response.status,400);
 }
 db.exec('ALTER TABLE market_evidence_blocks RENAME TO hidden_blocks');
 const response=await worker.fetch(request(paths[0]),env(binding),context);assert.equal(response.status,503);
 assert(calls.every(sql=>/^SELECT/i.test(sql)));db.close();
});
test('read/reducer failures cannot invoke repair or expose raw database details',async()=>{
 const read={prepare(){throw new Error('private-db-details');}};const response=await worker.fetch(request(paths[0]),env(read),context);
 assert.equal(response.status,503);assert.equal((await response.json()).error,'measurement_read_unavailable');
 const db=setup();db.exec(`INSERT INTO production_signals VALUES('x','production',${now-1000},'5m','buy',100,110,120,90,'active',NULL,NULL,NULL)`);
 const report=await readForwardValidation(createForwardReadCapability(sqliteBinding(db,{selectOnly:true})),new URL(`https://local.invalid/forward-validation?limit=1`),paths[0],now);
 assert.equal(report.records[0].evidenceState,'ENTRY_EVIDENCE_NOT_CAPTURED');db.close();
});
test('dedicated reader structurally imports no writer, KV, DO, fetch or schema initializer',async()=>{
 const reader=await fs.readFile(new URL('./forward-validation-reader.js',import.meta.url),'utf8');
 assert(!/signal-evidence-store|measurement-collector|signal-measurement|waitUntil|setAlarm|\.run\(|\.exec\(|fetch\(/.test(reader));
 assert(!/\b(CREATE|ALTER|INSERT|UPDATE|DELETE)\b/.test(reader));
});
test('pagination stable at fixed as-of; invalid cursors and POST cannot access a writer',async()=>{
 const db=setup(),calls=[],binding=sqliteBinding(db,{selectOnly:true,calls});
 for(let i=0;i<3;i++)db.prepare('INSERT INTO production_signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(`signal:${i}`,'production',now-1000-i,'5m','buy',100,110,120,90,'active',null,null,null);
 const first=await worker.fetch(request('/forward-validation?limit=1'),env(binding),context);assert.equal(first.status,200);assert(first.headers.get('cache-control').includes('no-store'));const a=await first.json();
 assert.equal(a.summary.totalOfficial,3);assert.equal(a.summary.directional.UNRESOLVED,3);assert.equal(a.summary.directionalAccuracy,null);assert(a.pagination.hasMore);
 const second=await worker.fetch(request(`/forward-validation?limit=1&cursor=${a.pagination.nextCursor}`),env(binding),context);const b=await second.json();assert.equal(second.status,200);assert.notEqual(a.records[0].signalId,b.records[0].signalId);assert.equal(a.asOf,b.asOf);
 const denied=await worker.fetch(new Request('https://local.invalid/forward-validation',{method:'POST',headers}),env(trap),context);assert.equal(denied.status,405);
 const missing=await worker.fetch(request('/forward-validation'),{...env(trap),GSX_WRITE_TOKEN:undefined},context);assert.equal(missing.status,503);
 assert(calls.every(sql=>/^SELECT/i.test(sql)));db.close();
});

test('incompatible schema version, even empty, returns read-only not-ready error',async()=>{
 const db=setup();db.exec('UPDATE measurement_schema_meta SET version=999');
 for(const path of paths){const response=await worker.fetch(request(path),env(sqliteBinding(db,{selectOnly:true})),context);assert.equal(response.status,503);assert.equal((await response.json()).error,'measurement_schema_not_ready');}db.close();
});
test('successful Official evidence is visible when legacy performance row is absent',async()=>{
 const db=setup();
 db.prepare('INSERT INTO measurement_cohorts(cohort_id,schema_version,effective_at,payload_json,payload_digest,recorded_at) VALUES(?,1,?,?,?,?)').run('cohort',now,'{}','fixture',now);
 db.prepare('INSERT INTO decision_cycle_evidence(cycle_id,cohort_id,evaluated_at,payload_json,block_ids_json,payload_digest,recorded_at) VALUES(?,?,?,?,?,?,?)').run('cycle','cohort',now,'{}','[]','fixture',now);
 const entry={officialSignalId:'orphan',createdAt:now-1000,direction:'buy',engine:{levels:{entry:100,tp1:110,tp2:120,sl:90}},officialPersistence:{performance:'FAILED'}};
 db.prepare('INSERT INTO signal_decision_evidence(evaluation_id,official_signal_id,kind,cycle_id,cohort_id,timeframe,evaluated_at,measurement_only,decision_use,payload_json,payload_digest,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)').run('attempt','orphan','OFFICIAL','cycle','cohort','5m',now,1,0,JSON.stringify(entry),'fixture',now);
 const response=await worker.fetch(request('/forward-validation'),env(sqliteBinding(db,{selectOnly:true})),context);
 assert.equal(response.status,200);const body=await response.json();assert.equal(body.summary.totalOfficial,1);assert.equal(body.records[0].signalId,'orphan');assert.equal(body.records[0].entry,100);assert.equal(body.records[0].outcome.directionalOutcome,'UNRESOLVED');db.close();
});
test('corrupt projection/reducer failure is read-only and cannot repair itself',async()=>{
 const db=setup();db.prepare('INSERT INTO production_signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run('corrupt','production',now-1000,'5m','buy',100,110,120,90,'active',null,null,null);
 db.prepare('INSERT INTO signal_measurement_state(subject_id,cohort_id,state_version,payload_json,updated_at) VALUES(?,?,1,?,?)').run('corrupt',null,JSON.stringify({outcome:{directionalOutcome:'BAD',extendedOutcome:'UNRESOLVED'}}),now-500);
 const calls=[];const response=await worker.fetch(request('/forward-validation'),env(sqliteBinding(db,{selectOnly:true,calls})),context);
 assert.equal(response.status,503);assert.equal((await response.json()).error,'measurement_read_unavailable');assert(calls.every(sql=>/^SELECT/.test(sql)));assert.equal(JSON.parse(db.prepare('SELECT payload_json FROM signal_measurement_state').get().payload_json).outcome.directionalOutcome,'BAD');db.close();
});
