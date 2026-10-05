// Reproducible local SQLite payload/allocation and operation measurements only.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {database,cyclePackets,lifecycle,confirmation,now} from '../test-fixtures/b1-m2.mjs';
import {createOfflineMeasurementConsumer} from '../measurement-consumer.js';
import {rebuildLifecycleProjection} from '../measurement-lifecycle.js';
import {sqliteRecordPayloadBytes} from '../measurement-resource-budget.js';
import {buildMeasurementEnvelope} from '../measurement-envelope.js';
const output=process.argv[2];if(!output)throw new Error('output path required');
const tables=['measurement_ingress_receipts','measurement_lifecycle_facts','measurement_lifecycle_projection','measurement_ingress_recovery','measurement_decision_bindings','measurement_decision_conflicts'];
function payloads(db){return Object.fromEntries(tables.map(table=>[table,db.prepare(`SELECT * FROM ${table}`).all().map(row=>sqliteRecordPayloadBytes(Object.values(row)))]));}
function allocation(db){
 const objects=db.prepare("SELECT name,pgsize,payload,ncell FROM dbstat WHERE name IN (SELECT name FROM sqlite_master WHERE type='table' AND name IN ("+tables.map(()=>'?').join(',')+") OR type='index' AND tbl_name IN ("+tables.map(()=>'?').join(',')+"))").all(...tables,...tables);
 const rows=payloads(db),recordBytes=Object.values(rows).flat().reduce((a,b)=>a+b,0),physicalBytes=objects.reduce((n,r)=>n+r.pgsize,0);
 return {recordBytes,physicalBytes,physicalToRecordFactor:physicalBytes/recordBytes,
  indexes:Object.fromEntries([...new Set(objects.map(r=>r.name))].map(name=>[name,objects.filter(r=>r.name===name).reduce((n,r)=>n+r.pgsize,0)])),
  rows:Object.fromEntries(Object.entries(rows).map(([name,bytes])=>[name,{count:bytes.length,min:bytes.length?Math.min(...bytes):0,max:bytes.length?Math.max(...bytes):0,total:bytes.reduce((a,b)=>a+b,0)}]))};
}
async function operations(consumer,calls,wire){calls.length=0;const result=await consumer.ingest(wire);return {result,preparedReads:calls.filter(s=>/^SELECT/.test(s)).length,preparedWrites:calls.filter(s=>/^(INSERT|UPDATE|DELETE)/.test(s)).length};}
const report={basis:'LOCAL SQLite record payload before allocation; physical dbstat includes table/index pages; synthetic mature rowid sensitivity is not a full production workload',profiles:{},allocation:{}};
for(const heavy of [false,true]){
 const {db,binding,calls}=await database(),consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});
 const p=(await cyclePackets({heavy})).find(p=>p.envelope.kind==='OFFICIAL_CREATION'),id=p.envelope.semanticId;
 const measures={};measures.creation=await operations(consumer,calls,p.wire);assert(measures.creation.result.ok);
 measures.tp1=await operations(consumer,calls,(await lifecycle(id,'tp1')).wire);
 const tp2=await lifecycle(id,'tp2',{at:now+120000});measures.tp2=await operations(consumer,calls,tp2.wire);
 measures.duplicate=await operations(consumer,calls,tp2.wire);
 const e=tp2.envelope,changed=await buildMeasurementEnvelope({kind:e.kind,semanticId:e.semanticId,payload:{...e.payload,observedPrice:4100},occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt,preparedAt:e.clocks.preparedAt});
 measures.conflict=await operations(consumer,calls,changed.wire);
 calls.length=0;await rebuildLifecycleProjection(binding,id,{rebuiltAt:now+86400000});measures.rebuild={preparedReads:calls.filter(s=>/^SELECT/.test(s)).length,preparedWrites:calls.filter(s=>/^INSERT/.test(s)).length,factsDecoded:3};
 const link=await confirmation(id,'5m:1790865000000:buy');measures.confirmation=await operations(consumer,calls,link.wire);
 report.profiles[heavy?'heavy':'representative']={payloadBytes:payloads(db),operations:measures};db.close();
}
{
 const {db,binding,calls}=await database(),consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});
 const ps=await cyclePackets(),cycle=ps[0],creation=ps.find(p=>p.envelope.kind==='OFFICIAL_CREATION');
 const fill=db.prepare("INSERT INTO signal_measurement_state(subject_id,state_version,payload_json,updated_at) VALUES(?,1,'{}',?)");
 for(let i=0;i<4032;i++)fill.run('capacity:'+i,now);
 report.recovery={pending:await operations(consumer,calls,cycle.wire)};
 assert.equal(report.recovery.pending.result.processingStatus,'PENDING');
 report.recovery.pendingRowBytes=payloads(db).measurement_ingress_recovery;
 db.prepare("DELETE FROM signal_measurement_state WHERE subject_id LIKE 'capacity:%'").run();
 report.recovery.repaired=await operations(consumer,calls,cycle.wire);assert.equal(report.recovery.repaired.result.processingStatus,'CURRENT');
 await consumer.ingest(creation.wire);
 const e=creation.envelope,d=structuredClone(e.payload);d.decisionEvidence.engine.levels.entry+=1;
 const bad=await buildMeasurementEnvelope({kind:e.kind,semanticId:e.semanticId,payload:d,occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt,preparedAt:e.clocks.preparedAt});
 // Same-kind conflicts remain 1-read/0-write. A different-kind census conflict
 // prices the canonical-conflict incident and projection quarantine separately.
 const census=ps.find(p=>p.envelope.kind==='EVALUATION_CENSUS'&&p.envelope.semanticId===e.payload.evaluationId);
 const cp=structuredClone(census.envelope.payload);cp.decisionEvidence.engine.levels.entry+=1;
 const {censusSnapshot}=await import('../evidence-codec.js');cp.census=censusSnapshot(cp.decisionEvidence);
 const ce=census.envelope,conflict=await buildMeasurementEnvelope({kind:ce.kind,semanticId:ce.semanticId,payload:cp,occurredAt:ce.clocks.occurredAt,observedAt:ce.clocks.observedAt,preparedAt:ce.clocks.preparedAt});
 report.recovery.crossKindConflict=await operations(consumer,calls,conflict.wire);assert.equal(report.recovery.crossKindConflict.result.status,'INTEGRITY_CONFLICT');
 report.recovery.conflictRowBytes=payloads(db).measurement_decision_conflicts;db.close();
}
for(const [name,rowidBase]of [['fresh',0],['90day',181440],['365dayStress',735840]]){
 const {db,binding}=await database(),consumer=createOfflineMeasurementConsumer(binding,{clock:()=>now+86400000});
 // 512 independent lifecycle subjects, two immutable events each. Explicit
 // rowids price B-tree/index width at mature populations without claiming that
 // a fresh small database has exercised a year of ingestion/cleanup.
 for(let i=0;i<512;i++){
  const id=`30m:${now-i*1800000}:buy`;
  for(const [event,at]of [['tp1',now+60000],['tp2',now+120000]]){
   const packet=await lifecycle(id,event);const e=packet.envelope;
   const p=await buildMeasurementEnvelope({kind:e.kind,semanticId:e.semanticId,payload:{...e.payload,occurredAt:at,closedAt:event==='tp1'?null:at},occurredAt:at,observedAt:at+10,preparedAt:at+20});
   const result=await consumer.ingest(p.wire);assert(result.ok,JSON.stringify(result));
  }
 }
 // Integer rowids are transport/storage references, not B1 semantic identities.
 for(const table of tables){
  // Receipt/fact immutability deliberately forbids updates. Rebase only a local
  // allocation copy, preserving every column and rebuilding its real indexes.
  db.exec(`CREATE TEMP TABLE allocation_copy AS SELECT rowid+${rowidBase} AS mature_rowid,* FROM ${table}`);
  const triggers=db.prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?").all(table);
  for(const trigger of triggers)db.exec(`DROP TRIGGER ${trigger.name}`);
  db.exec('PRAGMA foreign_keys=OFF');db.exec(`DELETE FROM ${table}; INSERT INTO ${table}(rowid,${db.prepare(`PRAGMA table_info(${table})`).all().map(r=>r.name).join(',')}) SELECT * FROM allocation_copy; DROP TABLE allocation_copy;`);
  for(const trigger of triggers)db.exec(trigger.sql);db.exec('PRAGMA foreign_keys=ON');
 }
 assert.equal(db.prepare('PRAGMA foreign_key_check').all().length,0);
 // VACUUM can renumber text-PK table rowids; do not erase the width fixture.
 report.allocation[name]={...allocation(db),maximumRowids:Object.fromEntries(tables.map(t=>[t,db.prepare(`SELECT MAX(rowid) n FROM ${t}`).get().n])),
  databaseFreelistPages:db.prepare('PRAGMA freelist_count').get().freelist_count};db.close();
}
await fs.writeFile(output,JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));
