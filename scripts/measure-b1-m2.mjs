// Reproducible local SQLite payload/allocation and operation measurements only.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {database,cyclePackets,lifecycle,confirmation,now} from '../test-fixtures/b1-m2.mjs';
import {createOfflineMeasurementConsumer} from '../measurement-consumer.js';
import {rebuildLifecycleProjection} from '../measurement-lifecycle.js';
import {sqliteRecordPayloadBytes} from '../measurement-resource-budget.js';
import {buildMeasurementEnvelope} from '../measurement-envelope.js';
const output=process.argv[2];if(!output)throw new Error('output path required');
const tables=['measurement_ingress_receipts','measurement_lifecycle_facts','measurement_lifecycle_projection'];
function payloads(db){return Object.fromEntries(tables.map(table=>[table,db.prepare(`SELECT * FROM ${table}`).all().map(row=>sqliteRecordPayloadBytes(Object.values(row)))]));}
function allocation(db){
 const objects=db.prepare("SELECT name,pgsize,payload,ncell FROM dbstat WHERE name LIKE 'measurement_ingress_%' OR name LIKE 'measurement_lifecycle_%' OR name LIKE 'sqlite_autoindex_measurement_ingress_%' OR name LIKE 'sqlite_autoindex_measurement_lifecycle_%'").all();
 const rows=payloads(db),recordBytes=Object.values(rows).flat().reduce((a,b)=>a+b,0),physicalBytes=objects.reduce((n,r)=>n+r.pgsize,0);
 return {recordBytes,physicalBytes,physicalToRecordFactor:physicalBytes/recordBytes,
  indexes:Object.fromEntries([...new Set(objects.map(r=>r.name))].map(name=>[name,objects.filter(r=>r.name===name).reduce((n,r)=>n+r.pgsize,0)])),
  rows:Object.fromEntries(Object.entries(rows).map(([name,bytes])=>[name,{count:bytes.length,min:Math.min(...bytes),max:Math.max(...bytes),total:bytes.reduce((a,b)=>a+b,0)}]))};
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
