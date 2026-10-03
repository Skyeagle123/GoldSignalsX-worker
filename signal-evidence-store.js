// Writer-only module. No trading capability, no schema initialization.
import {canonicalSerialize,digestPayload} from './signal-evidence.js';
function referenceStatements(db,links){
 const statements=[];for(const link of links){if(!['CYCLE','OUTCOME'].includes(link.ownerType)||link.blockIds.length>80)throw new Error('measurement_reference_invalid');
  const ids=[...new Set(link.blockIds)];for(let start=0;start<ids.length;start+=33){const chunk=ids.slice(start,start+33);statements.push(db.prepare(`INSERT INTO market_evidence_references(owner_type,owner_id,block_id) VALUES ${chunk.map(()=>'(?,?,?)').join(',')} ON CONFLICT(owner_type,owner_id,block_id) DO NOTHING`).bind(...chunk.flatMap(id=>[link.ownerType,link.ownerId,id])));}
 }return statements;
}
function validateMeasurementPayload(type,payload){
 if(type!=='market'&&(payload?.measurementOnly!==true||payload?.decisionUse!==false))throw new Error('measurement_decision_use_forbidden');
}
const TABLES={
 cohort:{table:'measurement_cohorts',key:'cohort_id',columns:['cohort_id','schema_version','effective_at','payload_json','payload_digest','recorded_at']},
 market:{table:'market_evidence_blocks',key:'block_id',columns:['block_id','timeframe','from_at','to_at','payload_json','payload_digest','recorded_at']},
 cycle:{table:'decision_cycle_evidence',key:'cycle_id',columns:['cycle_id','cohort_id','evaluated_at','payload_json','block_ids_json','payload_digest','recorded_at']},
 decision:{table:'signal_decision_evidence',key:'evaluation_id',columns:['evaluation_id','candidate_key','official_signal_id','kind','cycle_id','cohort_id','timeframe','evaluated_at','measurement_only','decision_use','payload_json','payload_digest','recorded_at']},
 outcome:{table:'signal_outcome_evidence',key:'event_id',columns:['event_id','subject_id','event_type','occurred_at','available_at','payload_json','payload_digest','block_ids_json','recorded_at']}
};
export function measurementWriter(db,{maxPayloadBytes=65536,maxWrites=40}={}){
 let used=0,ready=false;
 async function schemaReady(){if(ready)return;const row=await db.prepare('SELECT version FROM measurement_schema_meta WHERE singleton=1').first();if(row?.version!==1)throw new Error('measurement_schema_not_ready');ready=true;}

 return Object.freeze({
  async immutable(type,values,payload){
   const config=TABLES[type];if(!config)throw new Error('measurement_record_invalid');validateMeasurementPayload(type,payload);
   if(used++>=maxWrites)throw new Error('measurement_write_budget_exhausted');
   await schemaReady();
   const text=canonicalSerialize(payload,maxPayloadBytes),digest=await digestPayload(text);
   if(type==='market'&&values.block_id!==`market:${digest}`)throw new Error('measurement_integrity_conflict');
   const prepared={...values,payload_json:text,payload_digest:digest};
   const columns=config.columns;
   const insert=db.prepare(`INSERT INTO ${config.table} (${columns.join(',')}) VALUES (${columns.map(()=>'?').join(',')}) ON CONFLICT(${config.key}) DO NOTHING`)
    .bind(...columns.map(c=>prepared[c]??null));
   const ids=type==='outcome'?JSON.parse(values.block_ids_json||'[]'):[];
   if(ids.length){if(typeof db.batch!=='function')throw new Error('measurement_atomic_batch_unavailable');
    await db.batch([insert,...referenceStatements(db,[{ownerType:'OUTCOME',ownerId:values.event_id,blockIds:ids}])]);
   }else await insert.run();
   const stored=await db.prepare(`SELECT payload_digest FROM ${config.table} WHERE ${config.key}=?`).bind(values[config.key]).first();
   if(stored?.payload_digest!==digest)throw new Error('measurement_integrity_conflict');
   return {digest,bytes:new TextEncoder().encode(text).length};
  },
  async immutableBatch(records,{links=[]}={}){
   if(!Array.isArray(records)||records.length>80||used+records.length>maxWrites)throw new Error('measurement_write_budget_exhausted');
   await schemaReady();
   const prepared=[],identities=new Map();
   for(const {type,values,payload} of records){const config=TABLES[type];if(!config)throw new Error('measurement_record_invalid');validateMeasurementPayload(type,payload);
    const text=canonicalSerialize(payload,maxPayloadBytes),digest=await digestPayload(text),key=values[config.key];
    if(type==='market'&&key!==`market:${digest}`)throw new Error('measurement_integrity_conflict');
    const identity=`${type}:${key}`;if(identities.has(identity)){if(identities.get(identity)!==digest)throw new Error('measurement_integrity_conflict');continue;}
    identities.set(identity,digest);prepared.push({type,config,values:{...values,payload_json:text,payload_digest:digest}});
   }
   if((links.length||prepared.some(r=>r.type==='decision'))&&typeof db.batch!=='function')throw new Error('measurement_atomic_batch_unavailable');
   used+=prepared.length;const statements=[],verify=[];
   for(const type of Object.keys(TABLES)){
    const rows=prepared.filter(x=>x.type===type);if(!rows.length)continue;const config=TABLES[type];
    const stored=(await db.prepare(`SELECT ${config.key},payload_digest FROM ${config.table} WHERE ${config.key} IN (${rows.map(()=>'?').join(',')})`).bind(...rows.map(x=>x.values[config.key])).all()).results||[];
    const existing=new Map(stored.map(x=>[x[config.key],x.payload_digest]));
    for(const row of rows)if(existing.has(row.values[config.key])&&existing.get(row.values[config.key])!==row.values.payload_digest)throw new Error('measurement_integrity_conflict');
    const insert=rows.filter(x=>!existing.has(x.values[config.key])),chunkSize=Math.floor(100/config.columns.length);
    for(let start=0;start<insert.length;start+=chunkSize){const chunk=insert.slice(start,start+chunkSize);
     statements.push(db.prepare(`INSERT INTO ${config.table} (${config.columns.join(',')}) VALUES ${chunk.map(()=>`(${config.columns.map(()=>'?').join(',')})`).join(',')} ON CONFLICT(${config.key}) DO NOTHING`)
      .bind(...chunk.flatMap(x=>config.columns.map(c=>x.values[c]??null))));
    }verify.push({config,rows});
   }
   // Queue only levels actually computed by the engine. The mutable bounded cursor
   // is inserted atomically with immutable entry evidence; retry never resets it.
   for(const record of prepared.filter(x=>x.type==='decision')){const payload=records.find(x=>x.type==='decision'&&x.values.evaluation_id===record.values.evaluation_id).payload;
    if(payload.levelsStatus!=='COMPUTED_BY_ENGINE')continue;const v=record.values,subjectId=v.official_signal_id||v.evaluation_id,createdAt=payload.createdAt??v.evaluated_at;
    const state=canonicalSerialize({subjectId,processedThrough:createdAt,covered:[],gaps:[],barriers:{},global:{mfe:null,mae:null},queued:true});
    statements.push(db.prepare('INSERT INTO signal_measurement_state(subject_id,cohort_id,state_version,payload_json,updated_at,evaluation_id,kind,next_observe_at,observation_end_at) VALUES(?,?,1,?,?,?,?,?,?) ON CONFLICT(subject_id) DO NOTHING')
      .bind(subjectId,v.cohort_id,state,createdAt,v.evaluation_id,v.kind,createdAt,v.kind==='CANDIDATE'?createdAt+3600000:null));
   }
   statements.push(...referenceStatements(db,links));
   if(statements.length){if(typeof db.batch==='function')await db.batch(statements);else for(const statement of statements)await statement.run();}
   for(const {config,rows} of verify){const stored=(await db.prepare(`SELECT ${config.key},payload_digest FROM ${config.table} WHERE ${config.key} IN (${rows.map(()=>'?').join(',')})`).bind(...rows.map(x=>x.values[config.key])).all()).results||[];
    const actual=new Map(stored.map(x=>[x[config.key],x.payload_digest]));
    if(rows.some(row=>actual.get(row.values[config.key])!==row.values.payload_digest))throw new Error('measurement_integrity_conflict');
   }
   return {records:prepared.length,insertStatements:statements.length};
  },
  async linkMarket(ownerType,ownerId,blockIds){
   if(!['CYCLE','OUTCOME'].includes(ownerType)||!Array.isArray(blockIds)||blockIds.length>80)throw new Error('measurement_reference_invalid');
   const unique=[...new Set(blockIds)];for(let start=0;start<unique.length;start+=33){
    if(used++>=maxWrites)throw new Error('measurement_write_budget_exhausted');
    const ids=unique.slice(start,start+33);
    await db.prepare(`INSERT INTO market_evidence_references(owner_type,owner_id,block_id) VALUES ${ids.map(()=>'(?,?,?)').join(',')} ON CONFLICT(owner_type,owner_id,block_id) DO NOTHING`)
     .bind(...ids.flatMap(id=>[ownerType,ownerId,id])).run();
   }
  },
  async state(subjectId,cohortId,payload,at){
   if(used++>=maxWrites)throw new Error('measurement_write_budget_exhausted');
   await schemaReady();
   const text=canonicalSerialize(payload,maxPayloadBytes);
   await db.prepare('INSERT INTO signal_measurement_state(subject_id,cohort_id,state_version,payload_json,updated_at,next_observe_at) VALUES(?,?,1,?,?,?) ON CONFLICT(subject_id) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at,next_observe_at=excluded.next_observe_at WHERE excluded.updated_at>=signal_measurement_state.updated_at')
    .bind(subjectId,cohortId,text,at,subjectId==='b1:collector'||payload.measurementStatus?.startsWith('HORIZON_ATTEMPT_COMPLETE')||(payload.outcome?.terminalLifecycleOutcome&&payload.outcome.terminalLifecycleOutcome!=='ACTIVE')?null:payload.nextObservationAt??at+300000).run();
  },
  async loadState(subjectId){return db.prepare('SELECT payload_json FROM signal_measurement_state WHERE subject_id=?').bind(subjectId).first();}
 });
}
