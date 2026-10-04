// Writer-only module. No trading capability or request-time schema initialization.
import {canonicalSerialize,digestPayload,parseEvidence} from './signal-evidence.js';
import {encodeEvidence,normalizeDefinitions,censusSnapshot,compactMeasurement,digestBytes,digestHex,finalResultColumns,encodeOutcomeEvidence} from './evidence-codec.js';
const TABLES={
 cohort:{table:'measurement_cohorts',key:'cohort_id',columns:['cohort_id','schema_version','effective_at','payload_json','payload_digest','recorded_at']},
 market:{table:'market_evidence_blocks',key:'block_id',columns:['block_id','timeframe','from_at','to_at','payload_json','payload_digest','recorded_at']},
 cycle:{table:'decision_cycle_evidence',key:'cycle_id',columns:['cycle_id','cohort_id','evaluated_at','payload_json','block_ids_json','payload_digest','recorded_at']},
 decision:{table:'signal_decision_evidence',key:'evaluation_id',columns:['evaluation_id','candidate_key','official_signal_id','kind','cycle_id','cohort_id','timeframe','evaluated_at','measurement_only','decision_use','payload_json','payload_digest','recorded_at']},
 outcome:{table:'measurement_outcome_records',readTable:'signal_outcome_evidence',key:'event_id',columns:['subject_ref','event_suffix','full_event_id','event_kind','event_label','occurred_at','available_offset','payload_digest','payload_blob','uncompressed_length','recorded_offset','retention_bucket','persisted_offset']},
 definition:{table:'measurement_definitions',key:'definition_id',columns:['definition_id','payload_json','payload_digest','recorded_at']},
 rich:{table:'measurement_rich_evidence',key:'evidence_id',columns:['evidence_id','owner_type','owner_id','payload_json','payload_digest','payload_blob','codec','uncompressed_length','recorded_at','retain_until']},
 final:{table:'measurement_final_results',key:'subject_id',columns:['subject_id','directional','extended','terminal','coverage','ordering','horizon','occurred_from','occurred_to','mfe','mae','mfe_r','mae_r','tp1_min','tp1_max','sl_min','sl_max','numeric_flags','evidence_as_of','reducer_version','payload_digest','recorded_at','retain_until']}
};
const DAY=86400000;
function refs(db,links){const result=[];for(const link of links){if(!['CYCLE','OUTCOME'].includes(link.ownerType)||link.blockIds.length>80)throw new Error('measurement_reference_invalid');
 const owner=link.ownerType==='CYCLE'?['decision_cycle_evidence','cycle_id',1]:['signal_outcome_evidence','event_id',2];
 const ids=[...new Set(link.blockIds)];for(let i=0;i<ids.length;i+=98){const chunk=ids.slice(i,i+98);if(chunk.length)result.push(db.prepare(`INSERT INTO measurement_market_links(owner_kind,owner_ref,block_ref) SELECT ${owner[2]},o.rowid,b.rowid FROM ${owner[0]} o JOIN market_evidence_blocks b WHERE o.${owner[1]}=? AND b.block_id IN (${chunk.map(()=>'?').join(',')}) ON CONFLICT(owner_kind,owner_ref,block_ref) DO NOTHING`).bind(link.ownerId,...chunk));}}return result;}
function pins(db,officialId,ids){const result=[];for(let i=0;i<ids.length;i+=50){const chunk=[...new Set(ids)].slice(i,i+50);if(chunk.length)result.push(db.prepare(`INSERT INTO measurement_official_pins(official_signal_id,block_id) VALUES ${chunk.map(()=>'(?,?)').join(',')} ON CONFLICT(official_signal_id,block_id) DO NOTHING`).bind(...chunk.flatMap(id=>[officialId,id])));}return result;}
export function measurementWriter(db,{maxPayloadBytes=65536,maxWrites=80}={}){
 let used=0,ready=false;
 async function schemaReady(){if(ready)return;const row=await db.prepare('SELECT version FROM measurement_schema_meta WHERE singleton=1').first();if(row?.version!==3)throw new Error('measurement_schema_not_ready');ready=true;}
 async function prepareRecord({type,values,payload},officialSubjects){
  const config=TABLES[type];if(!config)throw new Error('measurement_record_invalid');
  if(!['market','definition','rich','final'].includes(type)&&(payload?.measurementOnly!==true||payload?.decisionUse!==false))throw new Error('measurement_decision_use_forbidden');
  const logical=canonicalSerialize(payload,maxPayloadBytes),digest=await digestPayload(logical),key=values[config.key];
  if(type==='market'&&key!==`market:${digest}`)throw new Error('measurement_integrity_conflict');
  let scalar=payload,richPayload=null;const extras=[];
  if(type==='decision'){
   scalar=censusSnapshot(payload);if(values.kind==='OFFICIAL'&&values.candidate_key===values.official_signal_id)values={...values,candidate_key:null};const normalized=await normalizeDefinitions(payload);richPayload=normalized.payload;
   for(const d of normalized.definitions)extras.push({type:'definition',values:{definition_id:d.id,recorded_at:values.recorded_at},payload:d.payload});
   // Shared values are copied into one immutable cycle context. Distinct clocks,
   // quotes and attempt operands remain in the per-attempt payload.
   richPayload={...richPayload,sharedContextRef:values.cycle_id};delete richPayload.versions;delete richPayload.admissionContext;
  }else if(type==='market'){scalar={blockId:key,evidenceRef:`market:${key}`,timeframe:values.timeframe,from:values.from_at,to:values.to_at};richPayload=payload;
  }else if(type==='cycle'){scalar={cycleStartedAt:payload.cycleStartedAt};values={...values,block_ids_json:'[]'};richPayload=payload;
  }else if(type==='outcome'){const envelope=await encodeOutcomeEvidence(payload,{maxBytes:maxPayloadBytes});const prefix=payload.subjectId+':',kinds=['TP1','TP2','SL','WINDOW_FINALIZED','FINAL_MEASUREMENT','COVERAGE_CHECKPOINT','EXPIRED','TP2_REACHED','CLOSED_OTHER'],kind=kinds.indexOf(payload.eventType);scalar={};values={...values,event_suffix:payload.eventId.startsWith(prefix)?payload.eventId.slice(prefix.length):null,full_event_id:payload.eventId.startsWith(prefix)?null:payload.eventId,event_kind:kind<0?255:kind,event_label:kind<0?payload.eventType:null,block_ids_json:'[]',payload_blob:envelope.data,uncompressed_length:envelope.length};}
  const out=[{type,config,values:{...values,...(type==='final'?finalResultColumns(payload):{payload_json:canonicalSerialize(scalar,maxPayloadBytes)}),payload_digest:digestBytes(digest)},original:payload}];
  if(richPayload){const envelope=await encodeEvidence(richPayload,{maxBytes:maxPayloadBytes});
   const official=type==='decision'?values.kind==='OFFICIAL':type==='outcome'?officialSubjects.has(values.subject_id):type==='cycle'?officialSubjects.size>0:false;
   const retention=type==='market'?30:type==='cycle'?365:90;
   out.push({type:'rich',config:TABLES.rich,values:{evidence_id:`${type}:${key}`,owner_type:type,owner_id:key,payload_json:'{}',payload_digest:digestBytes(envelope.digest),payload_blob:envelope.data,codec:envelope.codec,uncompressed_length:envelope.length,recorded_at:values.recorded_at,retain_until:official?null:values.recorded_at+retention*DAY}});
  }
  for(const extra of extras)out.push(...await prepareRecord(extra,officialSubjects));return out;
 }
 async function commit(records,{links=[],officialPins=[],extraStatements=[]}={}){
  await schemaReady();const subjects=new Set(officialPins.map(x=>x.officialId));for(const r of records)if(r.type==='decision'&&r.values.kind==='OFFICIAL')subjects.add(r.values.official_signal_id);
  for(const r of records)if(r.type==='outcome'){const official=await db.prepare("SELECT official_signal_id FROM signal_decision_evidence WHERE official_signal_id=? AND kind='OFFICIAL'").bind(r.values.subject_id).first();if(official)subjects.add(r.values.subject_id);}
  const all=[];for(const record of records)all.push(...await prepareRecord(record,subjects));
  const unique=new Map();for(const r of all){const identity=`${r.type}:${r.values[r.config.key]}`;if(unique.has(identity)&&digestHex(unique.get(identity).values.payload_digest)!==digestHex(r.values.payload_digest))throw new Error('measurement_integrity_conflict');unique.set(identity,r);}
  const prepared=[...unique.values()];if(used+prepared.length>maxWrites||prepared.length>160)throw new Error('measurement_write_budget_exhausted');used+=prepared.length;
  const statements=[],verify=[];
  for(const [type,config]of Object.entries(TABLES)){const rows=prepared.filter(r=>r.type===type);if(!rows.length)continue;
   const stored=(await db.prepare(`SELECT ${config.key},payload_digest FROM ${config.readTable||config.table} WHERE ${config.key} IN (${rows.map(()=>'?').join(',')})`).bind(...rows.map(r=>r.values[config.key])).all()).results||[];
   const existing=new Map(stored.map(r=>[r[config.key],digestHex(r.payload_digest)]));for(const r of rows)if(existing.has(r.values[config.key])&&existing.get(r.values[config.key])!==digestHex(r.values.payload_digest))throw new Error('measurement_integrity_conflict');
   const insert=rows.filter(r=>!existing.has(r.values[config.key])),n=Math.floor(100/config.columns.length);
   if(type==='outcome'){
    for(const r of insert){const v=r.values;statements.push(db.prepare('INSERT INTO measurement_subjects(subject_id,recorded_at) VALUES(?,?) ON CONFLICT(subject_id) DO NOTHING').bind(v.subject_id,v.recorded_at));
     if(!Number.isSafeInteger(v.available_at)||!Number.isSafeInteger(v.recorded_at))throw new Error('measurement_time_invalid');const columns=config.columns.filter(c=>c!=='subject_ref'),expr=columns.map(c=>c==='persisted_offset'?"CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)-s.persisted_at":['available_offset','recorded_offset'].includes(c)?'?-s.recorded_at':'?');statements.push(db.prepare(`INSERT INTO ${config.table}(subject_ref,${columns.join(',')}) SELECT s.subject_ref,${expr.join(',')} FROM measurement_subjects s WHERE s.subject_id=? ON CONFLICT DO NOTHING`).bind(...columns.filter(c=>c!=='persisted_offset').map(c=>c==='available_offset'?v.available_at:c==='recorded_offset'?v.recorded_at:c==='retention_bucket'?Math.floor(v.recorded_at/DAY):v[c]??null),v.subject_id));}
   }else for(let i=0;i<insert.length;i+=n){const chunk=insert.slice(i,i+n);statements.push(db.prepare(`INSERT INTO ${config.table} (${config.columns.join(',')}) VALUES ${chunk.map(()=>`(${config.columns.map(()=>'?').join(',')})`).join(',')} ON CONFLICT(${config.key}) DO NOTHING`).bind(...chunk.flatMap(r=>config.columns.map(c=>r.values[c]??null))));}
   verify.push({config,rows});
  }
  for(const r of prepared.filter(x=>x.type==='decision')){const p=r.original,v=r.values;if(p.levelsStatus!=='COMPUTED_BY_ENGINE')continue;const subject=v.official_signal_id||v.evaluation_id,at=p.createdAt??v.evaluated_at;
   if(v.kind==='OFFICIAL'){const l=p.engine.levels;statements.push(db.prepare('INSERT INTO measurement_official_levels(official_signal_id,entry,tp1,tp2,sl) VALUES(?,?,?,?,?) ON CONFLICT(official_signal_id) DO NOTHING').bind(v.official_signal_id,l.entry,l.tp1,l.tp2,l.sl));}
   statements.push(db.prepare('INSERT INTO signal_measurement_state(subject_id,cohort_id,state_version,payload_json,updated_at,evaluation_id,kind,next_observe_at,observation_end_at) SELECT ?,?,1,?,?,?,?,?,? WHERE NOT EXISTS(SELECT 1 FROM measurement_final_results WHERE subject_id=?) ON CONFLICT(subject_id) DO NOTHING').bind(subject,v.cohort_id,canonicalSerialize({subjectId:subject,processedThrough:at,covered:[],gaps:[],barriers:{},global:{mfe:null,mae:null},queued:true}),at,v.evaluation_id,v.kind,at,v.kind==='CANDIDATE'?at+3600000:null,subject));
  }
  statements.push(...refs(db,links));for(const pin of officialPins)statements.push(...pins(db,pin.officialId,pin.blockIds));statements.push(...extraStatements);
  if(statements.length){if(typeof db.batch==='function')await db.batch(statements);else if(statements.length===1)await statements[0].run();else throw new Error('measurement_atomic_batch_unavailable');}
  for(const {config,rows}of verify){const actual=(await db.prepare(`SELECT ${config.key},payload_digest FROM ${config.readTable||config.table} WHERE ${config.key} IN (${rows.map(()=>'?').join(',')})`).bind(...rows.map(r=>r.values[config.key])).all()).results||[];const map=new Map(actual.map(r=>[r[config.key],digestHex(r.payload_digest)]));if(rows.some(r=>map.get(r.values[config.key])!==digestHex(r.values.payload_digest)))throw new Error('measurement_integrity_conflict');}
  return {records:prepared.length,insertStatements:statements.length};
 }
 return Object.freeze({
  async immutable(type,values,payload){const ids=type==='outcome'?JSON.parse(values.block_ids_json||'[]'):[];return commit([{type,values,payload}],{links:ids.length?[{ownerType:'OUTCOME',ownerId:values.event_id,blockIds:ids}]:[]});},
  immutableBatch:commit,
  async linkMarket(ownerType,ownerId,blockIds){await schemaReady();if(used++>=maxWrites)throw new Error('measurement_write_budget_exhausted');for(const statement of refs(db,[{ownerType,ownerId,blockIds}]))await statement.run();},
  async pinOfficial(officialId,blockIds){await schemaReady();if(used++>=maxWrites)throw new Error('measurement_write_budget_exhausted');for(const statement of pins(db,officialId,blockIds))await statement.run();},
  async finalize(subjectId,kind,packagePayload,at,{blockIds=[]}={}){
   // Immutable final package + compact result + deletion are one transaction.
   // Failure rolls back all three and retains the accumulator for recovery.
   const eventId=`${subjectId}:final`,windowReferences=[],records=[];
   for(const [name,window]of Object.entries(packagePayload.quality?.windows||{})){
    if(window.state!=='FINALIZED')continue;const id=`${subjectId}:window:${name}`;windowReferences.push(id);
    const existing=await db.prepare('SELECT event_id FROM signal_outcome_evidence WHERE event_id=?').bind(id).first();
    if(!existing)records.push({type:'outcome',values:{event_id:id,subject_id:subjectId,event_type:'WINDOW_FINALIZED',available_at:at,recorded_at:at,block_ids_json:'[]'},payload:{subjectId,eventId:id,evidenceType:'WINDOW_FINALIZED',eventType:'WINDOW_FINALIZED',availableAt:at,observedAt:at,occurredFrom:packagePayload.occurredFrom,occurredTo:window.measuredTo??packagePayload.occurredTo,window,measurementOnly:true,decisionUse:false}});
   }
   const p={subjectId,eventId,evidenceType:'FINAL_MEASUREMENT',eventType:'FINAL_MEASUREMENT',availableAt:packagePayload.availableAt,
    observedAt:packagePayload.observedAt,occurredFrom:packagePayload.occurredFrom,occurredTo:packagePayload.occurredTo,
    outcome:packagePayload.outcome,global:packagePayload.global,covered:packagePayload.covered,gaps:packagePayload.gaps,
    measurementHorizonAt:packagePayload.measurementHorizonAt,windowReferences,barrierReferences:Object.values(packagePayload.barriers||{}).map(e=>e.eventId),
    measurementOnly:true,decisionUse:false};
   const lastWindow=Object.entries(packagePayload.quality?.windows||{}).filter(([,w])=>w.state==='FINALIZED').at(-1);
   if(lastWindow){const [name,w]=lastWindow,reference=`${subjectId}:window:${name}`;
    const globalKeys=['mfe','mae','mfeR','maeR','mfeWitness','maeWitness','coverage'];
    if(packagePayload.global&&Object.keys(packagePayload.global).every(k=>globalKeys.includes(k))&&globalKeys.every(k=>canonicalSerialize(packagePayload.global[k])===canonicalSerialize(w[k]))){p.globalReference=reference;delete p.global;}
    if(canonicalSerialize(p.covered)===canonicalSerialize(w.covered)&&canonicalSerialize(p.gaps)===canonicalSerialize(w.gaps)){p.coverageReference=reference;delete p.covered;delete p.gaps;}
   }
   records.push({type:'outcome',values:{event_id:eventId,subject_id:subjectId,event_type:'FINAL_MEASUREMENT',available_at:p.availableAt,recorded_at:at,block_ids_json:'[]'},payload:p},
    {type:'final',values:{subject_id:subjectId,recorded_at:at,retain_until:kind==='OFFICIAL'?null:at+365*DAY},payload:{...p,global:packagePayload.global}});
   return commit(records,{links:blockIds.length?[{ownerType:'OUTCOME',ownerId:eventId,blockIds}]:[],officialPins:kind==='OFFICIAL'?[{officialId:subjectId,blockIds}]:[],
    extraStatements:[db.prepare('DELETE FROM signal_measurement_state WHERE subject_id=? AND EXISTS(SELECT 1 FROM measurement_final_results WHERE subject_id=?)').bind(subjectId,subjectId)]});
  },
  async state(subjectId,cohortId,payload,at){
   if(used++>=maxWrites)throw new Error('measurement_write_budget_exhausted');await schemaReady();
   const e=await encodeEvidence(payload,{maxBytes:maxPayloadBytes});
   const projection={outcome:payload.outcome??null,global:payload.global?{mfe:payload.global.mfe,mae:payload.global.mae,mfeR:payload.global.mfeR,maeR:payload.global.maeR,coverage:payload.global.coverage}:null,availableAt:payload.outcome?.evidenceAsOf??at};
   await db.prepare('INSERT INTO signal_measurement_state(subject_id,cohort_id,state_version,payload_json,updated_at,next_observe_at,payload_blob,codec,uncompressed_length,payload_digest) VALUES(?,?,1,?,?,?,?,?,?,?) ON CONFLICT(subject_id) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at,next_observe_at=excluded.next_observe_at,payload_blob=excluded.payload_blob,codec=excluded.codec,uncompressed_length=excluded.uncompressed_length,payload_digest=excluded.payload_digest WHERE excluded.updated_at>=signal_measurement_state.updated_at').bind(subjectId,cohortId,canonicalSerialize(projection,maxPayloadBytes),at,subjectId==='b1:collector'?null:payload.nextObservationAt??at+300000,e.data,e.codec,e.length,digestBytes(e.digest)).run();
  },
  async loadState(subjectId){return db.prepare('SELECT * FROM signal_measurement_state WHERE subject_id=?').bind(subjectId).first();}
 });
}
