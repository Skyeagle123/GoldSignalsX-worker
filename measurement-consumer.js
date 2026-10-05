// Offline M2 only. Explicit Measurement DB capability; no Worker/env/transport.
import {ENVELOPE_VERSION,PRODUCER_NAMESPACE,ENVELOPE_KINDS,PRODUCER_BOUNDS,validateDataEvidence} from './measurement-envelope.js';
import {canonicalSerialize,digestPayload,parseEvidence} from './signal-evidence.js';
import {encodeEvidence,digestBytes,censusSnapshot} from './evidence-codec.js';
import {measurementWriter} from './signal-evidence-store.js';
import {rebuildLifecycleProjection} from './measurement-lifecycle.js';

const textEncoder=new TextEncoder();
const fail=reason=>{throw new Error(reason);};
const identity=v=>typeof v==='string'&&v.length>0&&textEncoder.encode(v).length<=1024;
const unknown=v=>v===null||v===undefined||v?.$unavailable==='undefined';
function clock(v,ceiling){if(unknown(v))return null;if(!Number.isSafeInteger(v)||v<0||v>ceiling)fail('measurement_ingress_clock_invalid');return v;}
function exactKeys(v,keys){if(!v||Array.isArray(v)||typeof v!=='object'||Object.keys(v).length!==keys.length||keys.some(k=>!Object.hasOwn(v,k)))fail('measurement_envelope_shape_invalid');}
function flag(p){if(p?.measurementOnly!==true||p?.decisionUse!==false)fail('measurement_decision_use_forbidden');}
function decision(d,receivedAt){
 flag(d);if(!identity(d.evaluationId)||!identity(d.cycleId)||!['CANDIDATE','OFFICIAL'].includes(d.kind))fail('measurement_decision_identity_invalid');
 for(const k of ['createdAt','evaluatedAt','capturedAt'])clock(d[k],receivedAt);
 if(d.kind==='OFFICIAL'&&!identity(d.officialSignalId))fail('measurement_decision_identity_invalid');
 for(const level of ['entry','tp1','tp2','sl']){const v=d.engine?.levels?.[level];if(!unknown(v)&&!Number.isFinite(v))fail('measurement_levels_invalid');}
}
function decisionValues(v,d){
 if(v?.evaluation_id!==d.evaluationId||v.cycle_id!==d.cycleId||v.kind!==d.kind||v.official_signal_id!==d.officialSignalId||
  v.timeframe!==d.timeframe||v.measurement_only!==1||v.decision_use!==0)fail('measurement_semantic_identity_invalid');
}
function validatePayload(e,receivedAt){
 const p=e.payload;
 if(e.kind==='DECISION_CYCLE'){
  exactKeys(p,['records','officialPins','links']);if(!Array.isArray(p.records)||p.records.length>160||!Array.isArray(p.officialPins)||!Array.isArray(p.links))fail('measurement_cycle_invalid');
  const cycles=p.records.filter(r=>r.type==='cycle');if(cycles.length!==1||cycles[0].values.cycle_id!==e.semanticId||cycles[0].payload.cycleId!==e.semanticId)fail('measurement_semantic_identity_invalid');
  for(const r of p.records){
   if(!['cohort','market','cycle','decision'].includes(r.type)||!r.values||!r.payload)fail('measurement_cycle_record_invalid');
   clock(r.values.recorded_at,receivedAt);
   if(r.type==='decision'){decision(r.payload,receivedAt);decisionValues(r.values,r.payload);if(r.payload.cycleId!==e.semanticId)fail('measurement_semantic_identity_invalid');}
  }
 }else if(e.kind==='EVALUATION_CENSUS'){
  decision(p.decisionEvidence,receivedAt);decisionValues(p.values,p.decisionEvidence);if(e.semanticId!==p.decisionEvidence.evaluationId)fail('measurement_semantic_identity_invalid');
  if(canonicalSerialize(p.census)!==canonicalSerialize(censusSnapshot(p.decisionEvidence)))fail('measurement_census_conflict');
 }else{
  if(e.kind==='OFFICIAL_CREATION'){
   decision(p.decisionEvidence,receivedAt);
   if(e.semanticId!==p.officialSignalId||p.officialSignalId!==p.decisionEvidence.officialSignalId||p.evaluationId!==p.decisionEvidence.evaluationId||p.decisionEvidence.kind!=='OFFICIAL'||p.officialPersisted!==p.decisionEvidence.exposure?.officialPersisted||canonicalSerialize(p.performancePersistence)!==canonicalSerialize(p.decisionEvidence.officialPersistence))fail('measurement_semantic_identity_invalid');
  }else if(e.kind==='LIFECYCLE_FACT'){
   flag(p);
   if(!identity(p.signalId)||!['tp1','tp2','sl','expired'].includes(p.event)||e.semanticId!==`production:${p.signalId}:${p.event}`)fail('measurement_semantic_identity_invalid');
   for(const k of ['createdAt','occurredAt','closedAt'])clock(p[k],receivedAt);
   if(canonicalSerialize(p.occurredAt)!==canonicalSerialize(e.clocks.occurredAt))fail('measurement_lifecycle_clock_conflict');
   const expected={tp1:'tp1',tp2:'tp2',sl:'stopped',expired:'expired'}[p.event];
   if(!unknown(p.status)&&p.status!==expected)fail('measurement_lifecycle_status_conflict');
   if(!unknown(p.closedAt)&&p.event==='tp1')fail('measurement_lifecycle_status_conflict');
   if(p.performancePersistence?.signal?.signal_id!=null&&p.performancePersistence.signal.signal_id!==p.signalId)fail('measurement_semantic_identity_invalid');
   clock(p.performancePersistence?.signal?.updated_at,receivedAt);clock(p.trigger?.at,receivedAt);
   for(const v of [p.observedPrice,p.level,p.trigger?.price])if(!unknown(v)&&!Number.isFinite(v))fail('measurement_lifecycle_value_invalid');
   if(!unknown(p.createdAt)&&!unknown(p.occurredAt)&&p.createdAt>p.occurredAt||!unknown(p.createdAt)&&!unknown(p.closedAt)&&p.createdAt>p.closedAt)fail('measurement_lifecycle_clock_conflict');
  }else if(e.kind==='CONFIRMATION_LINK'){
   flag(p);
   if(!identity(p.primarySignalId)||!identity(p.confirmationSignalId)||p.primarySignalId===p.confirmationSignalId||e.semanticId!==`${p.primarySignalId}:confirmation:${p.confirmationSignalId}`)fail('measurement_semantic_identity_invalid');
   if(!unknown(p.linkPersistence)&&!['SUCCEEDED','FAILED'].includes(p.linkPersistence))fail('measurement_confirmation_invalid');
  }else if(e.kind==='CAPTURE_GAP'){flag(p);if(p.captureGap!==true)fail('measurement_capture_gap_invalid');}
 }
}

export async function validateMeasurementWire(wire,{receivedAt}={}){
 if(!Number.isSafeInteger(receivedAt)||receivedAt<0)fail('measurement_ingress_clock_invalid');
 // Wire strings only: no caller-controlled object methods or descriptors.
 if(typeof wire!=='string'||wire.length>PRODUCER_BOUNDS.maxWireBytes||textEncoder.encode(wire).length>PRODUCER_BOUNDS.maxWireBytes)fail('measurement_ingress_wire_bound');
 const e=JSON.parse(wire);validateDataEvidence(e);
 exactKeys(e,['version','producerNamespace','kind','semanticId','semanticKey','eventId','payloadDigest','measurementOnly','decisionUse','clocks','payload']);
 if(e.version!==ENVELOPE_VERSION)fail('measurement_envelope_version_invalid');
 if(e.producerNamespace!==PRODUCER_NAMESPACE)fail('measurement_envelope_namespace_invalid');
 if(!ENVELOPE_KINDS.includes(e.kind)||!identity(e.semanticId))fail('measurement_envelope_identity_invalid');flag(e);
 if(!['DECISION_CYCLE','EVALUATION_CENSUS','OFFICIAL_CREATION'].includes(e.kind)&&textEncoder.encode(wire).length>PRODUCER_BOUNDS.maxFactWireBytes)fail('measurement_ingress_wire_bound');
 exactKeys(e.clocks,['occurredAt','observedAt','preparedAt']);for(const v of Object.values(e.clocks))clock(v,receivedAt);
 const digest=await digestPayload(canonicalSerialize({clocks:{occurredAt:e.clocks.occurredAt,observedAt:e.clocks.observedAt},payload:e.payload},PRODUCER_BOUNDS.maxWireBytes));
 if(digest!==e.payloadDigest)fail('measurement_envelope_digest_mismatch');
 if(e.semanticKey!==canonicalSerialize([PRODUCER_NAMESPACE,e.kind,e.semanticId]))fail('measurement_semantic_identity_invalid');
 const eventId=`m1:${await digestPayload(canonicalSerialize([PRODUCER_NAMESPACE,e.kind,e.semanticId,digest]))}`;
 if(e.eventId!==eventId)fail('measurement_event_identity_mismatch');
 validatePayload(e,receivedAt);return e;
}

export function createOfflineMeasurementConsumer(db,{clock:now=()=>Date.now()}={}){
 if(!db||typeof db.prepare!=='function'||typeof db.batch!=='function')fail('measurement_atomic_db_required');
 async function ingest(wire,{receivedAt=now()}={}){
  try{
   const e=await validateMeasurementWire(wire,{receivedAt});
   const ingestedAt=now();if(!Number.isSafeInteger(ingestedAt)||ingestedAt<receivedAt)fail('measurement_ingress_clock_invalid');
   const found=await db.prepare('SELECT * FROM measurement_ingress_receipts WHERE event_id=? OR semantic_key=?').bind(e.eventId,e.semanticKey).all();
   if(found.results?.length){
    if(found.results.some(r=>r.event_id!==e.eventId||r.payload_digest!==e.payloadDigest||r.semantic_key!==e.semanticKey))fail('measurement_ingress_integrity_conflict');
    return await finish(e,{status:'DUPLICATE',durable:true,ingestedAt:found.results[0].ingested_at},ingestedAt);
   }
   const c=e.clocks,mask=['occurredAt','observedAt','preparedAt'].reduce((n,k,i)=>n+(c[k]?.$unavailable?1<<i:0),0);
   const receipt=db.prepare(`INSERT INTO measurement_ingress_receipts
    (event_id,semantic_key,semantic_id,event_kind,producer_namespace,envelope_version,payload_digest,occurred_at,observed_at,prepared_at,clock_unavailable,received_at,ingested_at,ingestion_status)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'ACCEPTED') ON CONFLICT DO NOTHING`).bind(e.eventId,e.semanticKey,e.semanticId,e.kind,e.producerNamespace,e.version,e.payloadDigest,
     clock(c.occurredAt,receivedAt),clock(c.observedAt,receivedAt),clock(c.preparedAt,receivedAt),mask,receivedAt,ingestedAt);
   const p=parseEvidence(canonicalSerialize(e.payload,PRODUCER_BOUNDS.maxWireBytes));
   if(e.kind==='DECISION_CYCLE'){
    await measurementWriter(db,{maxWrites:160}).immutableBatch(p.records,{links:p.links,officialPins:p.officialPins,extraStatements:[receipt]});
   }else if(e.kind==='EVALUATION_CENSUS'){
    const parent=await db.prepare('SELECT cycle_id FROM decision_cycle_evidence WHERE cycle_id=?').bind(p.values.cycle_id).first();
    if(!parent)return {ok:false,status:'DEPENDENCY_PENDING',retryable:true,durable:false,captureGap:true};
    await measurementWriter(db,{maxWrites:160}).immutableBatch([{type:'decision',values:p.values,payload:p.decisionEvidence}],{extraStatements:[receipt]});
   }else{
    const encoded=await encodeEvidence(p);const signalId=subject(e);
    const fact=db.prepare(`INSERT INTO measurement_lifecycle_facts(event_id,signal_id,fact_kind,payload_blob,codec,uncompressed_length,payload_digest)
     VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`).bind(e.eventId,signalId,e.kind,encoded.data,encoded.codec,encoded.length,digestBytes(encoded.digest));
    await db.batch([receipt,fact]);
   }
   // A concurrent identical insert may win. First durable availability is the
   // stored receipt's clock, never this retry's local time.
   const stored=await db.prepare('SELECT * FROM measurement_ingress_receipts WHERE event_id=?').bind(e.eventId).first();
   if(!stored||stored.payload_digest!==e.payloadDigest||stored.semantic_key!==e.semanticKey)fail('measurement_ingress_integrity_conflict');
   return await finish(e,{status:'ACCEPTED',durable:true,ingestedAt:stored.ingested_at},ingestedAt);
  }catch(error){return {ok:false,status:/integrity_conflict/.test(String(error?.message))?'INTEGRITY_CONFLICT':'REJECTED',
   error:String(error?.message||'measurement_ingress_failed'),captureGap:true,durable:false};}
 }
 async function finish(e,result,rebuiltAt){
  const signalId=subject(e);
  if(signalId){try{await rebuildLifecycleProjection(db,signalId,{rebuiltAt});}catch(error){return {ok:true,...result,projectionStatus:'PENDING',projectionError:String(error?.message),captureGap:true};}}
  return {ok:true,...result,projectionStatus:signalId?'CURRENT':'NOT_APPLICABLE'};
 }
 return Object.freeze({mode:'OFFLINE',ingest});
}
function subject(e){return e.kind==='OFFICIAL_CREATION'?e.payload.officialSignalId:e.kind==='LIFECYCLE_FACT'?e.payload.signalId:
 e.kind==='CONFIRMATION_LINK'?e.payload.confirmationSignalId:null;}
