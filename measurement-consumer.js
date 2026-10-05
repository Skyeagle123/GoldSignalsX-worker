// Offline M2 only. Explicit Measurement DB capability; no Worker/env/transport.
import {ENVELOPE_VERSION,PRODUCER_NAMESPACE,ENVELOPE_KINDS,PRODUCER_BOUNDS,validateDataEvidence} from './measurement-envelope.js';
import {canonicalSerialize,digestPayload,parseEvidence} from './signal-evidence.js';
import {encodeEvidence,digestBytes,censusSnapshot} from './evidence-codec.js';
import {measurementWriter} from './signal-evidence-store.js';
import {rebuildLifecycleProjection,validateLifecycleProjectionReturn,lifecycleCapability,fenceLifecycleFailure,clearLifecycleFailure} from './measurement-lifecycle.js';

const textEncoder=new TextEncoder();
const fail=reason=>{throw new Error(reason);};
const identity=v=>typeof v==='string'&&v.length>0&&textEncoder.encode(v).length<=1024;
const unknown=v=>v===null||v===undefined||(v&&typeof v==='object'&&Object.keys(v).length===1&&v.$unavailable==='undefined');
function clock(v,ceiling){if(unknown(v))return null;if(!Number.isSafeInteger(v)||v<0||v>ceiling)fail('measurement_ingress_clock_invalid');return v;}
function requiredClock(v,ceiling){const t=clock(v,ceiling);if(t===null)fail('measurement_ingress_clock_invalid');return t;}
function exactKeys(v,keys){if(!v||Array.isArray(v)||typeof v!=='object'||Object.keys(v).length!==keys.length||keys.some(k=>!Object.hasOwn(v,k)))fail('measurement_envelope_shape_invalid');}
function flag(p){if(p?.measurementOnly!==true||p?.decisionUse!==false)fail('measurement_decision_use_forbidden');}
const frames=['1m','5m','15m','30m','60m','240m','1d'];
function tags(v){
 if(!v||typeof v!=='object')return;
 if(Object.hasOwn(v,'$number')||Object.hasOwn(v,'$unavailable')){
  if(Object.keys(v).length!==1||!(v.$unavailable==='undefined'||['-0','NaN','Infinity','-Infinity'].includes(v.$number)))fail('measurement_codec_tag_invalid');return;
 }
 if(Object.keys(v).some(k=>k.startsWith('$')))fail('measurement_codec_tag_invalid');
 for(const x of Object.values(v))tags(x);
}
const equal=(a,b)=>canonicalSerialize(a,PRODUCER_BOUNDS.maxWireBytes)===canonicalSerialize(b,PRODUCER_BOUNDS.maxWireBytes);
function ids(v){if(!Array.isArray(v)||v.length>80||v.some(x=>!identity(x))||new Set(v).size!==v.length)fail('measurement_reference_invalid');return v;}
function manifest(m,receivedAt,blocks=null){
 if(unknown(m))return [];
 if(m.status==='UNAVAILABLE'){exactKeys(m,['status']);return [];}
 if(!frames.includes(m.tf)||!Number.isInteger(m.count)||m.count<0||m.count>2000||!Array.isArray(m.references)||m.references.length>80||m.ordering!=='AS_CONSUMED')fail('measurement_manifest_invalid');
 let count=0;
 for(const r of m.references){
  exactKeys(r,['blockId','start','count','availableAt','sourceAvailability']);
  if(!identity(r.blockId)||!Number.isInteger(r.start)||r.start<0||!Number.isInteger(r.count)||r.count<1)fail('measurement_reference_invalid');clock(r.availableAt,receivedAt);
  if(blocks){const b=blocks.get(r.blockId);if(!b||b.tf!==m.tf||r.start+r.count>b.rows.length)fail('measurement_dependency_ownership_invalid');}
  count+=r.count;
 }
 if(count!==m.count)fail('measurement_manifest_invalid');return m.references.map(r=>r.blockId);
}
function decision(d,receivedAt){
 exactKeys(d,['admissionContext','broadMtf','callerGates','candidateKey','capturedAt','createdAt','cycleId','decisionUse','direction','engine','engineMtf','evaluatedAt','evaluationId','evidenceCompleteness','exposure','inputManifest','kind','levelsStatus','measurementOnly','officialPersistence','officialSignalId','outcome','quote','skippedReason','sourceCandle','symbol','timeframe','versions']);
 flag(d);if(!identity(d.evaluationId)||!identity(d.cycleId)||!['CANDIDATE','OFFICIAL'].includes(d.kind))fail('measurement_decision_identity_invalid');
 if(!frames.includes(d.timeframe)||!unknown(d.direction)&&!['buy','sell'].includes(d.direction)||!d.evaluationId.startsWith(d.cycleId+':'))fail('measurement_decision_identity_invalid');
 for(const k of ['createdAt','evaluatedAt','capturedAt'])clock(d[k],receivedAt);
 if(!d.inputManifest||!Array.isArray(d.inputManifest.engineMtf))fail('measurement_manifest_invalid');
 for(const m of [d.inputManifest.primary,...d.inputManifest.engineMtf,d.inputManifest.research1m])manifest(m,receivedAt);
 if(!['COMPUTED_BY_ENGINE','NOT_COMPUTED_BY_ENGINE'].includes(d.levelsStatus)||d.kind==='CANDIDATE'&&d.officialSignalId!==null||!unknown(d.candidateKey)&&!identity(d.candidateKey))fail('measurement_decision_identity_invalid');
 if(d.kind==='OFFICIAL'&&!identity(d.officialSignalId))fail('measurement_decision_identity_invalid');
 for(const level of ['entry','tp1','tp2','sl']){const v=d.engine?.levels?.[level];if(d.levelsStatus==='COMPUTED_BY_ENGINE'&&!Number.isFinite(v)||!unknown(v)&&!Number.isFinite(v))fail('measurement_levels_invalid');}
}
function decisionValues(v,d){
 exactKeys(v,['evaluation_id','candidate_key','official_signal_id','kind','cycle_id','cohort_id','timeframe','evaluated_at','measurement_only','decision_use','recorded_at']);
 if(v?.evaluation_id!==d.evaluationId||v.cycle_id!==d.cycleId||v.kind!==d.kind||v.official_signal_id!==d.officialSignalId||
  v.timeframe!==d.timeframe||v.candidate_key!==d.candidateKey||(!unknown(d.evaluatedAt)&&v.evaluated_at!==d.evaluatedAt)||v.recorded_at!==d.capturedAt||v.measurement_only!==1||v.decision_use!==0)fail('measurement_semantic_identity_invalid');
}
async function validatePayload(e,receivedAt){
 const p=parseEvidence(canonicalSerialize(e.payload,PRODUCER_BOUNDS.maxWireBytes));
 if(e.kind==='DECISION_CYCLE'){
  exactKeys(p,['records','officialPins','links']);if(!Array.isArray(p.records)||p.records.length>160||!Array.isArray(p.officialPins)||!Array.isArray(p.links))fail('measurement_cycle_invalid');
  const cycles=p.records.filter(r=>r.type==='cycle');if(cycles.length!==1||cycles[0].values.cycle_id!==e.semanticId||cycles[0].payload.cycleId!==e.semanticId)fail('measurement_semantic_identity_invalid');
  for(const r of p.records){
   exactKeys(r,['type','values','payload']);
   if(!['cohort','market','cycle','decision'].includes(r.type)||!r.values||!r.payload)fail('measurement_cycle_record_invalid');
   const columns={cohort:['cohort_id','schema_version','effective_at','recorded_at'],market:['block_id','timeframe','from_at','to_at','recorded_at'],cycle:['cycle_id','cohort_id','evaluated_at','block_ids_json','recorded_at']};
   if(columns[r.type])exactKeys(r.values,columns[r.type]);
   requiredClock(r.values.recorded_at,receivedAt);
   if(r.type==='decision'){requiredClock(r.values.evaluated_at,receivedAt);decision(r.payload,receivedAt);decisionValues(r.values,r.payload);if(r.payload.cycleId!==e.semanticId)fail('measurement_semantic_identity_invalid');}
  }
  const cohort=p.records.filter(r=>r.type==='cohort');if(cohort.length!==1)fail('measurement_cycle_record_invalid');
  const h=cohort[0],cohortDigest=await digestPayload(canonicalSerialize(h.payload));
  const cohortId=btoa(String.fromCharCode(...cohortDigest.match(/../g).map(x=>parseInt(x,16)))).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');
  if(h.values.cohort_id!==cohortId||h.values.schema_version!==1||h.values.effective_at!==h.payload.measurementEffectiveAt)fail('measurement_semantic_identity_invalid');flag(h.payload);requiredClock(h.values.effective_at,receivedAt);
  const c=cycles[0];exactKeys(c.payload,['cycleId','cycleStartedAt','evaluatedAt','capturedAt','manifests','newsContext','filters','candidateOrdering','comparatorVersion','actualExposureTrace','exposureDecisionOrder','measurementOnly','decisionUse']);flag(c.payload);
  if(c.values.cohort_id!==cohortId||c.values.evaluated_at!==c.payload.evaluatedAt||c.values.recorded_at!==c.payload.capturedAt||h.values.recorded_at!==c.payload.capturedAt)fail('measurement_semantic_identity_invalid');
  for(const k of ['cycleStartedAt','evaluatedAt','capturedAt'])requiredClock(c.payload[k],receivedAt);
  if(!equal(e.clocks.occurredAt,c.payload.evaluatedAt)||!equal(e.clocks.observedAt,c.payload.capturedAt))fail('measurement_semantic_identity_invalid');
  if(!Array.isArray(c.payload.candidateOrdering)||!c.payload.manifests)fail('measurement_cycle_invalid');
  const blocks=new Map();
  for(const r of p.records.filter(r=>r.type==='market')){
   exactKeys(r.payload,['schema','tf','rows','ordering']);const b=r.payload;
   if(b.schema!==1||!frames.includes(b.tf)||b.ordering!=='AS_CONSUMED'||!Array.isArray(b.rows)||!b.rows.length||b.rows.length>128)fail('measurement_market_invalid');
   for(const row of b.rows){exactKeys(row,['t','o','h','l','c','v','provider','sessionId','sourceAvailableAt']);requiredClock(row.t,receivedAt);clock(row.sourceAvailableAt,receivedAt);for(const k of ['o','h','l','c','v'])if(!unknown(row[k])&&!Number.isFinite(row[k]))fail('measurement_market_invalid');}
   if(r.values.block_id!==`market:${await digestPayload(canonicalSerialize(b))}`||r.values.timeframe!==b.tf||r.values.from_at!==b.rows[0].t||r.values.to_at!==b.rows.at(-1).t)fail('measurement_semantic_identity_invalid');
   if(blocks.has(r.values.block_id))fail('measurement_cycle_record_invalid');blocks.set(r.values.block_id,b);
  }
  const decisions=p.records.filter(r=>r.type==='decision');if(new Set(decisions.map(r=>r.payload.evaluationId)).size!==decisions.length)fail('measurement_decision_identity_invalid');
  for(const m of Object.values(c.payload.manifests))manifest(m,receivedAt,blocks);
  for(const r of decisions){if(r.values.cohort_id!==cohortId||r.values.evaluated_at!==(unknown(r.payload.evaluatedAt)?c.payload.evaluatedAt:r.payload.evaluatedAt))fail('measurement_semantic_identity_invalid');for(const m of [r.payload.inputManifest.primary,...r.payload.inputManifest.engineMtf,r.payload.inputManifest.research1m])manifest(m,receivedAt,blocks);if(!equal(r.payload.versions,h.payload))fail('measurement_semantic_identity_invalid');}
  if(p.links.length!==1)fail('measurement_reference_invalid');
  const link=p.links[0];exactKeys(link,['ownerType','ownerId','blockIds']);ids(link.blockIds);
  if(link.ownerType!=='CYCLE'||link.ownerId!==e.semanticId||!equal([...link.blockIds].sort(),[...blocks.keys()].sort())||!equal(ids(JSON.parse(c.values.block_ids_json)),link.blockIds))fail('measurement_dependency_ownership_invalid');
  const officials=decisions.filter(r=>r.payload.kind==='OFFICIAL');if(p.officialPins.length!==officials.length)fail('measurement_reference_invalid');
  const pinned=new Set();for(const pin of p.officialPins){exactKeys(pin,['officialId','blockIds']);ids(pin.blockIds);const d=officials.find(r=>r.payload.officialSignalId===pin.officialId)?.payload;
   if(!d||pinned.has(pin.officialId))fail('measurement_dependency_ownership_invalid');pinned.add(pin.officialId);
   const expected=[...new Set([d.inputManifest.primary,...d.inputManifest.engineMtf,d.inputManifest.research1m].flatMap(m=>m?.references?.map(r=>r.blockId)||[]))];
   if(!equal([...pin.blockIds].sort(),expected.sort()))fail('measurement_dependency_ownership_invalid');
  }
 }else if(e.kind==='EVALUATION_CENSUS'){
  exactKeys(p,['values','census','decisionEvidence']);requiredClock(p.values?.evaluated_at,receivedAt);
  if(p.values?.evaluated_at!==e.clocks.occurredAt)fail('measurement_semantic_identity_invalid');
  const cohortDigest=await digestPayload(canonicalSerialize(p.decisionEvidence?.versions));
  const cohortId=btoa(String.fromCharCode(...cohortDigest.match(/../g).map(x=>parseInt(x,16)))).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');
  if(p.values?.cohort_id!==cohortId)fail('measurement_semantic_identity_invalid');
  if(!equal(e.clocks.observedAt,p.decisionEvidence?.capturedAt))fail('measurement_semantic_identity_invalid');
  decision(p.decisionEvidence,receivedAt);decisionValues(p.values,p.decisionEvidence);if(e.semanticId!==p.decisionEvidence.evaluationId)fail('measurement_semantic_identity_invalid');
  if(canonicalSerialize(p.census)!==canonicalSerialize(censusSnapshot(p.decisionEvidence)))fail('measurement_census_conflict');
 }else{
  if(e.kind==='OFFICIAL_CREATION'){
   exactKeys(p,['evaluationId','officialSignalId','officialPersisted','performancePersistence','decisionEvidence']);
   decision(p.decisionEvidence,receivedAt);
   if(!equal(e.clocks.occurredAt,p.decisionEvidence.createdAt)||!equal(e.clocks.observedAt,p.decisionEvidence.capturedAt))fail('measurement_semantic_identity_invalid');
   if(e.semanticId!==p.officialSignalId||p.officialSignalId!==p.decisionEvidence.officialSignalId||p.evaluationId!==p.decisionEvidence.evaluationId||p.decisionEvidence.kind!=='OFFICIAL'||p.officialPersisted!==p.decisionEvidence.exposure?.officialPersisted||canonicalSerialize(p.performancePersistence)!==canonicalSerialize(p.decisionEvidence.officialPersistence))fail('measurement_semantic_identity_invalid');
  }else if(e.kind==='LIFECYCLE_FACT'){
   exactKeys(p,['signalId','event','createdAt','timeframe','direction','occurredAt','observedPrice','level','trigger','status','closedAt','performancePersistence','orderingQuality','measurementOnly','decisionUse']);
   if(!unknown(p.timeframe)&&!frames.includes(p.timeframe)||!unknown(p.direction)&&!['buy','sell'].includes(p.direction))fail('measurement_lifecycle_value_invalid');
   exactKeys(p.trigger,['price','at','source']);
   flag(p);
   if(!identity(p.signalId)||!['tp1','tp2','sl','expired'].includes(p.event)||e.semanticId!==`production:${p.signalId}:${p.event}`)fail('measurement_semantic_identity_invalid');
   for(const k of ['createdAt','occurredAt','closedAt'])clock(p[k],receivedAt);
   if(canonicalSerialize(p.occurredAt)!==canonicalSerialize(e.clocks.occurredAt))fail('measurement_lifecycle_clock_conflict');
   const expected={tp1:'tp1',tp2:'tp2',sl:'stopped',expired:'expired'}[p.event];
   if(!unknown(p.status)&&p.status!==expected)fail('measurement_lifecycle_status_conflict');
   if(!unknown(p.closedAt)&&p.event==='tp1')fail('measurement_lifecycle_status_conflict');
   if(p.performancePersistence?.signal?.signal_id!=null&&p.performancePersistence.signal.signal_id!==p.signalId)fail('measurement_semantic_identity_invalid');
   for(const k of ['created_at','updated_at','closed_at'])clock(p.performancePersistence?.signal?.[k],receivedAt);clock(p.trigger?.at,receivedAt);
   for(const v of [p.observedPrice,p.level,p.trigger?.price])if(!unknown(v)&&!Number.isFinite(v))fail('measurement_lifecycle_value_invalid');
   if(!unknown(p.createdAt)&&!unknown(p.occurredAt)&&p.createdAt>p.occurredAt||!unknown(p.createdAt)&&!unknown(p.closedAt)&&p.createdAt>p.closedAt)fail('measurement_lifecycle_clock_conflict');
  }else if(e.kind==='CONFIRMATION_LINK'){
   exactKeys(p,['primarySignalId','confirmationSignalId','link','linkPersistence','measurementOnly','decisionUse']);
   flag(p);
   if(!identity(p.primarySignalId)||!identity(p.confirmationSignalId)||p.primarySignalId===p.confirmationSignalId||e.semanticId!==`${p.primarySignalId}:confirmation:${p.confirmationSignalId}`)fail('measurement_semantic_identity_invalid');
   if(!unknown(p.linkPersistence)&&!['SUCCEEDED','FAILED'].includes(p.linkPersistence))fail('measurement_confirmation_invalid');
   if(unknown(p.link)){if(p.linkPersistence==='SUCCEEDED')fail('measurement_confirmation_invalid');}
   else if(Object.hasOwn(p.link,'signalId')){
    exactKeys(p.link,['signalId','timeframe']);
    if(p.link.signalId!==p.confirmationSignalId||!frames.includes(p.link.timeframe))fail('measurement_confirmation_invalid');
   }else{
    exactKeys(p.link,['type','confirmationSignalId','primarySignalId','tf','side','conf','score','signalBarTs','confirmedAt']);
    if(p.link.type!=='later-confirmation'||p.link.confirmationSignalId!==p.confirmationSignalId||p.link.primarySignalId!==p.primarySignalId||!frames.includes(p.link.tf)||!['buy','sell'].includes(p.link.side)||!Number.isFinite(p.link.conf)||!Number.isFinite(p.link.score))fail('measurement_confirmation_invalid');
    requiredClock(p.link.signalBarTs,receivedAt);requiredClock(p.link.confirmedAt,receivedAt);
    if(!equal(p.link.confirmedAt,e.clocks.occurredAt))fail('measurement_confirmation_invalid');
   }
  }else if(e.kind==='CAPTURE_GAP'){flag(p);if(p.captureGap!==true||p.durable!==false)fail('measurement_capture_gap_invalid');}
 }
}

export async function validateMeasurementWire(wire,{receivedAt}={}){
 if(!Number.isSafeInteger(receivedAt)||receivedAt<0)fail('measurement_ingress_clock_invalid');
 // Wire strings only: no caller-controlled object methods or descriptors.
 if(typeof wire!=='string'||wire.length>PRODUCER_BOUNDS.maxWireBytes||textEncoder.encode(wire).length>PRODUCER_BOUNDS.maxWireBytes)fail('measurement_ingress_wire_bound');
 const e=JSON.parse(wire);validateDataEvidence(e);tags(e);
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
 await validatePayload(e,receivedAt);return e;
}

function decisions(e){return e.kind==='DECISION_CYCLE'?e.payload.records.filter(r=>r.type==='decision').map(r=>r.payload):
 ['EVALUATION_CENSUS','OFFICIAL_CREATION'].includes(e.kind)?[e.payload.decisionEvidence]:[];}

export function createOfflineMeasurementConsumer(db,{clock:now=()=>Date.now()}={}){
 if(!db||typeof db.prepare!=='function'||typeof db.batch!=='function')fail('measurement_atomic_db_required');
 lifecycleCapability(db);
 async function ingest(wire,{receivedAt=now()}={}){
  let e,bindings=[],contradictions=[],committed=false,batchAttempted=false;
  // Track durable batch confirmation even if the historical writer's later
  // verification fails. No post-commit error becomes malformed-input rejection.
  const tracked={prepare:sql=>db.prepare(sql),async batch(statements){batchAttempted=true;const result=await db.batch(statements);committed=true;return result;}};
  try{
   e=await validateMeasurementWire(wire,{receivedAt});
   const startedAt=now();if(!Number.isSafeInteger(startedAt)||startedAt<receivedAt)fail('measurement_ingress_clock_invalid');
   for(const d of decisions(e))bindings.push({id:d.evaluationId,signalId:d.officialSignalId,attemptedSignalId:d.officialSignalId,digest:await digestPayload(canonicalSerialize(d))});
   const found=await db.prepare('SELECT r.*,s.ingested_at FROM measurement_ingress_receipts r LEFT JOIN measurement_ingress_recovery s ON s.event_id=r.event_id WHERE r.event_id=? OR r.semantic_key=?').bind(e.eventId,e.semanticKey).all();
   const duplicate=!!found.results?.length;
   // Check before the writer's own same-record preflight, which can otherwise
   // reject a changed census before the cross-kind conflict trigger runs.
   // The trigger remains the atomic guard against concurrent first deliveries.
   for(const b of bindings){const original=await db.prepare('SELECT payload_digest,official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=?').bind(b.id).first();
    if(original&&original.payload_digest!==b.digest){contradictions.push({...b,signalId:original.official_signal_id});fenceLifecycleFailure(db,original.official_signal_id,b.id);fail('measurement_decision_integrity_conflict');}}
   if(duplicate&&found.results.some(r=>r.event_id!==e.eventId||r.payload_digest!==e.payloadDigest||r.semantic_key!==e.semanticKey))fail('measurement_ingress_integrity_conflict');
   const c=e.clocks,mask=['occurredAt','observedAt','preparedAt'].reduce((n,k,i)=>n+(c[k]?.$unavailable?1<<i:0),0);
   const receipt=db.prepare(`INSERT INTO measurement_ingress_receipts
    (event_id,semantic_key,semantic_id,event_kind,producer_namespace,envelope_version,payload_digest,occurred_at,observed_at,prepared_at,clock_unavailable,received_at,processing_started_at,ingestion_status)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'ACCEPTED') ON CONFLICT DO NOTHING`).bind(e.eventId,e.semanticKey,e.semanticId,e.kind,e.producerNamespace,e.version,e.payloadDigest,
     clock(c.occurredAt,receivedAt),clock(c.observedAt,receivedAt),clock(c.preparedAt,receivedAt),mask,receivedAt,startedAt);
   const extra=[receipt,...bindings.map(b=>db.prepare('INSERT INTO measurement_decision_bindings(evaluation_id,payload_digest,official_signal_id,first_event_id) VALUES(?,?,?,?) ON CONFLICT DO NOTHING').bind(b.id,b.digest,b.signalId,e.eventId)),
    db.prepare("INSERT INTO measurement_ingress_recovery(event_id,ingested_at,processing_status,processing_gaps_json) VALUES(?,NULL,'PENDING','[]') ON CONFLICT DO NOTHING").bind(e.eventId)];
   const p=parseEvidence(canonicalSerialize(e.payload,PRODUCER_BOUNDS.maxWireBytes));let writeResult={processingCaptureGaps:[]};
   if(e.kind==='DECISION_CYCLE'){
    // Reusing the immutable writer on replay repairs only missing processing
    // initialization; existing/finalized subjects are never reinitialized.
    writeResult=await measurementWriter(tracked,{maxWrites:160}).immutableBatch(p.records,{links:p.links,officialPins:p.officialPins,extraStatements:extra});
   }else if(e.kind==='EVALUATION_CENSUS'){
    const parent=await db.prepare('SELECT cycle_id FROM decision_cycle_evidence WHERE cycle_id=?').bind(p.values.cycle_id).first();
    if(!parent)return {ok:false,status:'DEPENDENCY_PENDING',retryable:true,durable:false,captureGap:true};
    writeResult=await measurementWriter(tracked,{maxWrites:160}).immutableBatch([{type:'decision',values:p.values,payload:p.decisionEvidence}],{extraStatements:extra});
   }else{
    const encoded=await encodeEvidence(p);const signalId=subject(e);
    const fact=db.prepare(`INSERT INTO measurement_lifecycle_facts(event_id,signal_id,fact_kind,payload_blob,codec,uncompressed_length,payload_digest)
     VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`).bind(e.eventId,signalId,e.kind,encoded.data,encoded.codec,encoded.length,digestBytes(encoded.digest));
    await tracked.batch([...extra,fact]);
   }
   // Evidence is committed before this clock is sampled. A separate durable
   // fence exposes it analytically; NULL means unavailable until retry repairs.
   const confirmedAt=now();if(!Number.isSafeInteger(confirmedAt)||confirmedAt<startedAt)fail('measurement_ingress_clock_invalid');
   const gaps=writeResult.processingCaptureGaps||[];
   await db.prepare('UPDATE measurement_ingress_recovery SET ingested_at=COALESCE(ingested_at,?),processing_status=?,processing_gaps_json=? WHERE event_id=?').bind(confirmedAt,gaps.length?'PENDING':'CURRENT',canonicalSerialize(gaps),e.eventId).run();
   const stored=await db.prepare('SELECT r.*,s.ingested_at,s.processing_status,s.processing_gaps_json FROM measurement_ingress_receipts r JOIN measurement_ingress_recovery s ON s.event_id=r.event_id WHERE r.event_id=?').bind(e.eventId).first();
   if(!stored||stored.payload_digest!==e.payloadDigest||stored.semantic_key!==e.semanticKey)fail('measurement_ingress_integrity_conflict');
   return await finish(e,{status:duplicate?'DUPLICATE':'ACCEPTED',durable:true,ingestedAt:stored.ingested_at,processingStatus:stored.processing_status,
    processingCaptureGaps:JSON.parse(stored.processing_gaps_json),...(stored.processing_status==='PENDING'?{captureGap:true,retryable:true}:{})},now());
  }catch(error){
   const reason=String(error?.message||'measurement_ingress_failed');
   if(/decision_integrity_conflict/.test(reason)){
    // SQL's canonical-conflict trigger is validated contradiction evidence.
    // Suspend evaluations/attempted subjects before any fallible enrichment.
    if(!contradictions.length)for(const b of bindings)fenceLifecycleFailure(db,b.signalId,b.id);
    // The conflicting batch rolled back. Persist a small immutable incident so
    // an earlier valid projection cannot remain usable after contradiction.
    try{for(const b of [...contradictions,...bindings.filter(b=>!contradictions.some(c=>c.id===b.id))]){const known=contradictions.find(c=>c.id===b.id);const old=known?{official_signal_id:known.signalId,payload_digest:null}:await db.prepare('SELECT payload_digest,official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=?').bind(b.id).first();if(old&&old.payload_digest!==b.digest){
     // First durably fence the subject as PENDING. If the later immutable
     // marker fails, this intent still blocks publication and collection.
     fenceLifecycleFailure(db,old.official_signal_id,b.id);
     await db.batch([db.prepare("INSERT INTO measurement_decision_quarantine(evaluation_id,state,detected_at) VALUES(?,'PENDING',?) ON CONFLICT DO NOTHING").bind(b.id,now())]);
     clearLifecycleFailure(db,old.official_signal_id,b.id);if(b.attemptedSignalId!==old.official_signal_id)clearLifecycleFailure(db,b.attemptedSignalId);
     await db.prepare('INSERT INTO measurement_decision_conflicts(evaluation_id,rejected_digest,detected_at) VALUES(?,?,?) ON CONFLICT DO NOTHING').bind(b.id,b.digest,now()).run();
     if(old.official_signal_id){await db.prepare("UPDATE measurement_lifecycle_projection SET integrity_status='CONFLICT',status=NULL,closed_at=NULL WHERE signal_id=?").bind(old.official_signal_id).run();await rebuildLifecycleProjection(db,old.official_signal_id,{rebuiltAt:now()});}
    }else if(old){clearLifecycleFailure(db,b.signalId,b.id);}}
     const signalId=subject(e);if(signalId)await rebuildLifecycleProjection(db,signalId,{rebuiltAt:now()});
    }catch{return {ok:false,status:'INTEGRITY_CONFLICT',error:reason,durable:false,captureGap:true,retryable:true,conflictStatus:'RECONCILIATION_PENDING',projectionStatus:'PENDING',quarantineDurability:'UNKNOWN_OR_PENDING'};}
    return {ok:false,status:'INTEGRITY_CONFLICT',error:reason,durable:false,captureGap:true,conflictStatus:'RECORDED'};
   }
   if(committed)return {ok:true,status:'ACCEPTED',durable:true,acknowledgementStatus:'RECONCILIATION_PENDING',projectionStatus:'PENDING',processingStatus:'PENDING',retryable:true,captureGap:true,error:reason};
   if(batchAttempted&&!/integrity_conflict/.test(reason))return {ok:false,status:'DURABILITY_UNKNOWN',durable:null,retryable:true,captureGap:true,error:reason};
   if(e&&!/integrity_conflict/.test(reason))return {ok:false,status:'RETRYABLE',durable:null,retryable:true,captureGap:true,error:reason};
   return {ok:false,status:/integrity_conflict/.test(reason)?'INTEGRITY_CONFLICT':'REJECTED',error:reason,captureGap:true,durable:false};
  }
 }
 async function finish(e,result,rebuiltAt){
  for(const d of decisions(e)){const c=lifecycleCapability(db);if(c.exhausted||c.pending.has(d.officialSignalId)||c.pendingEvaluations.has(d.evaluationId))return {ok:true,...result,projectionStatus:'PENDING',captureGap:true,retryable:true};if(!subject(e)){const q=await db.prepare('SELECT state FROM measurement_decision_quarantine WHERE evaluation_id=?').bind(d.evaluationId).first();if(q)return {ok:true,...result,projectionStatus:q.state==='CONFLICT'?'CONFLICT':'PENDING',captureGap:true,retryable:true};}}
  const signalId=subject(e);
  if(signalId){try{const projection=await rebuildLifecycleProjection(db,signalId,{rebuiltAt});if(projection)validateLifecycleProjectionReturn(db,projection);if(projection?.integrityStatus==='CONFLICT')return {ok:true,...result,projectionStatus:'CONFLICT',captureGap:true};}catch(error){return {ok:true,...result,projectionStatus:'PENDING',projectionError:String(error?.message),captureGap:true,retryable:true};}}
  return {ok:true,...result,projectionStatus:signalId?'CURRENT':'NOT_APPLICABLE'};
 }
 return Object.freeze({mode:'OFFLINE',ingest});
}
function subject(e){return e.kind==='OFFICIAL_CREATION'?e.payload.officialSignalId:e.kind==='LIFECYCLE_FACT'?e.payload.signalId:
 e.kind==='CONFIRMATION_LINK'?e.payload.confirmationSignalId:null;}
