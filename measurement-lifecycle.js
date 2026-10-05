// M2 projection cache. Immutable captured facts, never trading tables, are truth.
import {encodeEvidence,decodeStoredEvidence,digestBytes} from './evidence-codec.js';
import {canonicalSerialize} from './signal-evidence.js';

// Existing performance persistence uses 'sl', not the Signal object's 'stopped'.
const terminal={tp2:'tp2',sl:'sl',expired:'expired'};
const value=v=>v===undefined||v?.$unavailable?null:v;
// Offline adapters must preserve this enumerable capability token when wrapped.
// This bounded, NONDURABLE failure fence is used only if even a pending durable
// quarantine cannot be written. It never substitutes for the durable intent:
// callers must retain/retry that rejected packet, including after process loss.
const capabilityToken=Symbol('measurement.lifecycle.capability');
const capabilities=new WeakMap();
export function lifecycleCapability(db){
 let c=db[capabilityToken]||capabilities.get(db);
 if(!c){c={pending:new Set(),exhausted:false};capabilities.set(db,c);if(Object.isExtensible(db))Object.defineProperty(db,capabilityToken,{value:c,enumerable:true});}
 return c;
}
export function fenceLifecycleFailure(db,signalId){if(!signalId)return;const c=lifecycleCapability(db);if(c.pending.size>=4032&&!c.pending.has(signalId))c.exhausted=true;else c.pending.add(signalId);}
export function clearLifecycleFailure(db,signalId){lifecycleCapability(db).pending.delete(signalId);}
export function assertLifecycleCapability(db,signalId){const c=lifecycleCapability(db);if(c.exhausted||c.pending.has(signalId))throw new Error('measurement_quarantine_capability_pending');}

export function projectLifecycle(signalId,facts){
 const result={signalId,createdAt:null,timeframe:null,direction:null,levels:{entry:null,tp1:null,tp2:null,sl:null},
  status:null,closedAt:null,observedClosedAt:null,closureBasis:null,tp1At:null,tp2At:null,slAt:null,expiredAt:null,officialPersisted:null,
  performancePersistence:null,persistenceFailures:[],confirmations:[],owningPrimaryId:null,linkPersistence:null,
  availableAt:Math.max(...facts.map(f=>f.ingested_at)),factCount:facts.length,integrityStatus:'PARTIAL',
  orderingQuality:'NOT_ASSERTED',measurementOnly:true,decisionUse:false};
 const conflicts=new Set();const merge=(object,key,v)=>{v=value(v);if(v===null)return;
  if(object[key]!==null&&canonicalSerialize(object[key])!==canonicalSerialize(v))conflicts.add(key);else object[key]=v;};
 const persisted=[],terminals=[];
 // Arrival order is never a lifecycle ordering rule. Stable sorting only makes
 // the cache representation deterministic, including conflict diagnostics.
 for(const fact of [...facts].sort((a,b)=>a.event_id.localeCompare(b.event_id))){
  const p=fact.payload;
  if(fact.fact_kind==='OFFICIAL_CREATION'){
   const d=p.decisionEvidence;merge(result,'createdAt',d.createdAt);merge(result,'timeframe',d.timeframe);merge(result,'direction',d.direction);
   for(const key of Object.keys(result.levels))merge(result.levels,key,d.engine?.levels?.[key]);
   merge(result,'officialPersisted',p.officialPersisted);merge(result,'performancePersistence',p.performancePersistence?.performance);
  }else if(fact.fact_kind==='LIFECYCLE_FACT'){
   merge(result,'createdAt',p.createdAt);merge(result,'timeframe',p.timeframe);merge(result,'direction',p.direction);
   const at=value(p.occurredAt);const key={tp1:'tp1At',tp2:'tp2At',sl:'slAt',expired:'expiredAt'}[p.event];merge(result,key,at);
   const expected=terminal[p.event]??'tp1',storedStatus=value(p.performancePersistence?.signal?.status);
   // A successful call may return a previously terminal row without applying
   // this attempted event. Do not turn that attempt into a new persisted state.
   if(p.performancePersistence?.ok===true&&(storedStatus===expected||storedStatus===null&&value(p.status)!==null)){
    persisted.push(p);if(terminal[p.event])terminals.push(p);
   }
   if(p.performancePersistence?.ok===false)result.persistenceFailures.push({eventId:fact.event_id,event:p.event,result:p.performancePersistence});
  }else if(fact.fact_kind==='CONFIRMATION_LINK'){
   merge(result,'owningPrimaryId',p.primarySignalId);merge(result,'linkPersistence',p.linkPersistence);
   result.confirmations.push({confirmationSignalId:p.confirmationSignalId,primarySignalId:p.primarySignalId,link:p.link,
    linkPersistence:p.linkPersistence,eventId:fact.event_id});
  }
 }
 if(result.officialPersisted===true&&result.performancePersistence==='SUCCEEDED')result.status='active';
 if(persisted.some(p=>p.event==='tp1'))result.status='tp1';
 if(terminals.length){
  const states=new Set(terminals.map(p=>terminal[p.event]));
  if(states.size!==1)conflicts.add('terminal');
   else{result.status=terminal[terminals[0].event];for(const p of terminals){
     merge(result,'observedClosedAt',p.closedAt);
     // Captured occurredAt is the exact performanceEventAt operand used by the
     // unchanged persistence code for closed_at. Never substitute arrival time.
     merge(result,'closedAt',p.occurredAt);
    }result.closureBasis='CAPTURED_PERSISTED_EVENT_OCCURRENCE';
    if(result.tp1At!==null&&result.closedAt!==null&&result.tp1At>result.closedAt)conflicts.add('lifecycleChronology');}
 }
 result.conflicts=[...conflicts].sort();
 if(conflicts.size){result.integrityStatus='CONFLICT';result.status=null;result.closedAt=null;}
 else if(result.createdAt!==null&&result.timeframe!==null&&result.direction!==null&&Object.values(result.levels).every(x=>x!==null)&&result.status!==null&&(!Object.values(terminal).includes(result.status)||result.closedAt!==null))result.integrityStatus='COMPLETE';
 return result;
}

export async function rebuildLifecycleProjection(db,signalId,{rebuiltAt=Date.now(),maxFacts=4096}={}){
 assertLifecycleCapability(db,signalId);
 if(!Number.isSafeInteger(rebuiltAt)||rebuiltAt<0||!Number.isInteger(maxFacts)||maxFacts<1||maxFacts>4096)throw new Error('measurement_projection_bound');
 const revision=(await db.prepare('SELECT revision FROM measurement_lifecycle_revisions WHERE signal_id=?').bind(signalId).first())?.revision??0;
 const rows=(await db.prepare(`SELECT f.*,r.ingested_at FROM measurement_lifecycle_facts f
  JOIN measurement_ingress_recovery r ON r.event_id=f.event_id AND r.ingested_at IS NOT NULL WHERE f.signal_id=? ORDER BY f.event_id LIMIT ?`).bind(signalId,maxFacts+1).all()).results||[];
 // Work bound only, not an evidence admission/count/retention policy. All facts
 // remain immutable; a pending projection is unavailable to the collector.
 if(rows.length>maxFacts)throw new Error('measurement_projection_work_exceeded');
 if(!rows.length)return null;
 const facts=[];for(const row of rows)facts.push({...row,payload:await decodeStoredEvidence(row)});
 const p=projectLifecycle(signalId,facts);
 const conflicts=await db.prepare('SELECT c.evaluation_id FROM measurement_decision_conflicts c JOIN measurement_decision_bindings b ON b.evaluation_id=c.evaluation_id WHERE b.official_signal_id=?').bind(signalId).all();
 const pending=await db.prepare("SELECT q.state FROM measurement_decision_quarantine q JOIN measurement_decision_bindings b USING(evaluation_id) WHERE b.official_signal_id=? AND q.state='PENDING'").bind(signalId).first();
 if(pending)throw new Error('measurement_quarantine_pending');
 if(conflicts.results?.length){p.integrityStatus='CONFLICT';p.status=null;p.closedAt=null;p.conflicts.push('canonicalDecision');}
 const encoded=await encodeEvidence(p);
 if(p.availableAt>rebuiltAt)throw new Error('measurement_projection_clock_invalid');
 assertLifecycleCapability(db,signalId);
 await db.prepare(`INSERT INTO measurement_lifecycle_projection
  (signal_id,status,closed_at,created_at,timeframe,direction,entry,tp1,tp2,sl,available_at,fact_count,integrity_status,payload_blob,codec,uncompressed_length,payload_digest,rebuilt_at,source_revision)
  SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
  WHERE (SELECT COUNT(*) FROM measurement_lifecycle_facts WHERE signal_id=?)=?
  AND COALESCE((SELECT revision FROM measurement_lifecycle_revisions WHERE signal_id=?),0)=?
  ON CONFLICT(signal_id) DO UPDATE SET status=excluded.status,closed_at=excluded.closed_at,created_at=excluded.created_at,
  timeframe=excluded.timeframe,direction=excluded.direction,entry=excluded.entry,tp1=excluded.tp1,tp2=excluded.tp2,sl=excluded.sl,
  available_at=excluded.available_at,fact_count=excluded.fact_count,integrity_status=excluded.integrity_status,
  payload_blob=excluded.payload_blob,codec=excluded.codec,uncompressed_length=excluded.uncompressed_length,payload_digest=excluded.payload_digest,rebuilt_at=excluded.rebuilt_at,source_revision=excluded.source_revision
  WHERE excluded.fact_count>=measurement_lifecycle_projection.fact_count`).bind(signalId,p.status,p.closedAt,p.createdAt,p.timeframe,p.direction,
   p.levels.entry,p.levels.tp1,p.levels.tp2,p.levels.sl,p.availableAt,p.factCount,p.integrityStatus,encoded.data,encoded.codec,encoded.length,digestBytes(encoded.digest),rebuiltAt,revision,signalId,p.factCount,signalId,revision).run();
 assertLifecycleCapability(db,signalId);
 const stored=await db.prepare('SELECT p.fact_count,p.payload_digest,p.integrity_status,p.source_revision,v.revision FROM measurement_lifecycle_projection p JOIN measurement_lifecycle_revisions v USING(signal_id) WHERE p.signal_id=?').bind(signalId).first();
 if(!stored||stored.source_revision!==revision||stored.revision!==revision||stored.integrity_status!==p.integrityStatus||stored.fact_count!==p.factCount||canonicalSerialize([...stored.payload_digest])!==canonicalSerialize([...digestBytes(encoded.digest)]))throw new Error('measurement_projection_rebuild_raced');
 return p;
}
