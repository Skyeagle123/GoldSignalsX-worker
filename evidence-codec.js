// Pure, bounded lossless storage codec. No database/network/trading capabilities.
import {canonicalSerialize,parseEvidence,digestPayload} from './signal-evidence.js';
export const CODEC_VERSION='gzip-canonical-blob-v2';
export async function encodeEvidence(value,{maxBytes=65536}={}){
 const logical=canonicalSerialize(value,maxBytes),bytes=new TextEncoder().encode(logical);
 const compressed=new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer());
 return {codec:CODEC_VERSION,length:bytes.length,digest:await digestPayload(logical),data:compressed};
}
export async function decodeEvidence(envelope,{maxBytes=65536}={}){
 if(envelope.codec!==CODEC_VERSION||!Number.isInteger(envelope.length)||envelope.length<0||envelope.length>maxBytes||!(envelope.data instanceof Uint8Array)&&!Array.isArray(envelope.data)||envelope.data.length>maxBytes*2)throw new Error('measurement_codec_bound');
 const bytes=Uint8Array.from(envelope.data);
 const reader=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();
 const chunks=[];let size=0;
 try{while(true){const r=await reader.read();if(r.done)break;size+=r.value.length;if(size>maxBytes||size>envelope.length){await reader.cancel();throw new Error('measurement_decompression_bound');}chunks.push(r.value);}}finally{reader.releaseLock();}
 if(size!==envelope.length)throw new Error('measurement_codec_length');
 const out=new Uint8Array(size);let offset=0;for(const chunk of chunks){out.set(chunk,offset);offset+=chunk.length;}
 const logical=new TextDecoder('utf-8',{fatal:true}).decode(out);
 if(await digestPayload(logical)!==envelope.digest)throw new Error('measurement_integrity_conflict');
 return parseEvidence(logical);
}
// This immutable codebook is part of the capture contract. Unknown strings are
// kept literally; nothing is discarded to meet a byte target.
export const CENSUS_GATE_IDS=Object.freeze(['minimum-bars','candle-quality','atr','candle-age','receipt-age','provider-age','news-calendar','score','margin','candle-confirmation','mtf-confirmation','mtf-opposition','ny-session','pivot','price-source','price-alignment','new-candle','exposure','reservation','official-persistence']);
const STATES=['PASS','FAIL','NOT_EVALUATED','NOT_APPLICABLE'];
export const CENSUS_WORDS=[null,'buy','sell','none','OFFICIAL_PERSISTED','ENGINE_REJECTED','SKIPPED','OFFICIAL_PERSISTENCE_FAILED','CONFIRMATION_PERSISTED','accepted','blocked_opposite','confirmation','trend-up','trend-down','range','NOT_OBSERVED','NOT_EVALUATED','SUCCEEDED','FAILED'];
const word=v=>{const i=CENSUS_WORDS.indexOf(v??null);return i<0?v:i;};
const unword=v=>typeof v==='number'?CENSUS_WORDS[v]:v;
export function censusSnapshot(p){
 const e=p.engine||{},s=e.scoring||{},g=[...(e.gates||[]),...(p.callerGates||[])],unknown=[];
 let binary='';g.forEach((gate,i)=>{const id=CENSUS_GATE_IDS.indexOf(gate.id),state=STATES.indexOf(gate.result);if(id<0||state<0){unknown.push([i,gate.id,gate.result]);binary+=String.fromCharCode(255);}else binary+=String.fromCharCode(id*4+state);});
 const result={v:3,s:[s.bull??null,s.bear??null,s.margin??null,s.score??null,s.confirm??null,s.oppositions??null,e.finalConfidence??null],
 d:word(p.direction),o:word(p.outcome),l:p.levelsStatus==='COMPUTED_BY_ENGINE'?1:0,t:p.createdAt??null,g:btoa(binary),
 r:g.flatMap((gate,i)=>gate.reason==null?[]:[[i,word(gate.reason)]]),m:word(e.indicators?.marketProfile?.state),b:word(p.broadMtf?.summary?.level),
 n:word(p.admissionContext?.effectiveAdmissionDecision?.reason),p:p.exposure?.responsiblePrimaryId??null,f:word(p.officialPersistence?.performance)};
 if(p.skippedReason!=null)result.k=p.skippedReason;if(unknown.length)result.x=unknown;
 return [3,result.s,result.d,result.o,result.l,result.t,result.g,result.r,result.m,result.b,result.n,result.p,result.f,result.k??null,result.x??null];
}
export function restoreCensus(s,row={}){
 if(Array.isArray(s)&&s[0]===3){const [v,score,d,o,l,t,g,r,m,b,n,p,f,k,x]=s;s={v,s:score,d,o,l,t,g,r,m,b,n,p,f,k,x};}
 if(s.v!==3)return s;
 if(!Array.isArray(s.s)||s.s.length!==7||typeof s.g!=='string')throw new Error('measurement_census_invalid');
 const reasons=new Map(s.r||[]),unknown=new Map((s.x||[]).map(x=>[x[0],x.slice(1)]));
 const gates=Array.from(atob(s.g),(c,i)=>{const code=c.charCodeAt(0),custom=unknown.get(i);const id=custom?.[0]??CENSUS_GATE_IDS[Math.floor(code/4)],state=custom?.[1]??STATES[code%4];if(!id||!state)throw new Error('measurement_census_invalid');return [id,state,reasons.has(i)?unword(reasons.get(i)):null];});
 const outcome=unword(s.o),kind=row.kind||'CANDIDATE';
 return {evaluationId:row.evaluation_id,candidateKey:row.candidate_key??row.official_signal_id??null,officialSignalId:row.official_signal_id??null,cycleId:row.cycle_id,
 timeframe:row.timeframe,direction:unword(s.d),kind,createdAt:s.t,evaluatedAt:row.evaluated_at,outcome,skippedReason:s.k??null,
 levelsStatus:s.l?'COMPUTED_BY_ENGINE':'NOT_COMPUTED_BY_ENGINE',stage:outcome==='SKIPPED'?'NOT_EVALUATED':s.l?'TECHNICAL_CANDIDATE':'ENGINE_EVALUATED',
 measurementOnly:true,decisionUse:false,gates,engine:{scoring:{bull:s.s[0],bear:s.s[1],margin:s.s[2],score:s.s[3],confirm:s.s[4],oppositions:s.s[5]},finalConfidence:s.s[6],indicators:{marketProfile:{state:unword(s.m)}}},
 broadMtf:{summary:{level:unword(s.b)}},admissionContext:{effectiveAdmissionDecision:{reason:unword(s.n)}},exposure:{responsiblePrimaryId:s.p},officialPersistence:{performance:unword(s.f)},
 evidenceRef:`decision:${row.evaluation_id}`,cohortRef:row.cohort_id,retention:{censusDays:365,derivedDays:kind==='OFFICIAL'?null:90}};
}
export async function decodeStoredEvidence(row,options){
 if(!row)throw new Error('measurement_evidence_unavailable');
 return decodeEvidence({codec:row.codec,length:row.uncompressed_length,digest:digestHex(row.payload_digest),data:row.payload_blob},options);
}
// Definitions are immutable content-addressed records; actual operations/values
// and each attempt's clocks remain in the rich payload unchanged.
export async function normalizeDefinitions(snapshot){
 const p=parseEvidence(canonicalSerialize(snapshot)),definitions=[];
 const ledger=p.engine?.ledger;
 if(ledger){const rows=ledger.map(row=>Object.fromEntries(['componentId','version','weight','inputReferences','activationCondition'].filter(k=>Object.hasOwn(row,k)).map(k=>[k,row[k]])));
  const gateDefs=gates=>(gates||[]).map(g=>Object.fromEntries(['id','version','ordinal'].filter(k=>Object.hasOwn(g,k)).map(k=>[k,g[k]])));
  const payload={type:'DECISION_DEFINITIONS',version:1,rows,gates:gateDefs(p.engine?.gates),callerGates:gateDefs(p.callerGates)},id=`definition:${await digestPayload(canonicalSerialize(payload))}`;
  definitions.push({id,payload});p.definitionRef=id;
  for(const gates of [p.engine?.gates,p.callerGates])if(gates)for(const g of gates)for(const k of ['id','version','ordinal'])delete g[k];
  p.engine.ledger=ledger.map(row=>Object.fromEntries(Object.entries(row).filter(([k])=>!['componentId','version','weight','inputReferences','activationCondition'].includes(k))));
 }
 return {payload:p,definitions};
}
export function restoreDefinitions(payload,definitions){
 if(!payload.definitionRef)return payload;
 const d=definitions.get(payload.definitionRef);if(!d||d.rows.length!==payload.engine?.ledger?.length)throw new Error('measurement_definition_missing');
 payload.engine.ledger=payload.engine.ledger.map((row,i)=>({...d.rows[i],...row}));for(const [key,gates]of [['gates',payload.engine?.gates],['callerGates',payload.callerGates]])if(gates){if(d[key]?.length!==gates.length)throw new Error('measurement_definition_missing');const restored=gates.map((g,i)=>({...d[key][i],...g}));if(key==='gates')payload.engine.gates=restored;else payload.callerGates=restored;}delete payload.definitionRef;return payload;
}
export function compactMeasurement(p){return {subjectId:p.subjectId,eventId:p.eventId,evidenceType:p.evidenceType,eventType:p.eventType,availableAt:p.availableAt,observedAt:p.observedAt,occurredFrom:p.occurredFrom,occurredTo:p.occurredTo,outcome:p.outcome,global:p.global,
 measurementOnly:true,decisionUse:false,evidenceRef:`outcome:${p.eventId}`,coverageSummary:{coveredIntervals:p.covered?.length??0,gapIntervals:p.gaps?.length??0,coverage:p.global?.coverage??'UNKNOWN'}};}
export function restoreSharedDecision(payload,cycle,cohort,definitions){
 if(payload.sharedContextRef){if(!cycle||!cohort)throw new Error('measurement_shared_context_missing');payload.versions=cohort;payload.admissionContext=cycle.newsContext;delete payload.sharedContextRef;}
 return restoreDefinitions(payload,definitions);
}
export function reproducibility({kind,richAvailable,rawAvailable,skipped=false}){
 if(skipped||!richAvailable)return {level:'C',derived:richAvailable?'AVAILABLE':'EXPIRED_OR_NOT_CAPTURED',raw:rawAvailable?'AVAILABLE':'EXPIRED_OR_NOT_CAPTURED'};
 return {level:rawAvailable?'A':'B',derived:'AVAILABLE',raw:rawAvailable?'AVAILABLE':'EXPIRED_OR_NOT_CAPTURED',kind};
}

export function digestBytes(hex){if(!/^[a-f0-9]{64}$/.test(hex))throw new Error('measurement_digest_invalid');return Uint8Array.from(hex.match(/../g),x=>parseInt(x,16));}
export function digestHex(value){return typeof value==='string'?value:Array.from(value||[],x=>x.toString(16).padStart(2,'0')).join('');}

export const DIRECTIONAL_CODES=['SUCCESS','FAILURE','NO_DECISION','INSUFFICIENT_DATA','UNRESOLVED'];
export const EXTENDED_CODES=['TP2_REACHED','NOT_REACHED','UNRESOLVED'];
export const TERMINAL_CODES=['TP2','SL','EXPIRED','CLOSED_OTHER','ACTIVE'];
export const COVERAGE_CODES=['COMPLETE','OBSERVED_LOWER_BOUND','INSUFFICIENT'];
export const ORDERING_CODES=['EXACT_SEQUENCE','NONOVERLAPPING_INTERVALS','AMBIGUOUS','UNKNOWN'];
const code=(values,value,fallback)=>{const i=values.indexOf(value??fallback);if(i<0)throw new Error('measurement_projection_invalid');return i;};
export function finalResultColumns(p){
 const o=p.outcome||{},g=p.global||{},values=[g.mfe,g.mae,g.mfeR,g.maeR,o.timeToTp1?.minMs,o.timeToTp1?.maxMs,o.timeToSl?.minMs,o.timeToSl?.maxMs];
 let flags=0;const numeric=values.map((v,i)=>{let flag=v==null?0:Object.is(v,-0)?1:Number.isNaN(v)?2:v===Infinity?3:v===-Infinity?4:0;flags+=flag*16**i;return flag===1?0:flag>1?null:v??null;});
 return {directional:code(DIRECTIONAL_CODES,o.directionalOutcome,'UNRESOLVED'),extended:code(EXTENDED_CODES,o.extendedOutcome,'UNRESOLVED'),terminal:code(TERMINAL_CODES,o.terminalLifecycleOutcome,'ACTIVE'),
 coverage:code(COVERAGE_CODES,g.coverage,'INSUFFICIENT'),ordering:code(ORDERING_CODES,o.orderingQuality,'UNKNOWN'),horizon:p.measurementHorizonAt??null,occurred_from:p.occurredFrom??null,occurred_to:p.occurredTo??null,
 mfe:numeric[0],mae:numeric[1],mfe_r:numeric[2],mae_r:numeric[3],tp1_min:numeric[4],tp1_max:numeric[5],sl_min:numeric[6],sl_max:numeric[7],numeric_flags:flags,evidence_as_of:p.availableAt??null,reducer_version:1};
}
export function restoreFinalResult(row){
 const values=['mfe','mae','mfe_r','mae_r','tp1_min','tp1_max','sl_min','sl_max'].map((key,i)=>{const flag=Math.floor(row.numeric_flags/16**i)%16;return flag===1?-0:flag===2?NaN:flag===3?Infinity:flag===4?-Infinity:row[key]??null;});
 return {outcome:{directionalOutcome:DIRECTIONAL_CODES[row.directional],extendedOutcome:EXTENDED_CODES[row.extended],terminalLifecycleOutcome:TERMINAL_CODES[row.terminal],orderingQuality:ORDERING_CODES[row.ordering],timeToTp1:values[4]===null?null:{minMs:values[4],maxMs:values[5]},timeToSl:values[6]===null?null:{minMs:values[6],maxMs:values[7]}},global:{mfe:values[0],mae:values[1],mfeR:values[2],maeR:values[3],coverage:COVERAGE_CODES[row.coverage]},measurementHorizonAt:row.horizon,occurredFrom:row.occurred_from,occurredTo:row.occurred_to,availableAt:row.evidence_as_of,finalizedAt:row.recorded_at,reducerVersion:row.reducer_version};
}

// Versioned structural names only: values and array order remain untouched.
const OUTCOME_KEYS=['window','minutes','from','to','state','finalizationReason','coverage','covered','gaps','mfe','mae','mfeR','maeR','mfeWitness','maeWitness','lastObservationAt','observedCount','measuredTo','at','price','source','occurredFrom','occurredTo','sessionId','providerTimestamp','sequence','reason','outcome','directionalOutcome','extendedOutcome','terminalLifecycleOutcome','orderingQuality','timeToTp1','timeToSl','minMs','maxMs','evidenceAsOf','firstDirectionalWitness','global','measurementHorizonAt','sourceCompleteness','timeBasis','sequenceScope','priceBasis','eligible','level','observedAt','evaluatorVersion','coverageRef','windowReferences','barrierReferences','measurementOnly','decisionUse','marketManifest','references','blockId','start','count','availableAt','tf','ordering','measurementStatus','productionLifecycleEvidence'];
function structural(value,decode=false){if(Array.isArray(value))return value.map(x=>structural(x,decode));if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,v])=>{const index=OUTCOME_KEYS.indexOf(key);const name=decode?(key.startsWith('~')?key.slice(1):OUTCOME_KEYS[parseInt(key,36)]):index<0?'~'+key:index.toString(36);if(name===undefined)throw new Error('measurement_codec_key_invalid');return [name,structural(v,decode)];}));return value;}
const PACK_WORDS=Object.freeze([...OUTCOME_KEYS,'FINALIZED','HORIZON','TERMINAL','COMPLETE','OBSERVED_LOWER_BOUND','INSUFFICIENT','SUCCESS','FAILURE','UNRESOLVED','NO_DECISION','INSUFFICIENT_DATA','TP2_REACHED','NOT_REACHED','ACTIVE','TP2','SL','EXPIRED','CLOSED_OTHER','EXACT_SEQUENCE','UNKNOWN','INTERVAL','AMBIGUOUS','NONOVERLAPPING_INTERVALS','mt5:received-tick','mt5:closed-1m-bar','canonical-bar-extrema','mt5-session-sequence','PROVIDER_TIME_TO_RECEIPT_BOUND','SOURCE_COVERAGE_NOT_PROVEN','BARRIER_BOUNDARY_ORDER_UNKNOWN','WINDOW_FINALIZED','FINAL_MEASUREMENT','BARRIER_OBSERVATION','COVERAGE_CHECKPOINT','LIFECYCLE_EVENT','canonical-midpoint','canonical-worker-price']);
const TIME_FIELDS=new Set(['from','to','at','occurredAt','occurredFrom','occurredTo','observedAt','availableAt','providerTimestamp','lastObservationAt','measuredTo','measurementHorizonAt','evidenceAsOf']);
function packOutcome(value,base){
 const bytes=[],utf8=new TextEncoder();const put=v=>bytes.push(v),integer=n=>{do{const b=n%128;n=Math.floor(n/128);put(b+(n?128:0));}while(n);};
 const string=s=>{const i=PACK_WORDS.indexOf(s);if(i>=0){put(7);integer(i);}else{put(6);const a=utf8.encode(s);integer(a.length);bytes.push(...a);}};
 function visit(v,key){if(v===null){put(0);return;}if(v===false){put(1);return;}if(v===true){put(2);return;}
  if(typeof v==='number'){if(Number.isSafeInteger(v)&&!Object.is(v,-0)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER/2){const relative=TIME_FIELDS.has(key)&&v>1e12&&Number.isSafeInteger(base);const n=relative?v-base:v;if(!Number.isSafeInteger(n)||Math.abs(n)>Number.MAX_SAFE_INTEGER/2)throw new Error('measurement_pack_integer');put(relative?9:3);integer(n<0?-n*2-1:n*2);}else{put(4);const a=new Uint8Array(8);new DataView(a.buffer).setFloat64(0,v,false);bytes.push(...a);}return;}
  if(typeof v==='string'){string(v);return;}if(Array.isArray(v)){
   if(v.length&&v.every(x=>x&&typeof x==='object'&&!Array.isArray(x)&&Number.isSafeInteger(x.from)&&Number.isSafeInteger(x.to)&&Math.abs(x.from-base)<Number.MAX_SAFE_INTEGER/4&&Math.abs(x.to-base)<Number.MAX_SAFE_INTEGER/4&&Object.keys(x).every(k=>['from','to','reason'].includes(k)))){
    put(10);integer(v.length);let previous=base;for(const x of v){const delta=x.from-previous,width=x.to-x.from;integer(delta<0?-delta*2-1:delta*2);integer(width<0?-width*2-1:width*2);put(Object.hasOwn(x,'reason')?1:0);if(Object.hasOwn(x,'reason'))visit(x.reason,'reason');previous=x.to;}return;
   }put(5);integer(v.length);for(const x of v)visit(x,key);return;}
  if(v&&typeof v==='object'){put(8);const keys=Object.keys(v);integer(keys.length);for(const k of keys){string(k);visit(v[k],k);}return;}throw new Error('measurement_pack_invalid');
 }visit(value);return Uint8Array.from(bytes);
}
function unpackOutcome(bytes,base){let at=0;const utf8=new TextDecoder('utf-8',{fatal:true});const get=()=>{if(at>=bytes.length)throw new Error('measurement_pack_truncated');return bytes[at++];};
 const integer=()=>{let n=0,m=1;for(let i=0;i<8;i++){const b=get();n+=(b%128)*m;if(!Number.isSafeInteger(n))throw new Error('measurement_pack_integer');if(b<128)return n;m*=128;}throw new Error('measurement_pack_integer');};
 function visit(depth=0){if(depth>24)throw new Error('measurement_depth_exceeded');const tag=get();if(tag<3)return [null,false,true][tag];if(tag===3||tag===9){const n=integer(),v=n%2?-(n+1)/2:n/2;return tag===9?v+base:v;}if(tag===4){if(at+8>bytes.length)throw new Error('measurement_pack_truncated');const v=new DataView(bytes.buffer,bytes.byteOffset+at,8).getFloat64(0,false);at+=8;return v;}if(tag===6){const n=integer();if(n>bytes.length-at)throw new Error('measurement_pack_truncated');const v=utf8.decode(bytes.subarray(at,at+n));at+=n;return v;}if(tag===7){const s=PACK_WORDS[integer()];if(s===undefined)throw new Error('measurement_pack_word');return s;}if(tag===5){const n=integer();if(n>bytes.length-at)throw new Error('measurement_pack_bound');return Array.from({length:n},()=>visit(depth+1));}if(tag===10){const n=integer();if(n>bytes.length-at)throw new Error('measurement_pack_bound');let previous=base;const rows=[];for(let i=0;i<n;i++){const a=integer(),b=integer(),from=previous+(a%2?-(a+1)/2:a/2),to=from+(b%2?-(b+1)/2:b/2);const reason=get();if(reason>1)throw new Error('measurement_pack_tag');const row={from,to};if(reason)row.reason=visit(depth+1);rows.push(row);previous=to;}return rows;}if(tag===8){const n=integer();if(n>bytes.length-at)throw new Error('measurement_pack_bound');const p=Object.create(null);for(let i=0;i<n;i++){const k=visit(depth+1);if(typeof k!=='string'||Object.hasOwn(p,k))throw new Error('measurement_pack_key');p[k]=visit(depth+1);}return p;}throw new Error('measurement_pack_tag');}
 const v=visit();if(at!==bytes.length)throw new Error('measurement_pack_trailing');return v;
}
export async function encodeOutcomeEvidence(p,{maxBytes=65536}={}){
 const logical=canonicalSerialize(p,maxBytes),body=JSON.parse(logical);
 for(const k of ['subjectId','eventId','eventType','availableAt','measurementOnly','decisionUse'])delete body[k];
 const packed=packOutcome(body,p.availableAt);if(packed.length>maxBytes)throw new Error('measurement_payload_exceeded');
 const data=new Uint8Array(await new Response(new Blob([packed]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer());
 return {codec:'gzip-outcome-binary-v4',length:new TextEncoder().encode(logical).length,digest:await digestPayload(logical),data};
}
export async function decodeStoredOutcome(row,{maxBytes=65536}={}){
 if(!row.payload_blob)return parseEvidence(row.payload_json);
 if(row.codec!=='gzip-outcome-binary-v4'||row.uncompressed_length>maxBytes)throw new Error('measurement_codec_bound');
 const stream=new Blob([Uint8Array.from(row.payload_blob)]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();let length=0,chunks=[];
 try{while(true){const r=await stream.read();if(r.done)break;length+=r.value.length;if(length>maxBytes){await stream.cancel();throw new Error('measurement_decompression_bound');}chunks.push(r.value);}}finally{stream.releaseLock();}
 const bytes=new Uint8Array(length);let at=0;for(const c of chunks){bytes.set(c,at);at+=c.length;}
 const body=unpackOutcome(bytes,row.available_at);
 const p={...body,measurementOnly:true,decisionUse:false,subjectId:row.subject_id,eventId:row.event_id,evidenceType:body.evidenceType??(row.event_type==='TP1'||row.event_type==='TP2'||row.event_type==='SL'?'BARRIER_OBSERVATION':row.event_type),eventType:row.event_type,availableAt:row.available_at};
 const text=canonicalSerialize(p,maxBytes);if(new TextEncoder().encode(text).length!==row.uncompressed_length||await digestPayload(text)!==digestHex(row.payload_digest))throw new Error('measurement_integrity_conflict');return parseEvidence(text);
}

export function hydrateFinalOutcome(p,events){
 if(p.evidenceType!=='FINAL_MEASUREMENT')return p;const result={...p};
 const get=id=>{const w=events.find(e=>e.eventId===id)?.window;if(!w)throw new Error('measurement_window_reference_missing');return w;};
 if(p.globalReference){const w=get(p.globalReference);result.global=Object.fromEntries(['mfe','mae','mfeR','maeR','mfeWitness','maeWitness','coverage'].map(k=>[k,w[k]]));}
 if(p.coverageReference){const w=get(p.coverageReference);result.covered=w.covered;result.gaps=w.gaps;}
 result.quality={windows:Object.fromEntries((p.windowReferences||[]).map(id=>[id.slice(id.lastIndexOf(':')+1),get(id)]))};return result;
}

export async function decodeProcessingState(row){return row?.payload_blob?decodeStoredEvidence(row):row?parseEvidence(row.payload_json):null;}
