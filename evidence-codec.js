// Pure, bounded lossless storage codec. No database/network/trading capabilities.
import {canonicalSerialize,parseEvidence,digestPayload} from './signal-evidence.js';
export const CODEC_VERSION='gzip-canonical-v1';
export async function encodeEvidence(value,{maxBytes=65536}={}){
 const logical=canonicalSerialize(value,maxBytes),bytes=new TextEncoder().encode(logical);
 const compressed=new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer());
 let binary='';for(const b of compressed)binary+=String.fromCharCode(b);
 return {codec:CODEC_VERSION,length:bytes.length,digest:await digestPayload(logical),data:btoa(binary)};
}
export async function decodeEvidence(envelope,{maxBytes=65536}={}){
 if(envelope.codec!==CODEC_VERSION||!Number.isInteger(envelope.length)||envelope.length<0||envelope.length>maxBytes||typeof envelope.data!=='string'||envelope.data.length>maxBytes*2)throw new Error('measurement_codec_bound');
 const binary=atob(envelope.data),bytes=Uint8Array.from(binary,c=>c.charCodeAt(0));
 const reader=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();
 const chunks=[];let size=0;
 try{while(true){const r=await reader.read();if(r.done)break;size+=r.value.length;if(size>maxBytes||size>envelope.length){await reader.cancel();throw new Error('measurement_decompression_bound');}chunks.push(r.value);}}finally{reader.releaseLock();}
 if(size!==envelope.length)throw new Error('measurement_codec_length');
 const out=new Uint8Array(size);let offset=0;for(const chunk of chunks){out.set(chunk,offset);offset+=chunk.length;}
 const logical=new TextDecoder('utf-8',{fatal:true}).decode(out);
 if(await digestPayload(logical)!==envelope.digest)throw new Error('measurement_integrity_conflict');
 return parseEvidence(logical);
}
export function censusSnapshot(p){
 const e=p.engine||{},s=e.scoring||{};
 return {evaluationId:p.evaluationId,candidateKey:p.candidateKey,officialSignalId:p.officialSignalId,cycleId:p.cycleId,
  timeframe:p.timeframe,direction:p.direction,kind:p.kind,createdAt:p.createdAt,evaluatedAt:p.evaluatedAt,
  outcome:p.outcome,skippedReason:p.skippedReason,levelsStatus:p.levelsStatus,measurementOnly:true,decisionUse:false,
  gates:[...(e.gates||[]),...(p.callerGates||[])].map(g=>[g.id,g.result,g.reason??null]),
  engine:{scoring:{bull:s.bull??null,bear:s.bear??null,margin:s.margin??null,score:s.score??null,confirm:s.confirm??null,oppositions:s.oppositions??null},finalConfidence:e.finalConfidence??null,
   indicators:{marketProfile:{state:e.indicators?.marketProfile?.state??null}},...(p.kind==='OFFICIAL'?{levels:e.levels??null}:{})},
  broadMtf:{summary:{level:p.broadMtf?.summary?.level??null}},
  admissionContext:{effectiveAdmissionDecision:{reason:p.admissionContext?.effectiveAdmissionDecision?.reason??null}},
  exposure:{responsiblePrimaryId:p.exposure?.responsiblePrimaryId??null},officialPersistence:p.officialPersistence??null,
  inputDependencies:[...new Set([p.inputManifest?.primary,...(p.inputManifest?.engineMtf||[]),p.inputManifest?.research1m].flatMap(m=>m?.references?.map(r=>r.blockId)||[]))],evidenceRef:`decision:${p.evaluationId}`,cohortRef:p.versions?.cohortId??null,retention:{censusDays:365,derivedDays:p.kind==='OFFICIAL'?null:90}};
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
