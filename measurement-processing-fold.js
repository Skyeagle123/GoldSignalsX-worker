// Versioned pagination adapter. Occurrence comparisons retain the accepted
// interval/sequence reducer; traversal keys never establish a barrier winner.
import {need,hash} from './measurement-independent-contract.js';
import {mergeIntervals,covers,emptyQuality} from './quality-measurement.js';
import {compareOccurrence,reduceOutcome,eventInterval} from './forward-validation.js';
import {capturedRecord,capturedRecordById,validatedCapturedRecord,freshEnumeration,sampleSelectionKey,compareSampleKeys} from './measurement-processing-samples.js';

const types=['TP1','TP2','SL'];
const touch=(subject,type,price)=>Number.isFinite(subject[type.toLowerCase()])&&(subject.side==='sell'?(type==='SL'?price>=subject.sl:price<=subject[type.toLowerCase()]):(type==='SL'?price<=subject.sl:price>=subject[type.toLowerCase()]));
export function sampleObservations(subject,s,end){
 const v=s.value;if(s.type==='TICK'){
  const m=v.measurement;if(m.provider!=='mt5'||v.ts<subject.createdAt||v.ts>end)return null;
  if(!Number.isFinite(m.providerTimestamp)||Math.min(m.providerTimestamp,v.ts)<subject.createdAt||Math.max(m.providerTimestamp,v.ts)>end)return {uncertain:types.some(t=>touch(subject,t,v.price)),to:v.ts,observations:[],witnesses:[]};
  const witness={occurredFrom:Math.min(m.providerTimestamp,v.ts),occurredTo:Math.max(m.providerTimestamp,v.ts),observedAt:v.ts,timeBasis:'PROVIDER_TIME_TO_RECEIPT_BOUND',sourceCompleteness:'UNKNOWN',source:'mt5:received-tick',providerTimestamp:m.providerTimestamp,sessionId:m.sessionId,sequence:m.sequence,sequenceScope:m.sessionId?'mt5-session-sequence':null,priceBasis:m.priceBasis??null,orderingQuality:Number.isSafeInteger(m.sequence)&&m.sessionId?'EXACT_SEQUENCE':'UNKNOWN'};
  return {to:v.ts,tick:v,observations:[{at:v.ts,price:v.price,source:witness.source,occurredFrom:witness.occurredFrom,occurredTo:witness.occurredTo,sessionId:m.sessionId,providerTimestamp:m.providerTimestamp,sequence:m.sequence}],witnesses:types.filter(t=>touch(subject,t,v.price)).map(type=>({...witness,type,price:v.price}))};
 }
 if(v.provider!=='mt5'||v.t<subject.createdAt||v.t+60000>end)return null;
 const witness={occurredFrom:v.t,occurredTo:v.t+60000,source:'mt5:closed-1m-bar',priceBasis:'canonical-bar-extrema',orderingQuality:'INTERVAL',sessionId:v.sessionId??null,sequence:null};
 return {to:v.t+60000,interval:{from:v.t,to:v.t+60000},observations:[v.h,v.l].map(price=>({at:v.t+60000,price,source:witness.source,occurredFrom:v.t,occurredTo:v.t+60000})),witnesses:types.flatMap(type=>{const price=subject.side==='sell'?(type==='SL'?v.h:v.l):(type==='SL'?v.l:v.h);return touch(subject,type,price)?[{...witness,type,price}]:[];})};
}
export function emptySampleFold(subject){return {subjectId:subject.id,processedThrough:subject.createdAt,covered:[],coverageOverflow:false,boundaryUnknown:false,quality:emptyQuality(subject),global:{mfe:null,mae:null},lastTick:null,minimum:{},minimumEnd:{},counts:{TP1:0,TP2:0,SL:0},outcome:null,barriers:{},gaps:[]};}
function addCoverage(fold,interval){const rows=mergeIntervals([...fold.covered,interval]);if(rows.length>32)fold.coverageOverflow=true;fold.covered=rows.slice(-32);}
function extrema(subject,target,o){const move=subject.side==='sell'?subject.entry-o.price:o.price-subject.entry,f=Math.max(0,move),a=Math.max(0,-move);if(target.mfe===null||f>target.mfe){target.mfe=f;target.mfeWitness=o;}if(target.mae===null||a>target.mae){target.mae=a;target.maeWitness=o;}}
export async function foldSelectedSample(subject,fold,s,key,{asOf,availableAt}){
 const end=Math.min(subject.closedAt??asOf,asOf),part=sampleObservations(subject,s,end);if(!part)return;
 fold.processedThrough=Math.max(fold.processedThrough,part.to);if(part.uncertain)fold.boundaryUnknown=true;
 if(part.tick){const v=part.tick,m=v.measurement,p=fold.lastTick;
  if(p&&p.measurement.sessionId===m.sessionId&&m.sequence===p.measurement.sequence+1&&v.ts>=p.ts&&v.ts-p.ts<=20000&&m.providerTimestamp>=p.measurement.providerTimestamp&&m.providerTimestamp-p.measurement.providerTimestamp<=20000)addCoverage(fold,{from:Math.max(subject.createdAt,p.ts),to:v.ts});
  fold.lastTick={ts:v.ts,measurement:{sessionId:m.sessionId,sequence:m.sequence,providerTimestamp:m.providerTimestamp}};
 }
 if(part.interval)addCoverage(fold,part.interval);
 const seen=new Set();for(const o of part.observations){const id=o.at+':'+o.price;if(seen.has(id))continue;seen.add(id);extrema(subject,fold.global,o);for(const w of Object.values(fold.quality.windows)){if(o.at<w.from||o.at>Math.min(w.to,end))continue;extrema(subject,w,o);w.lastObservationAt=o.at;w.observedCount++;}}
 for(const w of part.witnesses){const event={...w,eventId:subject.id+':sample-barrier:'+s.id+':'+w.type,subjectId:subject.id,evidenceType:'BARRIER_OBSERVATION',eventType:w.type,level:subject[w.type.toLowerCase()],eligible:true,availableAt,observedAt:w.observedAt??availableAt,evaluatorVersion:'pa-sample-witness-v2',measurementOnly:true,decisionUse:false,key};
  fold.counts[w.type]++;fold.minimumEnd[w.type]=Math.min(fold.minimumEnd[w.type]??Infinity,w.occurredTo);
  if(!fold.minimum[w.type]||compareSampleKeys(key,fold.minimum[w.type].key)<0)fold.minimum[w.type]=event;
 }
}
export function finalizeSampleMetrics(subject,fold,asOf){
 const end=Math.min(subject.closedAt??asOf,asOf),gaps=[];let through=subject.createdAt;
 for(const interval of fold.covered){if(interval.from>through)gaps.push({from:through,to:interval.from,reason:'SOURCE_COVERAGE_NOT_PROVEN'});through=Math.max(through,interval.to);}
 if(through<end)gaps.push({from:through,to:end,reason:'SOURCE_COVERAGE_NOT_PROVEN'});
 if(fold.coverageOverflow)gaps.push({from:subject.createdAt,to:end,reason:'coverage_interval_budget_exhausted'});
 if(fold.boundaryUnknown)gaps.push({from:subject.createdAt,to:end,reason:'BARRIER_BOUNDARY_ORDER_UNKNOWN'});
 // A bounded conservative gap envelope cannot accidentally forget a gap.
 fold.gaps=gaps.length<=32?gaps:[{from:Math.min(...gaps.map(g=>g.from)),to:Math.max(...gaps.map(g=>g.to)),reason:'SOURCE_COVERAGE_NOT_PROVEN'}];
 const risk=Math.abs(subject.entry-subject.sl);
 for(const target of [fold.global,...Object.values(fold.quality.windows)]){target.mfeR=target.mfe!==null&&risk?target.mfe/risk:null;target.maeR=target.mae!==null&&risk?target.mae/risk:null;}
 for(const w of Object.values(fold.quality.windows)){
  const stop=Math.min(w.to,end);w.covered=mergeIntervals(fold.covered.map(i=>({from:Math.max(i.from,w.from),to:Math.min(i.to,stop)})));
  w.gaps=fold.gaps.filter(g=>g.to>=w.from&&g.from<=stop);const complete=covers(w.covered,w.from,stop)&&!w.gaps.some(g=>g.from<stop&&g.to>w.from);
  w.coverage=complete?'COMPLETE':w.mfe!==null?'OBSERVED_LOWER_BOUND':'INSUFFICIENT';
  if(asOf>=w.to&&fold.processedThrough>=stop||subject.closedAt!=null&&subject.closedAt<w.to&&fold.processedThrough>=stop){w.state='FINALIZED';w.finalizationReason=subject.closedAt!=null&&subject.closedAt<w.to?'TERMINAL':'HORIZON';w.measuredTo=stop;}
 }
 fold.quality.processedThrough=fold.processedThrough;
}
const inBucket=(w,b)=>b==='TP1'?['TP1','TP2'].includes(w.eventType):w.eventType===b;
const opposite=b=>b==='SL'?'TP1':'SL';
const covered=(subject,fold,w)=>covers(fold.covered,subject.createdAt,w.occurredTo)&&!fold.gaps.some(g=>g.from<w.occurredTo&&g.to>subject.createdAt);
const cursor=()=>({enumeration:freshEnumeration(),recordId:null,index:0,type:0});
export function freshWitnessVerification(){return {phase:'FAST',fast:0,walk:cursor(),ownBefore:false,oppositeBad:false,winners:{},checks:{},initialized:false,outer:cursor(),candidate:null,inner:null,opponents:null,opponent:null,dominance:null,anyMinimal:false};}

// Fixed nested cursors allow exact first-hit verification without an unbounded
// antichain of incomparable witnesses. No buffer saturation becomes ambiguity.
async function nextWitness(db,archive,state,subject,c,budget,cache,pin=state.pin){
 while(budget.remaining>0||cache.has(c.recordId)){
  let record,p;
  if(c.recordId){const saved=cache.get(c.recordId);if(saved){({record,p}=saved);}else{if(!budget.remaining)return undefined;if(!budget.invocation)budget.remaining--;record=await capturedRecordById(db,archive,state.pin,c.recordId);need(record&&record.digest===c.recordDigest,'pa_processing_witness_missing');p=await validatedCapturedRecord(record,state.pin);cache.set(record.id,{record,p});}}
  else{record=await capturedRecord(db,archive,pin,c.enumeration,budget);if(record===undefined)return undefined;if(!record)return null;p=await validatedCapturedRecord(record,state.pin);c.recordId=record.id;c.recordDigest=record.digest;c.index=0;c.type=0;cache.set(record.id,{record,p});}
  while(c.index<p.samples.length){const s=p.samples[c.index],part=sampleObservations(subject,s,Math.min(subject.closedAt??state.pin.asOf,state.pin.asOf));const list=part?.witnesses??[];
   if(c.type<list.length){const w=list[c.type++];return {...w,eventId:subject.id+':sample-barrier:'+s.id+':'+w.type,subjectId:subject.id,eventType:w.type,evidenceType:'BARRIER_OBSERVATION',eligible:true,availableAt:record.available_at,key:await sampleSelectionKey(p,s)};}
   c.index++;c.type=0;
  }
  c.recordId=null;
 }
 return undefined;
}
function win(v,b,w){if(!v.winners[b]||compareSampleKeys(w.key,v.winners[b].key)<0)v.winners[b]=w;}
function outcomeFromProof(subject,state){
 const f=state.fold,v=state.verification,end=Math.min(subject.closedAt??state.pin.asOf,state.pin.asOf);
 const events=Object.values(f.minimum),base=reduceOutcome(subject,events,{asOf:state.pin.asOf,measurementEndAt:end,coverageIntervals:f.covered,coverageGaps:f.gaps});
 const first=v.winners.TP1??v.winners.SL;
 if(first){base.directionalOutcome=v.winners.TP1?'SUCCESS':'FAILURE';base.reason=v.winners.TP1?'TP1_BEFORE_SL':'SL_BEFORE_TP1';base.firstDirectionalWitness={eventId:first.eventId,eventType:first.eventType,...eventInterval(first)};base.timeToTp1=v.winners.TP1?{minMs:first.occurredFrom-subject.createdAt,maxMs:first.occurredTo-subject.createdAt}:null;base.timeToSl=v.winners.SL?{minMs:first.occurredFrom-subject.createdAt,maxMs:first.occurredTo-subject.createdAt}:null;}
 else{base.firstDirectionalWitness=null;base.timeToTp1=null;base.timeToSl=null;const hits=f.counts.TP1+f.counts.TP2+f.counts.SL;if(hits){base.directionalOutcome='INSUFFICIENT_DATA';base.reason='ORDERING_OR_COVERAGE_INSUFFICIENT';}else{Object.assign(base,reduceOutcome(subject,[],{asOf:state.pin.asOf,measurementEndAt:end,coverageIntervals:f.covered,coverageGaps:f.gaps}));}}
 if(v.winners.TP2)base.extendedOutcome='TP2_REACHED';else base.extendedOutcome=base.terminalLifecycleOutcome!=='ACTIVE'&&base.coverage==='COMPLETE'&&!f.counts.TP2?'NOT_REACHED':'UNRESOLVED';
 f.outcome=base;f.global.coverage=base.coverage==='COMPLETE'?'COMPLETE':f.global.mfe!==null?'OBSERVED_LOWER_BOUND':'INSUFFICIENT';f.barriers=Object.fromEntries(Object.values(v.winners).map(w=>[w.eventId,w]));return base;
}
export async function verifySampleOutcome(db,archive,state,subject,budget){
 const v=state.verification,f=state.fold,cache=new Map();let comparisons=0;
 const next=(c,pin)=>nextWitness(db,archive,state,subject,c,budget,cache,pin);
 const buckets=['TP1','SL','TP2'];
 while(comparisons++<512){
  if(v.phase==='FAST'){
   const b=buckets[v.fast];if(!b){
    // No eligible witness can win if its minimum interval end exceeds the
    // certified initial coverage component (or a material gap begins before it).
    const possible=buckets.filter(k=>{const m=k==='TP1'?Math.min(f.minimumEnd.TP1??Infinity,f.minimumEnd.TP2??Infinity):f.minimumEnd[k]??Infinity;return covered(subject,f,{occurredTo:m});});
    if(possible.every(k=>v.winners[k])){v.phase='DONE';continue;}
    v.phase='OUTER';continue;
   }
   const a=b==='TP1'?[f.minimum.TP1,f.minimum.TP2].filter(Boolean).sort((x,y)=>compareSampleKeys(x.key,y.key))[0]:f.minimum[b];
   if(!a||!covered(subject,f,a)){v.fast++;v.walk=cursor();continue;}
   if(!v.initialized){const old=state.priorChecks?.[b];v.incremental=old?.id===a.eventId&&!!state.selectionPin;if(v.incremental){v.ownBefore=old.ownBefore;v.oppositeBad=old.oppositeBad;}v.initialized=true;}
   const w=await next(v.walk,v.incremental?state.selectionPin:state.pin);if(w===undefined)return false;if(w===null){v.checks[b]={id:a.eventId,ownBefore:v.ownBefore,oppositeBad:v.oppositeBad};if(!v.ownBefore&&!v.oppositeBad)win(v,b,a);v.fast++;v.walk=cursor();v.ownBefore=false;v.oppositeBad=false;v.initialized=false;continue;}
   if(inBucket(w,b)&&compareOccurrence(w,a)==='BEFORE')v.ownBefore=true;
   if(inBucket(w,opposite(b))&&compareOccurrence(a,w)!=='BEFORE')v.oppositeBad=true;
   continue;
  }
  if(v.phase==='OUTER'){
   if(v.extraCandidate){v.candidate=v.extraCandidate;v.extraCandidate=null;v.bucket='TP2';v.inner=cursor();v.phase='MINIMAL';continue;}
   const w=await next(v.outer);if(w===undefined)return false;if(w===null){v.phase='DONE';continue;}
   v.candidate=w;v.bucket=w.eventType==='SL'?'SL':'TP1';if(w.eventType==='TP2')v.extraCandidate=w;v.inner=cursor();v.phase='MINIMAL';continue;
  }
  if(v.phase==='MINIMAL'){
   const w=await next(v.inner);if(w===undefined)return false;
   if(w&&inBucket(w,v.bucket)&&compareOccurrence(w,v.candidate)==='BEFORE'){v.phase='OUTER';continue;}
   if(w===null){v.anyMinimal=true;if(!covered(subject,f,v.candidate)){v.phase='OUTER';continue;}v.opponents=cursor();v.phase='OPPOSITES';}continue;
  }
  if(v.phase==='OPPOSITES'){
   const w=await next(v.opponents);if(w===undefined)return false;if(w===null){win(v,v.bucket,v.candidate);v.phase='OUTER';continue;}
   if(!inBucket(w,opposite(v.bucket))||compareOccurrence(v.candidate,w)==='BEFORE')continue;
   v.opponent=w;v.dominance=cursor();v.phase='DOMINANCE';continue;
  }
  if(v.phase==='DOMINANCE'){
   const w=await next(v.dominance);if(w===undefined)return false;
   if(w&&inBucket(w,opposite(v.bucket))&&compareOccurrence(w,v.opponent)==='BEFORE'){v.phase='OPPOSITES';continue;}
   if(w===null)v.phase='OUTER';continue;
  }
  if(v.phase==='DONE'){outcomeFromProof(subject,state);return true;}
 }
 return false;
}
