// Pure bounded observations. Bar extrema are intervals, never invented tick times.
import {mergeIntervals,accumulateQuality} from './quality-measurement.js';
import {reduceOutcome} from './forward-validation.js';
const finite=Number.isFinite;
export function foldPostEntry(subject,previous,{ticks=[],bars=[],asOf,availableAt=asOf}){
 const prior=previous||{processedThrough:subject.createdAt,covered:[],gaps:[],barriers:{},global:{mfe:null,mae:null}};
 const end=finite(subject.closedAt)?Math.min(subject.closedAt,asOf):asOf;
 const observations=[],covered=[],uncertainties=(prior.gaps||[]).filter(g=>g.reason==='BARRIER_BOUNDARY_ORDER_UNKNOWN'),barriers={...prior.barriers};
 const boundaryTicks=new Set(prior.processedBoundaryTicks||[]);
 const tickIdentity=t=>`${t.ts}:${t.price}:${t.measurement?.sessionId??''}:${t.measurement?.sequence??''}`;
 const events=[];const touch=(type,price,witness)=>{
  const target=subject[type.toLowerCase()];const hit=subject.side==='sell'?(type==='SL'?price>=target:price<=target):(type==='SL'?price<=target:price>=target);
  const key=`${type}:${witness.source}`;
  if(!hit||barriers[key]||!finite(target))return;
  const event={...witness,eventId:`${subject.id}:barrier:${type}:${witness.source}`,subjectId:subject.id,evidenceType:'BARRIER_OBSERVATION',
   eventType:type,level:target,price,eligible:true,availableAt,observedAt:witness.observedAt??availableAt,evaluatorVersion:'b1-witness-v1',measurementOnly:true,decisionUse:false};
  barriers[key]=event;event.coverageRef=`${subject.id}:coverage:${asOf}`;events.push(event);
 };
 const market=[];
 for(const tick of ticks){
  const metadata=tick.measurement;if(!metadata||metadata.provider!=='mt5'||!finite(tick.ts)||!finite(tick.price)||tick.ts>end)continue;
  market.push({from:tick.ts,to:tick.ts,kind:'tick',tick});
 }
 for(const bar of bars){
  if(bar.provider!=='mt5'||bar.t<subject.createdAt||bar.t+60000>end||bar.t+60000<=prior.processedThrough)continue;
  if(![bar.o,bar.h,bar.l,bar.c].every(finite))continue;
  market.push({from:bar.t,to:bar.t+60000,kind:'bar',bar});
 }
 market.sort((a,b)=>a.to-b.to||a.from-b.from);let lastTick=null;
 for(const row of market){
  if(row.kind==='tick'){
   const {tick}=row,m=tick.measurement;
   if(lastTick){const p=lastTick.measurement;
    // Canonical-input continuity requires consecutive sequence, same session and
    // bounded receipt/provider cadence. Sequence alone cannot bridge an outage.
    // This certifies received input coverage, never completeness of broker ticks.
    if(p.sessionId&&p.sessionId===m.sessionId&&Number.isSafeInteger(p.sequence)&&m.sequence===p.sequence+1
     &&tick.ts>=lastTick.ts&&tick.ts-lastTick.ts<=20000
     &&finite(p.providerTimestamp)&&finite(m.providerTimestamp)
     &&m.providerTimestamp>=p.providerTimestamp&&m.providerTimestamp-p.providerTimestamp<=20000)
     covered.push({from:Math.max(subject.createdAt,lastTick.ts),to:tick.ts});
   }lastTick=tick;
   if(tick.ts<subject.createdAt||tick.ts<prior.processedThrough||(tick.ts===prior.processedThrough&&boundaryTicks.has(tickIdentity(tick))))continue;
   const providerAt=finite(m.providerTimestamp)?m.providerTimestamp:null;
   if(providerAt===null||Math.min(providerAt,tick.ts)<subject.createdAt||Math.max(providerAt,tick.ts)>end){
    const possibleTouch=['TP1','TP2','SL'].some(type=>{const level=subject[type.toLowerCase()];return finite(level)&&(subject.side==='sell'?(type==='SL'?tick.price>=level:tick.price<=level):(type==='SL'?tick.price<=level:tick.price>=level));});
    if(possibleTouch)uncertainties.push({from:subject.createdAt,to:tick.ts,reason:'BARRIER_BOUNDARY_ORDER_UNKNOWN'});
    continue;
   }
   const witness={occurredFrom:Math.min(providerAt,tick.ts),occurredTo:Math.max(providerAt,tick.ts),observedAt:tick.ts,timeBasis:'PROVIDER_TIME_TO_RECEIPT_BOUND',sourceCompleteness:'UNKNOWN',source:'mt5:received-tick',providerTimestamp:m.providerTimestamp,sessionId:m.sessionId,
    sequence:m.sequence,sequenceScope:m.sessionId?'mt5-session-sequence':null,priceBasis:m.priceBasis,
    orderingQuality:Number.isSafeInteger(m.sequence)&&m.sessionId?'EXACT_SEQUENCE':'UNKNOWN'};
   observations.push({at:tick.ts,price:tick.price,source:witness.source,occurredFrom:witness.occurredFrom,occurredTo:witness.occurredTo,sessionId:m.sessionId??null,providerTimestamp:m.providerTimestamp,sequence:m.sequence??null});
   for(const type of ['TP1','TP2','SL'])touch(type,tick.price,witness);
  }else{
   const {bar}=row;covered.push({from:bar.t,to:bar.t+60000});
   const witness={occurredFrom:bar.t,occurredTo:bar.t+60000,source:'mt5:closed-1m-bar',priceBasis:'canonical-bar-extrema',
    orderingQuality:'INTERVAL',sessionId:bar.sessionId??null,sequence:null};
   observations.push({at:bar.t+60000,price:bar.h,source:witness.source,occurredFrom:bar.t,occurredTo:bar.t+60000},
    {at:bar.t+60000,price:bar.l,source:witness.source,occurredFrom:bar.t,occurredTo:bar.t+60000});
   for(const type of ['TP1','TP2','SL'])touch(type,subject.side==='sell'?(type==='SL'?bar.h:bar.l):(type==='SL'?bar.l:bar.h),witness);
  }
 }
 // A bar witness includes all barrier touches in its interval. Evaluator ordering is NOT market ordering.
 const coverage=mergeIntervals([...prior.covered,...covered]).slice(-32);
 const gaps=[...uncertainties.slice(-16)];let coveredTo=subject.createdAt;for(const interval of coverage){if(interval.from>coveredTo)gaps.push({from:coveredTo,to:interval.from,reason:'SOURCE_COVERAGE_NOT_PROVEN'});coveredTo=Math.max(coveredTo,interval.to);}if(coveredTo<end)gaps.push({from:coveredTo,to:end,reason:'SOURCE_COVERAGE_NOT_PROVEN'});
 const through=Math.min(end,Math.max(prior.processedThrough,...market.map(x=>x.to)));
 const previousQuality=prior.quality?structuredClone(prior.quality):null;if(previousQuality)for(const w of Object.values(previousQuality.windows))if(w.state!=='FINALIZED')w.gaps=[];
 const quality=accumulateQuality(subject,previousQuality,observations,{asOf,processedThrough:through,certifiedIntervals:coverage,gaps:gaps.slice(0,32)});
 const global={...prior.global};for(const observation of observations){
  const move=subject.side==='sell'?subject.entry-observation.price:observation.price-subject.entry;
  const f=Math.max(0,move),a=Math.max(0,-move);if(global.mfe===null||f>global.mfe){global.mfe=f;global.mfeWitness=observation;}
  if(global.mae===null||a>global.mae){global.mae=a;global.maeWitness=observation;}
 }
 const risk=Math.abs(subject.entry-subject.sl);global.mfeR=global.mfe!==null&&risk?global.mfe/risk:null;global.maeR=global.mae!==null&&risk?global.mae/risk:null;
 let outcome=reduceOutcome(subject,Object.values(barriers),{asOf:availableAt,measurementEndAt:end,coverageIntervals:coverage,coverageGaps:gaps});
 if(['SUCCESS','FAILURE'].includes(prior.outcome?.directionalOutcome)&&outcome.directionalOutcome!==prior.outcome.directionalOutcome){outcome={...outcome,directionalOutcome:prior.outcome.directionalOutcome,firstDirectionalWitness:prior.outcome.firstDirectionalWitness,timeToTp1:prior.outcome.timeToTp1,timeToSl:prior.outcome.timeToSl,reason:'PREVIOUSLY_PROVEN_FIRST_BARRIER_PRESERVED'};}
 global.coverage=outcome.coverage==='COMPLETE'?'COMPLETE':global.mfe!==null?'OBSERVED_LOWER_BOUND':'INSUFFICIENT';
 const processedBoundaryTicks=[...new Set([...(through===prior.processedThrough?prior.processedBoundaryTicks||[]:[]),...ticks.filter(t=>t.ts===through).map(tickIdentity)])].slice(-32);
 // Freeze the coverage actually available at discovery. This is not a recovery
 // accumulator and must never be replaced by coverage collected later.
 const coverageId=`${subject.id}:coverage:${availableAt}:${Object.keys(barriers).length}`;
 for(const event of events)event.coverageRef=coverageId;
 if(events.length)events.unshift({subjectId:subject.id,eventId:coverageId,
  evidenceType:'COVERAGE_CHECKPOINT',eventType:'COVERAGE_CHECKPOINT',checkpointRole:'BARRIER_COVERAGE',
  occurredFrom:subject.createdAt,occurredTo:end,availableAt,observedAt:availableAt,
  covered:coverage,gaps:gaps.slice(0,32),sourceCompleteness:'UNKNOWN',
  evaluatorVersion:'b1-witness-v1',measurementOnly:true,decisionUse:false});
 return {state:{subjectId:subject.id,processedThrough:through,processedBoundaryTicks,covered:coverage,gaps:gaps.slice(0,32),barriers,quality,global,outcome},events};
}
