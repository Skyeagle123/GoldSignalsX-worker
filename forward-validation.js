import { covers } from './quality-measurement.js';

export const OUTCOME_REDUCER_VERSION='tp1-first-v1';
const finite=x=>typeof x==='number'&&Number.isFinite(x);
const TERMINAL={tp2:'TP2',sl:'SL',stopped:'SL',expired:'EXPIRED',closed_other:'CLOSED_OTHER'};

export function eventInterval(event) {
  if(finite(event.occurredAt)) return {from:event.occurredAt,to:event.occurredAt};
  if(finite(event.occurredFrom)&&finite(event.occurredTo)&&event.occurredTo>=event.occurredFrom)
    return {from:event.occurredFrom,to:event.occurredTo};
  return null;
}

export function compareOccurrence(a,b) {
  const x=eventInterval(a),y=eventInterval(b);
  if(!x||!y)return 'UNKNOWN';
  if(x.to<y.from)return 'BEFORE';
  if(y.to<x.from)return 'AFTER';
  if(a.orderingQuality==='EXACT_SEQUENCE'&&b.orderingQuality==='EXACT_SEQUENCE'
    &&a.sequenceScope&&a.sequenceScope===b.sequenceScope&&a.sessionId===b.sessionId
    &&Number.isSafeInteger(a.sequence)&&Number.isSafeInteger(b.sequence)&&a.sequence!==b.sequence)
    return a.sequence<b.sequence?'BEFORE':'AFTER';
  return 'AMBIGUOUS';
}

function earliest(rows) {
  // Sorting intervals does not resolve overlapping intervals. All potentially
  // first witnesses remain candidates until sequence/interval evidence resolves them.
  return rows.filter(a=>!rows.some(b=>a!==b&&compareOccurrence(b,a)==='BEFORE'));
}

export function reduceOutcome(subject,events,{asOf,coverageIntervals=[],coverageGaps=[]}={}) {
  if(!finite(asOf)||!finite(subject.createdAt))throw new Error('outcome_time_invalid');
  const terminalLifecycleOutcome=finite(subject.closedAt)&&subject.closedAt>asOf?'ACTIVE':TERMINAL[subject.status]||'ACTIVE';
  const closed=terminalLifecycleOutcome!=='ACTIVE'&&finite(subject.closedAt);
  const end=closed?Math.min(subject.closedAt,asOf):asOf;
  const covered=to=>covers(coverageIntervals,subject.createdAt,to)
    &&!coverageGaps.some(g=>g.from<to&&g.to>subject.createdAt);
  const witnesses=events.filter(e=>e.evidenceType==='BARRIER_OBSERVATION'&&e.eligible===true
    &&finite(e.availableAt)&&e.availableAt<=asOf&&eventInterval(e)
    &&eventInterval(e).from>=subject.createdAt&&eventInterval(e).to<=end);
  const sl=earliest(witnesses.filter(e=>e.eventType==='SL'));
  const tp2=earliest(witnesses.filter(e=>e.eventType==='TP2'));
  const tp1=earliest(witnesses.filter(e=>e.eventType==='TP1'||e.eventType==='TP2'));
  let directionalOutcome='UNRESOLVED',reason='NO_PROVEN_FIRST_BARRIER',first=null;
  const firstCandidates=earliest([...tp1,...sl]);
  const success=tp1.find(t=>covered(eventInterval(t).to)&&sl.every(s=>compareOccurrence(t,s)==='BEFORE'));
  const failure=sl.find(s=>covered(eventInterval(s).to)&&tp1.every(t=>compareOccurrence(s,t)==='BEFORE'));
  if(success){directionalOutcome='SUCCESS';reason='TP1_BEFORE_SL';first=success;}
  else if(failure){directionalOutcome='FAILURE';reason='SL_BEFORE_TP1';first=failure;}
  else if(firstCandidates.length||closed){
    directionalOutcome=!firstCandidates.length&&covered(end)?'NO_DECISION':'INSUFFICIENT_DATA';
    reason=directionalOutcome==='NO_DECISION'?'CLOSED_WITHOUT_DIRECTIONAL_TOUCH':'ORDERING_OR_COVERAGE_INSUFFICIENT';
  }else if(coverageGaps.some(g=>g.from<end&&g.to>subject.createdAt)){
    directionalOutcome='INSUFFICIENT_DATA';reason='MATERIAL_COVERAGE_GAP';
  }
  const extended=tp2.find(t=>sl.every(s=>compareOccurrence(t,s)==='BEFORE')&&covered(eventInterval(t).to));
  const extendedOutcome=extended?'TP2_REACHED':closed&&covered(end)&&!tp2.length?'NOT_REACHED':'UNRESOLVED';
  return {directionalOutcome,extendedOutcome,terminalLifecycleOutcome,reason,
    outcomeReducerVersion:OUTCOME_REDUCER_VERSION,evidenceAsOf:asOf,
    firstDirectionalWitness:first?{eventId:first.eventId,eventType:first.eventType,...eventInterval(first)}:null,
    timeToTp1:first&&directionalOutcome==='SUCCESS'?{minMs:eventInterval(first).from-subject.createdAt,maxMs:eventInterval(first).to-subject.createdAt}:null,
    timeToSl:first&&directionalOutcome==='FAILURE'?{minMs:eventInterval(first).from-subject.createdAt,maxMs:eventInterval(first).to-subject.createdAt}:null,
    coverage:covered(end)?'COMPLETE':'INSUFFICIENT'};
}

export function summarizeOutcomes(records) {
  const directional=Object.fromEntries(['SUCCESS','FAILURE','NO_DECISION','INSUFFICIENT_DATA','UNRESOLVED'].map(k=>[k,0]));
  const extended=Object.fromEntries(['TP2_REACHED','NOT_REACHED','UNRESOLVED'].map(k=>[k,0]));
  const terminal=Object.fromEntries(['TP2','SL','EXPIRED','CLOSED_OTHER','ACTIVE'].map(k=>[k,0]));
  const byTimeframe={};let missingEntry=0;
  for(const r of records){directional[r.outcome.directionalOutcome]++;extended[r.outcome.extendedOutcome]++;terminal[r.outcome.terminalLifecycleOutcome]++;
    const bucket=byTimeframe[r.timeframe]||(byTimeframe[r.timeframe]={...Object.fromEntries(Object.keys(directional).map(k=>[k,0])),total:0});
    bucket[r.outcome.directionalOutcome]++;bucket.total++;if(!r.entryEvidence)missingEntry++;
  }
  const proven=directional.SUCCESS+directional.FAILURE;
  return {totalOfficial:records.length,directional,extended,terminal,provenDirectional:proven,
    directionalAccuracy:proven?directional.SUCCESS/proven:null,byTimeframe,missingEntryEvidence:missingEntry};
}
