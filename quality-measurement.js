// Measurement only. This module has no storage, clocks, network or trading capability.
export const QUALITY_VERSION = 2;
export const WINDOWS = Object.freeze([1, 5, 15, 30, 60]);
const finite = value => typeof value === 'number' && Number.isFinite(value);

export function mergeIntervals(intervals) {
  const rows = intervals.filter(x => finite(x.from) && finite(x.to) && x.to >= x.from)
    .map(x => ({from:x.from,to:x.to})).sort((a,b) => a.from-b.from || a.to-b.to);
  const merged=[];
  for (const row of rows) {
    const last=merged.at(-1);
    if (last && row.from<=last.to) last.to=Math.max(last.to,row.to);
    else merged.push(row);
  }
  return merged;
}

export function covers(intervals,from,to) {
  return finite(from)&&finite(to)&&to>=from&&mergeIntervals(intervals).some(x=>x.from<=from&&x.to>=to);
}

export function emptyQuality(subject) {
  return {schema:QUALITY_VERSION,subjectId:subject.id,createdAt:subject.createdAt,
    processedThrough:subject.createdAt,windows:Object.fromEntries(WINDOWS.map(minutes=>[`${minutes}m`,{
      minutes,from:subject.createdAt,to:subject.createdAt+minutes*60000,state:'COLLECTING',
      finalizationReason:null,coverage:'INSUFFICIENT',covered:[],gaps:[],mfe:null,mae:null,
      mfeWitness:null,maeWitness:null,lastObservationAt:null,observedCount:0
    }]))};
}

// certifiedIntervals describe input-stream coverage supplied by the collector;
// observation density alone MUST NOT fabricate coverage. Constant-size extrema
// and a bounded interval list survive eviction from the existing live tick ring.
export function accumulateQuality(subject,previous,observations,{asOf,processedThrough,
  certifiedIntervals=[],gaps=[],maxIntervals=32}={}) {
  if (!finite(asOf)||!finite(subject.createdAt)) throw new Error('measurement_time_invalid');
  const state=structuredClone(previous?.schema===QUALITY_VERSION?previous:emptyQuality(subject));
  if(state.subjectId!==subject.id||state.createdAt!==subject.createdAt) throw new Error('measurement_subject_conflict');
  const terminal=['tp2','sl','stopped','expired','closed_other'].includes(subject.status)
    &&finite(subject.closedAt)&&subject.closedAt<=asOf;
  const end=terminal?subject.closedAt:asOf;
  const through=finite(processedThrough)?Math.min(processedThrough,asOf):state.processedThrough;
  for (const window of Object.values(state.windows)) {
    if(window.state==='FINALIZED') continue;
    const effectiveEnd=Math.min(window.to,end);
    const rows=observations.filter(x=>finite(x.at)&&finite(x.price)&&x.at>=subject.createdAt&&x.at<=effectiveEnd)
      .sort((a,b)=>a.at-b.at);
    const seen=new Set();
    for(const row of rows) {
      const identity=`${row.at}:${row.price}`;if(seen.has(identity))continue;seen.add(identity);
      // All observations through the previous cursor have already been folded.
      if(row.at<state.processedThrough||(row.at===state.processedThrough&&(state.processedBoundaryObservations||[]).includes(identity))) continue;
      const move=subject.side==='sell'?subject.entry-row.price:row.price-subject.entry;
      const favorable=Math.max(0,move),adverse=Math.max(0,-move);
      if(window.mfe===null||favorable>window.mfe){window.mfe=favorable;window.mfeWitness={...row};}
      if(window.mae===null||adverse>window.mae){window.mae=adverse;window.maeWitness={...row};}
      window.lastObservationAt=row.at;window.observedCount++;
    }
    const intervals=certifiedIntervals.map(x=>({from:Math.max(x.from,window.from),to:Math.min(x.to,effectiveEnd)}));
    const merged=mergeIntervals([...window.covered,...intervals]);
    if(merged.length>maxIntervals) {
      // Forgetting intervals can only weaken coverage; never bridge a gap.
      window.gaps.push({from:window.from,to:effectiveEnd,reason:'coverage_interval_budget_exhausted'});
    }
    window.covered=merged.slice(-maxIntervals);
    window.gaps=[...window.gaps,...gaps.filter(x=>x.to>=window.from&&x.from<=effectiveEnd)]
      .slice(-maxIntervals);
    const complete=covers(window.covered,window.from,effectiveEnd)&&!window.gaps.some(x=>x.from<effectiveEnd&&x.to>window.from);
    window.coverage=complete?'COMPLETE':window.mfe!==null?'OBSERVED_LOWER_BOUND':'INSUFFICIENT';
    const ended=terminal&&subject.closedAt<window.to;
    if((ended||asOf>=window.to)&&through>=effectiveEnd) {
      window.state='FINALIZED';window.finalizationReason=ended?'TERMINAL':'HORIZON';window.measuredTo=effectiveEnd;
    }
    const risk=Math.abs(subject.entry-subject.sl);
    window.mfeR=window.mfe!==null&&risk>0?window.mfe/risk:null;
    window.maeR=window.mae!==null&&risk>0?window.mae/risk:null;
  }
  const boundaryAt=Math.max(state.processedThrough,through);
  state.processedBoundaryObservations=[...new Set([...(boundaryAt===state.processedThrough?state.processedBoundaryObservations||[]:[]),...observations.filter(o=>o.at===boundaryAt).map(o=>`${o.at}:${o.price}`)])].slice(-32);
  state.processedThrough=boundaryAt;
  return state;
}

export function legacyWindowQuality(window) {
  return {measurementVersion:1,nominalWindowUsable:false,
    reason:window?.finalized&&window?.status==='closed_early'?'LEGACY_PREMATURE_FINALIZATION':'LEGACY_COVERAGE_NOT_PROVEN'};
}
