import assert from 'node:assert/strict';
import {test} from 'node:test';
import {accumulateQuality,emptyQuality,legacyWindowQuality} from './quality-measurement.js';
import {reduceOutcome,compareOccurrence,summarizeOutcomes} from './forward-validation.js';
const subject={id:'fixture',createdAt:1000,entry:100,sl:90,side:'buy',status:'active'};
const obs=(at,price)=>({at,price});
const event=(eventType,at,extra={})=>({eventId:`${eventType}:${at}`,evidenceType:'BARRIER_OBSERVATION',eventType,occurredAt:at,availableAt:at,eligible:true,...extra});
const outcome=(events,status='expired',extra={})=>reduceOutcome({...subject,status,closedAt:90000},events,{asOf:100000,coverageIntervals:[{from:1000,to:100000}],...extra});

test('active windows update until horizons; no premature finalization',()=>{
 let state=accumulateQuality(subject,null,[obs(2000,101)],{asOf:11000,processedThrough:11000});
 assert.equal(state.windows['1m'].state,'COLLECTING');
 state=accumulateQuality(subject,state,[obs(2000,101),obs(20000,104)],{asOf:21000,processedThrough:21000});
 assert.equal(state.windows['1m'].mfe,4);assert.equal(state.windows['1m'].observedCount,2);
 state=accumulateQuality(subject,state,[obs(50000,103)],{asOf:61000,processedThrough:61000,certifiedIntervals:[{from:1000,to:61000}]});
 assert.equal(state.windows['1m'].state,'FINALIZED');assert.equal(state.windows['1m'].coverage,'COMPLETE');
 for(const w of ['5m','15m','30m','60m'])assert.equal(state.windows[w].state,'COLLECTING');
});
test('terminal closure and coverage are independent',()=>{
 const state=accumulateQuality({...subject,status:'sl',closedAt:11000},null,[obs(2000,99)],{asOf:12000,processedThrough:11000});
 for(const w of Object.values(state.windows)){assert.equal(w.state,'FINALIZED');assert.equal(w.finalizationReason,'TERMINAL');assert.equal(w.coverage,'OBSERVED_LOWER_BOUND');}
});
test('eviction, retry, gaps and empty observations do not fabricate extrema',()=>{
 let state=accumulateQuality(subject,null,[obs(2000,120)],{asOf:3000,processedThrough:3000});
 state=accumulateQuality(subject,state,[obs(4000,101)],{asOf:5000,processedThrough:5000,gaps:[{from:3000,to:4000,reason:'lost'}]});
 assert.equal(state.windows['60m'].mfe,20);assert.equal(state.windows['60m'].coverage,'OBSERVED_LOWER_BOUND');
 const retry=accumulateQuality(subject,state,[obs(4000,101)],{asOf:5000,processedThrough:5000});
 assert.equal(retry.windows['60m'].observedCount,2);
 const empty=accumulateQuality(subject,null,[],{asOf:61000,processedThrough:61000});
 assert.equal(empty.windows['1m'].mfe,null);assert.equal(empty.windows['1m'].mae,null);assert.equal(empty.windows['1m'].coverage,'INSUFFICIENT');
 assert.equal(legacyWindowQuality({status:'closed_early',finalized:true}).nominalWindowUsable,false);
});
test('every horizon finalizes independently once observations processed',()=>{
 for(const minutes of [1,5,15,30,60]){
 const at=subject.createdAt+minutes*60000;
 const pending=accumulateQuality(subject,null,[],{asOf:at,processedThrough:at-1});assert.equal(pending.windows[`${minutes}m`].state,'COLLECTING');
 const ended=accumulateQuality(subject,pending,[],{asOf:at,processedThrough:at});assert.equal(ended.windows[`${minutes}m`].state,'FINALIZED');
 }
});
test('TP1 then SL remains success; TP1 then TP2 extended success',()=>{
 assert.equal(outcome([event('TP1',2000),event('SL',3000)],'sl').directionalOutcome,'SUCCESS');
 const two=outcome([event('TP1',2000),event('TP2',3000)],'tp2');assert.equal(two.directionalOutcome,'SUCCESS');assert.equal(two.extendedOutcome,'TP2_REACHED');
});
test('SL before TP1 fails; expiry with/without coverage distinguishes no decision',()=>{
 assert.equal(outcome([event('SL',2000),event('TP1',3000)],'sl').directionalOutcome,'FAILURE');
 assert.equal(outcome([]).directionalOutcome,'NO_DECISION');
 assert.equal(outcome([], 'expired',{coverageIntervals:[]}).directionalOutcome,'INSUFFICIENT_DATA');
});
test('ambiguous bar order and insertion order cannot establish direction',()=>{
 const events=['TP1','SL'].map(t=>event(t,2000,{occurredAt:null,occurredFrom:1500,occurredTo:2500}));
 assert.equal(outcome(events).directionalOutcome,'INSUFFICIENT_DATA');assert.deepEqual(outcome(events),outcome(events.toReversed()));
 assert.equal(compareOccurrence(...events),'AMBIGUOUS');
});
test('exact scoped sequence resolves equal timestamps',()=>{
 const common={orderingQuality:'EXACT_SEQUENCE',sequenceScope:'mt5-ingest',sessionId:'session'};
 assert.equal(outcome([event('TP1',2000,{...common,sequence:1}),event('SL',2000,{...common,sequence:2})],'sl').directionalOutcome,'SUCCESS');
 assert.equal(compareOccurrence(event('TP1',2000,{...common,sequence:1}),event('SL',2000,{...common,sessionId:'other',sequence:2})),'AMBIGUOUS');
});
test('delayed discovery and TP2 implication preserve occurrence bounds',()=>{
 const r=outcome([event('TP2',2000,{observedAt:5000,availableAt:6000})],'tp2');
 assert.equal(r.directionalOutcome,'SUCCESS');assert.equal(r.timeToTp1.minMs,1000);assert.equal(r.firstDirectionalWitness.eventType,'TP2');
});
test('gap after proven success cannot revoke success; extended absence needs coverage',()=>{
 const r=outcome([event('TP1',2000)],'sl',{coverageIntervals:[{from:1000,to:3000}],coverageGaps:[{from:3000,to:90000}]});
 assert.equal(r.directionalOutcome,'SUCCESS');assert.equal(r.extendedOutcome,'UNRESOLVED');
});
test('active unresolved; no division fabricated; availability/horizon respected',()=>{
 assert.equal(outcome([],'active').directionalOutcome,'UNRESOLVED');assert.equal(summarizeOutcomes([]).directionalAccuracy,null);
 assert.equal(outcome([event('TP1',2000,{availableAt:100001})],'active').directionalOutcome,'UNRESOLVED');
 assert.equal(outcome([event('SL',999)],'active').directionalOutcome,'UNRESOLVED');
});
