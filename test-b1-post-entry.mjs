import assert from 'node:assert/strict';
import {test} from 'node:test';
import {foldPostEntry} from './post-entry-evidence.js';
const subject={id:'local-subject',createdAt:60000,entry:100,tp1:110,tp2:120,sl:90,side:'buy',status:'active'};
const tick=(ts,price,sequence)=>({ts,price,measurement:{provider:'mt5',sessionId:'local-session',sequence,providerTimestamp:ts,priceBasis:'canonical-midpoint'}});
test('incremental tick extrema survive eviction, retries and horizons',()=>{
 let folded=foldPostEntry(subject,null,{asOf:70000,ticks:[tick(60000,100,1),tick(65000,109,2)],bars:[]});
 assert.equal(folded.state.quality.windows['1m'].state,'COLLECTING');assert.equal(folded.state.global.mfe,9);
 folded=foldPostEntry(subject,folded.state,{asOf:90000,ticks:[tick(80000,101,3)],bars:[]});
 assert.equal(folded.state.global.mfe,9);
 const retry=foldPostEntry(subject,folded.state,{asOf:90000,ticks:[tick(80000,101,3)],bars:[]});assert.equal(retry.events.length,0);
});
test('sequence continuity/source changes do not fabricate complete coverage',()=>{
 const r=foldPostEntry(subject,null,{asOf:70000,ticks:[tick(60000,100,1),tick(65000,111,3)],bars:[]});
 assert.equal(r.state.outcome.directionalOutcome,'INSUFFICIENT_DATA');assert.equal(r.state.global.coverage,'OBSERVED_LOWER_BOUND');
 const witness=r.events.find(e=>e.evidenceType==='BARRIER_OBSERVATION');assert.equal(witness.occurredFrom,65000);assert.equal(witness.sequence,3);
});
test('barrier intervals preserve ambiguity and no partial full-bar contamination',()=>{
 const bar={t:60000,o:100,h:121,l:89,c:100,provider:'mt5'};
 const r=foldPostEntry(subject,null,{asOf:120000,bars:[bar],ticks:[]});
 assert.equal(r.state.outcome.directionalOutcome,'INSUFFICIENT_DATA');
 for(const event of r.events){assert.equal(event.occurredAt,undefined);assert.equal(event.occurredFrom,60000);assert.equal(event.occurredTo,120000);}
 const late=foldPostEntry({...subject,createdAt:60001},null,{asOf:120000,bars:[bar],ticks:[]});assert.equal(late.events.length,0);assert.equal(late.state.global.mfe,null);
});
test('TP1 then SL is success when exact covered sequence proves order',()=>{
 const r=foldPostEntry({...subject,status:'sl',closedAt:68000},null,{asOf:70000,ticks:[tick(60000,100,1),tick(65000,111,2),tick(68000,89,3)],bars:[]});
 assert.equal(r.state.outcome.directionalOutcome,'SUCCESS');assert.equal(r.state.outcome.terminalLifecycleOutcome,'SL');
});
test('consecutive sequence cannot certify an outage or provider-clock gap',()=>{
 for(const second of [tick(100000,111,2),{...tick(65000,111,2),measurement:{...tick(65000,111,2).measurement,providerTimestamp:100000}}]){
  const r=foldPostEntry(subject,null,{asOf:110000,ticks:[tick(60000,100,1),second],bars:[]});
  assert.equal(r.state.outcome.directionalOutcome,'INSUFFICIENT_DATA');assert.equal(r.state.global.coverage,'OBSERVED_LOWER_BOUND');
 }
});
test('new same-receipt-time sequence updates extrema; duplicate retry does not',()=>{
 const first=foldPostEntry(subject,null,{asOf:65000,ticks:[tick(60000,100,1),tick(65000,105,2)],bars:[]});
 const next=foldPostEntry(subject,first.state,{asOf:65000,ticks:[tick(65000,105,2),tick(65000,111,3)],bars:[]});
 assert.equal(next.state.global.mfe,11);assert.equal(next.state.quality.windows['1m'].mfe,11);assert.equal(next.events.filter(e=>e.evidenceType==='BARRIER_OBSERVATION').length,1);
 const retry=foldPostEntry(subject,next.state,{asOf:65000,ticks:[tick(65000,105,2),tick(65000,111,3)],bars:[]});assert.equal(retry.events.length,0);assert.equal(retry.state.quality.windows['1m'].observedCount,next.state.quality.windows['1m'].observedCount);
});
test('pre-entry/receipt-straddling barrier cannot be silently discarded to prove later success',()=>{
 const stale={...tick(61000,89,2),measurement:{...tick(61000,89,2).measurement,providerTimestamp:59000}};
 const result=foldPostEntry(subject,null,{asOf:65000,ticks:[tick(60000,100,1),stale,tick(65000,111,3)],bars:[]});
 assert.equal(result.state.outcome.directionalOutcome,'INSUFFICIENT_DATA');assert(result.state.gaps.some(g=>g.reason==='BARRIER_BOUNDARY_ORDER_UNKNOWN'));
 const next=foldPostEntry(subject,result.state,{asOf:70000,ticks:[tick(65000,111,3),tick(70000,112,4)],bars:[]});assert.equal(next.state.outcome.directionalOutcome,'INSUFFICIENT_DATA');
});
