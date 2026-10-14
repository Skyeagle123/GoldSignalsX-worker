import {identity,need,time,serialize,hash,bytes,LIMITS,canonicalSerialize,parseEvidence} from './measurement-independent-contract.js';
// Separately versioned, already-observed samples. No Engine calls or inferred tick order.
export const MARKET_VERSION='measurement-market-v1';
export async function prepareMarketEvidence({cohort,source,session,ticks=[],bars=[],from,to,observedAt,truncated=false,gaps=[]}){
 identity(cohort);identity(source);if(session!==null)identity(session);time(from);time(to);time(observedAt);need(from<=to&&to<=observedAt&&ticks.length<=2400&&bars.length<=2000&&gaps.length<=32,'pa_market_bound');
 const samples=[];
 for(const t of ticks){need(Number.isFinite(t.price)&&t.price>0&&Number.isSafeInteger(t.ts)&&t.ts<=observedAt,'pa_tick');const m=t.measurement;need(m&&session!==null&&m.sessionId===session&&m.provider===source&&Number.isSafeInteger(m.sequence)&&m.sequence>=0&&Number.isFinite(m.providerTimestamp)&&m.providerTimestamp>=from&&m.providerTimestamp<=to,'pa_tick_provenance');
  const identityMaterial=[MARKET_VERSION,cohort,source,session,m.sequence,m.providerTimestamp];samples.push({id:'tick:'+await hash(identityMaterial),type:'TICK',value:t,occurredAt:m.providerTimestamp,observedAt:t.ts});}
 for(const b of bars){need(b.provider===source&&[b.o,b.h,b.l,b.c].every(Number.isFinite)&&b.l<=Math.min(b.o,b.c)&&b.h>=Math.max(b.o,b.c)&&Number.isSafeInteger(b.t)&&b.t>=from&&b.t+60000<=to&&(b.sessionId??null)===session,'pa_bar');samples.push({id:'bar:'+await hash([MARKET_VERSION,cohort,source,session,b.t]),type:'CLOSED_BAR',value:b,occurredAt:b.t,observedAt:Number.isSafeInteger(b.availableAt)?b.availableAt:null,exportObservedAt:observedAt,sourceReceiptBasis:Number.isSafeInteger(b.availableAt)?'CAPTURED_SOURCE_AVAILABILITY':'UNAVAILABLE_IN_SOURCE',ordering:'INTERVAL_ONLY'});}
 const packets=[];let group=[];
 const finish=async()=>{if(!group.length&&packets.length)return;const payload={version:MARKET_VERSION,cohort,source,session,from,to,observedAt,samples:group,gaps,truncated,coverage:'OBSERVED_SOURCE_ONLY',brokerTickCompleteness:'UNKNOWN',measurementOnly:true,decisionUse:false};const wire=serialize(payload,LIMITS.rowBytes);packets.push(Object.freeze({id:'market:'+await hash(wire),wire,bytes:bytes(wire)}));group=[];};
 for(const s of samples){if(bytes(serialize([...group,s],LIMITS.eventBytes))>48000)await finish();group.push(s);}await finish();return Object.freeze(packets);
}
export async function validateMarketEvidence(wire){need(typeof wire==='string'&&bytes(wire)<=LIMITS.rowBytes,'pa_market_input_bound');const p=parseEvidence(wire);need(p?.version===MARKET_VERSION&&p.measurementOnly===true&&p.decisionUse===false&&bytes(wire)<=LIMITS.rowBytes,'pa_market_version');
 const rebuilt=await prepareMarketEvidence({...p,ticks:p.samples.filter(s=>s.type==='TICK').map(s=>s.value),bars:p.samples.filter(s=>s.type==='CLOSED_BAR').map(s=>s.value)});
 need(rebuilt.length===1&&rebuilt[0].wire===wire,'pa_market_identity');return p;}
