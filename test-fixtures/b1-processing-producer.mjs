// Test-only extraction of the preserved genuine Package-B synthetic producer.
// Uses accepted Engine/M1 exports; no Package-B driver, state or cloud code.
import * as engine from '../signal-engine.js';
import * as sm from '../signal-measurement.js';
import {prepareCycleEnvelopes} from '../measurement-producer.js';
import {buildMeasurementEnvelope} from '../measurement-envelope.js';
import {prepareMarketEvidence} from '../measurement-market-envelope.js';
import {fragmentMeasurementPacket} from '../measurement-transport-fragment.js';
const RUNTIME_SHA='687ee3d5bfb4355bc26b50762b73f7d984ab5cef';
// Synthetic inputs go through accepted Engine/journal/producer functions. No trading persistence.
export async function syntheticWorkload(worker,{runId,tick,at,variant='normal'}){
 const frames={};
 for(const [tf,n]of Object.entries({'1m':2000,'5m':600,'15m':300,'30m':200,'60m':120,'240m':80,'1d':60})){
  const step=engine.SIGNAL_TF_MS[tf],times=[];for(let t=Math.floor(at/step)*step-step;times.length<n;t-=step)if(worker.isGoldMarketOpen(t))times.push(t);times.reverse();
  frames[tf]={tf,provider:'mt5',bars:times.map((t,i)=>{const c=variant==='rejected'?4100:4100+(i-n+1)*.35;return {t,o:variant==='rejected'?c:c-.3,h:c+.1,l:c-.4,c,v:1,provider:'mt5'};})};frames[tf].quality=engine.evaluateCandleQuality(frames[tf].bars,tf);
 }
 const news=worker.mergeSignalRiskContext({ok:true,stale:false,cache:'hit',source:'gdelt',updatedAt:at-31000,safety:{blockTechnicalSignal:false}},{ok:true,stale:false,cache:'hit',source:'official-calendar',updatedAt:at-21000,fetchedAt:at-20500,events:[]},at,{cycleId:`${runId}:${tick}`,cycleStartedAt:at});
 const live={price:4100,ts:at-113,receivedAt:at-47,source:'mt5'},filters={nyFilterOn:false,pivotFilterOn:false};
 sm.rememberDecisionQuote(live,{...live,bid:4099.913,ask:4100.087,midpoint:4100,spread:.174,sessionId:runId,sequence:tick});
 const j=sm.beginDecisionCycle(at,frames,live,news,filters);j.capturedAt=at;j.failedOfficialId=null;j.candidates=[];j.evaluations=new Map();
 for(const [tf,q]of Object.entries(frames)){
  const trace={},includedMtf=engine.HIGHER_SIGNAL_TIMEFRAMES[tf]??[],result=engine.computeServerSignal(q.bars,{tf,mtf:includedMtf.map(k=>frames[k]),live,barsSource:'d1',evaluationAt:at,news,dataQuality:q.quality,filters},trace);
  sm.observeAttempt(j,{tf,trace,result,requestedMtf:includedMtf,includedMtf,callerGates:[{id:'new-candle',result:'PASS',operands:{signalBarTs:result.lastTs,existingSignalId:null},ordinal:trace.gates.length}]});j.evaluations.set(tf,{...result,provider:q.provider,quality:q.quality,evaluatedAt:at});
  if(['buy','sell'].includes(result.side))j.candidates.push({id:`${runId}:${tick}:${tf}:${result.side}`,tf,side:result.side,entry:result.entry,tp1:result.tp1,tp2:result.tp2,sl:result.sl,conf:result.conf,score:result.score,signalBarTs:result.lastTs,createdAt:at,status:'active'});
 }
 for(const c of j.candidates)c.mtfAtEntry=worker.createMtfAtEntrySnapshot(c.tf,j.evaluations.get(c.tf).mtf,(engine.HIGHER_SIGNAL_TIMEFRAMES[c.tf]??[]).map(tf=>frames[tf]),at);
 j.broadMatrices=new Map(j.candidates.map(c=>[c.id,worker.buildMtfDirectionMatrix(c,j.evaluations)]));
 const trace={},exposure=worker.decideGoldExposure(null,{now:at,candidates:j.candidates,officialSignals:[]},trace);j.exposureResult={...exposure,measurementBefore:null,measurementTrace:trace};j.decisions=new Map(exposure.decisions.map(d=>[d.signalId,d]));j.officialIds=new Set(exposure.decisions.filter(d=>d.decision==='accepted').map(d=>d.signalId));j.confirmationIds=new Set(exposure.decisions.filter(d=>d.decision==='confirmation').map(d=>d.signalId));
 // This is an observed synthetic persistence result, not a claim about trading Production.
 for(const c of j.candidates)if(j.officialIds.has(c.id))sm.rememberOfficialPersistence(c,'created',{ok:true});
 const packets=[...await prepareCycleEnvelopes(sm.decisionJournalObservations(j),{codeCommit:RUNTIME_SHA,measurementEffectiveAt:at-1,deploymentId:'package-b-synthetic'},{preparedAt:at})];
 const primary=[...j.officialIds][0];for(const id of j.confirmationIds)if(primary)packets.push(await buildMeasurementEnvelope({kind:'CONFIRMATION_LINK',semanticId:`${primary}:confirmation:${id}`,occurredAt:at,observedAt:null,preparedAt:at,payload:{primarySignalId:primary,confirmationSignalId:id,link:{signalId:id,timeframe:j.candidates.find(c=>c.id===id).tf},linkPersistence:'SUCCEEDED',measurementOnly:true,decisionUse:false}}));
 const cohort=packets[0].envelope.payload.records.find(r=>r.type==='cohort').values.cohort_id;
 const market=await prepareMarketEvidence({cohort,source:'mt5',session:runId,ticks:[{ts:at,price:4100,measurement:{provider:'mt5',sessionId:runId,sequence:tick,providerTimestamp:at}}],bars:[],from:at,to:at,observedAt:at,gaps:[{reason:'SYNTHETIC_BOUNDED_SAMPLE; SOURCE_COMPLETENESS_UNKNOWN'}]});
 const messages=[];for(const p of packets)for(const body of await fragmentMeasurementPacket(p,{receivedAt:at}))messages.push({body,kind:p.envelope.kind,eventId:p.envelope.eventId});for(const p of market)messages.push({body:JSON.stringify({version:'measurement-market-queue-v1',id:p.id,wire:p.wire}),kind:'MARKET_EXPORT',eventId:p.id});
 return {messages,packets,market,cohort,officials:[...j.officialIds],candidates:j.candidates.map(c=>c.id),createdAt:at};
}
