// Measurement-only sidecar. No trading, Exposure, Telegram or Risk capabilities.
import {canonicalSerialize,parseEvidence,digestPayload,cohortManifest,decisionSnapshot,exposureEvidenceState} from './signal-evidence.js';
import {buildMarketManifest,sliceMarketManifest} from './market-evidence.js';
import {measurementWriter} from './signal-evidence-store.js';
const journals=new WeakMap(),quotes=new WeakMap(),persistenceEvidence=new WeakMap();
export function rememberOfficialPersistence(signal,event,result){
 try{if(event==='created')persistenceEvidence.set(signal,{performance:result?.ok===true?'SUCCEEDED':'FAILED',reason:result?.error??null});}catch{}
 return result;
}
export function rememberDecisionQuote(result,quote){try{quotes.set(result,{canonicalPrice:result.price,bid:quote.bid??null,ask:quote.ask??null,midpoint:quote.midpoint??null,spread:quote.spread??null,providerTimestamp:quote.ts??null,receivedAt:quote.receivedAt??null,provider:quote.source??null,sessionId:quote.sessionId??null,sequence:quote.sequence??null,priceBasis:'ACTUAL_CANONICAL_PRICE_USED'});}catch{}return result;}
export function beginDecisionCycle(now,frames,live,news,filters){
 try{return {cycleId:news?.admissionContext?.cycle?.cycleId||`cycle:${now}`,cycleStartedAt:news?.admissionContext?.cycle?.cycleStartedAt??now,
  evaluatedAt:now,frames,live,quoteEvidence:quotes.get(live)??null,newsContext:news?.admissionContext??null,filters,attempts:[]};}catch{return null;}
}
export function observeAttempt(journal,value){try{if(journal&&journal.attempts.length<7)journal.attempts.push(value);}catch{}}
export function finishDecisionCycle(journal,result,{decisions,candidates,officialIds,confirmationIds,exposureResult,signals,evaluations,broadMatrices,failedOfficialId=null}={}){
 try{if(journal){journal.decisions=decisions;journal.candidates=candidates;journal.officialIds=officialIds;
  journal.confirmationIds=confirmationIds;journal.exposureResult=exposureResult;journal.signals=signals;journal.evaluations=evaluations;journal.broadMatrices=broadMatrices;journal.failedOfficialId=failedOfficialId;
  journals.set(result,journal);}}catch{}return result;
}
export function takeDecisionCycle(result){const journal=journals.get(result);journals.delete(result);return journal;}
export async function persistDecisionCycle(db,journal,provenance,{maxWrites=160,maxBytes=65536}={}){
 if(!journal)return {ok:true,skipped:'no_evaluated_cycle'};
 try{
  const configFingerprint=await digestPayload(canonicalSerialize(journal.filters));
  const cohort=cohortManifest({...provenance,configFingerprint});
  const cohortDigest=await digestPayload(canonicalSerialize(cohort));
  const cohortStorageRef=btoa(String.fromCharCode(...cohortDigest.match(/../g).map(x=>parseInt(x,16)))).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');
  const writer=measurementWriter(db,{maxWrites,maxPayloadBytes:maxBytes});
  const capturedAt=journal.capturedAt??(journal.capturedAt=Date.now());
  const manifests={},blocks=new Map();
  const consumed=new Set(journal.attempts.filter(x=>x.trace).flatMap(x=>[x.tf,...x.includedMtf]));
  for(const tf of consumed){
   const built=await buildMarketManifest(tf,journal.frames[tf].bars,journal.evaluatedAt);
   manifests[tf]=built.manifest;for(const block of built.blocks)blocks.set(block.blockId,block);
  }
  const optional=journal.frames['1m']?.bars;
  if(optional?.length){let built;
   if(manifests['1m'])built={manifest:manifests['1m'],blocks:[]};else built=await buildMarketManifest('1m',optional,journal.evaluatedAt);
   manifests.research1m=sliceMarketManifest(built.manifest,Math.max(0,optional.length-60),Math.min(60,optional.length));
   const required=new Set(manifests.research1m.references.map(r=>r.blockId));for(const block of built.blocks)if(required.has(block.blockId))blocks.set(block.blockId,block);
  }
  // Size validation occurs before any persistence. No effect on trading.
  const snapshots=journal.attempts.map((attempt,ordinal)=>{
   const candidate=journal.candidates?.find(c=>c.tf===attempt.tf);
   const decision=candidate?journal.decisions?.get(candidate.id):null;
   const snapshot=decisionSnapshot({...attempt,cycleId:journal.cycleId,ordinal,live:journal.live,
    bars:journal.frames[attempt.tf].bars,createdAt:candidate?.createdAt??null,decision,
    admissionContext:journal.newsContext,officialPersisted:journal.officialIds?.has(candidate?.id)||false,actualOfficialSignalId:candidate?.id??null,
    confirmationPersisted:journal.confirmationIds?.has(candidate?.id)||false,
    competingCandidateIds:journal.candidates?.map(x=>x.id)||[],exposureBefore:exposureEvidenceState(journal.exposureResult?.measurementBefore),officialPersistenceFailed:journal.failedOfficialId===candidate?.id,
    exposureAfter:exposureEvidenceState(journal.exposureResult?.state)});
   snapshot.capturedAt=capturedAt;snapshot.versions=cohort;
   snapshot.officialPersistence=candidate?persistenceEvidence.get(candidate)??{performance:'NOT_OBSERVED'}:{performance:'NOT_EVALUATED'};
   snapshot.inputManifest={primary:manifests[attempt.tf]??null,engineMtf:attempt.includedMtf?.map(tf=>manifests[tf])||[],research1m:manifests.research1m??{status:'UNAVAILABLE'}};
   snapshot.callerGates=[...(attempt.callerGates||[])];
   const nextOrdinal=attempt.trace?.gates?.length??0;
   if(!snapshot.callerGates.some(g=>g.id==='new-candle'))snapshot.callerGates.push({id:'new-candle',result:'NOT_EVALUATED',ordinal:nextOrdinal});
   snapshot.callerGates.push({id:'exposure',result:decision?(decision.decision==='accepted'?'PASS':'FAIL'):'NOT_EVALUATED',reason:decision?.decision??null,operands:decision??null,ordinal:nextOrdinal+1},
    {id:'reservation',result:decision?.decision==='accepted'?'PASS':'NOT_EVALUATED',ordinal:nextOrdinal+2},
    {id:'official-persistence',result:snapshot.officialSignalId?'PASS':snapshot.exposure.officialPersistenceFailed?'FAIL':'NOT_EVALUATED',ordinal:nextOrdinal+3});
   snapshot.broadMtf={measurementOnly:true,decisionUse:false,frames:Object.fromEntries([...(journal.evaluations||[])].map(([tf,v])=>[tf,{side:v.side,bull:v.bull??null,bear:v.bear??null,marketProfile:v.regime??null,lastTs:v.lastTs??null,evaluatedAt:v.evaluatedAt}]))};
   snapshot.broadMtf=journal.broadMatrices?.get(candidate?.id)??snapshot.broadMtf;
   snapshot.quote=journal.quoteEvidence??snapshot.quote;
   snapshot.engineMtf.frames=(snapshot.engineMtf.frames||[]).map(frame=>({...frame,requested:true,included:true,
    inputManifest:manifests[frame.tf]??null,sourceQuality:journal.frames[frame.tf]?.quality??null,
    sourceProvider:journal.frames[frame.tf]?.provider??null,
    candleAgeMs:Number.isFinite(snapshot.evaluatedAt)&&Number.isFinite(frame.lastClosedBar)?snapshot.evaluatedAt-frame.lastClosedBar:null,
    contributionOperations:(attempt.trace?.ledger||[]).filter(c=>c.componentId.startsWith('mtf-')).flatMap(c=>c.operations.filter(o=>o.frame===frame.tf)),
    admissionGateResults:(attempt.trace?.gates||[]).filter(g=>g.id.startsWith('mtf-')),
    qualityInterpretation:'EXACT_EXISTING_QUALITY; NO_NEW_FRESHNESS_RULE'}));
   snapshot.engineMtf.excluded=attempt.requestedMtf.filter(tf=>!attempt.includedMtf.includes(tf)).map(tf=>({tf,reason:journal.frames[tf]?.quality?.ok===false?'CANDLE_QUALITY_FAILED':'FRAME_UNAVAILABLE',quality:journal.frames[tf]?.quality??null}));
   snapshot.sourceCandle={openAt:snapshot.sourceCandle,closeAt:snapshot.sourceCandle!=null?snapshot.sourceCandle+({'1m':60000,'5m':300000,'15m':900000,'30m':1800000,'60m':3600000,'240m':14400000,'1d':86400000})[attempt.tf]:null};
   canonicalSerialize(snapshot,maxBytes);return snapshot;
  });
  const cycle={cycleId:journal.cycleId,cycleStartedAt:journal.cycleStartedAt,evaluatedAt:journal.evaluatedAt,
   capturedAt,manifests,newsContext:journal.newsContext,filters:journal.filters,
   candidateOrdering:journal.candidates?.map(x=>({id:x.id,conf:x.conf,tf:x.tf,signalBarTs:x.signalBarTs}))||[],
   comparatorVersion:'confidence-tf-rank-bar-id-v1',actualExposureTrace:journal.exposureResult?.measurementTrace??{status:'NOT_CAPTURED'},exposureDecisionOrder:journal.exposureResult?.decisions??null,measurementOnly:true,decisionUse:false};
  canonicalSerialize(cycle,maxBytes);
  const records=[{type:'cohort',values:{cohort_id:cohortStorageRef,schema_version:1,effective_at:cohort.measurementEffectiveAt,recorded_at:capturedAt},payload:cohort}];
  for(const b of blocks.values())records.push({type:'market',values:{block_id:b.blockId,timeframe:b.tf,from_at:b.from,to_at:b.to,recorded_at:capturedAt},payload:parseEvidence(b.payload)});
  records.push({type:'cycle',values:{cycle_id:journal.cycleId,cohort_id:cohortStorageRef,evaluated_at:journal.evaluatedAt,block_ids_json:JSON.stringify([...blocks.keys()]),recorded_at:capturedAt},payload:cycle});
  for(const snapshot of snapshots)records.push({type:'decision',values:{evaluation_id:snapshot.evaluationId,candidate_key:snapshot.candidateKey,
   official_signal_id:snapshot.officialSignalId,kind:snapshot.kind,cycle_id:journal.cycleId,cohort_id:cohortStorageRef,timeframe:snapshot.timeframe,
   evaluated_at:snapshot.evaluatedAt??journal.evaluatedAt,measurement_only:1,decision_use:0,recorded_at:capturedAt},payload:snapshot});
  const officialPins=snapshots.filter(s=>s.kind==='OFFICIAL').map(s=>({officialId:s.officialSignalId,blockIds:[...new Set([s.inputManifest.primary,...s.inputManifest.engineMtf,s.inputManifest.research1m].flatMap(m=>m?.references?.map(r=>r.blockId)||[]))]}));
  const persistence=await writer.immutableBatch(records,{captureCensusOnBudget:true,officialPins,links:[{ownerType:'CYCLE',ownerId:journal.cycleId,blockIds:[...blocks.keys()]}]});
  return {ok:true,attempts:snapshots.length,blocks:blocks.size,capturedAt,persistence};
 }catch(error){return {ok:false,error:String(error?.message||'measurement_capture_failed'),captureGap:true};}
}
