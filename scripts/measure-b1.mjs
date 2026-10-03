// Synthetic local resource assessment only; never contacts a service.
import {DatabaseSync} from 'node:sqlite';
import {transactionalBinding} from '../test-fixtures/b1-sqlite.mjs';
import {beginDecisionCycle,observeAttempt,finishDecisionCycle,takeDecisionCycle,persistDecisionCycle} from '../signal-measurement.js';
import fs from 'node:fs';
import {foldPostEntry} from '../post-entry-evidence.js';
import {performance} from 'node:perf_hooks';
import {computeServerSignal as oldEngine} from '../test-fixtures/signal-engine-pre-b1.js';
import {computeServerSignal,SIGNAL_TF_MS,HIGHER_SIGNAL_TIMEFRAMES,evaluateCandleQuality} from '../signal-engine.js';
import {canonicalSerialize,decisionSnapshot} from '../signal-evidence.js';
import {buildMarketManifest} from '../market-evidence.js';
const now=Date.UTC(2026,8,28,15,10),limits={'1m':2000,'5m':600,'15m':300,'30m':200,'60m':120,'240m':80,'1d':60};
const frames={};for(const [tf,n]of Object.entries(limits)){const step=SIGNAL_TF_MS[tf],end=Math.floor(now/step)*step;frames[tf]={tf,bars:Array.from({length:n},(_,i)=>{const c=4100+(i-n+1)*.35;return{t:end-(n-i)*step,o:c-.3,h:c+.1,l:c-.4,c,v:1,provider:'mt5'};})};frames[tf].quality=evaluateCandleQuality(frames[tf].bars,tf);}
const live={price:4100,ts:now,receivedAt:now,source:'mt5'},filters={nyFilterOn:false,pivotFilterOn:false};
const start=performance.now(),manifests={},blocks=new Map();for(const [tf,f]of Object.entries(frames)){const built=await buildMarketManifest(tf,f.bars,now);manifests[tf]=built.manifest;for(const b of built.blocks)blocks.set(b.blockId,b);}
const marketMs=performance.now()-start;
const samples={},sampleTexts={};let ledgerMs=0;
for(const [name,options]of Object.entries({official:{tf:'5m'},candidate:{tf:'15m'},earlyRejection:{tf:'5m',dataQuality:{ok:false,reason:'fixture'}}})){
 const tf=options.tf,trace={},mtf=(HIGHER_SIGNAL_TIMEFRAMES[tf]||[]).map(f=>frames[f]);const at=performance.now();
 const result=computeServerSignal(frames[tf].bars,{tf,mtf,live,barsSource:'d1',evaluationAt:now,filters,...options},trace);ledgerMs+=performance.now()-at;
 const snapshot=decisionSnapshot({cycleId:'synthetic',ordinal:0,tf,bars:frames[tf].bars,live,trace,result,createdAt:now,requestedMtf:HIGHER_SIGNAL_TIMEFRAMES[tf],includedMtf:mtf.map(f=>f.tf),officialPersisted:name==='official',decision:{decision:name==='official'?'accepted':'blocked_opposite'}});
 snapshot.inputManifest={primary:manifests[tf],engineMtf:mtf.map(f=>manifests[f.tf]),research1m:manifests['1m']};
 const time=performance.now(),serialized=canonicalSerialize(snapshot);
 const j=beginDecisionCycle(now,frames,live,null,filters);observeAttempt(j,{tf,trace,result,requestedMtf:HIGHER_SIGNAL_TIMEFRAMES[tf]||[],includedMtf:mtf.map(f=>f.tf)});
 const id=`${tf}:${frames[tf].bars.at(-1).t}:${result.side}`;
 const captured=finishDecisionCycle(j,{}, {decisions:new Map([[id,{decision:name==='official'?'accepted':'blocked_opposite'}]]),candidates:['buy','sell'].includes(result.side)?[{id,tf,createdAt:now,conf:result.conf}]:[],officialIds:new Set(name==='official'?[id]:[]),confirmationIds:new Set(),exposureResult:{state:null},evaluations:new Map([[tf,result]])});
 const sampleDb=new DatabaseSync(':memory:');sampleDb.exec(fs.readFileSync(new URL('../migrations/0001_measurement_evidence.sql',import.meta.url),'utf8'));
 const write=await persistDecisionCycle(transactionalBinding(sampleDb),takeDecisionCycle(captured),{codeCommit:'a'.repeat(40),measurementEffectiveAt:now});
 if(!write.ok)throw new Error(write.error);const actual=sampleDb.prepare('SELECT payload_json FROM signal_decision_evidence').get().payload_json;sampleDb.close();
 sampleTexts[name]=actual;samples[name]={bytes:Buffer.byteLength(actual),minimalContractBytes:Buffer.byteLength(serialized),constructionSerializationAndLocalWriteMs:performance.now()-time};
}
const cycle=canonicalSerialize({cycleId:'synthetic',manifests});
const outcome=canonicalSerialize({eventId:'synthetic:TP1',subjectId:'synthetic',evidenceType:'BARRIER_OBSERVATION',eventType:'TP1',occurredAt:now,availableAt:now,price:4110,source:'mt5:received-tick',orderingQuality:'EXACT_SEQUENCE',sessionId:'synthetic',sequence:10});
const rawBytes=[...blocks.values()].reduce((n,b)=>n+Buffer.byteLength(b.payload),0);
const changed=new Map();for(const [tf,f]of Object.entries(frames)){const step=SIGNAL_TF_MS[tf],moved=f.bars.slice(1);moved.push({...f.bars.at(-1),t:f.bars.at(-1).t+step});const built=await buildMarketManifest(tf,moved,now+300000);for(const b of built.blocks)if(!blocks.has(b.blockId))changed.set(b.blockId,b);}
const rollingBytes=[...changed.values()].reduce((n,b)=>n+Buffer.byteLength(b.payload),0);
const attemptsDay=1728,cyclesDay=288,officialDay=10,GiB=1024**3;
const checkpoint=foldPostEntry({id:'synthetic',createdAt:now,side:'buy',entry:4100,tp1:4110,tp2:4120,sl:4090,status:'active'},null,{asOf:now+300000,ticks:Array.from({length:301},(_,i)=>({ts:now+i*1000,price:4100+Math.sin(i/10),measurement:{provider:'mt5',providerTimestamp:now+i*1000,sessionId:'synthetic',sequence:i+1}})),bars:[]});
const checkpointBytes=Buffer.byteLength(canonicalSerialize({...checkpoint.state,eventId:'synthetic:coverage',evidenceType:'COVERAGE_CHECKPOINT',marketManifest:manifests['1m']}));
const seen=new Set(blocks.keys()),newSizes=new Map(),officialReferenced=new Set();let realRollingBytes=0,realRollingBlocks=0;
let optionalResearchBytes=0;
const rolling=structuredClone(frames);
for(let cycle=1;cycle<=288;cycle++)for(const [tf,f]of Object.entries(rolling)){
 const shifts=Math.floor(cycle*300000/SIGNAL_TF_MS[tf])-Math.floor((cycle-1)*300000/SIGNAL_TF_MS[tf]);
 for(let i=0;i<shifts;i++){const last=f.bars.at(-1);f.bars.shift();f.bars.push({...last,t:last.t+SIGNAL_TF_MS[tf],o:last.c,c:last.c+.1,h:last.c+.3,l:last.c-.2});}
 const built=await buildMarketManifest(tf,f.bars,now+cycle*300000);
 if(cycle%28===0)for(const block of built.blocks)officialReferenced.add(block.blockId);for(const block of built.blocks)if(!seen.has(block.blockId)){seen.add(block.blockId);newSizes.set(block.blockId,Buffer.byteLength(block.payload));realRollingBytes+=Buffer.byteLength(block.payload);realRollingBlocks++;}
}
const retainedOfficialNewBytes=[...officialReferenced].reduce((n,id)=>n+(newSizes.get(id)||0),0);
for(let cycle=1;cycle<=288;cycle++){const context=frames['1m'].bars.slice(-60).map(b=>({...b,t:b.t+cycle*300000}));const built=await buildMarketManifest('1m-research',context,now+cycle*300000);optionalResearchBytes+=built.blocks.reduce((n,b)=>n+Buffer.byteLength(b.payload),0);}
const paired=[];for(let i=0;i<30;i++){const bars=frames['5m'].bars,options={tf:'5m',mtf:[frames['15m'],frames['60m']],live,evaluationAt:now,barsSource:'d1',filters};let t=performance.now();oldEngine(bars,options);const oldMs=performance.now()-t;t=performance.now();computeServerSignal(bars,options,{});paired.push(performance.now()-t-oldMs);}
paired.sort((a,b)=>a-b);
// SQLite page allocation includes the actual schema/indexes. Synthetic local
// sample, not Cloudflare billing or the current Production database size.
const physical=new DatabaseSync(':memory:');physical.exec(fs.readFileSync(new URL('../migrations/0001_measurement_evidence.sql',import.meta.url),'utf8'));
physical.prepare('INSERT INTO measurement_cohorts(cohort_id,schema_version,effective_at,payload_json,payload_digest,recorded_at) VALUES(?,1,?,?,?,?)').run('synthetic',now,'{}','x',now);
const insertCycle=physical.prepare('INSERT INTO decision_cycle_evidence(cycle_id,cohort_id,evaluated_at,payload_json,block_ids_json,payload_digest,recorded_at) VALUES(?,?,?,?,?,?,?)');
const insertDecision=physical.prepare('INSERT INTO signal_decision_evidence(evaluation_id,candidate_key,official_signal_id,kind,cycle_id,cohort_id,timeframe,evaluated_at,measurement_only,decision_use,payload_json,payload_digest,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)');
const insertRef=physical.prepare('INSERT INTO market_evidence_references VALUES(?,?,?)');
for(const b of blocks.values())physical.prepare('INSERT INTO market_evidence_blocks(block_id,timeframe,from_at,to_at,payload_json,payload_digest,recorded_at) VALUES(?,?,?,?,?,?,?)').run(b.blockId,b.tf,b.from,b.to,b.payload,b.digest,now);
for(let i=0;i<5000;i++){const cycleId=`synthetic:${Math.floor(i/7)}`;if(i%7===0){insertCycle.run(cycleId,'synthetic',now,cycle,JSON.stringify([...blocks.keys()]),'x',now);for(const id of blocks.keys())insertRef.run('CYCLE',cycleId,id);}
 insertDecision.run(`attempt:${i}`,`5m:${now+i}:buy`,null,'CANDIDATE',cycleId,'synthetic','5m',now+i,1,0,sampleTexts.candidate,'x',now);
}
const physicalBytes=Number(physical.prepare('PRAGMA page_count').get().page_count)*Number(physical.prepare('PRAGMA page_size').get().page_size);
const payloadBytes=5000*samples.candidate.bytes+Math.ceil(5000/7)*Buffer.byteLength(cycle)+rawBytes;
const physicalFactor=physicalBytes/payloadBytes;const beforeOutcomes=physicalBytes;const checkpointText=canonicalSerialize({...checkpoint.state,eventId:'synthetic:coverage',evidenceType:'COVERAGE_CHECKPOINT',marketManifest:manifests['1m']});
const putOutcome=physical.prepare('INSERT INTO signal_outcome_evidence(event_id,subject_id,event_type,available_at,payload_json,payload_digest,block_ids_json,recorded_at) VALUES(?,?,?,?,?,?,?,?)');
for(let i=0;i<5000;i++)putOutcome.run(`checkpoint:${i}`,`attempt:${i}`,'COVERAGE_CHECKPOINT',now+i,checkpointText,'x','[]',now+i);
const outcomePageBytes=Number(physical.prepare('PRAGMA page_count').get().page_count)*Number(physical.prepare('PRAGMA page_size').get().page_size)-beforeOutcomes;
const outcomeFactor=outcomePageBytes/(5000*Buffer.byteLength(checkpointText));physical.close();
const plannedWithOutcomeAllocation=(samples.candidate.bytes*attemptsDay*90+realRollingBytes*90+optionalResearchBytes*90+retainedOfficialNewBytes*275+Buffer.byteLength(cycle)*cyclesDay*90+samples.official.bytes*officialDay*365)*physicalFactor/GiB+checkpointBytes*8*cyclesDay*90*outcomeFactor/GiB;
const maxSevenAttemptsAdditional=samples.candidate.bytes*288*90*physicalFactor/GiB;
// One current Official remains owned until terminal under the unchanged policy;
// retain up to one extra Official checkpoint/cycle for the additional 275 days.
const officialExtraCheckpoints=checkpointBytes*cyclesDay*275*outcomeFactor/GiB;
console.log(JSON.stringify({MEASURED:{samples,cycleBytes:Buffer.byteLength(cycle),outcomeBytes:Buffer.byteLength(outcome),marketBlockMaxBytes:Math.max(...[...blocks.values()].map(b=>Buffer.byteLength(b.payload))),initialMarketBlocks:blocks.size,initialRawBytes:rawBytes,rollingChangedBlocks:changed.size,rollingChangedBytes:rollingBytes,marketConstructHashMs:marketMs,checkpointBytes,physicalSampleBytes:physicalBytes,physicalSamplePayloadBytes:payloadBytes,physicalFactor,realClockRawBytesPerDay:realRollingBytes,realClockNewBlocksPerDay:realRollingBlocks,threeEngineTracesMs:ledgerMs,outcomeSamplePhysicalBytes:outcomePageBytes,outcomeSampleAllocationFactor:outcomeFactor,pairedInstrumentationDeltaMedianMs:paired[15],optionalResearchRawBytesPerDay:optionalResearchBytes,officialReferencedNewRawBytesPerDay:retainedOfficialNewBytes},ESTIMATED:{assumptions:{attemptsDay,cyclesDay,officialDay,allFramesAdvanceOneBarPerCycle:'conservative stress, slower frames actually change less frequently',indexes:'local SQLite factor includes all schema indexes/references; approximation only',checkpoints:'8 subjects/cycle worst bounded collector throughput',plan:'OWNER_CONFIRMED_WORKERS_PAID; production occupancy UNKNOWN'},rowsPerEvaluation:'1 immutable attempt; shared cohort/cycle/raw/reference rows; <=8 subject checkpoints/states per cycle',rowsPerDay:{decision:attemptsDay,cycle:cyclesDay,outcomeCheckpointMax:8*cyclesDay,newRawBlocks:realRollingBlocks},indexAmplification:'decision: PK + 4 indexes (+ partial Official unique); outcome: PK + subject/time; refs: PK + block',candidateRows90d:attemptsDay*90,candidatePayload30dGiB:samples.candidate.bytes*attemptsDay*30/GiB,candidatePayload90dGiB:samples.candidate.bytes*attemptsDay*90/GiB,officialPayload365dGiB:samples.official.bytes*officialDay*365/GiB,rawRolling90dGiB:rollingBytes*cyclesDay*90/GiB,cycle90dGiB:Buffer.byteLength(cycle)*cyclesDay*90/GiB,checkpoints90dGiB:checkpointBytes*8*cyclesDay*90/GiB,realClockRaw90dGiB:realRollingBytes*90/GiB,serialized90dTotalGiB:(samples.candidate.bytes*attemptsDay*90+realRollingBytes*90+Buffer.byteLength(cycle)*cyclesDay*90+checkpointBytes*8*cyclesDay*90)/GiB,physical90dPlanningGiB:(samples.candidate.bytes*attemptsDay*90+realRollingBytes*90+Buffer.byteLength(cycle)*cyclesDay*90+checkpointBytes*8*cyclesDay*90)*physicalFactor/GiB,legacyIndividualImmutableStatementsPerInitialCycle:2*(1+blocks.size+1+7),batchedInitialImmutableStatementsEstimate:15,longRetentionAdditionalRawGiB:retainedOfficialNewBytes*275/GiB,optionalResearchRaw90dGiB:optionalResearchBytes*90/GiB,conservativePhysicalIncluding365dOfficialRawGiB:((samples.candidate.bytes*attemptsDay*90+realRollingBytes*90+optionalResearchBytes*90+retainedOfficialNewBytes*275+Buffer.byteLength(cycle)*cyclesDay*90+checkpointBytes*8*cyclesDay*90+samples.official.bytes*officialDay*365)*physicalFactor)/GiB,mixedAllocation90dPlus365dOfficialRawGiB:plannedWithOutcomeAllocation,sevenAttemptsAnd365dOfficialCheckpointsGiB:plannedWithOutcomeAllocation+maxSevenAttemptsAdditional+officialExtraCheckpoints,notes:'Serialized payloads only; DB allocation, indexes, Official reference retention and outcome checkpoints add overhead.'}},null,2));
