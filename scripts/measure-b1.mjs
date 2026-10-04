// Synthetic local sizing only. Owner dashboard occupancy is supplied evidence;
// this script never accesses Production or any network service.
import {DatabaseSync} from 'node:sqlite';
import fs from 'node:fs';
import {performance} from 'node:perf_hooks';
import {transactionalBinding} from '../test-fixtures/b1-sqlite.mjs';
import {beginDecisionCycle,observeAttempt,finishDecisionCycle,takeDecisionCycle,persistDecisionCycle} from '../signal-measurement.js';
import {computeServerSignal,SIGNAL_TF_MS,HIGHER_SIGNAL_TIMEFRAMES,evaluateCandleQuality} from '../signal-engine.js';
import {measurementWriter} from '../signal-evidence-store.js';
import {encodeEvidence,decodeEvidence} from '../evidence-codec.js';
import {canonicalSerialize,parseEvidence} from '../signal-evidence.js';
import {foldPostEntry} from '../post-entry-evidence.js';
import {buildMarketManifest} from '../market-evidence.js';
const now=Date.UTC(2026,8,28,15,10),DAY=86400000,frames={};
for(const [tf,n]of Object.entries({'1m':2000,'5m':600,'15m':300,'30m':200,'60m':120,'240m':80,'1d':60})){const step=SIGNAL_TF_MS[tf],end=Math.floor(now/step)*step;frames[tf]={tf,bars:Array.from({length:n},(_,i)=>{const c=4100+(i-n+1)*.35;return{t:end-(n-i)*step,o:c-.3,h:c+.1,l:c-.4,c,v:1,provider:'mt5'};})};frames[tf].quality=evaluateCandleQuality(frames[tf].bars,tf);}
function database(){const db=new DatabaseSync(':memory:');for(const name of ['0001_measurement_evidence.sql','0002_measurement_storage_tiers.sql'])db.exec(fs.readFileSync(new URL(`../migrations/${name}`,import.meta.url),'utf8'));return db;}
const samples={},stored={};let largestBlock=0;
for(const [name,options]of Object.entries({official:{tf:'5m'},candidate:{tf:'15m'},earlyRejection:{tf:'5m',dataQuality:{ok:false,reason:'synthetic'}}})){
 const tf=options.tf,trace={},mtf=(HIGHER_SIGNAL_TIMEFRAMES[tf]||[]).map(f=>frames[f]),live={price:4100,ts:now,receivedAt:now,source:'mt5'},filters={nyFilterOn:false,pivotFilterOn:false};
 const before=performance.now(),result=computeServerSignal(frames[tf].bars,{tf,mtf,live,barsSource:'d1',evaluationAt:now,filters,...options},trace);
 const j=beginDecisionCycle(now,frames,live,null,filters);observeAttempt(j,{tf,trace,result,requestedMtf:HIGHER_SIGNAL_TIMEFRAMES[tf]||[],includedMtf:mtf.map(f=>f.tf)});
 const id=`${tf}:${frames[tf].bars.at(-1).t}:${result.side}`,captured=finishDecisionCycle(j,{}, {decisions:new Map([[id,{decision:name==='official'?'accepted':'blocked_opposite'}]]),candidates:['buy','sell'].includes(result.side)?[{id,tf,createdAt:now,conf:result.conf}]:[],officialIds:new Set(name==='official'?[id]:[]),confirmationIds:new Set(),exposureResult:{state:null},evaluations:new Map([[tf,result]])});
 const db=database(),binding=transactionalBinding(db),write=await persistDecisionCycle(binding,takeDecisionCycle(captured),{codeCommit:'a'.repeat(40),measurementEffectiveAt:now});if(!write.ok)throw new Error(write.error);
 const census=db.prepare('SELECT * FROM signal_decision_evidence').get(),rich=db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='decision'").get(),cycle=db.prepare('SELECT * FROM decision_cycle_evidence').get(),shared=db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='cycle'").get();
 const raw=db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='market'").all();largestBlock=Math.max(largestBlock,...raw.map(r=>Buffer.byteLength(r.payload_json)));
 samples[name]={censusBytes:Buffer.byteLength(census.payload_json),richEncodedBytes:Buffer.byteLength(rich.payload_json),richLogicalBytes:JSON.parse(rich.payload_json).length,constructionSerializationHashAndLocalPersistenceMs:performance.now()-before,insertStatements:write.persistence.insertStatements,immutableRows:write.persistence.records};
 stored[name]={census,rich,cycle,shared,raw,definitions:db.prepare('SELECT * FROM measurement_definitions').all(),cohort:db.prepare('SELECT * FROM measurement_cohorts').get(),blocks:db.prepare('SELECT * FROM market_evidence_blocks').all(),references:db.prepare('SELECT * FROM market_evidence_references').all(),pins:db.prepare('SELECT * FROM measurement_official_pins').all()};
 db.close();
}
const subject={id:'sample',createdAt:now,side:'buy',entry:4100,tp1:4110,tp2:4120,sl:4090,status:'active'};
const ticks=Array.from({length:3601},(_,i)=>({ts:now+i*1000,price:4100+Math.sin(i/10),measurement:{provider:'mt5',providerTimestamp:now+i*1000,sessionId:'synthetic',sequence:i+1}}));
const folded=foldPostEntry(subject,null,{asOf:now+3600000,ticks,bars:[]});
const db=database(),writer=measurementWriter(transactionalBinding(db)),stateText=canonicalSerialize(folded.state);await writer.state('sample',null,folded.state,now);
const finalStart=performance.now();await writer.finalize('sample','CANDIDATE',{...folded.state,availableAt:now+3600000,observedAt:now+3600000,occurredFrom:now,occurredTo:now+3600000},now+3600000);
const finalRich=db.prepare('SELECT * FROM measurement_rich_evidence').get(),finalResult=db.prepare('SELECT * FROM measurement_final_results').get(),finalScalar=db.prepare('SELECT * FROM signal_outcome_evidence').get();
const finalMs=performance.now()-finalStart;
const windowSizes=[];for(const [name,window]of Object.entries(folded.state.quality.windows)){const p={subjectId:'sample',eventId:`sample:window:${name}`,evidenceType:'WINDOW_FINALIZED',eventType:'WINDOW_FINALIZED',availableAt:now+3600000,observedAt:now+3600000,window,measurementOnly:true,decisionUse:false};windowSizes.push(Buffer.byteLength(JSON.stringify(await encodeEvidence(p))));}
const barrier={subjectId:'sample',eventId:'sample:TP1',evidenceType:'BARRIER_OBSERVATION',eventType:'TP1',price:4110,source:'mt5:tick',sessionId:'synthetic',sequence:10,occurredAt:now,observedAt:now,availableAt:now,orderingQuality:'EXACT_SEQUENCE',measurementOnly:true,decisionUse:false};
const candidateOutcomeBytes=Buffer.byteLength(finalRich.payload_json)+Buffer.byteLength(finalScalar.payload_json)+windowSizes.reduce((a,b)=>a+b,0)+5*512+3*Buffer.byteLength(canonicalSerialize(barrier));
// Actual schema/index page allocation. Repeat representative retained packages,
// not compressibility targets; include base and rich rows, cycle/refs/registry.
const physical=database(),pageBytes=d=>Number(d.prepare('PRAGMA page_count').get().page_count)*Number(d.prepare('PRAGMA page_size').get().page_size);
const emptyBytes=pageBytes(physical),sample=stored.candidate;let logicalBytes=0;
function insert(table,row){const keys=Object.keys(row).filter(k=>k!=='persisted_at');physical.prepare(`INSERT INTO ${table}(${keys.join(',')}) VALUES(${keys.map(()=>'?').join(',')})`).run(...keys.map(k=>row[k]));logicalBytes+=keys.reduce((n,k)=>n+(typeof row[k]==='string'?Buffer.byteLength(row[k]):8),0);}
insert('measurement_cohorts',sample.cohort);for(const d of sample.definitions)insert('measurement_definitions',d);for(const b of sample.blocks)insert('market_evidence_blocks',b);for(const r of sample.raw)insert('measurement_rich_evidence',r);
for(let i=0;i<1000;i++){
 const cycleId=`physical:${Math.floor(i/6)}`,evalId=`${cycleId}:${i%6}`;
 if(i%6===0){insert('decision_cycle_evidence',{...sample.cycle,cycle_id:cycleId});insert('measurement_rich_evidence',{...sample.shared,evidence_id:`cycle:${cycleId}`,owner_id:cycleId});for(const ref of sample.references)insert('market_evidence_references',{...ref,owner_id:cycleId});}
 insert('signal_decision_evidence',{...sample.census,evaluation_id:evalId,cycle_id:cycleId,candidate_key:`15m:${now+i}:buy`});
 insert('measurement_rich_evidence',{...sample.rich,evidence_id:`decision:${evalId}`,owner_id:evalId});
 insert('signal_outcome_evidence',{...finalScalar,event_id:`${evalId}:final`,subject_id:evalId});insert('measurement_rich_evidence',{...finalRich,evidence_id:`outcome:${evalId}:final`,owner_id:`${evalId}:final`});insert('measurement_final_results',{...finalResult,subject_id:evalId});
}
const allocationBytes=pageBytes(physical)-emptyBytes,allocationFactor=allocationBytes/logicalBytes;
const indexPages=physical.prepare("SELECT name,COUNT(*) AS pages,SUM(pgsize) AS bytes FROM dbstat GROUP BY name ORDER BY name").all();
// One-day rolling content-addressed raw stream, encoded by the actual codec.
// Canonical research context slices share the same 1m namespace.
const seen=new Set();let rawBytesDay=0,newBlocksDay=0,pinBytesDay=0,referenceIdsDay=0;
const rolling=structuredClone(frames);const rawStart=performance.now();
for(let c=0;c<=288;c++)for(const [tf,f]of Object.entries(rolling)){
 if(c){const shifts=Math.floor(c*300000/SIGNAL_TF_MS[tf])-Math.floor((c-1)*300000/SIGNAL_TF_MS[tf]);for(let i=0;i<shifts;i++){const last=f.bars.at(-1);f.bars.shift();f.bars.push({...last,t:last.t+SIGNAL_TF_MS[tf],o:last.c,c:last.c+.1,h:last.c+.3,l:last.c-.2});}}
 const built=await buildMarketManifest(tf,f.bars,now+c*300000);
 // 1m is optional research only: retain exact blocks containing last60 rows.
 const selected=tf==='1m'?built.blocks.filter(b=>b.to>=f.bars.at(-60).t):built.blocks;
 if(c%28===0){for(const block of selected)pinBytesDay+=Buffer.byteLength(JSON.stringify(await encodeEvidence(parseEvidence(block.payload))))+256;referenceIdsDay+=selected.length;}
 for(const block of selected)if(!seen.has(block.blockId)){seen.add(block.blockId);rawBytesDay+=Buffer.byteLength(JSON.stringify(await encodeEvidence(parseEvidence(block.payload))))+256;newBlocksDay++;}
}
const rawMeasureMs=performance.now()-rawStart;
// Every attempt incurs rich derived + full rich outcome cost, even early rejects:
// a deliberately conservative estimate independent of outcomes or rarity.
const census=Math.max(...Object.values(samples).map(s=>s.censusBytes))+512;
const derived=Math.max(...Object.values(samples).map(s=>s.richEncodedBytes))+384;
const finalCompact=Buffer.byteLength(finalResult.payload_json)+256;
const sharedCycle=Buffer.byteLength(sample.cycle.payload_json)+Buffer.byteLength(sample.shared.payload_json)+sample.references.length*192+512;
const officialPackage=samples.official.censusBytes+samples.official.richEncodedBytes+candidateOutcomeBytes+finalCompact+2048;
const allocation=Math.max(1.25,allocationFactor),contingency=1.5,cleanupLagDays=2,activeRecoveryBytes=128*1024**2,registryReserveBytes=16*1024**2;
function projection(days,attemptsDay){const richDays=Math.min(days,90+cleanupLagDays),rawDays=Math.min(days,30+cleanupLagDays),censusDays=Math.min(days,365+cleanupLagDays);
 const parts={census:attemptsDay*censusDays*census,derived:attemptsDay*richDays*derived,candidateOutcomes:attemptsDay*richDays*candidateOutcomeBytes,compactResults:attemptsDay*censusDays*finalCompact,sharedCycles:288*censusDays*sharedCycle,raw:rawBytesDay*rawDays,officialForensics:10*days*officialPackage,officialPins:pinBytesDay*Math.max(0,days-rawDays),processingRecovery:activeRecoveryBytes,definitionsRegistry:registryReserveBytes};
 const logical=Object.values(parts).reduce((a,b)=>a+b,0),bytes=logical*allocation*contingency;return {parts,logicalBytes:logical,allocationFactor:allocation,contingencyFactor:contingency,B1GB:bytes/1e9,totalWithOwnerObservedProductionGB:(bytes+23.91e6)/1e9,headroomGB:(10e9-bytes-23.91e6)/1e9};}
const projections=Object.fromEntries([30,90,365].map(d=>[d,projection(d,1728)])),stress=projection(365,2016);
console.log(JSON.stringify({MEASURED:{samples,sharedCycleEncodedBytes:Buffer.byteLength(sample.shared.payload_json),cycleScalarBytes:Buffer.byteLength(sample.cycle.payload_json),definitionBytes:sample.definitions.reduce((n,d)=>n+Buffer.byteLength(d.payload_json),0),compressedMarketBlockMaxBytes:largestBlock,candidateOutcomePackageBytes:candidateOutcomeBytes,windowEncodedBytes:windowSizes,finalCompactBytes:Buffer.byteLength(finalResult.payload_json),accumulatorActiveBytes:Buffer.byteLength(stateText),finalizationMs:finalMs,completedProcessingRows:db.prepare('SELECT count(*) n FROM signal_measurement_state').get().n,allocationBytes,allocationLogicalBytes:logicalBytes,allocationFactor,indexPages,rawEncodedBytesPerSimulatedDay:rawBytesDay,newRawBlocksPerSimulatedDay:newBlocksDay,officialPinsBytesPerSimulatedDay:pinBytesDay,officialPinReferencesPerSimulatedDay:referenceIdsDay,rawMeasurementRuntimeMs:rawMeasureMs},ESTIMATED:{assumptions:{attemptsDay:1728,stressAttemptsDay:2016,cyclesDay:288,officialDay:10,retentionDays:{census:365,derived:90,candidateOutcome:90,compactResult:365,raw:30,Official:'>=365/until cohort review'},cleanupLagDays,allocation,contingency,activeRecoveryBytes,registryReserveBytes,notes:'All attempts incur maximal representative rich/outcome cost; simulated raw stream, not Production costs. Official pins counted conservatively including duplicates. Outcome checkpoints limited to five finalized windows + final package + three barriers.'},projections,sevenAttemptStress:stress,ownerObservedProduction:{database:'gsx-bars',bytes:23.91e6,tables:12,observedAt:'2026-10-04 approximately 10:39 Asia/Beirut',source:'OWNER_DASHBOARD_OBSERVATION; NOT REQUERIED'},resourceGate:stress.B1GB<6&&stress.totalWithOwnerObservedProductionGB<8&&stress.headroomGB>=2?'PASS':'FAIL'}},null,2));
db.close();physical.close();
