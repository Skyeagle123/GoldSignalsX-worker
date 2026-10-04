import {outcomeProfileBudget} from '../measurement-resource-budget.js';
// Synthetic local sizing only. Owner dashboard occupancy is supplied evidence;
// this script never accesses Production or any network service.
import {DatabaseSync} from 'node:sqlite';
import fs from 'node:fs';
import {performance} from 'node:perf_hooks';
import {transactionalBinding} from '../test-fixtures/b1-sqlite.mjs';
import {beginDecisionCycle,observeAttempt,finishDecisionCycle,takeDecisionCycle,persistDecisionCycle} from '../signal-measurement.js';
import {computeServerSignal,SIGNAL_TF_MS,HIGHER_SIGNAL_TIMEFRAMES,evaluateCandleQuality} from '../signal-engine.js';
import {measurementWriter} from '../signal-evidence-store.js';
import {encodeEvidence,decodeStoredEvidence} from '../evidence-codec.js';
import {outcomeProfile} from '../test-fixtures/b1-outcome-profiles.mjs';
import {canonicalSerialize,parseEvidence} from '../signal-evidence.js';
import {foldPostEntry} from '../post-entry-evidence.js';
import {buildMarketManifest} from '../market-evidence.js';
const now=Date.UTC(2026,8,28,15,10),DAY=86400000,frames={};
for(const [tf,n]of Object.entries({'1m':2000,'5m':600,'15m':300,'30m':200,'60m':120,'240m':80,'1d':60})){const step=SIGNAL_TF_MS[tf],end=Math.floor(now/step)*step;frames[tf]={tf,bars:Array.from({length:n},(_,i)=>{const c=4100+(i-n+1)*.35;return{t:end-(n-i)*step,o:c-.3,h:c+.1,l:c-.4,c,v:1,provider:'mt5'};})};frames[tf].quality=evaluateCandleQuality(frames[tf].bars,tf);}
function database(){const db=new DatabaseSync(':memory:');for(const name of ['0001_measurement_evidence.sql','0002_measurement_storage_tiers.sql'])db.exec(fs.readFileSync(new URL(`../migrations/${name}`,import.meta.url),'utf8'));return db;}
const samples={},stored={};let largestBlock=0;
const tablePayload=(db,name)=>db.prepare("SELECT COALESCE(SUM(payload),0) n FROM dbstat WHERE name=?").get(name).n;
for(const [name,options]of Object.entries({official:{tf:'5m'},candidate:{tf:'15m'},earlyRejection:{tf:'5m',dataQuality:{ok:false,reason:'synthetic'}}})){
 const tf=options.tf,trace={},mtf=(HIGHER_SIGNAL_TIMEFRAMES[tf]||[]).map(f=>frames[f]),live={price:4100,ts:now,receivedAt:now,source:'mt5'},filters={nyFilterOn:false,pivotFilterOn:false};
 const before=performance.now(),result=computeServerSignal(frames[tf].bars,{tf,mtf,live,barsSource:'d1',evaluationAt:now,filters,...options},trace);
 const j=beginDecisionCycle(now,frames,live,null,filters);observeAttempt(j,{tf,trace,result,requestedMtf:HIGHER_SIGNAL_TIMEFRAMES[tf]||[],includedMtf:mtf.map(f=>f.tf)});
 const id=`${tf}:${frames[tf].bars.at(-1).t}:${result.side}`,captured=finishDecisionCycle(j,{}, {decisions:new Map([[id,{decision:name==='official'?'accepted':'blocked_opposite'}]]),candidates:['buy','sell'].includes(result.side)?[{id,tf,createdAt:now,conf:result.conf}]:[],officialIds:new Set(name==='official'?[id]:[]),confirmationIds:new Set(),exposureResult:{state:null},evaluations:new Map([[tf,result]])});
 const db=database(),binding=transactionalBinding(db),write=await persistDecisionCycle(binding,takeDecisionCycle(captured),{codeCommit:'a'.repeat(40),measurementEffectiveAt:now});if(!write.ok)throw new Error(write.error);
 const census=db.prepare('SELECT * FROM signal_decision_evidence').get(),rich=db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='decision'").get(),cycle=db.prepare('SELECT * FROM decision_cycle_evidence').get(),shared=db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='cycle'").get();
 const raw=db.prepare("SELECT * FROM measurement_rich_evidence WHERE owner_type='market'").all();largestBlock=Math.max(largestBlock,...raw.map(r=>r.payload_blob.length+256));
 samples[name]={censusBytes:tablePayload(db,'signal_decision_evidence'),richEncodedBytes:rich.payload_blob.length,richRowBytes:rich.payload_blob.length+Buffer.byteLength(rich.evidence_id)+Buffer.byteLength(rich.owner_id)+Buffer.byteLength(rich.owner_type)+Buffer.byteLength(rich.codec)+rich.payload_digest.length+32,richLogicalBytes:rich.uncompressed_length,cycleContextBytes:tablePayload(db,'decision_cycle_evidence')+shared.payload_blob.length+Buffer.byteLength(shared.evidence_id)+Buffer.byteLength(shared.owner_id)+Buffer.byteLength(shared.owner_type)+Buffer.byteLength(shared.codec)+shared.payload_digest.length+32+tablePayload(db,'measurement_market_links'),constructionSerializationHashAndLocalPersistenceMs:performance.now()-before,insertStatements:write.persistence.insertStatements,immutableRows:write.persistence.records};
 stored[name]={census,rich,cycle,shared,raw,definitions:db.prepare('SELECT * FROM measurement_definitions').all(),cohort:db.prepare('SELECT * FROM measurement_cohorts').get(),blocks:db.prepare('SELECT * FROM market_evidence_blocks').all(),references:db.prepare('SELECT * FROM measurement_market_links').all(),pins:db.prepare('SELECT * FROM measurement_official_pins').all()};
 db.close();
}

const profiles={};let maxActive=0,finalCompact=0;
for(const name of ['complete','gaps','incremental','session-transition','fragmented','correction']){
 const p=await outcomeProfile(name,{sessionId:'01c4fef2-36fb-47df-82e1-10288f2e329a',realistic:true});
 const activeDb=database(),writer=measurementWriter(transactionalBinding(activeDb));await writer.state(p.subject.id,null,p.folded.state,p.end);
 const active=tablePayload(activeDb,'signal_measurement_state');activeDb.close();maxActive=Math.max(maxActive,active);finalCompact=Math.max(finalCompact,p.finalBytes);
 profiles[name]={bytes:p.bytes,compactFinalBytes:p.finalBytes,activeStateBytes:active,rows:p.rows.length,witnesses:p.events.filter(e=>e.evidenceType==='BARRIER_OBSERVATION').length,windows:p.events.filter(e=>e.evidenceType==='WINDOW_FINALIZED').length,coverageRecords:p.events.filter(e=>e.evidenceType==='COVERAGE_CHECKPOINT').length,corrections:p.events.filter(e=>e.eventType==='CORRECTION').length};p.db.close();
}
// Uniform test-matrix average is a local sensitivity statistic, not an assumed
// Production distribution. Stress below also charges the heaviest tested valid
// profile to EVERY attempt and includes full rich evidence for every attempt.
const matrixAverage=Object.values(profiles).reduce((n,p)=>n+p.bytes,0)/Object.keys(profiles).length;
const worstOutcome=Math.max(...Object.values(profiles).map(p=>p.bytes));
const representative=stored.candidate;
const outcomeSample=await outcomeProfile('correction',{sessionId:'01c4fef2-36fb-47df-82e1-10288f2e329a',realistic:true});
const outcomeRows=outcomeSample.db.prepare('SELECT * FROM measurement_outcome_records').all(),finalRow=outcomeSample.db.prepare('SELECT * FROM measurement_final_results').get();
const physical=database(),pageBytes=db=>Number(db.prepare('PRAGMA page_count').get().page_count)*4096;
const empty=pageBytes(physical);
function insert(table,row){const keys=Object.keys(row).filter(k=>!['payload_json'].includes(k)||table!=='measurement_final_results');physical.prepare(`INSERT INTO ${table}(${keys.join(',')}) VALUES(${keys.map(()=>'?').join(',')})`).run(...keys.map(k=>row[k]));}
insert('measurement_cohorts',representative.cohort);for(const d of representative.definitions)insert('measurement_definitions',d);
for(const b of representative.blocks)insert('market_evidence_blocks',b);for(const r of representative.raw)insert('measurement_rich_evidence',r);
for(let i=0;i<1000;i++){
 const id=`cycle:1790608200000:${i}`,cycle=`cycle:1790608200000:${Math.floor(i/7)}`;
 if(i%7===0){insert('decision_cycle_evidence',{...representative.cycle,cycle_id:cycle});insert('measurement_rich_evidence',{...representative.shared,evidence_id:`cycle:${cycle}`,owner_id:cycle});const owner=physical.prepare('SELECT rowid FROM decision_cycle_evidence WHERE cycle_id=?').get(cycle).rowid;for(const b of physical.prepare('SELECT rowid FROM market_evidence_blocks').all())physical.prepare('INSERT INTO measurement_market_links VALUES(1,?,?)').run(owner,b.rowid);}
 physical.prepare('INSERT INTO measurement_subjects(subject_ref,subject_id,recorded_at,persisted_at) VALUES(?,?,?,?)').run(700000+i,id,1790608200000,1790608200000);
 for(const row of outcomeRows)insert('measurement_outcome_records',{...row,record_id:i*outcomeRows.length+1+outcomeRows.indexOf(row),subject_ref:700000+i});
 insert('measurement_final_results',{...finalRow,subject_id:id});
 insert('signal_decision_evidence',{...representative.census,evaluation_id:id,cycle_id:cycle,candidate_key:`5m:${1790608200000+i}:buy`});insert('measurement_rich_evidence',{...representative.rich,evidence_id:`decision:${id}`,owner_id:id});
}
const stat=physical.prepare('SELECT name,SUM(payload) payload,SUM(pgsize) bytes FROM dbstat GROUP BY name').all();
const tableNames=new Set(physical.prepare("SELECT name FROM sqlite_schema WHERE type='table'").all().map(r=>r.name));
const payload=stat.filter(r=>tableNames.has(r.name)).reduce((n,r)=>n+r.payload,0),allocation=(pageBytes(physical)-empty)/payload;
const allocationFactor=Math.max(1.345,allocation),contingency=1.5;
const census=Math.max(...Object.values(samples).map(x=>x.censusBytes)),rich=Math.max(...Object.values(samples).map(x=>x.richRowBytes)),cycle=Math.max(...Object.values(samples).map(x=>x.cycleContextBytes));
// Bound raw growth using the actual content-addressed codec on one rolling day.
const seen=new Set(),rolling=structuredClone(frames);let rawDay=0,rawRowsDay=0,pinDay=0,postEntryRawDay=0,postEntryPinDay=0;const pinned=new Set();
for(let c=0;c<=288;c++)for(const [tf,f]of Object.entries(rolling)){
 if(c){const shifts=Math.floor(c*300000/SIGNAL_TF_MS[tf])-Math.floor((c-1)*300000/SIGNAL_TF_MS[tf]);for(let i=0;i<shifts;i++){const last=f.bars.at(-1);f.bars.shift();f.bars.push({...last,t:last.t+SIGNAL_TF_MS[tf],o:last.c,c:last.c+.1,h:last.c+.3,l:last.c-.2});}}
 const built=await buildMarketManifest(tf,f.bars,now+c*300000),selected=tf==='1m'?built.blocks.filter(b=>b.to>=f.bars.at(-60).t):built.blocks;
 for(const b of selected){const e=await encodeEvidence(parseEvidence(b.payload)),bytes=e.data.length+512;if(c%29===0&&!pinned.has(b.blockId)){pinDay+=bytes;pinned.add(b.blockId);}if(!seen.has(b.blockId)){seen.add(b.blockId);rawDay+=bytes;rawRowsDay++;}}
}
// Include collector slices, not just at-entry research arrays. A received 1m
// stream is shared across subjects; every bounded processing-lag class is charged.
const oneMinute=structuredClone(frames['1m'].bars);
for(let c=0;c<288;c++){
 if(c)for(let i=0;i<5;i++){const last=oneMinute.at(-1);oneMinute.shift();oneMinute.push({...last,t:last.t+60000,o:last.c,c:last.c+.1,h:last.c+.3,l:last.c-.2});}
 for(const minutes of [5,10,15,20,25,30,35,40,45,50,55,60,120,360,720]){
  const built=await buildMarketManifest('1m',oneMinute.slice(-minutes),now+c*300000);
  for(const b of built.blocks){const e=await encodeEvidence(parseEvidence(b.payload)),bytes=e.data.length+512;
   if(!seen.has(b.blockId)){seen.add(b.blockId);rawDay+=bytes;postEntryRawDay+=bytes;rawRowsDay++;}
   // One ongoing Primary, processed each cycle, pins its exact newly consumed
   // five-minute interval. Longer recovery slices are in the general pool.
   if(minutes===5&&!pinned.has(b.blockId)){pinned.add(b.blockId);pinDay+=bytes;postEntryPinDay+=bytes;}
  }
 }
}
const official= samples.official.censusBytes+samples.official.richRowBytes+worstOutcome+finalCompact+cycle;
const cleanupLagDays=4; // Generated ratio-preserving catch-up validated in targeted tests.
function project(days,attempts){const parts={census:attempts*Math.min(days,365+cleanupLagDays)*census,rich:attempts*Math.min(days,90+cleanupLagDays)*rich,outcomes:attempts*Math.min(days,90+cleanupLagDays)*worstOutcome,final:attempts*Math.min(days,365+cleanupLagDays)*finalCompact,cycles:288*Math.min(days,365+cleanupLagDays)*cycle,raw:rawDay*Math.min(days,30+cleanupLagDays),officialPins:pinDay*Math.max(0,days-Math.min(days,30+cleanupLagDays)),officialForensics:10*days*official,recovery:24*1024**2,registry:16*1024**2};const bytes=Object.values(parts).reduce((a,b)=>a+b,0)*allocationFactor*contingency;return {parts,B1GB:bytes/1e9,totalGB:(bytes+23.91e6)/1e9,headroomGB:(10e9-bytes-23.91e6)/1e9};}
const projections=Object.fromEntries([30,90,365].map(d=>[d,project(d,1728)])),stress=project(365,2016);
const budgetChecks={census:census<=384,earlyRich:samples.earlyRejection.richEncodedBytes<=2560,technicalRich:samples.candidate.richEncodedBytes<=4608,finalResult:finalCompact<=192,cycleContext:cycle<=1536,rawBlock:largestBlock<=6144,rawDaily:rawDay<=8*1024**2,officialRawDaily:pinDay<=1024**2,officialPackage:official<=32768,activeState:maxActive<=6144,testMatrixOutcomeAverage:outcomeProfileBudget(Object.values(profiles)).planningAveragePass,reviewCeiling:Object.values(profiles).every(p=>p.bytes<=8192)};
console.log(JSON.stringify({MEASURED:{samples,profiles,testMatrixAverage:matrixAverage,maximumTestedOutcomeBytes:worstOutcome,allocationFactorMeasured:allocation,allocationPages:stat,rawDayBytes:rawDay,postEntryRawDayBytes:postEntryRawDay,postEntryPinDayBytes:postEntryPinDay,rawRowsDay,generatedRates:{attemptsDay:2016,outcomeRowsDay:2016*20,richRowsDay:2016+288+rawRowsDay,censusRowsDay:2016,finalRowsDay:2016,cycleRowsDay:288,rawRowsDay},cleanupCapacityPerCategoryDay:256*288,cleanupLagValidation:{scale:48,subjectsDay:42,outcomeRowsSubject:20,cyclesDay:6,limit:256,outageHours:48,backlogZeroByDay:4,independentFinalExpiry:true,processingBudgetSubjects:4032,processingBudgetBytes:24*1024**2},officialPinDayBytes:pinDay,maximumRawBlockBytes:largestBlock,maximumActiveBytes:maxActive,compactFinalBytes:finalCompact},ESTIMATED:{assumptions:{standardAttemptsDay:1728,stressAttemptsDay:2016,cyclesDay:288,officialDay:10,allocationFactor,contingency,cleanupLagDays,cleanupLagStatus:'VALIDATED_RATIO_PRESERVING_GENERATED_WORKLOAD; 48H_OUTAGE; CLEARED_WITHIN_DAY_4',outcomeDistribution:'NO_PRODUCTION_DISTRIBUTION_ASSUMED; ALL-HEAVY SENSITIVITY',ownerProductionBytes:23.91e6},projections,stress},budgetChecks,resourceGate:Object.values(budgetChecks).every(Boolean)&&stress.B1GB<6&&stress.totalGB<8&&stress.headroomGB>=2?'PASS':'FAIL'},null,2));
physical.close();outcomeSample.db.close();
