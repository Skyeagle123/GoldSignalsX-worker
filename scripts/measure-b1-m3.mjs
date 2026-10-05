// Local fixture measurements only: no cloud resources, transport network or SQL.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
import {loadM1Workers,completeJournal,cycleEnvironment,now,provenance} from '../test-fixtures/b1-m1.mjs';
import * as sm from '../signal-measurement.js';
import * as engine from '../signal-engine.js';
import {prepareCycleEnvelopes} from '../measurement-producer.js';
import {fragmentMeasurementPacket,serializedBytes} from '../measurement-transport-fragment.js';
import {createOfflineMultipartTransport,createMockDurableTransportStore} from '../measurement-transport-multipart.js';

const output=process.argv[2];if(!output)throw new Error('local output path required');
const originalClock=Date.now;Date.now=()=>now+1234;
const originalFetch=globalThis.fetch;globalThis.fetch=()=>{throw new Error('M3_NO_NETWORK');};
try{
 const {after}=await loadM1Workers(),sources={representative:completeJournal(after,sm),heavy:completeJournal(after,sm,{heavy:true}),active:completeJournal(after,sm,{heavy:true,active:true})};
 // Match the accepted M1 benchmark: include actual local persistence receipts.
 for(const j of Object.values(sources))for(const c of j.candidates.filter(c=>j.officialIds.has(c.id))){const x=cycleEnvironment(after);try{await after.ensurePerformanceSchema(x.env);const r=await after.recordProductionPerformanceSafely(x.env,{...c,origin:'server'},'created');assert(r.ok);sm.rememberOfficialPersistence(c,'created',r);}finally{x.db.database.close();}}
 const source=sources.representative,tf='5m',trace={},includedMtf=engine.HIGHER_SIGNAL_TIMEFRAMES[tf]||[];
 const result=engine.computeServerSignal(source.frames[tf].bars,{tf,mtf:includedMtf.map(tf=>source.frames[tf]),live:source.live,barsSource:'d1',evaluationAt:now,filters:source.filters,dataQuality:{ok:false,reason:'synthetic-quality-failure'}},trace);
 const early=sm.beginDecisionCycle(now,source.frames,source.live,null,source.filters);early.capturedAt=now+1234;early.failedOfficialId=null;sm.observeAttempt(early,{tf,trace,result,requestedMtf:includedMtf,includedMtf});sources.early=early;
 const rows=[],prepared={};
 for(const [profile,j]of Object.entries(sources)){
  const packets=await prepareCycleEnvelopes(sm.decisionJournalObservations(j),provenance,{preparedAt:now+5000});prepared[profile]=packets;
  console.error('measuring',profile);
  const cycle=packets[0],heap=process.memoryUsage(),cpu=process.cpuUsage(),at=performance.now();
  const fragments=await fragmentMeasurementPacket(cycle,{receivedAt:now+86400000}),elapsedMs=performance.now()-at,usage=process.cpuUsage(cpu),end=process.memoryUsage();
  rows.push({profile,eventBytes:cycle.wireBytes,fragments:fragments.length,minimumFragmentEnvelopeBytes:Math.min(...fragments.map(serializedBytes)),maximumFragmentEnvelopeBytes:Math.max(...fragments.map(serializedBytes)),totalFragmentEnvelopeBytes:fragments.reduce((n,f)=>n+serializedBytes(f),0),fragmentationWallMs:elapsedMs,processCpuMs:(usage.user+usage.system)/1000,heapDeltaBytes:end.heapUsed-heap.heapUsed,rssDeltaBytes:end.rss-heap.rss});
 }
 const staged=createOfflineMultipartTransport({clock:()=>now+86400000,consumer:{async ingest(){throw new Error('incomplete must not dispatch');}},store:createMockDurableTransportStore({maxEvents:64})});
 // Distinct complete-payload event identities, 64 concurrently pending records.
 let firstId;
 for(let i=0;i<64;i++){
  const at=now+i*60000,journal=completeJournal(after,sm,{cycleAt:at});const [packet]=await prepareCycleEnvelopes(sm.decisionJournalObservations(journal),provenance,{preparedAt:at+5000});
  const fs=await fragmentMeasurementPacket(packet,{receivedAt:now+86400000});firstId??=fs[0];await staged.receive(fs[0]);
 }
 const capacityBefore=staged.metrics(),beforeDuplicate=staged.metrics().stagingBytes;
 for(let i=0;i<100;i++)await staged.receive(firstId);const afterDuplicate=staged.metrics();assert.equal(afterDuplicate.stagingBytes,beforeDuplicate);assert.equal(afterDuplicate.duplicateFragments,100);
 const t=createOfflineMultipartTransport({clock:()=>now+86400000,maxAttempts:1,consumer:{async ingest(){return {ok:false,status:'DEPENDENCY_PENDING',durable:false,retryable:true,error:'mock_dependency'};}}});
 const census=prepared.representative.find(p=>p.envelope.kind==='EVALUATION_CENSUS');await t.send(census);
 // Admission charges are bounded live token sets, not retained abandoned counts.
 const admissionStore=createMockDurableTransportStore();let unblock,entered;const blocked=new Promise(r=>unblock=r),arrived=new Promise(r=>entered=r);
 const owner=createOfflineMultipartTransport({store:admissionStore,clock:()=>now+86400000,consumer:{async ingest(){entered();await blocked;return {ok:true,status:'ACCEPTED',durable:true};}}});
 const pending=Array.from({length:16},()=>owner.send(census));await arrived;const admissionBeforeCrash=owner.metrics();owner.crash();owner.crash();const admissionAfterCrash=owner.metrics();
 const restarted=createOfflineMultipartTransport({store:admissionStore,clock:()=>now+86400000,consumer:{async ingest(){return {ok:true,status:'DUPLICATE',durable:true};}}});await restarted.retry(census.envelope.eventId);unblock();await Promise.allSettled(pending);const admissionAfterLateCompletion=restarted.metrics();
 assert.equal(admissionAfterCrash.validationsInFlight,0);assert.equal(admissionAfterLateCompletion.framingsInFlight,0);
 const report={node:process.version,localOnly:true,productionResourceBudgetAcceptance:false,profiles:rows,staging:{maximumTestedConcurrentEvents:capacityBefore.events,bytes:capacityBefore.stagingBytes,duplicateDeliveries:100,duplicateAdditionalStagingBytes:afterDuplicate.stagingBytes-beforeDuplicate,metrics:afterDuplicate},retrySnapshotBytes:serializedBytes(JSON.stringify(t.inspect(census.envelope.eventId))),dlqSnapshotIncludingOriginalFragmentsBytes:serializedBytes(JSON.stringify(t.dlq())),admission:{beforeCrash:admissionBeforeCrash,afterCrash:admissionAfterCrash,afterLateCompletion:admissionAfterLateCompletion,ownershipLimits:{receiveTokens:64,sendTokens:16},metadataSampleBytes:serializedBytes(JSON.stringify({receiveTokens:admissionBeforeCrash.ownedValidations,sendTokens:admissionBeforeCrash.ownedFramings})),note:"Token sets are volatile, runtime-owned and released idempotently; serialized counter sample is not a heap measurement."},transportPreparedSqlOperations:0,transportReads:0,transportWrites:0,notes:['Single local sample per profile; heap/RSS include GC and concurrent Node runtime activity.','Mock store is retained in-memory state, not real durable storage.','64 partial events retain one fragment each; full 64-heavy-event capture exceeds the 16 MiB store bound and is not claimed.']};
 await fs.writeFile(output,JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report,null,2));
}finally{globalThis.fetch=originalFetch;Date.now=originalClock;}
