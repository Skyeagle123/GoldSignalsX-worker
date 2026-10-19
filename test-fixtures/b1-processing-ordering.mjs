import {processingBudget,ProcessingYield} from '../measurement-processing-budget.js';
import {processingAdapter} from '../measurement-d1-adapter.js';
import assert from 'node:assert/strict';
import {packageDatabase} from './b1-package-a.mjs';
import {prepareMarketEvidence} from '../measurement-market-envelope.js';
import {ingestMarketEvidence} from '../measurement-market-store.js';
import {freshSelection,selectSampleBlock,selectedSample} from '../measurement-processing-samples.js';
import {emptySampleFold,foldSelectedSample,finalizeSampleMetrics,freshWitnessVerification,verifySampleOutcome} from '../measurement-processing-fold.js';
import {bytes,serialize} from '../measurement-independent-contract.js';
export const AT=1790953800000;
export const subject={id:'ordered-official',side:'buy',createdAt:AT,entry:100,tp1:110,tp2:120,sl:90,status:'active',closedAt:null};
export const tick=(i,price=100,{at=AT+i,providerAt=at,session='ordering',sequence=i}={})=>({ts:at,price,measurement:{provider:'mt5',sessionId:session,sequence,providerTimestamp:providerAt}});
export async function marketPackets(ticks,{cohort='synthetic-b1-ordering',session=ticks[0]?.measurement.sessionId??'ordering',bars=[],group=ticks.length||1,observedAt=Math.max(AT+7200000,...ticks.map(t=>t.ts),...bars.map(b=>b.t+60000)),from=Math.min(AT,...ticks.map(t=>t.measurement.providerTimestamp),...bars.map(b=>b.t)),to=Math.max(AT,...ticks.map(t=>t.measurement.providerTimestamp),...bars.map(b=>b.t+60000)),gaps=[],truncated=false}={}){
 const out=[];if(!ticks.length)return prepareMarketEvidence({cohort,source:'mt5',session,from,to,observedAt,bars,gaps,truncated});
 for(let i=0;i<ticks.length;i+=group)out.push(...await prepareMarketEvidence({cohort,source:'mt5',session,from,to,observedAt,ticks:ticks.slice(i,i+group),bars:i===0?bars:[],gaps,truncated}));return out;
}
// Local algorithm harness: real accepted packet ingestion, bounded source reads,
// JSON checkpoint/restart between opportunities. Transactional job fencing is
// independently exercised by actual scheduled/runtime regressions.
export async function orderedModel(packets,{subject:sub=subject,page=64,decode=8,now=AT+7200000,maxPasses=20000,archive=null,setup=null,onPass=null,database=null,ingest=true}={}){
 const x=database??await packageDatabase();try{
 if(ingest)for(const p of packets)await ingestMarketEvidence(x.db,p,{now});if(setup)await setup(x);
 const cohort=JSON.parse(packets[0].wire).cohort,hotRevision=x.raw.prepare('SELECT revision FROM pa_meta').get().revision,root=x.raw.prepare('SELECT root_key FROM pa_meta').get().root_key;
 let state={pin:{cohort,hotRevision,root,asOf:now},selection:freshSelection(page),cursor:null,fold:emptySampleFold(sub),phase:'SELECT',count:0};const stats={passes:0,maxDescriptors:0,maxSelectionBytes:0,maxCheckpointBytes:0,maxDecoded:0};
 for(let pass=0;pass<maxPasses;pass++){
  const budget=archive?processingBudget(x.db):{remaining:decode},cache=new Map(),reader=archive?await archive.processingSession(budget):null,db=archive?processingAdapter(x.db,budget):x.db;
  try{
  if(state.pin.root)state.pin.rootProof=await reader.processingRootStep(state.pin.root,state.pin.rootProof??null,budget);
  if((!state.pin.root||state.pin.rootProof.complete)&&state.phase==='SELECT'&&await selectSampleBlock(db,reader,state,budget))state.phase='FOLD';
  if(state.phase==='FOLD'){
   while(state.selection.position<state.selection.items.length){const d=state.selection.items[state.selection.position],r=await selectedSample(db,reader,state,d,budget,cache);if(!r)break;await foldSelectedSample(sub,state.fold,r.sample,d.key,{asOf:now,availableAt:r.availableAt});state.cursor=d.key;state.count++;state.selection.position++;}
   if(state.selection.position===state.selection.items.length){if(state.selection.more){state.selection=freshSelection(page);state.phase='SELECT';}else{finalizeSampleMetrics(sub,state.fold,now);state.verification=freshWitnessVerification();state.phase='VERIFY';}}
  }
  if(state.phase==='VERIFY'&&await verifySampleOutcome(db,reader,state,sub,budget))state.phase='DONE';
  }catch(e){if(!(e instanceof ProcessingYield))throw e;}
  stats.passes++;stats.maxDescriptors=Math.max(stats.maxDescriptors,state.selection.items.length);stats.maxSelectionBytes=Math.max(stats.maxSelectionBytes,bytes(serialize(state.selection.items)));stats.maxCheckpointBytes=Math.max(stats.maxCheckpointBytes,bytes(serialize(state)));stats.maxDecoded=Math.max(stats.maxDecoded,archive?budget.trace().marketDecodes:decode-budget.remaining);
  if(onPass)await onPass(x,state,pass);state=JSON.parse(JSON.stringify(state));if(state.phase==='DONE')return {fold:state.fold,count:state.count,stats,state};
 }
 assert.fail('bounded model did not finish: '+state.phase);
 }finally{if(!database)x.close();}
}
