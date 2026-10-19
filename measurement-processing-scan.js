import {ProcessingYield} from './measurement-processing-budget.js';
import {need,hash,serialize,parseEvidence,bytes} from './measurement-independent-contract.js';
import {SAMPLE_SCAN_VERSION,SAMPLE_ORDER_VERSION,SAMPLE_REDUCER_VERSION,freshSelection,selectSampleBlock,selectedSample,inspectCapturedDelta} from './measurement-processing-samples.js';
import {CONTINUITY_VERSION,emptySampleFold,foldSelectedSample,finalizeSampleMetrics,freshWitnessVerification,verifySampleOutcome} from './measurement-processing-fold.js';

function freshState(identity,boundary,marketRevision,now,subject){
 const b=JSON.parse(boundary.boundary);
 return {version:SAMPLE_SCAN_VERSION,ordering:SAMPLE_ORDER_VERSION,reducer:SAMPLE_REDUCER_VERSION,identity,boundary:boundary.boundary,pin:{cohort:identity.authority.context.cohort,epoch:b[0],root:b[1],catalogue:b[2],references:b[3],deletion:b[4],hotRevision:boundary.revision,asOf:now},inputMarketRevision:marketRevision,phase:'SELECT',cursor:null,selection:freshSelection(),fold:emptySampleFold(subject),verification:null,delta:null,count:0,prefixDigest:null,generation:0,previousCheckpoint:null};
}
export async function advanceSampleScan(db,archive,{previous,previousRow,identity,boundary,marketRevision,now,subject,hook=null,budget:sharedBudget=null}){
 let state=null;
 if(previous?.scanProgress?.version===SAMPLE_SCAN_VERSION){
  const s=previous.scanProgress;
  need(s.stateDigest===await hash(s.state)&&s.foldDigest===await hash(previous.folded.state),'pa_processing_scan_integrity');
  const receiptId='result:'+await hash([previous.subjectId,previousRow.revision,previous]);
  const receipt=await db.first('SELECT digest FROM pa_records WHERE id=? AND ready=1',receiptId);
  need(receipt&&receipt.digest===await hash(previousRow.payload),'pa_processing_scan_integrity');
  if(s.state.fold.continuityVersion===CONTINUITY_VERSION&&serialize(s.state.identity)===serialize(identity)&&s.state.boundary===boundary.boundary)state=structuredClone(s.state);
 }
 if(!state)state=freshState(identity,boundary,marketRevision,now,subject);
 state.previousCheckpoint=previousRow?await hash(previousRow.payload):null;state.generation++;
 const budget=sharedBudget??{remaining:8},cache=new Map(),dependencies=new Map();let transitions=0;
 let yielded=false;
 try{
 if(state.pin.root){need(archive?.processingRootStep,'pa_processing_archive_required');state.pin.rootProof=await archive.processingRootStep(state.pin.root,state.pin.rootProof??null,budget);if(!state.pin.rootProof.complete)return {state,complete:false,dependencies:[],decodedRecords:8-budget.remaining};}
 while(transitions++<24){
  if(state.phase==='DONE'){
   if(state.inputMarketRevision===marketRevision)break;
   state.delta={after:'',hotRevision:boundary.revision,asOf:now,marketRevision,changed:false,replay:false};state.phase='DELTA';
  }
  if(state.phase==='DELTA'){
   if(!await inspectCapturedDelta(db,archive,state,budget))break;
   await hook?.('SAMPLE_DELTA_VERIFIED',{replay:state.delta.replay});
   if(state.delta.replay){const d=state.delta;state=freshState(identity,{...boundary,revision:d.hotRevision},d.marketRevision,d.asOf,subject);}
   else{const priorRevision=state.pin.hotRevision;state.priorChecks=state.verification?.checks??null;state.pin.hotRevision=state.delta.hotRevision;state.pin.asOf=state.delta.asOf;state.inputMarketRevision=state.delta.marketRevision;state.delta=null;state.selectionPin={...state.pin,lowerRevision:priorRevision,skipCold:true};state.selection=freshSelection();state.verification=null;state.phase='SELECT';}
  }
  if(state.phase==='SELECT'){
   if(!await selectSampleBlock(db,archive,state,budget,{hook}))break;
   await hook?.('SAMPLE_SELECTION_COMPLETE',{count:state.selection.items.length});state.phase='FOLD';
  }
  if(state.phase==='FOLD'){
   const selection=state.selection;
   while(selection.position<selection.items.length){
    const item=selection.items[selection.position],selected=await selectedSample(db,archive,state,item,budget,cache);if(!selected)break;
    await hook?.('SAMPLE_BEFORE_FOLD',{id:item.id});
    await foldSelectedSample(subject,state.fold,selected.sample,item.key,{asOf:state.pin.asOf,availableAt:selected.availableAt});
    state.prefixDigest=await hash([state.prefixDigest,item.id,item.digest,item.key]);state.cursor=item.key;state.count++;selection.position++;dependencies.set(item.recordId,item.recordDigest);
   }
   if(selection.position<selection.items.length)break;
   if(selection.more){state.selection=freshSelection();state.phase='SELECT';if(!budget.remaining)break;continue;}
   finalizeSampleMetrics(subject,state.fold,state.pin.asOf);state.verification=freshWitnessVerification();state.phase='VERIFY';
  }
  if(state.phase==='VERIFY'){
   if(!await verifySampleOutcome(db,archive,state,subject,budget))break;
   state.phase='DONE';state.selection=freshSelection();
   // Preserve finite snapshot progress even if later arrivals remain. The next
   // invocation reconciles a finite delta, never restarts because revision grew.
   if(state.inputMarketRevision!==marketRevision)continue;
   break;
  }
 }
 }catch(e){if(!(e instanceof ProcessingYield))throw e;yielded=true;}
 need(bytes(serialize(state))<52000,'pa_processing_scan_bound');
 return {state,complete:!yielded&&state.phase==='DONE'&&state.inputMarketRevision===marketRevision,dependencies:[...dependencies],decodedRecords:8-budget.remaining};
}
