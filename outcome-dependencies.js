// Pure dependency contract shared by writer and SELECT-only validation.
export function outcomeDependencies(p){
 const refs=[[p.coverageRef,0,'COVERAGE_CHECKPOINT'],[p.supersedesEventId,1,null],
  ...((p.windowReferences||[]).map(id=>[id,2,'WINDOW_FINALIZED'])),
  ...((p.barrierReferences||[]).map(id=>[id,3,null])),
  [p.globalReference,4,'WINDOW_FINALIZED'],[p.coverageReference,5,'WINDOW_FINALIZED']];
 return refs.filter(([id])=>id!=null);
}
export function validateOutcomeDependencies(p,events){
 const issues=[];
 for(const [id,relation,type] of outcomeDependencies(p)){
  const target=events.find(e=>e.eventId===id);
  if(id===p.eventId||!target||target.subjectId!==p.subjectId||!Number.isSafeInteger(target.availableAt)||target.availableAt>p.availableAt||(type&&target.eventType!==type)||(relation===3&&target.evidenceType!=='BARRIER_OBSERVATION'))issues.push({eventId:p.eventId,reference:id,reason:'OUTCOME_DEPENDENCY_INVALID'});
 }
 if(p.evidenceType==='BARRIER_OBSERVATION'&&!p.coverageRef)issues.push({eventId:p.eventId,reason:'COVERAGE_REFERENCE_MISSING'});
 return issues;
}
