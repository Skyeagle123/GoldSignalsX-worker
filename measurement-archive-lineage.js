// Private content-addressed transition evidence, authorized by a completed SQL
// restore. It is historical archive authority, never a D1 write token.
import {need,serialize,hash,identity} from './measurement-independent-contract.js';
import {assertion,clearAssertions} from './measurement-d1-adapter.js';
export async function prepareArchiveTransition(archive,root,epoch,job){
 need(archive?.namespace&&root.epoch!==epoch,'pa_lineage_transition');
 const wire=serialize({format:'measurement-archive-transition-v1',sourceRoot:root.key,sourceEpoch:root.epoch,sourceGeneration:root.generation,targetEpoch:epoch,restoreIdentity:job.id,restoreGeneration:job.generation,restoreNonce:job.lease_nonce,sqlRestoreAuthorized:true},8192);
 return {wire,key:archive.namespace+'transitions/'+await hash(wire)};
}
export async function publicationLineage(db,archive,previous,epoch){
 if(!previous||previous.epoch===epoch)return {};
 const row=await db.first("SELECT l.* FROM pa_archive_lineage l JOIN pa_restores r ON r.id=l.restore_id CROSS JOIN pa_meta m WHERE l.target_epoch=? AND l.target_epoch=m.epoch AND r.state='COMPLETE' AND r.semantic_index_version=1",epoch);
 need(row&&row.source_root===previous.key&&row.source_epoch===previous.epoch&&row.source_generation===previous.generation,'pa_lineage_authority');
 await db.batch([assertion(db,"EXISTS(SELECT 1 FROM pa_archive_lineage l JOIN pa_restores r ON r.id=l.restore_id CROSS JOIN pa_meta m WHERE l.target_epoch=m.epoch AND l.target_epoch=? AND l.transition_key=? AND l.transition_wire=? AND r.state='COMPLETE' AND r.semantic_index_version=1)",epoch,row.transition_key,row.transition_wire),clearAssertions(db)]);
 const object=await archive.write('transitions',row.transition_wire);need(object.key===row.transition_key,'pa_lineage_identity');
 return {lineageTransition:object.key};
}
export async function verifyArchiveEdge(archive,child,parent){
 need(child.previous===parent.key&&parent.generation<child.generation,'pa_root_chain');
 if(child.epoch===parent.epoch){need(!child.lineageTransition,'pa_root_transition_unexpected');return;}
 need(typeof child.lineageTransition==='string','pa_root_transition_missing');
 let t;try{t=JSON.parse(await archive.read(child.lineageTransition));}catch(error){throw Error('pa_root_transition_unavailable:'+error.message);}
 need(t.format==='measurement-archive-transition-v1'&&t.sqlRestoreAuthorized===true&&t.sourceRoot===parent.key&&t.sourceEpoch===parent.epoch&&t.sourceGeneration===parent.generation&&t.targetEpoch===child.epoch&&t.targetEpoch!==t.sourceEpoch&&Number.isSafeInteger(t.restoreGeneration)&&t.restoreGeneration>0,'pa_root_transition_invalid');
 identity(t.restoreIdentity);identity(t.restoreNonce);
}
