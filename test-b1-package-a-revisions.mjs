import assert from 'node:assert/strict';
import {test} from 'node:test';
import {seedOfficial,gate,deliver,packageDatabase} from './test-fixtures/b1-package-a.mjs';
import {lifecycle} from './test-fixtures/b1-m2.mjs';
import {validateD1Snapshot,rebuildD1Projection} from './measurement-independent-store.js';
import {protectedEffect,createMeasurementD1Adapter} from './measurement-d1-adapter.js';
import {commitEnvelope} from './measurement-independent-store.js';
import {admitCorrection} from './measurement-corrections.js';
test('D1 delayed response remains historical; protected write rejects new terminal conflict',async()=>{const x=await seedOfficial();try{
 const captured=gate(),release=gate();const delayed=validateD1Snapshot(x.db,x.scopeId,{now:x.at,hook:async(_p,s)=>{captured.resolve(s);await release.promise;}});const old=await captured.promise;
 assert.equal(old.stateAtRevision,'COMPLETE');await deliver(x.db,await lifecycle(x.scopeId,'sl'),x.at);await deliver(x.db,await lifecycle(x.scopeId,'tp2'),x.at);await rebuildD1Projection(x.db,x.scopeId,{now:x.at});release.resolve();const response=await delayed;
 assert.equal(response.consistency,'VALIDATED_AT_REVISION');assert.equal(response.validatedRevision,old.validatedRevision);assert.equal(response.stateAtRevision,'COMPLETE');assert(!JSON.stringify(response).includes('CURRENT'));
 const fresh=await validateD1Snapshot(x.db,x.scopeId,{now:x.at});assert.equal(fresh.stateAtRevision,'CONFLICT');assert(fresh.validatedRevision>old.validatedRevision);
 await assert.rejects(protectedEffect(x.db,response,'unsafe',{outcome:'old'},{now:x.at}),/pa_fence_failed/);assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_effects').get().n,0);
}finally{x.close();}});
test('D1 unchanged validated state permits freshly fenced idempotent write',async()=>{const x=await seedOfficial();try{await protectedEffect(x.db,x.snapshot,'effect',{ok:true},{now:x.at});await protectedEffect(x.db,x.snapshot,'effect',{ok:true},{now:x.at});assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_effects').get().n,1);}finally{x.close();}});
test('D1 snapshot payload cannot be paired with its original digest',async()=>{const x=await seedOfficial();try{const changed=structuredClone(x.snapshot);changed.payload.status='tp2';await assert.rejects(protectedEffect(x.db,changed,'bad',{},{now:x.at}),/pa_snapshot_payload_digest/);}finally{x.close();}});
const changes={
 binding:x=>x.raw.prepare('INSERT INTO pa_bindings(evaluation_id,digest,signal_id,first_event,cohort) VALUES(?,?,?,?,?)').run('new-evaluation','0'.repeat(64),x.scopeId,'fixture',x.snapshot.scope.cohort),
 quarantine:x=>x.raw.prepare('UPDATE pa_scopes SET quarantine=1 WHERE id=?').run(x.scopeId),
 conflict:x=>x.raw.prepare('UPDATE pa_scopes SET conflict=1 WHERE id=?').run(x.scopeId),
 correction:async x=>admitCorrection(x.db,{id:'c',scopeId:x.scopeId,predecessor:'decision:'+x.evaluationId,reason:'captured-evidence-correction'},{now:x.at}),
 lifecycle:async x=>deliver(x.db,await lifecycle(x.scopeId,'sl'),x.at),
 availability:x=>x.raw.prepare('UPDATE pa_scopes SET revision=revision+1,available_at=? WHERE id=?').run(x.at+1,x.scopeId),
 pending:x=>x.raw.prepare('UPDATE pa_scopes SET revision=revision+1,pending=1 WHERE id=?').run(x.scopeId),
 epoch:x=>x.raw.prepare("UPDATE pa_meta SET epoch='replacement-epoch'").run(),
};
for(const [name,change]of Object.entries(changes))test('D1 fresh SQL fence rejects stale '+name+' authority',async()=>{const x=await seedOfficial();try{await change(x);await assert.rejects(protectedEffect(x.db,x.snapshot,'stale-'+name,{},{now:x.at}),/pa_fence_failed/);assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_effects').get().n,0);const fresh=await validateD1Snapshot(x.db,x.scopeId,{now:x.at});assert.notEqual(fresh.stateAtRevision,'COMPLETE');}finally{x.close();}});
test('D1 duplicate durability does not imply projection validation',async()=>{const x=await seedOfficial();try{await deliver(x.db,await lifecycle(x.scopeId,'sl'),x.at);const r=await commitEnvelope(x.db,x.creation.wire,{now:x.at});assert.equal(r.ingestion.status,'DUPLICATE');assert.equal(r.ingestion.durable,true);assert.equal(r.projection.stateAtRevision,'PENDING');assert.equal(r.projection.reason,'stale_projection');}finally{x.close();}});
test('D1 independent adapter instances share durable fences, not JavaScript registry',async()=>{const x=await seedOfficial();try{const isolateB=createMeasurementD1Adapter(x.binding);await changes.quarantine(x);await assert.rejects(protectedEffect(isolateB,x.snapshot,'isolate-b',{},{now:x.at}),/pa_fence_failed/);}finally{x.close();}});
test('D1 out-of-order responses never infer freshness from arrival order',async()=>{const x=await seedOfficial();try{const hold=gate(),seen=gate();const old=validateD1Snapshot(x.db,x.scopeId,{now:x.at,hook:async()=>{seen.resolve();await hold.promise;}});await seen.promise;await deliver(x.db,await lifecycle(x.scopeId,'tp1'),x.at);const newer=await rebuildD1Projection(x.db,x.scopeId,{now:x.at});hold.resolve();const earlier=await old;assert(newer.validatedRevision>earlier.validatedRevision);assert.equal(earlier.payload.status,'active');assert.equal(newer.payload.status,'tp1');}finally{x.close();}});
test('D1 empty binding absence is part of scope revision',async()=>{const x=await seedOfficial();try{const before=await validateD1Snapshot(x.db,x.evaluationId,{now:x.at});x.raw.prepare('INSERT INTO pa_bindings(evaluation_id,digest,signal_id,first_event,cohort) VALUES(?,?,?,?,?)').run('extra','0'.repeat(64),x.evaluationId,'fixture',before.scope.cohort);const after=await validateD1Snapshot(x.db,x.evaluationId,{now:x.at});assert(after.validatedRevision>before.validatedRevision);assert.notEqual(after.dependencyIdentity,before.dependencyIdentity);}finally{x.close();}});
test('D1 numeric revision reuse is rejected by epoch',async()=>{const x=await seedOfficial();try{x.raw.prepare('UPDATE pa_meta SET epoch=?').run('replacement');x.raw.prepare('UPDATE pa_scopes SET revision=? WHERE id=?').run(x.snapshot.validatedRevision,x.scopeId);await assert.rejects(protectedEffect(x.db,x.snapshot,'aba',{},{now:x.at}),/pa_fence_failed/);}finally{x.close();}});
