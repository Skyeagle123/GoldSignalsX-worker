import {validateMarketEvidence} from './measurement-market-envelope.js';
import {hash,serialize,need,LIMITS} from './measurement-independent-contract.js';
import {assertion,clearAssertions} from './measurement-d1-adapter.js';
import {stageRecord,finishRecord} from './measurement-independent-store.js';
import {reserve,estimateWrite,commitReservationStatements,releaseReservation,settleReservationStatements} from './measurement-reservations.js';
export const sampleDigest=s=>hash({type:s.type,value:s.value,occurredAt:s.occurredAt});
export async function ingestMarketEvidence(db,packet,{now,archive=null}={}){
 const p=await validateMarketEvidence(packet.wire);need(packet.id==='market:'+await hash(packet.wire),'pa_market_identity');
 const old=await db.first('SELECT * FROM pa_records WHERE id=? AND ready=1',packet.id);if(old)return {status:'DUPLICATE',durable:true};
 const samples=[];for(const s of p.samples){const digest=await sampleDigest(s);samples.push({id:s.id,digest});if(archive?.lookupRecord){const historical=await archive.lookupRecord('sample:'+s.id);need(!historical||historical.sampleDigest===digest,'pa_archived_sample_conflict');}}
 const id='market-scope:'+packet.id,reservation=await reserve(db,{id:'market:'+packet.id,workClass:'ingestion',owner:packet.id,bytes:estimateWrite(packet.wire.length,samples.length+8),now});
 try{await db.batch([...(reservation.state==='RESERVED'?commitReservationStatements(db,reservation,now):[]),
  assertion(db,`(SELECT COUNT(*) FROM pa_samples)+(SELECT COUNT(*) FROM json_each(?) x WHERE NOT EXISTS(SELECT 1 FROM pa_samples WHERE id=json_extract(x.value,'$.id')))<=16384`,serialize(samples)),
  db.statement('INSERT INTO pa_cohort_state(cohort,revision) VALUES(?,(SELECT revision+1 FROM pa_meta)) ON CONFLICT DO NOTHING',[p.cohort]),
  db.statement("INSERT INTO pa_scopes(id,cohort,revision,original_at,kind) VALUES(?,?,(SELECT revision+1 FROM pa_meta),?,'MARKET') ON CONFLICT DO NOTHING",[id,p.cohort,p.from]),
  db.statement("INSERT INTO pa_market_ingress VALUES(?,?,?,'PENDING') ON CONFLICT DO NOTHING",[packet.id,id,p.cohort]),clearAssertions(db)]);
 }catch(error){try{await releaseReservation(db,reservation,{now});}catch{}throw error;}
 const contradiction=await db.first('SELECT s.id FROM json_each(?) x JOIN pa_samples s ON s.id=json_extract(x.value,\'$.id\') WHERE s.digest!=json_extract(x.value,\'$.digest\') LIMIT 1',serialize(samples));
 if(contradiction){await db.batch([db.statement("UPDATE pa_market_ingress SET state='CONFLICT' WHERE id=? AND state='PENDING'",[packet.id]),db.statement('UPDATE pa_scopes SET conflict=1,quarantine=1 WHERE id=?',[id])]);throw Error('pa_market_sample_conflict');}
 await stageRecord(db,{id:packet.id,kind:'MARKET_EXPORT',scopeId:id,cohort:p.cohort,wire:packet.wire,createdAt:p.from,category:'MARKET'});
 await db.batch([assertion(db,'NOT EXISTS(SELECT 1 FROM json_each(?) x JOIN pa_samples s ON s.id=json_extract(x.value,\'$.id\') WHERE s.digest!=json_extract(x.value,\'$.digest\'))',serialize(samples)),
  db.statement('INSERT INTO pa_samples SELECT json_extract(value,\'$.id\'),json_extract(value,\'$.digest\'),?,?,? FROM json_each(?) WHERE true ON CONFLICT DO NOTHING',[packet.id,p.cohort,now,serialize(samples)]),
  ...finishRecord(db,packet.id,now),...settleReservationStatements(db,reservation),db.statement("INSERT INTO pa_jobs(id,class,scope_id,next_at,deadline) SELECT 'projection:'||id,'processing',id,?,? FROM pa_scopes WHERE cohort=? AND kind IN('CANDIDATE','OFFICIAL') AND original_at<=? ON CONFLICT(id) DO UPDATE SET state='PENDING',next_at=MIN(next_at,excluded.next_at)",[now,now+86400000,p.cohort,p.to]),db.statement("UPDATE pa_market_ingress SET state='COMMITTED' WHERE id=?",[packet.id]),
  db.statement("INSERT INTO pa_jobs(id,class,scope_id,next_at,deadline) VALUES(?,'archive',?,?,?) ON CONFLICT DO NOTHING",['archive-scope:'+id,id,now,now+14*86400000]),clearAssertions(db)]);return {status:'ACCEPTED',durable:true,recordId:packet.id};
}
