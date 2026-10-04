// Scheduled, bounded writer-side maintenance. Never imported by reporting.
const DAY=86400000;
export const RETENTION_LIMIT=256;
const chunks=rows=>Array.from({length:Math.ceil(rows.length/96)},(_,i)=>rows.slice(i*96,(i+1)*96));
export async function maintainMeasurementRetention(db,asOf,{candidateDays=90,limit=RETENTION_LIMIT}={}){
 if(candidateDays!==90||!Number.isInteger(limit)||limit<1||limit>RETENTION_LIMIT||!Number.isSafeInteger(asOf))throw new Error('measurement_retention_invalid');
 const select=async(sql,...values)=>(await db.prepare(sql).bind(...values).all()).results||[];
 const remove=async(table,key,ids)=>{for(const part of chunks(ids))await db.prepare(`DELETE FROM ${table} WHERE ${key} IN (${part.map(()=>'?').join(',')})`).bind(...part).run();};
 try{
  if((await db.prepare('SELECT version FROM measurement_schema_meta WHERE singleton=1').first())?.version!==3)throw new Error('measurement_schema_not_ready');
  if(typeof db.batch!=='function')throw new Error('measurement_retention_atomic_batch_unavailable');
  const rich=await select(`SELECT evidence_id FROM measurement_rich_evidence e WHERE retain_until<=? AND NOT (owner_type='market' AND EXISTS(SELECT 1 FROM measurement_official_pins p WHERE p.block_id=e.owner_id)) ORDER BY retain_until,evidence_id LIMIT ?`,asOf,limit);
  await remove('measurement_rich_evidence','evidence_id',rich.map(r=>r.evidence_id));
  const outcomes=await select(`SELECT r.record_id FROM measurement_outcome_records r JOIN measurement_subjects s ON s.subject_ref=r.subject_ref WHERE r.retention_bucket<=? AND s.recorded_at+r.recorded_offset<=? AND EXISTS(SELECT 1 FROM signal_decision_evidence d WHERE d.evaluation_id=s.subject_id AND d.kind='CANDIDATE') ORDER BY r.retention_bucket,r.record_id LIMIT ?`,Math.floor((asOf-90*DAY)/DAY),asOf-90*DAY,limit);
  await remove('measurement_outcome_records','record_id',outcomes.map(r=>r.record_id));
  const legacy=await select(`SELECT event_id FROM measurement_legacy_outcome_evidence o WHERE recorded_at<=? AND EXISTS(SELECT 1 FROM signal_decision_evidence d WHERE d.evaluation_id=o.subject_id AND d.kind='CANDIDATE') ORDER BY recorded_at,event_id LIMIT ?`,asOf-90*DAY,limit);
  await remove('measurement_legacy_outcome_evidence','event_id',legacy.map(r=>r.event_id));
  // Independent final-result expiry also collects legacy orphan results. Keep a
  // census until any later final-result retention deadline, never lose its path.
  const finals=await select(`SELECT subject_id FROM measurement_final_results WHERE retain_until<=? ORDER BY retain_until,subject_id LIMIT ?`,asOf,limit);
  await remove('measurement_final_results','subject_id',finals.map(r=>r.subject_id));
  const attempts=await select(`SELECT evaluation_id FROM signal_decision_evidence d WHERE kind='CANDIDATE' AND evaluated_at<=? AND NOT EXISTS(SELECT 1 FROM signal_measurement_state s WHERE s.evaluation_id=d.evaluation_id) AND NOT EXISTS(SELECT 1 FROM measurement_final_results f WHERE f.subject_id=d.evaluation_id) AND NOT EXISTS(SELECT 1 FROM measurement_outcome_records r JOIN measurement_subjects s ON s.subject_ref=r.subject_ref WHERE s.subject_id=d.evaluation_id) AND NOT EXISTS(SELECT 1 FROM measurement_legacy_outcome_evidence o WHERE o.subject_id=d.evaluation_id) ORDER BY evaluated_at,evaluation_id LIMIT ?`,asOf-365*DAY,limit);
  await remove('signal_decision_evidence','evaluation_id',attempts.map(r=>r.evaluation_id));
  const cycles=await select(`SELECT cycle_id FROM decision_cycle_evidence c WHERE evaluated_at<=? AND NOT EXISTS(SELECT 1 FROM signal_decision_evidence d WHERE d.cycle_id=c.cycle_id) ORDER BY evaluated_at,cycle_id LIMIT ?`,asOf-365*DAY,limit);
  await remove('decision_cycle_evidence','cycle_id',cycles.map(r=>r.cycle_id));
  const blocks=await select(`SELECT block_id FROM market_evidence_blocks b WHERE recorded_at<=? AND NOT EXISTS(SELECT 1 FROM measurement_official_pins p WHERE p.block_id=b.block_id) ORDER BY recorded_at,block_id LIMIT ?`,asOf-30*DAY,limit);
  for(const part of chunks(blocks.map(r=>r.block_id))){const q=part.map(()=>'?').join(',');await db.batch([
   db.prepare(`DELETE FROM market_evidence_references WHERE block_id IN (${q})`).bind(...part),
   db.prepare(`DELETE FROM measurement_market_links WHERE block_ref IN (SELECT rowid FROM market_evidence_blocks WHERE block_id IN (${q}))`).bind(...part),
   db.prepare(`DELETE FROM measurement_rich_evidence WHERE owner_type='market' AND owner_id IN (${q})`).bind(...part),
   db.prepare(`DELETE FROM market_evidence_blocks WHERE block_id IN (${q})`).bind(...part)]);}
  const recovery=await select(`SELECT subject_id FROM signal_measurement_state s WHERE EXISTS(SELECT 1 FROM measurement_final_results f WHERE f.subject_id=s.subject_id) ORDER BY updated_at,subject_id LIMIT ?`,limit);
  await remove('signal_measurement_state','subject_id',recovery.map(r=>r.subject_id));
  const subjects=await select(`SELECT subject_ref FROM measurement_subjects s WHERE NOT EXISTS(SELECT 1 FROM measurement_outcome_records r WHERE r.subject_ref=s.subject_ref) AND NOT EXISTS(SELECT 1 FROM measurement_final_results f WHERE f.subject_id=s.subject_id) AND NOT EXISTS(SELECT 1 FROM signal_measurement_state m WHERE m.subject_id=s.subject_id) AND NOT EXISTS(SELECT 1 FROM signal_decision_evidence d WHERE d.evaluation_id=s.subject_id OR d.official_signal_id=s.subject_id) ORDER BY subject_ref LIMIT ?`,limit);
  await remove('measurement_subjects','subject_ref',subjects.map(r=>r.subject_ref));
  const stale=await db.prepare(`SELECT COUNT(*) AS count,MIN(updated_at) AS oldest FROM signal_measurement_state WHERE kind='CANDIDATE' AND updated_at<?`).bind(asOf-2*DAY).first();
  return {ok:true,candidateAttemptsRemoved:attempts.length,richRemoved:rich.length,outcomesRemoved:outcomes.length+legacy.length,finalResultsRemoved:finals.length,cyclesRemoved:cycles.length,blocksRemoved:blocks.length,recoveryRemoved:recovery.length,subjectsRemoved:subjects.length,staleRecovery:stale,limitPerCategory:limit,officialRetention:'UNTIL_EXPLICIT_COHORT_REVIEW; NO_AUTOMATIC_OFFICIAL_DELETION'};
 }catch(error){return {ok:false,error:String(error?.message||'measurement_retention_failed')};}
}
