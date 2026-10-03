// Writer-side maintenance only. No reporting imports or request routes.
// Official evidence is NEVER automatically removed: >=365 days/until review is
// a minimum, not permission to delete an unreviewed cohort.
export async function maintainMeasurementRetention(db,asOf,{candidateDays=90,limit=100}={}){
 if(candidateDays<90||!Number.isInteger(limit)||limit<1||limit>100)throw new Error('measurement_retention_invalid');
 try{
  const schema=await db.prepare('SELECT version FROM measurement_schema_meta WHERE singleton=1').first();if(schema?.version!==1)throw new Error('measurement_schema_not_ready');
  const cutoff=asOf-candidateDays*86400000;
  const attempts=(await db.prepare("SELECT evaluation_id FROM signal_decision_evidence WHERE kind='CANDIDATE' AND evaluated_at<? ORDER BY evaluated_at,evaluation_id LIMIT ?").bind(cutoff,limit).all()).results||[];
  if(attempts.length){const ids=attempts.map(x=>x.evaluation_id),placeholders=ids.map(()=>'?').join(',');
   const statements=[db.prepare(`DELETE FROM signal_outcome_evidence WHERE subject_id IN (${placeholders})`).bind(...ids),
    db.prepare(`DELETE FROM signal_measurement_state WHERE subject_id IN (${placeholders})`).bind(...ids),
    db.prepare(`DELETE FROM signal_decision_evidence WHERE kind='CANDIDATE' AND evaluation_id IN (${placeholders})`).bind(...ids)];
   if(typeof db.batch!=='function')throw new Error('measurement_retention_atomic_batch_unavailable');
   await db.batch(statements);
  }
  // Reachability is transactional with immutable owner insertion. Indexed
  // references therefore permit collection without scanning every JSON manifest.
  const cycles=(await db.prepare(`SELECT cycle_id FROM decision_cycle_evidence c WHERE evaluated_at<?
   AND NOT EXISTS(SELECT 1 FROM signal_decision_evidence d WHERE d.cycle_id=c.cycle_id) ORDER BY evaluated_at LIMIT ?`).bind(cutoff,limit).all()).results||[];
  if(cycles.length)await db.prepare(`DELETE FROM decision_cycle_evidence WHERE cycle_id IN (${cycles.map(()=>'?').join(',')})`).bind(...cycles.map(x=>x.cycle_id)).run();
  const blocks=(await db.prepare(`SELECT block_id FROM market_evidence_blocks b WHERE recorded_at<?
   AND NOT EXISTS(SELECT 1 FROM market_evidence_references r WHERE r.block_id=b.block_id) ORDER BY recorded_at LIMIT ?`).bind(cutoff,limit).all()).results||[];
  if(blocks.length)await db.prepare(`DELETE FROM market_evidence_blocks WHERE block_id IN (${blocks.map(()=>'?').join(',')})`).bind(...blocks.map(x=>x.block_id)).run();
  return {ok:true,candidateAttemptsRemoved:attempts.length,cyclesRemoved:cycles.length,blocksRemoved:blocks.length,
   officialRetention:'UNTIL_EXPLICIT_COHORT_REVIEW; NO_AUTOMATIC_OFFICIAL_DELETION'};
 }catch(error){return {ok:false,error:String(error?.message||'measurement_retention_failed')};}
}
