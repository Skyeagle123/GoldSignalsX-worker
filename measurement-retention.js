// Scheduled writer-side bounded maintenance; reporting NEVER imports this module.
const DAY=86400000;
export async function maintainMeasurementRetention(db,asOf,{candidateDays=90,limit=100}={}){
 if(candidateDays!==90||!Number.isInteger(limit)||limit<1||limit>100)throw new Error('measurement_retention_invalid');
 try{
  if((await db.prepare('SELECT version FROM measurement_schema_meta WHERE singleton=1').first())?.version!==2)throw new Error('measurement_schema_not_ready');
  if(typeof db.batch!=='function')throw new Error('measurement_retention_atomic_batch_unavailable');
  const rich=(await db.prepare(`SELECT evidence_id FROM measurement_rich_evidence e WHERE retain_until<=? AND NOT (owner_type='market' AND EXISTS(SELECT 1 FROM measurement_official_pins p WHERE p.block_id=e.owner_id)) ORDER BY retain_until,evidence_id LIMIT ?`).bind(asOf,limit).all()).results||[];
  if(rich.length)await db.prepare(`DELETE FROM measurement_rich_evidence WHERE evidence_id IN (${rich.map(()=>'?').join(',')})`).bind(...rich.map(r=>r.evidence_id)).run();
  // Rich candidate outcomes expire at 90d; immutable compact final results and
  // all-attempt census remain through 365d. No outcome participates in selection.
  const outcomes=(await db.prepare(`SELECT event_id FROM signal_outcome_evidence o WHERE recorded_at<? AND EXISTS(SELECT 1 FROM signal_decision_evidence d WHERE d.evaluation_id=o.subject_id AND d.kind='CANDIDATE') ORDER BY recorded_at,event_id LIMIT ?`).bind(asOf-90*DAY,limit).all()).results||[];
  if(outcomes.length)await db.prepare(`DELETE FROM signal_outcome_evidence WHERE event_id IN (${outcomes.map(()=>'?').join(',')})`).bind(...outcomes.map(r=>r.event_id)).run();
  const attempts=(await db.prepare("SELECT evaluation_id FROM signal_decision_evidence d WHERE kind='CANDIDATE' AND evaluated_at<? AND NOT EXISTS(SELECT 1 FROM signal_measurement_state s WHERE s.evaluation_id=d.evaluation_id) ORDER BY evaluated_at,evaluation_id LIMIT ?").bind(asOf-365*DAY,limit).all()).results||[];
  if(attempts.length){const ids=attempts.map(r=>r.evaluation_id),q=ids.map(()=>'?').join(',');await db.batch([
   db.prepare(`DELETE FROM signal_outcome_evidence WHERE subject_id IN (${q})`).bind(...ids),
   db.prepare(`DELETE FROM measurement_final_results WHERE subject_id IN (${q}) AND retain_until<=?`).bind(...ids,asOf),
   db.prepare(`DELETE FROM signal_decision_evidence WHERE kind='CANDIDATE' AND evaluation_id IN (${q})`).bind(...ids)]);}
  const cycles=(await db.prepare(`SELECT cycle_id FROM decision_cycle_evidence c WHERE evaluated_at<? AND NOT EXISTS(SELECT 1 FROM signal_decision_evidence d WHERE d.cycle_id=c.cycle_id) ORDER BY evaluated_at,cycle_id LIMIT ?`).bind(asOf-365*DAY,limit).all()).results||[];
  if(cycles.length)await db.prepare(`DELETE FROM decision_cycle_evidence WHERE cycle_id IN (${cycles.map(()=>'?').join(',')})`).bind(...cycles.map(r=>r.cycle_id)).run();
  // General references are explanatory, not pins. Explicit Official dependency
  // pins alone protect raw evidence beyond 30d. Delete references atomically.
  const blocks=(await db.prepare(`SELECT block_id FROM market_evidence_blocks b WHERE recorded_at<? AND NOT EXISTS(SELECT 1 FROM measurement_official_pins p WHERE p.block_id=b.block_id) ORDER BY recorded_at,block_id LIMIT ?`).bind(asOf-30*DAY,limit).all()).results||[];
  if(blocks.length){const ids=blocks.map(r=>r.block_id),q=ids.map(()=>'?').join(',');await db.batch([
   db.prepare(`DELETE FROM market_evidence_references WHERE block_id IN (${q})`).bind(...ids),
   db.prepare(`DELETE FROM measurement_rich_evidence WHERE owner_type='market' AND owner_id IN (${q})`).bind(...ids),
   db.prepare(`DELETE FROM market_evidence_blocks WHERE block_id IN (${q})`).bind(...ids)]);}
  return {ok:true,candidateAttemptsRemoved:attempts.length,richRemoved:rich.length,outcomesRemoved:outcomes.length,cyclesRemoved:cycles.length,blocksRemoved:blocks.length,officialRetention:'UNTIL_EXPLICIT_COHORT_REVIEW; NO_AUTOMATIC_OFFICIAL_DELETION'};
 }catch(error){return {ok:false,error:String(error?.message||'measurement_retention_failed')};}
}
