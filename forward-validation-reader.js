// Fixed SELECT catalog only. No imports from writer/collector/schema initializer.
import {parseEvidence,FROZEN_BASELINE,MEASUREMENT_SCHEMA_VERSION,CAPTURE_VERSION} from './signal-evidence.js';
import {decodeStoredEvidence,decodeStoredOutcome,decodeProcessingState,restoreCensus,hydrateFinalOutcome,restoreSharedDecision,reproducibility,CENSUS_WORDS,restoreFinalResult} from './evidence-codec.js';
import {validateOutcomeDependencies} from './outcome-dependencies.js';
import {reduceOutcome,OUTCOME_REDUCER_VERSION} from './forward-validation.js';
// Evidence of successful Official persistence remains visible even when the legacy
// performance write failed independently. This union is SELECT-only, no repair.
const OFFICIAL_POPULATION=`(SELECT signal_id,source,created_at,timeframe,direction,entry,tp1,tp2,sl,status,closed_at,tp1_at,sl_at,NULL AS evidence_recorded_at FROM production_signals
 UNION ALL SELECT d.official_signal_id,'production',
 COALESCE(json_extract(d.payload_json,'$.createdAt'),json_extract(d.payload_json,'$[5]')),d.timeframe,CASE json_extract(d.payload_json,'$[2]') WHEN 1 THEN 'buy' WHEN 2 THEN 'sell' ELSE json_extract(d.payload_json,'$.direction') END,
 COALESCE(l.entry,json_extract(d.payload_json,'$.engine.levels.entry')),COALESCE(l.tp1,json_extract(d.payload_json,'$.engine.levels.tp1')),
 COALESCE(l.tp2,json_extract(d.payload_json,'$.engine.levels.tp2')),COALESCE(l.sl,json_extract(d.payload_json,'$.engine.levels.sl')),
 NULL,NULL,NULL,NULL,d.recorded_at FROM signal_decision_evidence d LEFT JOIN measurement_official_levels l ON l.official_signal_id=d.official_signal_id
 WHERE d.kind='OFFICIAL' AND NOT EXISTS(SELECT 1 FROM production_signals p WHERE p.signal_id=d.official_signal_id))`;
const SQL=Object.freeze({
 schema:"SELECT name,sql FROM sqlite_schema WHERE type IN ('table','view') AND name IN ('measurement_cohorts','decision_cycle_evidence','signal_decision_evidence','signal_outcome_evidence','signal_measurement_state','market_evidence_blocks','market_evidence_references','measurement_schema_meta','measurement_rich_evidence','measurement_definitions','measurement_final_results','measurement_official_pins','measurement_official_levels','measurement_outcome_records','measurement_subjects','measurement_market_links','measurement_outcome_dependencies')",
 versions:'SELECT version AS schema_version FROM measurement_schema_meta WHERE singleton=1',
 columns:`SELECT c.cohort_id,c.schema_version,c.persisted_at,m.block_id,m.payload_json,y.cycle_id,y.block_ids_json,d.evaluation_id,d.official_signal_id,d.kind,d.evaluated_at,o.subject_id,o.event_id,o.available_at,s.subject_id,s.updated_at,s.payload_json,s.evaluation_id,s.kind,s.next_observe_at,s.observation_end_at,d.persisted_at,o.persisted_at,r.owner_type,r.owner_id,r.block_id,h.evidence_id,h.owner_type,h.owner_id,h.retain_until,z.definition_id,j.subject_id,j.retain_until,p.official_signal_id,p.block_id FROM measurement_cohorts c,market_evidence_blocks m,decision_cycle_evidence y,signal_decision_evidence d,signal_outcome_evidence o,signal_measurement_state s,market_evidence_references r,measurement_rich_evidence h,measurement_definitions z,measurement_final_results j,measurement_official_pins p WHERE 0`,
 watermark:'SELECT MAX(available_at) AS at FROM signal_outcome_evidence WHERE available_at<=?',
 official:`SELECT p.*,d.payload_json AS entry_json,d.cohort_id,d.evaluation_id,d.candidate_key,d.cycle_id,d.kind,d.evaluated_at,COALESCE(s.payload_json,f.payload_json) AS state_json,COALESCE(s.updated_at,f.recorded_at) AS state_at,d.recorded_at AS entry_recorded_at,d.persisted_at AS entry_persisted_at
 FROM ${OFFICIAL_POPULATION} p LEFT JOIN signal_decision_evidence d ON d.official_signal_id=p.signal_id AND d.kind='OFFICIAL' AND d.recorded_at<=?2
 LEFT JOIN signal_measurement_state s ON s.subject_id=p.signal_id LEFT JOIN measurement_final_results f ON f.subject_id=p.signal_id
 WHERE p.source='production' AND p.created_at>=?1 AND p.created_at<=?2 AND (?3 IS NULL OR d.cohort_id=?4)
 AND (p.evidence_recorded_at IS NULL OR p.evidence_recorded_at<=?2)
 AND (?5 IS NULL OR p.created_at<?6 OR (p.created_at=?7 AND p.signal_id<?8))
 ORDER BY p.created_at DESC,p.signal_id DESC LIMIT ?9`,
 detail:`SELECT p.*,d.payload_json AS entry_json,d.cohort_id,d.evaluation_id,d.candidate_key,d.cycle_id,d.kind,d.evaluated_at,COALESCE(s.payload_json,f.payload_json) AS state_json,COALESCE(s.updated_at,f.recorded_at) AS state_at,d.recorded_at AS entry_recorded_at,d.persisted_at AS entry_persisted_at
 FROM ${OFFICIAL_POPULATION} p LEFT JOIN signal_decision_evidence d ON d.official_signal_id=p.signal_id AND d.kind='OFFICIAL' AND d.recorded_at<=?3
 LEFT JOIN signal_measurement_state s ON s.subject_id=p.signal_id LEFT JOIN measurement_final_results f ON f.subject_id=p.signal_id
 WHERE p.source='production' AND p.signal_id=?1 AND p.created_at>=?2 AND p.created_at<=?3 AND (p.evidence_recorded_at IS NULL OR p.evidence_recorded_at<=?3) AND (?4 IS NULL OR d.cohort_id=?4)`,
 candidates:`SELECT d.*,f.payload_json AS result_json FROM signal_decision_evidence d LEFT JOIN measurement_final_results f ON f.subject_id=d.evaluation_id
 WHERE kind='CANDIDATE' AND evaluated_at>=?1 AND evaluated_at<=?2 AND d.recorded_at<=?2 AND (?3 IS NULL OR cohort_id=?4)
 AND (?5 IS NULL OR evaluated_at<?6 OR (evaluated_at=?7 AND evaluation_id<?8))
 ORDER BY evaluated_at DESC,evaluation_id DESC LIMIT ?9`,
 events:`SELECT * FROM signal_outcome_evidence WHERE subject_id=?1 AND available_at<=?2 ORDER BY available_at,event_id LIMIT 129`,
 population:`SELECT * FROM (WITH raw AS (
 SELECT p.*,d.payload_json AS entry_json,d.evaluation_id,
 CASE WHEN s.updated_at<=?1 THEN s.payload_json WHEN f.recorded_at<=?1 THEN f.payload_json ELSE
 (SELECT e.payload_json FROM signal_outcome_evidence e WHERE e.subject_id=p.signal_id AND e.event_type='COVERAGE_CHECKPOINT' AND e.available_at<=?1 ORDER BY e.available_at DESC,e.event_id DESC LIMIT 1) END AS projection
 FROM ${OFFICIAL_POPULATION} p LEFT JOIN signal_decision_evidence d ON d.official_signal_id=p.signal_id AND d.kind='OFFICIAL' AND d.recorded_at<=?1
 LEFT JOIN signal_measurement_state s ON s.subject_id=p.signal_id LEFT JOIN measurement_final_results f ON f.subject_id=p.signal_id
 WHERE p.source='production' AND p.created_at>=?2 AND p.created_at<=?1 AND (p.evidence_recorded_at IS NULL OR p.evidence_recorded_at<=?1) AND (?3 IS NULL OR d.cohort_id=?3)
 ), checked AS (
 SELECT raw.*,
 EXISTS(SELECT 1 FROM signal_outcome_evidence e WHERE e.subject_id=raw.signal_id AND e.available_at<=?1 AND e.event_type='CORRECTION' AND e.available_at>=COALESCE(json_extract(projection,'$.availableAt'),0)) AS correction_pending,
 EXISTS(SELECT 1 FROM signal_outcome_evidence e WHERE e.subject_id=raw.signal_id AND e.available_at<=?1 AND e.event_type IN ('TP1','TP2','SL') AND e.available_at>COALESCE(json_extract(projection,'$.availableAt'),0)) AS witness_pending,
 EXISTS(SELECT 1 FROM signal_outcome_evidence e WHERE e.subject_id=raw.signal_id AND e.available_at<=?1 AND e.event_type IN ('TP1','TP2','SL') AND NOT EXISTS(
  SELECT 1 FROM measurement_outcome_dependencies x JOIN signal_outcome_evidence c ON c.rowid=abs(x.target_ref)
  WHERE abs(x.owner_ref)=e.rowid AND (x.relations&1)!=0 AND c.subject_id=e.subject_id AND c.event_type='COVERAGE_CHECKPOINT' AND c.available_at<=e.available_at)) AS dependency_invalid
 FROM raw
 ), classified AS (
 SELECT signal_id,created_at,status,closed_at,projection,timeframe,correction_pending,witness_pending,dependency_invalid,
 CASE WHEN (correction_pending OR witness_pending OR dependency_invalid) AND json_extract(projection,'$.outcome.directionalOutcome') IN ('SUCCESS','FAILURE') THEN 'INSUFFICIENT_DATA' ELSE COALESCE(json_extract(projection,'$.outcome.directionalOutcome'),CASE WHEN status IN ('active','tp1') OR closed_at>?1 THEN 'UNRESOLVED' ELSE 'INSUFFICIENT_DATA' END) END AS directional,
 CASE WHEN correction_pending OR witness_pending OR dependency_invalid THEN 'UNRESOLVED' ELSE COALESCE(json_extract(projection,'$.outcome.extendedOutcome'),'UNRESOLVED') END AS extended,
 CASE WHEN closed_at>?1 THEN 'ACTIVE' WHEN status='tp2' THEN 'TP2' WHEN status IN ('sl','stopped') THEN 'SL' WHEN status='expired' THEN 'EXPIRED' WHEN status='closed_other' THEN 'CLOSED_OTHER' ELSE 'ACTIVE' END AS terminal,
 CASE WHEN evaluation_id IS NULL THEN 1 ELSE 0 END AS missing_entry,
 CASE WHEN COALESCE(json_extract(entry_json,'$.engine.scoring.score'),json_extract(entry_json,'$[1][3]')) IS NULL THEN 'UNKNOWN' WHEN COALESCE(json_extract(entry_json,'$.engine.scoring.score'),json_extract(entry_json,'$[1][3]'))<8 THEN 'UNDER_8' WHEN COALESCE(json_extract(entry_json,'$.engine.scoring.score'),json_extract(entry_json,'$[1][3]'))<10 THEN '8_TO_UNDER_10' ELSE '10_PLUS' END AS score_band,
 CASE WHEN COALESCE(json_extract(entry_json,'$.engine.finalConfidence'),json_extract(entry_json,'$[1][6]')) IS NULL THEN 'UNKNOWN' WHEN COALESCE(json_extract(entry_json,'$.engine.finalConfidence'),json_extract(entry_json,'$[1][6]'))<70 THEN 'UNDER_70' WHEN COALESCE(json_extract(entry_json,'$.engine.finalConfidence'),json_extract(entry_json,'$[1][6]'))<80 THEN '70_TO_UNDER_80' ELSE '80_PLUS' END AS confidence_band,
 CASE WHEN COALESCE(json_extract(entry_json,'$.engine.scoring.confirm'),json_extract(entry_json,'$[1][4]')) IS NULL THEN 'UNKNOWN' WHEN COALESCE(json_extract(entry_json,'$.engine.scoring.oppositions'),json_extract(entry_json,'$[1][5]'))>0 THEN 'OPPOSITION_PRESENT' WHEN COALESCE(json_extract(entry_json,'$.engine.scoring.confirm'),json_extract(entry_json,'$[1][4]'))>0 THEN 'SUPPORT_WITHOUT_OPPOSITION' ELSE 'NO_DIRECTIONAL_SUPPORT' END AS engine_mtf,
 COALESCE(COALESCE(json_extract(entry_json,'$.broadMtf.summary.level'),CASE json_extract(entry_json,'$[9]') WHEN 1 THEN 'buy' WHEN 2 THEN 'sell' WHEN 3 THEN 'none' WHEN 4 THEN 'OFFICIAL_PERSISTED' WHEN 5 THEN 'ENGINE_REJECTED' WHEN 6 THEN 'SKIPPED' WHEN 7 THEN 'OFFICIAL_PERSISTENCE_FAILED' WHEN 8 THEN 'CONFIRMATION_PERSISTED' WHEN 9 THEN 'accepted' WHEN 10 THEN 'blocked_opposite' WHEN 11 THEN 'confirmation' WHEN 12 THEN 'trend-up' WHEN 13 THEN 'trend-down' WHEN 14 THEN 'range' WHEN 15 THEN 'NOT_OBSERVED' WHEN 16 THEN 'NOT_EVALUATED' WHEN 17 THEN 'SUCCEEDED' WHEN 18 THEN 'FAILED' ELSE CASE WHEN json_type(entry_json,'$[9]')='text' THEN json_extract(entry_json,'$[9]') ELSE NULL END END),'UNKNOWN') AS broad_mtf,
 COALESCE(COALESCE(json_extract(entry_json,'$.engine.indicators.marketProfile.state'),CASE json_extract(entry_json,'$[8]') WHEN 1 THEN 'buy' WHEN 2 THEN 'sell' WHEN 3 THEN 'none' WHEN 4 THEN 'OFFICIAL_PERSISTED' WHEN 5 THEN 'ENGINE_REJECTED' WHEN 6 THEN 'SKIPPED' WHEN 7 THEN 'OFFICIAL_PERSISTENCE_FAILED' WHEN 8 THEN 'CONFIRMATION_PERSISTED' WHEN 9 THEN 'accepted' WHEN 10 THEN 'blocked_opposite' WHEN 11 THEN 'confirmation' WHEN 12 THEN 'trend-up' WHEN 13 THEN 'trend-down' WHEN 14 THEN 'range' WHEN 15 THEN 'NOT_OBSERVED' WHEN 16 THEN 'NOT_EVALUATED' WHEN 17 THEN 'SUCCEEDED' WHEN 18 THEN 'FAILED' ELSE CASE WHEN json_type(entry_json,'$[8]')='text' THEN json_extract(entry_json,'$[8]') ELSE NULL END END),'UNKNOWN') AS regime,
 CASE WHEN entry_json IS NULL THEN 'UNKNOWN' WHEN CAST(strftime('%H',created_at/1000,'unixepoch') AS INTEGER)<7 THEN 'UTC_00_07' WHEN CAST(strftime('%H',created_at/1000,'unixepoch') AS INTEGER)<12 THEN 'UTC_07_12' WHEN CAST(strftime('%H',created_at/1000,'unixepoch') AS INTEGER)<17 THEN 'UTC_12_17' ELSE 'UTC_17_24' END AS session,
 CASE WHEN entry_json IS NULL THEN 'UNKNOWN' ELSE COALESCE(COALESCE(json_extract(entry_json,'$.admissionContext.effectiveAdmissionDecision.reason'),CASE json_extract(entry_json,'$[10]') WHEN 1 THEN 'buy' WHEN 2 THEN 'sell' WHEN 3 THEN 'none' WHEN 4 THEN 'OFFICIAL_PERSISTED' WHEN 5 THEN 'ENGINE_REJECTED' WHEN 6 THEN 'SKIPPED' WHEN 7 THEN 'OFFICIAL_PERSISTENCE_FAILED' WHEN 8 THEN 'CONFIRMATION_PERSISTED' WHEN 9 THEN 'accepted' WHEN 10 THEN 'blocked_opposite' WHEN 11 THEN 'confirmation' WHEN 12 THEN 'trend-up' WHEN 13 THEN 'trend-down' WHEN 14 THEN 'range' WHEN 15 THEN 'NOT_OBSERVED' WHEN 16 THEN 'NOT_EVALUATED' WHEN 17 THEN 'SUCCEEDED' WHEN 18 THEN 'FAILED' ELSE CASE WHEN json_type(entry_json,'$[10]')='text' THEN json_extract(entry_json,'$[10]') ELSE NULL END END),'UNKNOWN') END AS news_calendar,
 json_extract(projection,'$.global.mfeR') AS mfe_r,json_extract(projection,'$.global.maeR') AS mae_r,
 CASE WHEN NOT (correction_pending OR witness_pending OR dependency_invalid) THEN json_extract(projection,'$.outcome.timeToTp1.minMs') END AS tp1_min_ms,CASE WHEN NOT (correction_pending OR witness_pending OR dependency_invalid) THEN json_extract(projection,'$.outcome.timeToTp1.maxMs') END AS tp1_max_ms,
 CASE WHEN NOT (correction_pending OR witness_pending OR dependency_invalid) THEN json_extract(projection,'$.outcome.timeToSl.minMs') END AS sl_min_ms,CASE WHEN NOT (correction_pending OR witness_pending OR dependency_invalid) THEN json_extract(projection,'$.outcome.timeToSl.maxMs') END AS sl_max_ms
 FROM checked)
 SELECT correction_pending,witness_pending,dependency_invalid,timeframe,directional,extended,terminal,missing_entry,score_band,confidence_band,engine_mtf,broad_mtf,regime,session,news_calendar,COUNT(*) AS n,
 COUNT(mfe_r) AS mfe_observed,AVG(mfe_r) AS mean_observed_mfe_r,COUNT(mae_r) AS mae_observed,AVG(mae_r) AS mean_observed_mae_r,
 COUNT(tp1_min_ms) AS tp1_observed,AVG(tp1_min_ms) AS mean_tp1_min_ms,AVG(tp1_max_ms) AS mean_tp1_max_ms,COUNT(sl_min_ms) AS sl_observed,AVG(sl_min_ms) AS mean_sl_min_ms,AVG(sl_max_ms) AS mean_sl_max_ms
 FROM classified GROUP BY correction_pending,witness_pending,dependency_invalid,timeframe,directional,extended,terminal,missing_entry,score_band,confidence_band,engine_mtf,broad_mtf,regime,session,news_calendar)`,
 rich:'SELECT * FROM measurement_rich_evidence WHERE evidence_id=? AND recorded_at<=?',
 richMeta:'SELECT evidence_id,owner_type,owner_id,recorded_at,retain_until FROM measurement_rich_evidence WHERE evidence_id=? AND recorded_at<=?',
 final:'SELECT * FROM measurement_final_results WHERE subject_id=? AND recorded_at<=?',
 definition:'SELECT payload_json FROM measurement_definitions WHERE definition_id=?',
 cycle:'SELECT r.*,c.payload_json AS cohort_json FROM decision_cycle_evidence y JOIN measurement_cohorts c ON c.cohort_id=y.cohort_id LEFT JOIN measurement_rich_evidence r ON r.evidence_id=? WHERE y.cycle_id=? AND y.recorded_at<=? AND (r.recorded_at IS NULL OR r.recorded_at<=?)',
 rawCount:'SELECT COUNT(*) AS n FROM market_evidence_blocks WHERE block_id IN (SELECT value FROM json_each(?))',
 state:'SELECT * FROM signal_measurement_state WHERE subject_id=? AND updated_at<=?',
 collector:'SELECT payload_json,updated_at FROM signal_measurement_state WHERE subject_id=\'b1:collector\'',
 cohorts:'SELECT payload_json FROM measurement_cohorts ORDER BY effective_at,cohort_id'
});

// The closure exposes no SQL execution method, bindings, or mutating capability.
export function createForwardReadCapability(db){
 if(!db?.prepare)throw new Error('measurement_database_unavailable');
 const proofSql=SQL.population.slice(0,SQL.population.indexOf('\n SELECT correction_pending,'))+"\n SELECT * FROM classified WHERE directional IN ('SUCCESS','FAILURE') OR extended='TP2_REACHED' LIMIT 101)";
 return Object.freeze(Object.fromEntries(Object.entries({...SQL,proofs:proofSql}).map(([key,sql])=>[key,async (...values)=>{
  const statement=db.prepare(sql);const bound=values.length?statement.bind(...values):statement;
  return (await bound.all()).results||[];
 }])));
}

function encodeCursor(value){return btoa(JSON.stringify(value)).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');}
function parseCursor(value){
 try{
  if(value.length>512||!/^[A-Za-z0-9_-]+$/.test(value))throw 0;
  const v=JSON.parse(atob(value.replaceAll('-','+').replaceAll('_','/')));
  if(!Number.isSafeInteger(v.at)||typeof v.id!=='string'||v.id.length>180||!Number.isSafeInteger(v.asOf)||typeof v.filter!=='string')throw 0;
  return v;
 }catch{throw new Error('measurement_cursor_invalid');}
}

export function parseForwardQuery(url,now){
 const allowed=['from','asOf','limit','cursor','details','cohort'];
 if(url.searchParams.has('details')&&!['0','1'].includes(url.searchParams.get('details')))throw new Error('measurement_query_invalid');
 for(const key of url.searchParams.keys())if(!allowed.includes(key)||url.searchParams.getAll(key).length!==1)throw new Error('measurement_query_invalid');
 const integer=(name,fallback)=>{if(!url.searchParams.has(name))return fallback;const s=url.searchParams.get(name);if(!/^\d{1,16}$/.test(s))throw new Error('measurement_query_invalid');const n=Number(s);if(!Number.isSafeInteger(n))throw new Error('measurement_query_invalid');return n;};
 const from=integer('from',FROZEN_BASELINE.effectiveAt),limit=integer('limit',50),cohort=url.searchParams.get('cohort');
 const cursor=url.searchParams.has('cursor')?parseCursor(url.searchParams.get('cursor')):null;
 const asOf=integer('asOf',cursor?.asOf??now);
 if(from<FROZEN_BASELINE.effectiveAt||from>asOf||asOf>now||limit<1||limit>100||(cohort!==null&&!/^[A-Za-z0-9:_-]{1,180}$/.test(cohort)))throw new Error('measurement_query_invalid');
 const filter=JSON.stringify({from,cohort});if(cursor&&(cursor.asOf!==asOf||cursor.filter!==filter))throw new Error('measurement_cursor_invalid');
 return {from,asOf,limit,cohort,cursor,filter,details:url.searchParams.get('details')==='1'};
}

async function ready(read){
 const rows=await read.schema();if(rows.length!==17)throw new Error('measurement_schema_not_ready');
 const versions=await read.versions();if(versions.length!==1||versions.some(x=>x.schema_version!==MEASUREMENT_SCHEMA_VERSION))throw new Error('measurement_schema_not_ready');
 // Verify required fields through SELECT compilation, even for an empty schema.
 if(rows.some(x=>!x.sql))throw new Error('measurement_schema_not_ready');
 try{await read.columns();}catch(error){if(/no such (column|table)/i.test(String(error?.message)))throw new Error('measurement_schema_not_ready');throw error;}
}

function populationSummary(rows){
 const directional=Object.fromEntries(['SUCCESS','FAILURE','NO_DECISION','INSUFFICIENT_DATA','UNRESOLVED'].map(x=>[x,0]));
 const extended=Object.fromEntries(['TP2_REACHED','NOT_REACHED','UNRESOLVED'].map(x=>[x,0]));
 const terminal={TP2:0,SL:0,EXPIRED:0,CLOSED_OTHER:0,ACTIVE:0};const byTimeframe={};const dimensions=['score_band','confidence_band','engine_mtf','broad_mtf','regime','session','news_calendar'];const breakdowns=Object.fromEntries(dimensions.map(key=>[key,{}]));const observedMetrics={mfeCount:0,mfeWeightedSum:0,maeCount:0,maeWeightedSum:0,tp1Count:0,tp1MinWeightedSum:0,tp1MaxWeightedSum:0,slCount:0,slMinWeightedSum:0,slMaxWeightedSum:0};let total=0,missingEntry=0,correctionPending=0,pendingEvidence=0,invalidDependencies=0;
 for(const row of rows){if(!(row.directional in directional)||!(row.extended in extended)||!(row.terminal in terminal))throw new Error('measurement_projection_invalid');
  const count=Number(row.n);total+=count;correctionPending+=row.correction_pending?count:0;pendingEvidence+=row.witness_pending?count:0;invalidDependencies+=row.dependency_invalid?count:0;directional[row.directional]+=count;extended[row.extended]+=count;terminal[row.terminal]+=count;missingEntry+=row.missing_entry?count:0;
  const tf=byTimeframe[row.timeframe]||(byTimeframe[row.timeframe]={total:0,...Object.fromEntries(Object.keys(directional).map(x=>[x,0]))});tf.total+=count;tf[row.directional]+=count;
  for(const dimension of dimensions){const key=row[dimension]??'UNKNOWN',bucket=breakdowns[dimension][key]||(breakdowns[dimension][key]={total:0,...Object.fromEntries(Object.keys(directional).map(x=>[x,0]))});bucket.total+=count;bucket[row.directional]+=count;}
  observedMetrics.mfeCount+=Number(row.mfe_observed);observedMetrics.mfeWeightedSum+=Number(row.mean_observed_mfe_r||0)*Number(row.mfe_observed);observedMetrics.maeCount+=Number(row.mae_observed);observedMetrics.maeWeightedSum+=Number(row.mean_observed_mae_r||0)*Number(row.mae_observed);observedMetrics.tp1Count+=Number(row.tp1_observed);observedMetrics.tp1MinWeightedSum+=Number(row.mean_tp1_min_ms||0)*Number(row.tp1_observed);observedMetrics.tp1MaxWeightedSum+=Number(row.mean_tp1_max_ms||0)*Number(row.tp1_observed);
  observedMetrics.slCount+=Number(row.sl_observed);observedMetrics.slMinWeightedSum+=Number(row.mean_sl_min_ms||0)*Number(row.sl_observed);observedMetrics.slMaxWeightedSum+=Number(row.mean_sl_max_ms||0)*Number(row.sl_observed);
 }
 const denominator=directional.SUCCESS+directional.FAILURE;
 observedMetrics.meanObservedMfeR=observedMetrics.mfeCount?observedMetrics.mfeWeightedSum/observedMetrics.mfeCount:null;observedMetrics.meanObservedMaeR=observedMetrics.maeCount?observedMetrics.maeWeightedSum/observedMetrics.maeCount:null;observedMetrics.meanTimeToTp1Ms=observedMetrics.tp1Count?{min:observedMetrics.tp1MinWeightedSum/observedMetrics.tp1Count,max:observedMetrics.tp1MaxWeightedSum/observedMetrics.tp1Count}:null;observedMetrics.meanTimeToSlBeforeTp1Ms=observedMetrics.slCount?{min:observedMetrics.slMinWeightedSum/observedMetrics.slCount,max:observedMetrics.slMaxWeightedSum/observedMetrics.slCount}:null;observedMetrics.interpretation='OBSERVED_EXTREMA; LOWER_BOUNDS_MAY_BE_INCLUDED; NOT_COMPLETE_MARKET_EXCURSIONS';
 return {totalOfficial:total,correctionPending,pendingEvidence,invalidDependencies,directional,extended,terminal,provenDirectional:denominator,directionalAccuracy:denominator?directional.SUCCESS/denominator:null,byTimeframe,missingEntryEvidence:missingEntry,breakdowns,observedMetrics,
  interpretation:'ANALYTICAL_EVIDENCE_PROJECTION; LEGACY_RECORDED_BASELINE_REMAINS_SEPARATE'};
}

export async function readForwardValidation(read,url,path,now){
 const query=parseForwardQuery(url,now);await ready(read);
 const metadata={asOf:query.asOf,evidenceWatermark:(await read.watermark(query.asOf))[0]?.at??null,
  measurementSchemaVersion:MEASUREMENT_SCHEMA_VERSION,captureVersion:CAPTURE_VERSION,outcomeReducerVersion:OUTCOME_REDUCER_VERSION,
  frozenRecordedBaseline:FROZEN_BASELINE,cohorts:(await read.cohorts()).map(x=>parseEvidence(x.payload_json)),collectorStatus:(await read.collector())[0]??null};
 const params=[query.from,query.asOf,query.cohort,query.cohort,query.cursor?.at??null,query.cursor?.at??null,query.cursor?.at??null,query.cursor?.id??null,query.limit+1];
 const candidates=path==='/forward-validation/candidates';
 let rows;
 if(path==='/forward-validation'||candidates)rows=await read[candidates?'candidates':'official'](...params);
 else if(path.startsWith('/forward-validation/signals/')){
  const id=decodeURIComponent(path.slice('/forward-validation/signals/'.length));
  if(!/^[A-Za-z0-9:_-]{1,100}$/.test(id))throw new Error('measurement_signal_invalid');
  rows=await read.detail(id,query.from,query.asOf,query.cohort);
 }else throw new Error('measurement_route_invalid');
 const hasMore=rows.length>query.limit;rows=rows.slice(0,query.limit);
 const records=[];
 for(const row of rows){
  if(candidates){const census=restoreCensus(parseEvidence(row.payload_json),row);const r=(await read[query.details?'rich':'richMeta'](`decision:${row.evaluation_id}`,query.asOf))[0];const detailed=query.details&&r?await hydrateDecision(read,r,query.asOf):null;const ids=decisionDependencies(detailed);const raw=ids.length>0&&(await read.rawCount(JSON.stringify(ids)))[0]?.n===ids.length;
   const level=reproducibility({kind:'CANDIDATE',richAvailable:!!r,rawAvailable:raw,skipped:census.outcome==='SKIPPED'});
   const final=(await read.final(row.evaluation_id,query.asOf))[0];
   records.push({...census,measurementResult:final?restoreFinalResult(final):null,measurementResultValidation:'RECORDED_PROJECTION; NOT_REVALIDATED_AFTER_RESEARCH_RETENTION',reproducibility:{...level,rawVerification:query.details?'EXACT_DEPENDENCIES_CHECKED':'NOT_CHECKED_IN_COMPACT_REPORT'},...(query.details&&r?{derivedEvidence:detailed}:{})});continue;}
  const census=row.entry_json?restoreCensus(parseEvidence(row.entry_json),{...row,timeframe:row.timeframe,official_signal_id:row.signal_id}):null;
  const wantsDetails=query.details||path.startsWith('/forward-validation/signals/');
  const rich=census?(await read[wantsDetails?'rich':'richMeta'](`decision:${census.evaluationId}`,query.asOf))[0]:null;
  const detailed=wantsDetails&&rich?await hydrateDecision(read,rich,query.asOf):null;const entryEvidence=(query.details||path.startsWith('/forward-validation/signals/'))?detailed||census:census;
  const ids=decisionDependencies(detailed);const raw=ids.length>0&&(await read.rawCount(JSON.stringify(ids)))[0]?.n===ids.length;
  const evidenceLevel=reproducibility({kind:'OFFICIAL',richAvailable:!!rich,rawAvailable:raw});
  const processing=(await read.state(row.signal_id,query.asOf))[0];let state=processing?await decodeProcessingState(processing):row.state_json&&row.state_at<=query.asOf?parseEvidence(row.state_json):null;const final=(await read.final(row.signal_id,query.asOf))[0];if(!processing&&final)state=restoreFinalResult(final);
  const eventRows=await read.events(row.signal_id,query.asOf);if(eventRows.length>128)throw new Error('measurement_evidence_review_required');
  let events=await Promise.all(eventRows.map(x=>decodeStoredOutcome(x)));events=events.map(e=>hydrateFinalOutcome(e,events));
  if(query.details||path.startsWith('/forward-validation/signals/'))for(let i=0;i<events.length;i++)if(events[i].evidenceRef){const r=(await read.rich(events[i].evidenceRef,query.asOf))[0];if(r)events[i]=await decodeStoredEvidence(r);}
  if(!state){const checkpoints=events.filter(e=>e.evidenceType==='FINAL_MEASUREMENT'||(e.evidenceType==='COVERAGE_CHECKPOINT'&&e.checkpointRole!=='BARRIER_COVERAGE')).sort((a,b)=>b.availableAt-a.availableAt);state=checkpoints[0]??null;}
  const packageEvidence=events.find(e=>e.evidenceType==='FINAL_MEASUREMENT');if(packageEvidence&&final&&!processing)state={...packageEvidence,...restoreFinalResult(final)};
  const projected=state?.outcome;
  const reduced=reduceOutcome({id:row.signal_id,createdAt:row.created_at,status:row.status,closedAt:row.closed_at},events,
   {asOf:query.asOf,coverageIntervals:state?.covered||[],coverageGaps:state?.gaps||[],requireCoverageReferences:true});
  const dependencyIssues=events.flatMap(e=>validateOutcomeDependencies(e,events));
  const projectionAt=state?.availableAt??state?.outcome?.evidenceAsOf??row.state_at??null;
  const corrections=events.filter(e=>e.eventType==='CORRECTION');
  const pendingCorrections=corrections.filter(e=>!Number.isSafeInteger(projectionAt)||e.availableAt>=projectionAt);
  const newerWitnesses=events.filter(e=>e.evidenceType==='BARRIER_OBSERVATION'&&(!Number.isSafeInteger(projectionAt)||e.availableAt>projectionAt));
  const stale=pendingCorrections.length>0||newerWitnesses.length>0;
  let outcome=projected?{...projected,terminalLifecycleOutcome:reduced.terminalLifecycleOutcome}:reduced;
  if((dependencyIssues.length||stale||(['SUCCESS','FAILURE'].includes(outcome.directionalOutcome)&&reduced.directionalOutcome!==outcome.directionalOutcome))&&['SUCCESS','FAILURE'].includes(outcome.directionalOutcome))outcome={...outcome,directionalOutcome:'INSUFFICIENT_DATA',extendedOutcome:'UNRESOLVED',timeToTp1:null,timeToSl:null,reason:pendingCorrections.length?'CORRECTION_REDUCTION_PENDING':dependencyIssues.length?'OUTCOME_DEPENDENCY_INVALID':'PROJECTION_STALE_OR_CHALLENGED'};
  if((stale||dependencyIssues.length||reduced.extendedOutcome!=='TP2_REACHED')&&outcome.extendedOutcome==='TP2_REACHED')outcome={...outcome,extendedOutcome:'UNRESOLVED'};
  const correctionStatus={corrections:corrections.map(e=>({eventId:e.eventId,supersedesEventId:e.supersedesEventId,reason:e.reason,availableAt:e.availableAt})),pendingCount:pendingCorrections.length,projectionAt,stale,reducerStatus:pendingCorrections.length?'CORRECTION_REDUCTION_PENDING':stale?'NEW_EVIDENCE_REDUCTION_PENDING':'AS_RECORDED',dependencyIssues};
  records.push({signalId:row.signal_id,createdAt:row.created_at,timeframe:row.timeframe,direction:row.direction,
   entry:row.entry,tp1:row.tp1,tp2:row.tp2,sl:row.sl,recordedLifecycle:{status:row.status,tp1At:row.tp1_at,slAt:row.sl_at,closedAt:row.closed_at},
   terminalLifecycleEvidence:row.status==null?'NOT_PROVEN':'RECORDED_LIFECYCLE_FACT',entryEvidence,entryPersistence:{recordedAt:row.entry_recorded_at??null,persistedAt:row.entry_persisted_at??null,timeBasis:'APPLICATION_CAPTURE_AND_DATABASE_CLOCK'},evidenceState:entryEvidence?'CAPTURED':'ENTRY_EVIDENCE_NOT_CAPTURED',reproducibility:evidenceLevel,correctionStatus,outcome,quality:state?.quality??null,excursions:state?.global??null,evidenceAsOf:state?.availableAt??row.state_at??null});
 }
 const last=rows.at(-1),cursor=hasMore&&last?encodeCursor({at:candidates?last.evaluated_at:last.created_at,id:candidates?last.evaluation_id:last.signal_id,asOf:query.asOf,filter:query.filter}):null;
 let summary=null;
 if(!candidates){const groups=await read.population(query.asOf,query.from,query.cohort),proofs=await read.proofs(query.asOf,query.from,query.cohort);
  const bounded=proofs.length<=100;
  const dimensions=['timeframe','directional','extended','terminal','missing_entry','score_band','confidence_band','engine_mtf','broad_mtf','regime','session','news_calendar','correction_pending','witness_pending','dependency_invalid'];
  if(!bounded){for(const g of groups)if(['SUCCESS','FAILURE'].includes(g.directional)||g.extended==='TP2_REACHED'){g.directional='INSUFFICIENT_DATA';g.extended='UNRESOLVED';g.tp1_observed=0;g.sl_observed=0;}}
  else for(const row of proofs){
   const eventRows=await read.events(row.signal_id,query.asOf);
   const events=await Promise.all(eventRows.map(e=>decodeStoredOutcome(e)));
   const p=parseEvidence(row.projection),reduced=reduceOutcome({id:row.signal_id,createdAt:row.created_at,status:row.status,closedAt:row.closed_at},events,{asOf:query.asOf,coverageIntervals:p?.covered||[],coverageGaps:p?.gaps||[],requireCoverageReferences:true});
   const invalid=events.length>128||events.some(e=>validateOutcomeDependencies(e,events).length)||!events.some(e=>e.evidenceType==='BARRIER_OBSERVATION')||reduced.directionalOutcome!==row.directional||(row.extended==='TP2_REACHED'&&reduced.extendedOutcome!=='TP2_REACHED');
   if(invalid){const group=groups.find(g=>dimensions.every(k=>g[k]===row[k]));if(!group)throw new Error('measurement_projection_invalid');group.n--;
    for(const [prefix,key]of [['tp1','timeToTp1'],['sl','timeToSl']])if(p?.outcome?.[key]?.minMs!=null&&group[prefix+'_observed']>0){
     const count=Number(group[prefix+'_observed']);group[prefix+'_observed']=count-1;
     for(const edge of ['min','max']){const mean='mean_'+prefix+'_'+edge+'_ms';group[mean]=count>1?(Number(group[mean])*count-p.outcome[key][edge+'Ms'])/(count-1):null;}
    }
    groups.push({...group,n:1,directional:'INSUFFICIENT_DATA',extended:'UNRESOLVED',dependency_invalid:1,mfe_observed:0,mae_observed:0,tp1_observed:0,sl_observed:0});}
  }
  summary=populationSummary(groups);summary.validation={status:bounded?'AT_DISCOVERY_DEPENDENCIES_CHECKED':'SUMMARY_VALIDATION_BUDGET_EXHAUSTED',subjectLimit:100,eventLimit:128,provenOutcomesExcludedOnBudgetExhaustion:!bounded};
 }
 if(summary)summary.signalFrequency={officialPer24h:query.asOf>query.from?summary.totalOfficial*86400000/(query.asOf-query.from):null,from:query.from,to:query.asOf};
 return {ok:true,...metadata,records,summary,pagination:{limit:query.limit,hasMore,nextCursor:cursor},
  evidenceCompleteness:'PER_RECORD; ABSENCE_OF_CAPTURE_IS_NOT_ABSENCE_OF_SIGNAL_OR_CANDIDATE',
  legacyHandling:'ORIGINAL_FACTS_PRESERVED; NO_ENTRY_RECONSTRUCTION; V1_WINDOWS_NOT_NOMINAL_WINDOW_PROOF'};
}

async function hydrateDecision(read,row,asOf){
 const p=await decodeStoredEvidence(row);
 const shared=(await read.cycle(`cycle:${p.sharedContextRef}`,p.sharedContextRef,asOf,asOf))[0];
 const definitions=new Map();if(p.definitionRef){const d=(await read.definition(p.definitionRef))[0];if(d)definitions.set(p.definitionRef,parseEvidence(d.payload_json));}
 return restoreSharedDecision(p,shared?.payload_blob?await decodeStoredEvidence(shared):null,shared?parseEvidence(shared.cohort_json):null,definitions);
}

function decisionDependencies(p){return [...new Set([...(p?.inputManifest?.primary?.references||[]),...(p?.inputManifest?.engineMtf||[]).flatMap(x=>x.references||[]),...(p?.inputManifest?.research1m?.references||[])].map(x=>x.blockId))];}
