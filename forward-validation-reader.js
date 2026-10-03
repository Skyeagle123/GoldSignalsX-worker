// Fixed SELECT catalog only. No imports from writer/collector/schema initializer.
import {parseEvidence,FROZEN_BASELINE,MEASUREMENT_SCHEMA_VERSION,CAPTURE_VERSION} from './signal-evidence.js';
import {reduceOutcome,OUTCOME_REDUCER_VERSION} from './forward-validation.js';
// Evidence of successful Official persistence remains visible even when the legacy
// performance write failed independently. This union is SELECT-only, no repair.
const OFFICIAL_POPULATION=`(SELECT signal_id,source,created_at,timeframe,direction,entry,tp1,tp2,sl,status,closed_at,tp1_at,sl_at,NULL AS evidence_recorded_at FROM production_signals
 UNION ALL SELECT d.official_signal_id,'production',
 json_extract(d.payload_json,'$.createdAt'),d.timeframe,json_extract(d.payload_json,'$.direction'),
 json_extract(d.payload_json,'$.engine.levels.entry'),json_extract(d.payload_json,'$.engine.levels.tp1'),
 json_extract(d.payload_json,'$.engine.levels.tp2'),json_extract(d.payload_json,'$.engine.levels.sl'),
 NULL,NULL,NULL,NULL,d.recorded_at FROM signal_decision_evidence d
 WHERE d.kind='OFFICIAL' AND NOT EXISTS(SELECT 1 FROM production_signals p WHERE p.signal_id=d.official_signal_id))`;
const SQL=Object.freeze({
 schema:"SELECT name,sql FROM sqlite_schema WHERE type='table' AND name IN ('measurement_cohorts','decision_cycle_evidence','signal_decision_evidence','signal_outcome_evidence','signal_measurement_state','market_evidence_blocks','market_evidence_references','measurement_schema_meta')",
 versions:'SELECT version AS schema_version FROM measurement_schema_meta WHERE singleton=1',
 columns:`SELECT c.cohort_id,c.schema_version,c.persisted_at,m.block_id,m.payload_json,y.cycle_id,y.block_ids_json,d.evaluation_id,d.official_signal_id,d.kind,d.evaluated_at,o.subject_id,o.event_id,o.available_at,s.subject_id,s.updated_at,s.payload_json,s.evaluation_id,s.kind,s.next_observe_at,s.observation_end_at,d.persisted_at,o.persisted_at,r.owner_type,r.owner_id,r.block_id FROM measurement_cohorts c,market_evidence_blocks m,decision_cycle_evidence y,signal_decision_evidence d,signal_outcome_evidence o,signal_measurement_state s,market_evidence_references r WHERE 0`,
 watermark:'SELECT MAX(available_at) AS at FROM signal_outcome_evidence WHERE available_at<=?',
 official:`SELECT p.*,d.payload_json AS entry_json,d.cohort_id,s.payload_json AS state_json,s.updated_at AS state_at,d.recorded_at AS entry_recorded_at,d.persisted_at AS entry_persisted_at
 FROM ${OFFICIAL_POPULATION} p LEFT JOIN signal_decision_evidence d ON d.official_signal_id=p.signal_id AND d.kind='OFFICIAL' AND d.recorded_at<=?2
 LEFT JOIN signal_measurement_state s ON s.subject_id=p.signal_id
 WHERE p.source='production' AND p.created_at>=?1 AND p.created_at<=?2 AND (?3 IS NULL OR d.cohort_id=?4)
 AND (p.evidence_recorded_at IS NULL OR p.evidence_recorded_at<=?2)
 AND (?5 IS NULL OR p.created_at<?6 OR (p.created_at=?7 AND p.signal_id<?8))
 ORDER BY p.created_at DESC,p.signal_id DESC LIMIT ?9`,
 detail:`SELECT p.*,d.payload_json AS entry_json,d.cohort_id,s.payload_json AS state_json,s.updated_at AS state_at,d.recorded_at AS entry_recorded_at,d.persisted_at AS entry_persisted_at
 FROM ${OFFICIAL_POPULATION} p LEFT JOIN signal_decision_evidence d ON d.official_signal_id=p.signal_id AND d.kind='OFFICIAL' AND d.recorded_at<=?3
 LEFT JOIN signal_measurement_state s ON s.subject_id=p.signal_id
 WHERE p.source='production' AND p.signal_id=?1 AND p.created_at>=?2 AND p.created_at<=?3 AND (p.evidence_recorded_at IS NULL OR p.evidence_recorded_at<=?3)`,
 candidates:`SELECT evaluation_id,evaluated_at,timeframe,cohort_id,payload_json FROM signal_decision_evidence
 WHERE kind='CANDIDATE' AND evaluated_at>=?1 AND evaluated_at<=?2 AND recorded_at<=?2 AND (?3 IS NULL OR cohort_id=?4)
 AND (?5 IS NULL OR evaluated_at<?6 OR (evaluated_at=?7 AND evaluation_id<?8))
 ORDER BY evaluated_at DESC,evaluation_id DESC LIMIT ?9`,
 events:`SELECT payload_json FROM signal_outcome_evidence WHERE subject_id=?1 AND available_at<=?2
 AND (event_type!='COVERAGE_CHECKPOINT' OR event_id=(SELECT event_id FROM signal_outcome_evidence WHERE subject_id=?1 AND event_type='COVERAGE_CHECKPOINT' AND available_at<=?2 ORDER BY available_at DESC,event_id DESC LIMIT 1)) ORDER BY available_at,event_id`,
 population:`SELECT * FROM (WITH raw AS (
 SELECT p.*,d.payload_json AS entry_json,d.evaluation_id,
 CASE WHEN s.updated_at<=?1 THEN s.payload_json ELSE
 (SELECT e.payload_json FROM signal_outcome_evidence e WHERE e.subject_id=p.signal_id AND e.event_type='COVERAGE_CHECKPOINT' AND e.available_at<=?1 ORDER BY e.available_at DESC,e.event_id DESC LIMIT 1) END AS projection
 FROM ${OFFICIAL_POPULATION} p LEFT JOIN signal_decision_evidence d ON d.official_signal_id=p.signal_id AND d.kind='OFFICIAL' AND d.recorded_at<=?1
 LEFT JOIN signal_measurement_state s ON s.subject_id=p.signal_id
 WHERE p.source='production' AND p.created_at>=?2 AND p.created_at<=?1 AND (p.evidence_recorded_at IS NULL OR p.evidence_recorded_at<=?1) AND (?3 IS NULL OR d.cohort_id=?3)
 ), classified AS (
 SELECT timeframe,
 COALESCE(json_extract(projection,'$.outcome.directionalOutcome'),CASE WHEN status IN ('active','tp1') OR closed_at>?1 THEN 'UNRESOLVED' ELSE 'INSUFFICIENT_DATA' END) AS directional,
 COALESCE(json_extract(projection,'$.outcome.extendedOutcome'),'UNRESOLVED') AS extended,
 CASE WHEN closed_at>?1 THEN 'ACTIVE' WHEN status='tp2' THEN 'TP2' WHEN status IN ('sl','stopped') THEN 'SL' WHEN status='expired' THEN 'EXPIRED' WHEN status='closed_other' THEN 'CLOSED_OTHER' ELSE 'ACTIVE' END AS terminal,
 CASE WHEN evaluation_id IS NULL THEN 1 ELSE 0 END AS missing_entry,
 CASE WHEN json_extract(entry_json,'$.engine.scoring.score') IS NULL THEN 'UNKNOWN' WHEN json_extract(entry_json,'$.engine.scoring.score')<8 THEN 'UNDER_8' WHEN json_extract(entry_json,'$.engine.scoring.score')<10 THEN '8_TO_UNDER_10' ELSE '10_PLUS' END AS score_band,
 CASE WHEN json_extract(entry_json,'$.engine.finalConfidence') IS NULL THEN 'UNKNOWN' WHEN json_extract(entry_json,'$.engine.finalConfidence')<70 THEN 'UNDER_70' WHEN json_extract(entry_json,'$.engine.finalConfidence')<80 THEN '70_TO_UNDER_80' ELSE '80_PLUS' END AS confidence_band,
 CASE WHEN json_extract(entry_json,'$.engine.scoring.confirm') IS NULL THEN 'UNKNOWN' WHEN json_extract(entry_json,'$.engine.scoring.oppositions')>0 THEN 'OPPOSITION_PRESENT' WHEN json_extract(entry_json,'$.engine.scoring.confirm')>0 THEN 'SUPPORT_WITHOUT_OPPOSITION' ELSE 'NO_DIRECTIONAL_SUPPORT' END AS engine_mtf,
 COALESCE(json_extract(entry_json,'$.broadMtf.summary.level'),'UNKNOWN') AS broad_mtf,
 COALESCE(json_extract(entry_json,'$.engine.indicators.marketProfile.state'),'UNKNOWN') AS regime,
 CASE WHEN entry_json IS NULL THEN 'UNKNOWN' WHEN CAST(strftime('%H',created_at/1000,'unixepoch') AS INTEGER)<7 THEN 'UTC_00_07' WHEN CAST(strftime('%H',created_at/1000,'unixepoch') AS INTEGER)<12 THEN 'UTC_07_12' WHEN CAST(strftime('%H',created_at/1000,'unixepoch') AS INTEGER)<17 THEN 'UTC_12_17' ELSE 'UTC_17_24' END AS session,
 CASE WHEN entry_json IS NULL THEN 'UNKNOWN' ELSE COALESCE(json_extract(entry_json,'$.admissionContext.effectiveAdmissionDecision.reason'),'UNKNOWN') END AS news_calendar,
 json_extract(projection,'$.global.mfeR') AS mfe_r,json_extract(projection,'$.global.maeR') AS mae_r,
 json_extract(projection,'$.outcome.timeToTp1.minMs') AS tp1_min_ms,json_extract(projection,'$.outcome.timeToTp1.maxMs') AS tp1_max_ms,
 json_extract(projection,'$.outcome.timeToSl.minMs') AS sl_min_ms,json_extract(projection,'$.outcome.timeToSl.maxMs') AS sl_max_ms
 FROM raw)
 SELECT timeframe,directional,extended,terminal,missing_entry,score_band,confidence_band,engine_mtf,broad_mtf,regime,session,news_calendar,COUNT(*) AS n,
 COUNT(mfe_r) AS mfe_observed,AVG(mfe_r) AS mean_observed_mfe_r,COUNT(mae_r) AS mae_observed,AVG(mae_r) AS mean_observed_mae_r,
 COUNT(tp1_min_ms) AS tp1_observed,AVG(tp1_min_ms) AS mean_tp1_min_ms,AVG(tp1_max_ms) AS mean_tp1_max_ms,COUNT(sl_min_ms) AS sl_observed,AVG(sl_min_ms) AS mean_sl_min_ms,AVG(sl_max_ms) AS mean_sl_max_ms
 FROM classified GROUP BY timeframe,directional,extended,terminal,missing_entry,score_band,confidence_band,engine_mtf,broad_mtf,regime,session,news_calendar)`,
 collector:'SELECT payload_json,updated_at FROM signal_measurement_state WHERE subject_id=\'b1:collector\'',
 cohorts:'SELECT payload_json FROM measurement_cohorts ORDER BY effective_at,cohort_id'
});

// The closure exposes no SQL execution method, bindings, or mutating capability.
export function createForwardReadCapability(db){
 if(!db?.prepare)throw new Error('measurement_database_unavailable');
 return Object.freeze(Object.fromEntries(Object.entries(SQL).map(([key,sql])=>[key,async (...values)=>{
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
 const allowed=['from','asOf','limit','cursor','cohort'];
 for(const key of url.searchParams.keys())if(!allowed.includes(key)||url.searchParams.getAll(key).length!==1)throw new Error('measurement_query_invalid');
 const integer=(name,fallback)=>{if(!url.searchParams.has(name))return fallback;const s=url.searchParams.get(name);if(!/^\d{1,16}$/.test(s))throw new Error('measurement_query_invalid');const n=Number(s);if(!Number.isSafeInteger(n))throw new Error('measurement_query_invalid');return n;};
 const from=integer('from',FROZEN_BASELINE.effectiveAt),limit=integer('limit',50),cohort=url.searchParams.get('cohort');
 const cursor=url.searchParams.has('cursor')?parseCursor(url.searchParams.get('cursor')):null;
 const asOf=integer('asOf',cursor?.asOf??now);
 if(from<FROZEN_BASELINE.effectiveAt||from>asOf||asOf>now||limit<1||limit>100||(cohort&&!/^[A-Za-z0-9:_-]{1,180}$/.test(cohort)))throw new Error('measurement_query_invalid');
 const filter=JSON.stringify({from,cohort});if(cursor&&(cursor.asOf!==asOf||cursor.filter!==filter))throw new Error('measurement_cursor_invalid');
 return {from,asOf,limit,cohort,cursor,filter};
}

async function ready(read){
 const rows=await read.schema();if(rows.length!==8)throw new Error('measurement_schema_not_ready');
 const versions=await read.versions();if(versions.length!==1||versions.some(x=>x.schema_version!==MEASUREMENT_SCHEMA_VERSION))throw new Error('measurement_schema_not_ready');
 // Verify required fields through SELECT compilation, even for an empty schema.
 if(rows.some(x=>!x.sql||(!['market_evidence_references','measurement_schema_meta'].includes(x.name)&&!x.sql.includes('payload_json'))))throw new Error('measurement_schema_not_ready');
 try{await read.columns();}catch(error){if(/no such (column|table)/i.test(String(error?.message)))throw new Error('measurement_schema_not_ready');throw error;}
}

function populationSummary(rows){
 const directional=Object.fromEntries(['SUCCESS','FAILURE','NO_DECISION','INSUFFICIENT_DATA','UNRESOLVED'].map(x=>[x,0]));
 const extended=Object.fromEntries(['TP2_REACHED','NOT_REACHED','UNRESOLVED'].map(x=>[x,0]));
 const terminal={TP2:0,SL:0,EXPIRED:0,CLOSED_OTHER:0,ACTIVE:0};const byTimeframe={};const dimensions=['score_band','confidence_band','engine_mtf','broad_mtf','regime','session','news_calendar'];const breakdowns=Object.fromEntries(dimensions.map(key=>[key,{}]));const observedMetrics={mfeCount:0,mfeWeightedSum:0,maeCount:0,maeWeightedSum:0,tp1Count:0,tp1MinWeightedSum:0,tp1MaxWeightedSum:0,slCount:0,slMinWeightedSum:0,slMaxWeightedSum:0};let total=0,missingEntry=0;
 for(const row of rows){if(!(row.directional in directional)||!(row.extended in extended)||!(row.terminal in terminal))throw new Error('measurement_projection_invalid');
  const count=Number(row.n);total+=count;directional[row.directional]+=count;extended[row.extended]+=count;terminal[row.terminal]+=count;missingEntry+=row.missing_entry?count:0;
  const tf=byTimeframe[row.timeframe]||(byTimeframe[row.timeframe]={total:0,...Object.fromEntries(Object.keys(directional).map(x=>[x,0]))});tf.total+=count;tf[row.directional]+=count;
  for(const dimension of dimensions){const key=row[dimension]??'UNKNOWN',bucket=breakdowns[dimension][key]||(breakdowns[dimension][key]={total:0,...Object.fromEntries(Object.keys(directional).map(x=>[x,0]))});bucket.total+=count;bucket[row.directional]+=count;}
  observedMetrics.mfeCount+=Number(row.mfe_observed);observedMetrics.mfeWeightedSum+=Number(row.mean_observed_mfe_r||0)*Number(row.mfe_observed);observedMetrics.maeCount+=Number(row.mae_observed);observedMetrics.maeWeightedSum+=Number(row.mean_observed_mae_r||0)*Number(row.mae_observed);observedMetrics.tp1Count+=Number(row.tp1_observed);observedMetrics.tp1MinWeightedSum+=Number(row.mean_tp1_min_ms||0)*Number(row.tp1_observed);observedMetrics.tp1MaxWeightedSum+=Number(row.mean_tp1_max_ms||0)*Number(row.tp1_observed);
  observedMetrics.slCount+=Number(row.sl_observed);observedMetrics.slMinWeightedSum+=Number(row.mean_sl_min_ms||0)*Number(row.sl_observed);observedMetrics.slMaxWeightedSum+=Number(row.mean_sl_max_ms||0)*Number(row.sl_observed);
 }
 const denominator=directional.SUCCESS+directional.FAILURE;
 observedMetrics.meanObservedMfeR=observedMetrics.mfeCount?observedMetrics.mfeWeightedSum/observedMetrics.mfeCount:null;observedMetrics.meanObservedMaeR=observedMetrics.maeCount?observedMetrics.maeWeightedSum/observedMetrics.maeCount:null;observedMetrics.meanTimeToTp1Ms=observedMetrics.tp1Count?{min:observedMetrics.tp1MinWeightedSum/observedMetrics.tp1Count,max:observedMetrics.tp1MaxWeightedSum/observedMetrics.tp1Count}:null;observedMetrics.meanTimeToSlBeforeTp1Ms=observedMetrics.slCount?{min:observedMetrics.slMinWeightedSum/observedMetrics.slCount,max:observedMetrics.slMaxWeightedSum/observedMetrics.slCount}:null;observedMetrics.interpretation='OBSERVED_EXTREMA; LOWER_BOUNDS_MAY_BE_INCLUDED; NOT_COMPLETE_MARKET_EXCURSIONS';
 return {totalOfficial:total,directional,extended,terminal,provenDirectional:denominator,directionalAccuracy:denominator?directional.SUCCESS/denominator:null,byTimeframe,missingEntryEvidence:missingEntry,breakdowns,observedMetrics,
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
  rows=await read.detail(id,query.from,query.asOf);
 }else throw new Error('measurement_route_invalid');
 const hasMore=rows.length>query.limit;rows=rows.slice(0,query.limit);
 const records=[];
 for(const row of rows){
  if(candidates){records.push(JSON.parse(row.payload_json));continue;}
  const entryEvidence=row.entry_json?JSON.parse(row.entry_json):null;
  let state=row.state_json&&row.state_at<=query.asOf?parseEvidence(row.state_json):null;
  const events=(await read.events(row.signal_id,query.asOf)).map(x=>parseEvidence(x.payload_json));
  if(!state){const checkpoints=events.filter(e=>e.evidenceType==='COVERAGE_CHECKPOINT').sort((a,b)=>b.availableAt-a.availableAt);state=checkpoints[0]??null;}
  const projected=state?.outcome;
  const reduced=reduceOutcome({id:row.signal_id,createdAt:row.created_at,status:row.status,closedAt:row.closed_at},events,
   {asOf:query.asOf,coverageIntervals:state?.covered||[],coverageGaps:state?.gaps||[]});
  const outcome=projected?{...projected,terminalLifecycleOutcome:reduced.terminalLifecycleOutcome}:reduced;
  records.push({signalId:row.signal_id,createdAt:row.created_at,timeframe:row.timeframe,direction:row.direction,
   entry:row.entry,tp1:row.tp1,tp2:row.tp2,sl:row.sl,recordedLifecycle:{status:row.status,tp1At:row.tp1_at,slAt:row.sl_at,closedAt:row.closed_at},
   terminalLifecycleEvidence:row.status==null?'NOT_PROVEN':'RECORDED_LIFECYCLE_FACT',entryEvidence,entryPersistence:{recordedAt:row.entry_recorded_at??null,persistedAt:row.entry_persisted_at??null,timeBasis:'APPLICATION_CAPTURE_AND_DATABASE_CLOCK'},evidenceState:entryEvidence?'CAPTURED':'ENTRY_EVIDENCE_NOT_CAPTURED',outcome,quality:state?.quality??null,excursions:state?.global??null,evidenceAsOf:state?.availableAt??row.state_at??null});
 }
 const last=rows.at(-1),cursor=hasMore&&last?encodeCursor({at:candidates?last.evaluated_at:last.created_at,id:candidates?last.evaluation_id:last.signal_id,asOf:query.asOf,filter:query.filter}):null;
 const summary=candidates?null:populationSummary(await read.population(query.asOf,query.from,query.cohort));
 if(summary)summary.signalFrequency={officialPer24h:query.asOf>query.from?summary.totalOfficial*86400000/(query.asOf-query.from):null,from:query.from,to:query.asOf};
 return {ok:true,...metadata,records,summary,pagination:{limit:query.limit,hasMore,nextCursor:cursor},
  evidenceCompleteness:'PER_RECORD; ABSENCE_OF_CAPTURE_IS_NOT_ABSENCE_OF_SIGNAL_OR_CANDIDATE',
  legacyHandling:'ORIGINAL_FACTS_PRESERVED; NO_ENTRY_RECONSTRUCTION; V1_WINDOWS_NOT_NOMINAL_WINDOW_PROOF'};
}
