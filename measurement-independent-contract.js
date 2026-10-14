// Package-A shared bounded contracts. No binding acquisition or cloud requests.
import {canonicalSerialize,digestPayload,parseEvidence} from './signal-evidence.js';
export {canonicalSerialize,parseEvidence};
export const DAY=86400000;
export const LIMITS=Object.freeze({rowBytes:60000,chunkBytes:42000,parameters:90,statementBytes:90000,batch:80,eventBytes:2097152,
 scopeRecords:256,dependencies:128,reportRecords:128,reportObjects:512,reportBytes:8388608,activeEvents:1024,activeScopes:4032,
 jobs:8192,incidents:256,reservations:8192,publications:64,restoreRecords:128,corrections:256});
export const RESPONSE_VERSION='measurement-d1-response-v1';
export const GATES=Object.freeze(['B1_INDEPENDENT_CAPTURE_ENABLED','B1_REAL_QUEUE_HANDOFF_ENABLED','B1_MEASUREMENT_CONSUMER_ENABLED',
 'B1_POST_ENTRY_EXPORT_ENABLED','B1_ARCHIVE_WRITE_ENABLED','B1_ARCHIVE_REPORTING_ENABLED','B1_ARCHIVE_EVICTION_ENABLED',
 'B1_CORRECTION_ADMISSION_ENABLED','B1_RESTORE_ENABLED','B1_INDEPENDENT_REPORTING_ENABLED']);
export const enabled=(env,name)=>env?.[name]==='1';
export const bytes=v=>new TextEncoder().encode(typeof v==='string'?v:canonicalSerialize(v,LIMITS.eventBytes)).length;
export function need(ok,reason){if(!ok)throw Error(reason);}
export function identity(v){need(typeof v==='string'&&v.length>0&&bytes(v)<=1024,'pa_identity');return v;}
export function time(v){need(Number.isSafeInteger(v)&&v>=0,'pa_clock');return v;}
export const hash=v=>digestPayload(typeof v==='string'?v:canonicalSerialize(v,LIMITS.eventBytes));
export const uid=()=>crypto.randomUUID();
export function bounded(v,max,min=1){need(Number.isSafeInteger(v)&&v>=min&&v<=max,'pa_bound');return v;}
export function serialize(v,max=LIMITS.rowBytes){const wire=canonicalSerialize(v,LIMITS.eventBytes);need(bytes(wire)<=max,'pa_row_bound');return wire;}
export function retention(kind,created,{active=false,unresolved=false,holdUntil=0}={}){time(created);time(holdUntil);const candidate=kind==='CANDIDATE';return Object.freeze({
 rawLiveUntil:created+30*DAY,admitUntil:created+90*DAY,derivedUntil:created+90*DAY,correctionUntil:created+120*DAY,
 forensicUntil:candidate?created+120*DAY:Math.max(created+455*DAY,holdUntil),extended:!candidate&&(active||unresolved||holdUntil>created+455*DAY),
 canExpire:!active&&!unresolved, census:'LONG_TERM_ARCHIVE',definitions:'WHILE_DEPENDENT_EVIDENCE_RETAINED'});}
export function independentBindings(env,role){
 need(env&&typeof env==='object','pa_environment');
 if(role==='measurement')need(!['GSX_DB','GSX_KV','GOLD_FEED','TRADING_DB','TRADING_TOKEN','GSX_WRITE_TOKEN','MT5_INGEST_TOKEN','TELEGRAM_TOKEN','BOT_TOKEN','TWELVE_DATA_API_KEY'].some(k=>env[k]),'pa_mixed_bindings');
 if(role==='trading')need(!env.MEASUREMENT_DB&&!env.MEASUREMENT_ARCHIVE&&!env.MEASUREMENT_ADMIN_TOKEN,'pa_mixed_bindings');
 return true;
}
