import {BUDGET} from './measurement-reservations.js';
import {need,GATES,identity} from './measurement-independent-contract.js';
export const CATCHUP_WORKLOAD=Object.freeze(['newArrivals','census','officials','candidates','market','retries','multipart','corrections','archive','reporting']);
export const CATCHUP_RECOVERED_FIELDS=Object.freeze(['censusRecovered','officialRecovered','candidateRecovered','marketRecovered','retriesRecovered','multipartRecovered','correctionsRecovered','archiveVerified','archiveReadbackVerified','reportingComplete','reportingLoadObserved']);
export const CATCHUP_RESOURCE_FIELDS=Object.freeze(['normalLiveBytes','normalReservedBytes','recoveryReserveBytes','hardChargedBytes','stagedTransportBytes','archiveWorkBytes','reservationBytes','reservationCount']);
export const QUALIFICATION_MARKER='goldsignalsx-b1-isolated-qualification-v1';
export function verifySyntheticEnvironment(config,allowlist){
 need(config?.marker===QUALIFICATION_MARKER&&config.environment==='synthetic-b1-package-a','pa_synthetic_marker');
 for(const name of ['database','bucket','queue','dlq','restoreDatabase']){identity(config.resources?.[name]);need(config.resources[name]===allowlist?.[name]&&/^synthetic-b1-[a-z0-9-]+$/.test(config.resources[name])&&!/prod|production|trading/i.test(config.resources[name]),'pa_synthetic_resource_identity');}
 need(config.resources.database!==config.resources.restoreDatabase&&config.resources.queue!==config.resources.dlq,'pa_synthetic_resource_separation');return true;
}
export function qualificationPlan(config,allowlist){verifySyntheticEnvironment(config,allowlist);return Object.freeze({marker:QUALIFICATION_MARKER,environment:config.environment,resources:{...config.resources},
 scenarios:['rejected-candidate','eligible-candidate','official','confirmation','tp1','tp2','sl','expiry','failed-trading-persistence','post-entry-market','source-gaps','ordering','duplicate','conflict','multipart-expiry','retry','dlq','replay','correction','correction-after-eviction','archive-publication','archive-readback','eviction','archive-report','empty-restore','corrupt-object','missing-object','unsupported-codec','resource-saturation','multi-instance','rollback'],
 outage:{pauseHours:48,continuingArrivals:true,catchupTargetDays:7,requiredWorkload:CATCHUP_WORKLOAD,requiredRecoveryObservations:CATCHUP_RECOVERED_FIELDS,requiredResourceObservations:CATCHUP_RESOURCE_FIELDS,resourceModel:BUDGET},archiveOnlyRestore:{sourceUnavailable:true,emptyTarget:true,compareCensusAndResultDigests:true},execution:'NOT_AUTHORIZED_IN_PACKAGE_A',cloudCalls:0});}
export function rollbackPlan(mode,{artifact,compatibleSchema,compatibleArchive}={}){
 need(['CAPTURE','PROCESSING','VERSION'].includes(mode),'pa_rollback_mode');
 if(mode==='VERSION')need(artifact&&compatibleSchema===10&&['measurement-archive-v1','measurement-archive-v2'].includes(compatibleArchive),'pa_rollback_incompatible');
 return {mode,gates:mode==='CAPTURE'?{B1_MEASUREMENT_PLANE:'independent',B1_INDEPENDENT_CAPTURE_ENABLED:'0',B1_REAL_QUEUE_HANDOFF_ENABLED:'0',B1_POST_ENTRY_EXPORT_ENABLED:'0'}:mode==='PROCESSING'?{B1_MEASUREMENT_CONSUMER_ENABLED:'0',B1_ARCHIVE_EVICTION_ENABLED:'0'}:{B1_MEASUREMENT_PLANE:'independent',...Object.fromEntries(GATES.map(g=>[g,'0']))},artifact:artifact??null,preserveQueue:true,preserveDlq:true,preserveArchive:true,preserveAvailability:true,legacyFallback:false,execution:'PLAN_ONLY'};
}
export function heartbeatContract({cohort,observer,at,lastCapturedAt}){identity(cohort);identity(observer);need(Number.isSafeInteger(at)&&Number.isSafeInteger(lastCapturedAt)&&lastCapturedAt<=at,'pa_heartbeat');return {version:'measurement-heartbeat-v1',cohort,observer,at,lastCapturedAt,observedSilence:at-lastCapturedAt,coverage:'UNKNOWN_CAPTURE',count:'UNKNOWN',externalObserverRequired:true};}
