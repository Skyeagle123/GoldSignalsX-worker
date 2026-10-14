// Dependency-injected future Package-B runner. This module never provisions or
// discovers resources. Local validation uses a driver branded LOCAL_EMULATION.
import {qualificationPlan,verifySyntheticEnvironment,QUALIFICATION_MARKER} from './measurement-qualification.js';
import {need,identity,time,hash} from './measurement-independent-contract.js';
const requiredDriver=['identity','scenario','pauseConsumer','produceBounded','recoverBounded','observe','archiveOnlyRestore','rollbackRehearsal'];
export function validateQualificationExecution(config,allowlist,authorization,{local=false,now=Date.now()}={}){
 verifySyntheticEnvironment(config,allowlist?.resources);need(authorization?.marker===QUALIFICATION_MARKER&&authorization.environment===config.environment&&authorization.stage===(local?'LOCAL_EMULATION':'PACKAGE_B')&&authorization.expiresAt>now,'pa_qualification_authorization');
 need(allowlist?.bucketNamespace===config.bucketNamespace&&/^synthetic-b1-[a-z0-9-]+\/$/.test(config.bucketNamespace),'pa_qualification_bucket_namespace');
 for(const name of ['database','bucket','queue','dlq','restoreDatabase']){const id=config.resourceIds?.[name];identity(id);need(id===allowlist.resourceIds?.[name]&&id===authorization.resourceIds?.[name]&&!/REQUIRED|PLACEHOLDER|prod|production|trading/i.test(id),'pa_qualification_exact_resource_id');if(!local)need(!id.startsWith('LOCAL_'),'pa_qualification_mock_resource');}
 need(config.resourceIds.database!==config.resourceIds.restoreDatabase&&config.resourceIds.queue!==config.resourceIds.dlq,'pa_qualification_id_separation');return true;
}
export async function runIsolatedQualification({config,allowlist,authorization,driver,clock=()=>Date.now(),wait=ms=>new Promise(r=>setTimeout(r,ms)),maxIterations=15000}={}){
 const local=driver?.kind==='LOCAL_EMULATION';validateQualificationExecution(config,allowlist,authorization,{local,now:clock()});need(requiredDriver.every(k=>typeof driver[k]==='function')&&maxIterations>=1&&maxIterations<=15000,'pa_qualification_driver');
 const assertAuthorized=()=>validateQualificationExecution(config,allowlist,authorization,{local,now:clock()});
 const observedIdentity=await driver.identity();need(await hash(observedIdentity)===await hash({environment:config.environment,resources:config.resources,resourceIds:config.resourceIds,bucketNamespace:config.bucketNamespace}),'pa_qualification_observed_identity');
 const results=[];for(const name of qualificationPlan(config,allowlist.resources).scenarios){assertAuthorized();const result=await driver.scenario(name,{maxOperations:32,maxBytes:8388608});need(result?.passed===true,'pa_qualification_scenario_failed:'+name);results.push({scenario:name,passed:true});}
 const start=time(clock()),pauseThrough=start+48*3600000,catchupThrough=pauseThrough+7*86400000,observations=[];let iterations=0;
 assertAuthorized();await driver.pauseConsumer(true);
 while(clock()<pauseThrough){need(++iterations<=maxIterations,'pa_qualification_iteration_bound');assertAuthorized();await driver.produceBounded({maxPackets:16,continuingArrivals:true});const observation=await driver.observe();if(observations.length>=64)observations.shift();observations.push({at:clock(),...observation});await wait(Math.min(60000,pauseThrough-clock()));}
 assertAuthorized();await driver.pauseConsumer(false);let recovered=false;
 while(clock()<=catchupThrough){need(++iterations<=maxIterations,'pa_qualification_iteration_bound');assertAuthorized();await driver.produceBounded({maxPackets:16,continuingArrivals:true});await driver.recoverBounded({maxJobs:32});const observation=await driver.observe();if(observations.length>=64)observations.shift();observations.push({at:clock(),...observation});
  if(observation.backlog===0&&observation.censusRecovered&&observation.officialRecovered&&observation.candidateRecovered&&observation.marketRecovered&&observation.archiveVerified&&observation.reportingComplete){recovered=true;break;}if(clock()===catchupThrough)break;await wait(Math.min(60000,catchupThrough-clock()));}
 need(recovered,'pa_qualification_catchup_target_failed');assertAuthorized();const restore=await driver.archiveOnlyRestore({sourceUnavailable:true,emptyTarget:true,verifyDefinitions:true,compareCensusAndResultDigests:true});need(restore?.passed===true,'pa_qualification_archive_restore_failed');
 assertAuthorized();const rollback=await driver.rollbackRehearsal({preserveQueue:true,preserveDlq:true,preserveArchive:true,legacyFallback:false});need(rollback?.passed===true,'pa_qualification_rollback_failed');
 return {version:'measurement-qualification-result-v1',environment:config.environment,localEmulation:local,cloudExecutionObserved:!local,cloudQualified:false,acceptance:'REQUIRES_OWNER_REVIEW',startedAt:start,completedAt:clock(),pausedHours:48,catchupMilliseconds:clock()-pauseThrough,continuingArrivals:true,scenarios:results,restore,rollback,observations,iterations,productionActivationAuthorized:false};
}
