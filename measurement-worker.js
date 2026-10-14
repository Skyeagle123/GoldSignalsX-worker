// Dedicated independent entry. No Trading D1, Engine execution or trading writes.
import {enabled,independentBindings,need,identity,time} from './measurement-independent-contract.js';
import {createMeasurementD1Adapter,schemaReady} from './measurement-d1-adapter.js';
import {consumeDeadLetterBatch} from './measurement-transport-maintenance.js';
import {consumeQueueBatch} from './measurement-queue-consumer.js';
import {createPrivateArchiveAdapter,archiveReader,publishArchive,recoverRootCheckpoint} from './measurement-archive.js';
import {createIndependentReporting} from './measurement-reporting.js';
import {runRecovery} from './measurement-recovery.js';
import {recordIncident,measurementObservability} from './measurement-incidents.js';
import {validateMarketEvidence} from './measurement-market-envelope.js';
import {appendIndependentRecord} from './measurement-independent-store.js';
import {ingestMarketEvidence} from './measurement-market-store.js';
import {planArchiveMaintenance,compactIndependentState} from './measurement-independent-retention.js';
const response=(body,status=200)=>new Response(JSON.stringify(body),{status,headers:{'content-type':'application/json','cache-control':'no-store'}});
const authorized=(request,env)=>typeof env.MEASUREMENT_REPORT_TOKEN==='string'&&env.MEASUREMENT_REPORT_TOKEN.length>=32&&request.headers.get('authorization')==='Bearer '+env.MEASUREMENT_REPORT_TOKEN;
function compose(env){independentBindings(env,'measurement');need(env.MEASUREMENT_ENVIRONMENT==='synthetic-b1-package-a'&&typeof env.MEASUREMENT_EXPECTED_D1_ID==='string'&&env.MEASUREMENT_EXPECTED_D1_ID.length>0,'pa_environment_identity');
 const db=createMeasurementD1Adapter(env.MEASUREMENT_DB,{statementBudget:900}),archive=env.MEASUREMENT_ARCHIVE?createPrivateArchiveAdapter(env.MEASUREMENT_ARCHIVE,{namespace:env.MEASUREMENT_ARCHIVE_NAMESPACE,operationBudget:9000}):null;
 return {db,archive,reader:archive?archiveReader(archive,{getRoot:async()=> (await db.first('SELECT root_key FROM pa_meta')).root_key}):null};}
async function environmentReady(db,env){const m=await db.first('SELECT * FROM pa_environment WHERE singleton=1');need(m?.plane==='MEASUREMENT'&&m.environment===env.MEASUREMENT_ENVIRONMENT&&m.resource_id===env.MEASUREMENT_EXPECTED_D1_ID,'pa_database_identity');}
export default {
 async fetch(request,env){try{independentBindings(env,'measurement');const path=new URL(request.url).pathname;
  if(request.method!=='GET')return response({error:'METHOD_NOT_ALLOWED'},405);
  if(!authorized(request,env))return response({error:'UNAUTHORIZED'},401);
  if(!enabled(env,'B1_INDEPENDENT_REPORTING_ENABLED'))return response({status:'OFF'},503);
  const {db,archive}=compose(env);await environmentReady(db,env);if(path==='/health')return response(await measurementObservability(db,{now:Date.now()}));
  if(path!=='/report')return response({error:'NOT_FOUND'},404);
  const u=new URL(request.url),report=await createIndependentReporting(db,{archive:enabled(env,'B1_ARCHIVE_REPORTING_ENABLED')?archive:null,tokenKey:env.MEASUREMENT_REPORT_TOKEN});
  return response(await report.query({view:u.searchParams.get('view')??'CENSUS',cohort:identity(u.searchParams.get('cohort')),definition:identity(u.searchParams.get('definition')),from:time(Number(u.searchParams.get('from'))),to:time(Number(u.searchParams.get('to'))),asOf:time(Number(u.searchParams.get('asOf')))},{cursor:u.searchParams.get('cursor'),limit:Number(u.searchParams.get('limit')??32)}));
 }catch(error){return response({status:'UNAVAILABLE',reason:error.message,complete:false},503);}},
 async queue(batch,env){if(!enabled(env,'B1_MEASUREMENT_CONSUMER_ENABLED')){for(const m of batch.messages)m.retry({delaySeconds:60});return;}
  let db,reader;try{({db,reader}=compose(env));await schemaReady(db);await environmentReady(db,env);}catch{for(const m of batch.messages)m.retry({delaySeconds:60});return;}
  if(batch.queue===env.MEASUREMENT_DLQ_NAME){await consumeDeadLetterBatch(db,batch,{now:Date.now(),archive:reader});return;}
  need(batch.queue===env.MEASUREMENT_QUEUE_NAME,'pa_queue_identity');
  // Market packets are independently representable; no reinterpretation of M1 envelopes.
  const ordinary=[];for(const m of batch.messages){let body;try{body=typeof m.body==='string'?JSON.parse(m.body):m.body;}catch{}if(body?.version==='measurement-market-queue-v1'){
    try{await ingestMarketEvidence(db,body,{now:Date.now(),archive:reader});m.ack();
    }catch{m.retry({delaySeconds:30});}
   }else ordinary.push(m);}
  if(ordinary.length)await consumeQueueBatch(db,{messages:ordinary},{now:Date.now(),archive:reader,onIncident:input=>recordIncident(db,input,{now:Date.now()})});
 },
 async scheduled(_controller,env,ctx){if(!enabled(env,'B1_MEASUREMENT_CONSUMER_ENABLED'))return;const {db,archive,reader}=compose(env);await environmentReady(db,env);
  ctx.waitUntil((async()=>{if(enabled(env,'B1_ARCHIVE_WRITE_ENABLED'))await planArchiveMaintenance(db,{now:Date.now()});await runRecovery(db,{now:Date.now(),archive:reader,handlers:{archive:async({row,now,lease})=>{need(enabled(env,'B1_ARCHIVE_WRITE_ENABLED'),'pa_archive_off');if(row.id.startsWith('root-checkpoint:'))return recoverRootCheckpoint(db,archive,row.event_id,{now,lease});return publishArchive(db,archive,row.scope_id,{now,domain:row.event_id==='CENSUS'?'CENSUS':'EVIDENCE'});}}});await compactIndependentState(db,{now:Date.now(),archive:enabled(env,'B1_ARCHIVE_EVICTION_ENABLED')?archive:null});})());
 }
};
