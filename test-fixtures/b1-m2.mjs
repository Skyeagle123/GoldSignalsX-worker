import fs from 'node:fs/promises';
import {DatabaseSync} from 'node:sqlite';
import {transactionalBinding} from './b1-sqlite.mjs';
import {buildMeasurementEnvelope} from '../measurement-envelope.js';
import {prepareCycleEnvelopes} from '../measurement-producer.js';
import {completeJournal,loadM1Workers,provenance,now} from './b1-m1.mjs';
import * as sm from '../signal-measurement.js';
export {now};
export async function database(){
 const db=new DatabaseSync(':memory:');db.exec('PRAGMA foreign_keys=ON');
 for(const file of ['0001_measurement_evidence.sql','0002_measurement_storage_tiers.sql','0003_measurement_dependency_closure.sql','0004_measurement_ingress.sql'])db.exec(await fs.readFile(new URL('../migrations/'+file,import.meta.url),'utf8'));
 const calls=[];return {db,calls,binding:transactionalBinding(db,{calls})};
}
export async function cyclePackets({heavy=false}={}){
 const {after}=await loadM1Workers();const journal=completeJournal(after,sm,{heavy});
 return prepareCycleEnvelopes(sm.decisionJournalObservations(journal),provenance,{preparedAt:now+2000});
}
export async function lifecycle(signalId,event,{at=now+60000,createdAt=now,persisted=true,partial=false}={}){
 return buildMeasurementEnvelope({kind:'LIFECYCLE_FACT',semanticId:`production:${signalId}:${event}`,occurredAt:at,observedAt:at+10,preparedAt:at+20,
  payload:{signalId,event,createdAt:partial?null:createdAt,timeframe:partial?null:'30m',direction:partial?null:'buy',occurredAt:at,
   observedPrice:null,level:null,trigger:{price:null,at:null,source:null},status:partial?null:({tp1:'tp1',tp2:'tp2',sl:'stopped',expired:'expired'})[event],
   closedAt:event==='tp1'||partial?null:at,performancePersistence:{ok:persisted,...(!persisted?{error:'performance_record_failed'}:{signal:{signal_id:signalId,status:event==='sl'?'sl':event,final_status:event==='tp1'?null:event,updated_at:at+10}})},
   orderingQuality:'NOT_ASSERTED',measurementOnly:true,decisionUse:false}});
}
export async function confirmation(primarySignalId,confirmationSignalId,{persisted=true}={}){
 return buildMeasurementEnvelope({kind:'CONFIRMATION_LINK',semanticId:`${primarySignalId}:confirmation:${confirmationSignalId}`,occurredAt:now+30,observedAt:null,preparedAt:now+40,
  payload:{primarySignalId,confirmationSignalId,link:{signalId:confirmationSignalId,timeframe:'5m'},linkPersistence:persisted?'SUCCEEDED':'FAILED',measurementOnly:true,decisionUse:false}});
}
