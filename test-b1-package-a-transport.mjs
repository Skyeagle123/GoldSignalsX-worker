import {test} from 'node:test';
import assert from 'node:assert/strict';
import {packageDatabase} from './test-fixtures/b1-package-a.mjs';
import {cyclePackets,now} from './test-fixtures/b1-m2.mjs';
import {fragmentMeasurementPacket} from './measurement-transport-fragment.js';
import {stageQueueFragment,recoverTransportEvent,consumeQueueBatch} from './measurement-queue-consumer.js';
const packets=await cyclePackets();const at=now+86400000;
test('Package A durable fragments ack before completion; fresh isolate recovers',async()=>{const x=await packageDatabase();try{
 const p=packets[0],frames=await fragmentMeasurementPacket(p);let acks=0,retries=0;for(const frame of [...frames].reverse())await consumeQueueBatch(x.db,{messages:[{body:frame,ack:()=>acks++,retry:()=>retries++}]},{now:at});
 assert.equal(acks,frames.length);assert.equal(retries,0);assert.equal(x.raw.prepare('SELECT COUNT(*) n FROM pa_records WHERE ready=1').get().n,0);
 const result=await recoverTransportEvent(x.db,p.envelope.eventId,{now:at});assert.equal(result.ingestion.status,'ACCEPTED');assert.equal(result.ingestion.durable,true);assert.equal(result.responseVersion,'measurement-d1-response-v1');
 assert.equal(x.raw.prepare("SELECT COUNT(*) n FROM pa_records WHERE kind='CENSUS' AND ready=1").get().n,7);
}finally{x.close();}});
test('Package A SQL reservation race admits only one competing request',async()=>{const x=await packageDatabase();try{const {reserve}=await import('./measurement-reservations.js');const r=await Promise.allSettled(['a','b'].map(id=>reserve(x.db,{id,workClass:'ingestion',bytes:3000000000,owner:id,now:at})));assert.equal(r.filter(v=>v.status==='fulfilled').length,1);}finally{x.close();}});
