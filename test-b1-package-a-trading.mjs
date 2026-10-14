import {test} from 'node:test';
import assert from 'node:assert/strict';
import {independentTimingProfile} from './test-fixtures/b1-package-a-producer.mjs';
import {configureIndependentProducer,independentCohort,createQueueHandoff} from './measurement-queue-producer.js';
import {cyclePackets} from './test-fixtures/b1-m2.mjs';
test('Independent Queue preparation/export preserves advancing-clock timing 6/6/6 after authoritative work',async()=>{const result=await independentTimingProfile();assert.deepEqual(result.counts,{baseline:6,off:6,on:6});assert(result.profiles.on.queueMessages>0);});
test('Capture rollback keeps independent cohort selected with no legacy fallback',()=>{assert.equal(independentCohort({B1_MEASUREMENT_PLANE:'independent',B1_INDEPENDENT_CAPTURE_ENABLED:'0'}),true);assert.equal(configureIndependentProducer({B1_MEASUREMENT_PLANE:'independent'},{waitUntil(){throw Error('OFF');}}),null);});
test('Queue acceptance remains handoff-only; failed send preserves unknown acceptance',async()=>{const p=(await cyclePackets())[0],a=createQueueHandoff({async send(){}}),good=await a.send(p);assert.equal(good.status,'QUEUE_ACCEPTED');assert.equal(good.durable,false);assert.equal(good.analyticalCompletion,false);const b=createQueueHandoff({async send(){throw Error('unknown');}}),bad=await b.send(p);assert.equal(bad.status,'HANDOFF_ACCEPTANCE_UNKNOWN');assert.equal(bad.durable,null);assert.equal(bad.queueAcceptedFragments,0);});
