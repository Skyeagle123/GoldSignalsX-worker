import assert from 'node:assert/strict';
import {loadM1Workers,cycleEnvironment,now,provenance,BASELINE} from './b1-m1.mjs';
import {configureIndependentProducer} from '../measurement-queue-producer.js';
export async function independentTimingProfile(){
 const workers=await loadM1Workers(),saved={clock:Date.now,fetch:globalThis.fetch,microtask:globalThis.queueMicrotask},counts={},profiles={};let modeledNow=now;
 const active={id:'review-owner',tf:'5m',side:'buy',origin:'server',createdAt:now-10000,signalBarTs:now-300000,entry:4100,tp1:4099.99,tp2:4300,sl:4000,status:'active',tp1Hit:false,updatedAt:now-5000,lastPrice:4100,conf:90};
 const exposure={symbol:'XAUUSD',status:'active',side:'buy',primarySignalId:active.id,primaryTf:'5m',openedAt:now-10000,maxPositions:1,cooldownUntil:0,confirmations:[],blocked:[]};
 Date.now=()=>modeledNow;globalThis.fetch=()=>{throw Error('PACKAGE_A_NO_NETWORK');};
 try{for(const [name,module,on] of [['baseline',workers.before,false],['off',workers.after,false],['on',workers.after,true]]){
  modeledNow=now;const x=cycleEnvironment(module,{activeSignal:active,exposure,telegram:true}),trace=[],messages=[],pending=[],startMemory=process.memoryUsage(),cpu=process.cpuUsage();const feed=x.env.GOLD_FEED.getByName(),status=feed.status,manage=feed.manageGoldExposure,telegram=feed.queueTelegramEvent;
  feed.status=async()=>{const value=await status();value.latestQuote.receivedAt=now-19999;return value;};feed.manageGoldExposure=async v=>{trace.push('EXPOSURE');return manage(v);};feed.queueTelegramEvent=async v=>{trace.push('TELEGRAM');return telegram(v);};
  globalThis.queueMicrotask=job=>saved.microtask(()=>{modeledNow+=2;job();});
  Object.assign(x.env,{B1_MEASUREMENT_PLANE:name==='baseline'?'legacy':'independent',B1_INDEPENDENT_CAPTURE_ENABLED:on?'1':'0',B1_REAL_QUEUE_HANDOFF_ENABLED:on?'1':'0',B1_POST_ENTRY_EXPORT_ENABLED:on?'1':'0',B1_CODE_COMMIT:BASELINE,B1_MEASUREMENT_EFFECTIVE_AT:provenance.measurementEffectiveAt,MEASUREMENT_QUEUE:{async send(body){trace.push('QUEUE');messages.push(body);}}});
  const producer=configureIndependentProducer(x.env,{waitUntil:p=>pending.push(p)});const started=performance.now();
  try{const output=await module.runSignalCycle(x.env,null,{nyFilterOn:false,pivotFilterOn:false});const returnWallMs=performance.now()-started,returnCpu=process.cpuUsage(cpu);trace.push('TRADING_COMPLETE');assert.equal(messages.length,0);counts[name]=x.decisions.flatMap(d=>d.decisions).filter(d=>d.decision==='confirmation').length;
   const production=x.db.database.prepare("SELECT signal_id,status,tp1_at FROM production_signals WHERE signal_id='review-owner'").get();assert.equal(production.status,'tp1');
   if(producer)await producer.whenIdle();await Promise.all(pending);const usage=process.cpuUsage(cpu),memory=process.memoryUsage();if(on){assert(trace.indexOf('QUEUE')>trace.indexOf('TRADING_COMPLETE'));assert(trace.indexOf('QUEUE')>trace.lastIndexOf('TELEGRAM'));assert(trace.indexOf('QUEUE')>trace.indexOf('EXPOSURE'));assert.equal(producer.diagnostics().length,0);}
   profiles[name]={returnWallMs,returnCpuMs:(returnCpu.user+returnCpu.system)/1000,totalWallMs:performance.now()-started,totalCpuMs:(usage.user+usage.system)/1000,heapDeltaBytes:memory.heapUsed-startMemory.heapUsed,rssDeltaBytes:memory.rss-startMemory.rss,queueMessages:messages.length,queueBytes:messages.reduce((n,m)=>n+Buffer.byteLength(m),0),producerMetrics:producer?.metrics()??null,trace,production:{...production}};
  }finally{x.db.database.close();}
 }}finally{Date.now=saved.clock;globalThis.fetch=saved.fetch;globalThis.queueMicrotask=saved.microtask;}
 assert.deepEqual(counts,{baseline:6,off:6,on:6});assert.deepEqual(profiles.off.production,profiles.on.production);return {counts,profiles,localOnly:true,cloudCpuMemoryQualification:'REQUIRED'};
}
