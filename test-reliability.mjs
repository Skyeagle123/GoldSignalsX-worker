import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

const source = await fs.readFile(new URL('./goldsignalsx-worker.js', import.meta.url), 'utf8');
const signalEngineUrl = new URL('./signal-engine.js', import.meta.url).href;
const economicCalendarUrl = new URL('./economic-calendar.js', import.meta.url).href;
const testSource = source
  .replace("import { DurableObject } from 'cloudflare:workers';", 'class DurableObject {}')
  .replace("'./signal-engine.js'", JSON.stringify(signalEngineUrl))
  .replace("'./economic-calendar.js'", JSON.stringify(economicCalendarUrl));
const generatedUrl = new URL('./.test-reliability.generated.mjs', import.meta.url);
await fs.writeFile(generatedUrl, testSource);
const worker = await import(`${generatedUrl.href}?v=${Date.now()}`);
await fs.unlink(generatedUrl);

const openNow = Date.UTC(2026,8,15,12,0,0);
const closedNow = Date.UTC(2026,8,12,12,0,0);
const freshMt5 = {
  source:'mt5',price:4300,ts:openNow-1000,receivedAt:openNow-1000,
  event:'price'
};

function baseValues(now=openNow) {
  return new Map([
    ['system:signal-cycle',{status:'completed',source:'cron',startedAt:now-30_000,completedAt:now-1_000,errors:[]}],
    ['system:history-status',{updatedAt:now-2_000,frames:[{tf:'1m',ok:true}]}],
    ['telegram:health',{tokenConfigured:true,chatConfigured:true,botOk:true,chatOk:true,checkedAt:now-2_000,error:''}],
    ['telegram:last-delivery',{eventId:'trade:test',status:'sent',sentAt:now-3_000,updatedAt:now-3_000}],
    ['telegram:last-success',{eventId:'trade:test',status:'sent',sentAt:now-3_000,updatedAt:now-3_000}],
    ['calendar:official:v4',{ok:true,updatedAt:now-2_000,source:'official',events:[],sourceStatus:{official:{ok:true}}}],
    ['news:brief:v2',{ok:true,updatedAt:now-2_000,source:'gdelt',stale:false,refreshError:'',items:[],sourceStatus:{gdelt:{ok:true}}}]
  ]);
}

class MemoryKv {
  constructor(values,options={}) {
    this.values=values; this.fail=Boolean(options.fail); this.puts=0;
    this.failPutKeys=new Set(options.failPutKeys||[]);
  }
  async get(key) { if (this.fail) throw new Error('kv unavailable'); return this.values.get(key) ?? null; }
  async put(key,value) {
    this.puts+=1;
    if (this.failPutKeys.has(key)) throw new Error(`kv put failed: ${key}`);
    this.values.set(key,value);
  }
}

function d1(now=openNow,{fail=false,tableMissing=false}={}) {
  return {
    prepare(sql) {
      return {
        async first() {
          if (fail) throw new Error('d1 unavailable');
          if (sql.includes('signal_evaluation_telemetry')) {
            if (tableMissing) throw new Error('no such table: signal_evaluation_telemetry');
            return {latest_recorded_at:now-4_000};
          }
          return {ok:1};
        },
        async all() { if (fail) throw new Error('d1 unavailable'); return {results:[]}; }
      };
    }
  };
}

function envFor({now=openNow,feedStatus={ok:true,provider:'mt5',fallback:false,fallbackReason:'',latestQuote:freshMt5,mt5:{healthy:true,lastSeenAt:now-1000,lastAcceptedAt:now-1000}},kvOptions={},d1Options={}}={}) {
  return {
    MT5_INGEST_TOKEN:'configured',TWELVE_DATA_API_KEY:'configured',
    GOLD_FEED:{getByName:()=>({status:async()=>feedStatus})},
    GSX_KV:new MemoryKv(baseValues(now),kvOptions),
    GSX_DB:d1(now,d1Options)
  };
}

let result=await worker.buildOperationalHealth(envFor(),{now:openNow});
assert.equal(result.overall.state,'healthy');
assert.equal(result.components.feed.state,'healthy');
assert.equal(result.selectedPriceFeed,'mt5');
assert.equal(result.fallback,false);
assert.equal(result.components.telegram.lastSuccessfulDeliveryAt,openNow-3_000);
assert.equal(result.latestOperationalTelemetryAt,openNow-4_000);
const healthResponse=await worker.default.fetch(new Request('https://example.com/health'),envFor(),{});
assert.equal(healthResponse.status,200);
const healthPayload=await healthResponse.json();
assert.equal(healthPayload.healthSchema,1);
assert.equal(healthPayload.overall.state,'healthy');
assert.equal(healthPayload.components.kv.state,'healthy');

const privateMt5Status={
  healthy:true,lastSeenAt:openNow-1000,lastAcceptedAt:openNow-1000,
  sessionId:'private-session',symbolMetadata:{
    available:true,fresh:true,status:'available',reason:'',sessionId:'private-session',
    metadata:{contractSize:100,tickValue:1,profitCurrency:'USD'}
  }
};
result=await worker.buildOperationalHealth(envFor({
  feedStatus:{ok:true,provider:'mt5',fallback:false,latestQuote:freshMt5,mt5:privateMt5Status}
}),{now:openNow});
assert.deepEqual(result.components.feed.mt5,{
  configured:false,healthy:true,lastSeenAt:openNow-1000,lastAcceptedAt:openNow-1000,
  metadata:{available:true,fresh:true,status:'available',reason:''}
});
const serializedHealth=JSON.stringify(result);
assert.equal(serializedHealth.includes('private-session'),false);
assert.equal(serializedHealth.includes('contractSize'),false);
assert.equal(serializedHealth.includes('profitCurrency'),false);

result=await worker.buildOperationalHealth(envFor({kvOptions:{fail:true}}),{now:openNow});
assert.equal(result.components.kv.state,'unavailable');
assert.equal(result.components.kv.reason,'read_error');
assert.equal(result.overall.state,'unavailable');

result=await worker.buildOperationalHealth(envFor({d1Options:{fail:true}}),{now:openNow});
assert.equal(result.components.d1.state,'unavailable');
assert.equal(result.components.telemetry.state,'unavailable');
assert.equal(result.overall.state,'unavailable');

result=await worker.buildOperationalHealth(envFor({now:closedNow,kvOptions:{fail:true}}),{now:closedNow});
assert.equal(result.components.feed.state,'market_closed');
assert.equal(result.components.kv.state,'unavailable');
assert.equal(result.overall.state,'unavailable');

result=await worker.buildOperationalHealth(envFor({now:closedNow,d1Options:{fail:true}}),{now:closedNow});
assert.equal(result.components.feed.state,'market_closed');
assert.equal(result.components.d1.state,'unavailable');
assert.equal(result.overall.state,'unavailable');

result=await worker.buildOperationalHealth(envFor({now:closedNow,d1Options:{tableMissing:true}}),{now:closedNow});
assert.equal(result.components.feed.state,'market_closed');
assert.equal(result.components.d1.state,'healthy');
assert.equal(result.components.telemetry.state,'degraded');
assert.equal(result.overall.state,'degraded');

const staleEnv=envFor();
staleEnv.GSX_KV.values.set('news:brief:v2',{ok:true,updatedAt:openNow-2*60*60*1000,stale:true,refreshError:'news_refresh_failed'});
result=await worker.buildOperationalHealth(staleEnv,{now:openNow});
assert.equal(result.components.news.state,'degraded');
assert.equal(result.components.news.reason,'news_refresh_failed');
assert.equal(result.overall.state,'degraded');

const closedDegradedEnv=envFor({now:closedNow});
closedDegradedEnv.GSX_KV.values.set('news:brief:v2',{
  ok:true,updatedAt:closedNow-2*60*60*1000,stale:true,refreshError:'news_refresh_failed'
});
result=await worker.buildOperationalHealth(closedDegradedEnv,{now:closedNow});
assert.equal(result.components.feed.state,'market_closed');
assert.equal(result.components.news.state,'degraded');
assert.equal(result.overall.state,'degraded');

const staleCalendarEnv=envFor();
staleCalendarEnv.GSX_KV.values.set('calendar:official:v4',{
  ok:true,updatedAt:openNow-2*60*60*1000,stale:true,source:'official'
});
result=await worker.buildOperationalHealth(staleCalendarEnv,{now:openNow});
assert.equal(result.components.calendar.state,'degraded');
assert.equal(result.components.calendar.reason,'stale_cache');
staleCalendarEnv.GSX_KV.values.delete('calendar:official:v4');
result=await worker.buildOperationalHealth(staleCalendarEnv,{now:openNow});
assert.equal(result.components.calendar.state,'unavailable');
assert.equal(result.components.calendar.reason,'calendar_missing');

const telegramEnv=envFor();
telegramEnv.GSX_KV.values.set('telegram:health',{botOk:false,chatOk:true,checkedAt:openNow-1000,error:'telegram_timeout'});
result=await worker.buildOperationalHealth(telegramEnv,{now:openNow});
assert.equal(result.components.telegram.state,'degraded');
assert.equal(result.components.telegram.reason,'telegram_timeout');
telegramEnv.GSX_KV.values.set('telegram:health',{botOk:true,chatOk:true,checkedAt:openNow,error:''});
result=await worker.buildOperationalHealth(telegramEnv,{now:openNow});
assert.equal(result.components.telegram.state,'healthy');

const deliveryKv=new MemoryKv(new Map());
await worker.persistTelegramDelivery({GSX_KV:deliveryKv},'telegram:delivery:v2:sent',{
  eventId:'trade:sent',status:'sent',sentAt:openNow,updatedAt:openNow
});
assert.equal(JSON.parse(deliveryKv.values.get('telegram:last-success')).status,'sent');
await worker.persistTelegramDelivery({GSX_KV:deliveryKv},'telegram:delivery:v2:failed',{
  eventId:'trade:failed',status:'failed',sentAt:0,updatedAt:openNow+1
});
assert.equal(JSON.parse(deliveryKv.values.get('telegram:last-success')).eventId,'trade:sent');

const lastSuccessFailureKv=new MemoryKv(new Map(),{failPutKeys:['telegram:last-success']});
await worker.persistTelegramDelivery({GSX_KV:lastSuccessFailureKv},'telegram:delivery:v2:sent-safe',{
  eventId:'trade:sent-safe',status:'sent',sentAt:openNow,updatedAt:openNow
});
assert.equal(JSON.parse(lastSuccessFailureKv.values.get('telegram:delivery:v2:sent-safe')).status,'sent');
assert.equal(JSON.parse(lastSuccessFailureKv.values.get('telegram:last-delivery')).status,'sent');
assert.equal(lastSuccessFailureKv.values.has('telegram:last-success'),false);

let qualityCalls=0,mtfCalls=0;
await assert.rejects(worker.runIndependentOperationalCollectors({},
  {ok:false,error:'telemetry_recording_failed'},
  {
    quality:async()=>{ qualityCalls+=1; return {ok:true,updated:1}; },
    mtf:async()=>{ mtfCalls+=1; return {ok:true,updated:1}; }
  }
),/signal_telemetry:telemetry_recording_failed/);
assert.equal(qualityCalls,1);
assert.equal(mtfCalls,1);

qualityCalls=0; mtfCalls=0;
await assert.rejects(worker.runIndependentOperationalCollectors({},
  {ok:true,recorded:7},
  {
    quality:async()=>{ qualityCalls+=1; return {ok:false,error:'quality_metrics_collection_failed'}; },
    mtf:async()=>{ mtfCalls+=1; return {ok:true,updated:1}; }
  }
),/signal_quality:quality_metrics_collection_failed/);
assert.equal(qualityCalls,1);
assert.equal(mtfCalls,1);

qualityCalls=0; mtfCalls=0;
await assert.rejects(worker.runIndependentOperationalCollectors({},
  {ok:true,recorded:7},
  {
    quality:async()=>{ qualityCalls+=1; return {ok:true,updated:1}; },
    mtf:async()=>{ mtfCalls+=1; return {ok:false,error:'mtf_analysis_collection_failed'}; }
  }
),/mtf_analysis:mtf_analysis_collection_failed/);
assert.equal(qualityCalls,1);
assert.equal(mtfCalls,1);

const partialCycleEnv=envFor();
partialCycleEnv.GSX_KV.values.set('system:signal-cycle',{
  status:'partial',source:'cron',startedAt:openNow-30_000,completedAt:openNow-1_000,
  errors:['operational_collectors_failed:signal_quality:quality_metrics_collection_failed']
});
result=await worker.buildOperationalHealth(partialCycleEnv,{now:openNow});
assert.equal(result.components.cron.state,'degraded');
assert.equal(result.components.cron.reason,'cycle_partial');
assert.equal(result.overall.state,'degraded');

const fallbackEnv=envFor({feedStatus:{ok:true,provider:'twelve-data',fallback:true,fallbackReason:'mt5_stale',latestQuote:{...freshMt5,source:'twelve-data'},mt5:{healthy:false,lastSeenAt:openNow-20_000,lastAcceptedAt:openNow-20_000}}});
result=await worker.buildOperationalHealth(fallbackEnv,{now:openNow});
assert.equal(result.components.feed.state,'degraded');
assert.equal(result.fallback,true);
assert.equal(result.fallbackReason,'mt5_stale');
fallbackEnv.GOLD_FEED.getByName=()=>({status:async()=>({ok:true,provider:'mt5',fallback:false,fallbackReason:'',latestQuote:freshMt5,mt5:{healthy:true,lastSeenAt:openNow,lastAcceptedAt:openNow}})});
result=await worker.buildOperationalHealth(fallbackEnv,{now:openNow});
assert.equal(result.components.feed.state,'healthy');

const closedEnv=envFor({now:closedNow,feedStatus:{ok:false,provider:'',fallback:false,fallbackReason:'mt5_stale',latestQuote:null,mt5:{healthy:false,lastSeenAt:closedNow-20_000,lastAcceptedAt:closedNow-20_000}}});
result=await worker.buildOperationalHealth(closedEnv,{now:closedNow});
assert.equal(result.market.state,'closed');
assert.equal(result.components.feed.state,'market_closed');
assert.notEqual(result.overall.state,'unavailable');

assert.equal(closedEnv.GSX_KV.puts,0,'health must not write KV or trading state');
const cycleUnavailable=await worker.runSignalCycle({GSX_DB:null,GSX_KV:null},null);
assert.equal(cycleUnavailable.error,'signal_cycle_storage_unavailable');
console.log('reliability health tests passed');
