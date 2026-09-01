import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';

const originalFetch=globalThis.fetch;

if (typeof globalThis.crypto.subtle.timingSafeEqual !== 'function') {
  Object.defineProperty(globalThis.crypto.subtle, 'timingSafeEqual', {
    value(left, right) {
      const a=new Uint8Array(left),b=new Uint8Array(right);
      if (a.byteLength!==b.byteLength) return false;
      let diff=0;
      for (let i=0;i<a.byteLength;i++) diff|=a[i]^b[i];
      return diff===0;
    }
  });
}

const source = await fs.readFile(new URL('./goldsignalsx-worker.js', import.meta.url), 'utf8');
const signalEngineUrl = new URL('./signal-engine.js', import.meta.url).href;
const economicCalendarUrl = new URL('./economic-calendar.js', import.meta.url).href;
const testSource = source.replace(
  "import { DurableObject } from 'cloudflare:workers';",
  'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }'
).replace("'./signal-engine.js'", JSON.stringify(signalEngineUrl))
  .replace("'./economic-calendar.js'", JSON.stringify(economicCalendarUrl));
const generatedWorkerUrl=new URL('./.test-worker.generated.mjs',import.meta.url);
await fs.writeFile(generatedWorkerUrl,testSource);
const worker = await import(`${generatedWorkerUrl.href}?v=${Date.now()}`);
await fs.unlink(generatedWorkerUrl);
const {
  computeServerSignal,evaluateCandleQuality,normalizeSignalFilters,runServerBacktest
} = await import(signalEngineUrl);
const {
  GoldFeed,classifyNewsArticle,buildNewsBrief,enrichNewsBriefArabic,getGoldNewsBrief,
  parseGdeltSeenDate,parseTwelveDataTimeSeries,sendTelegramText,queueTelegramDelivery,
  signalTelegramText,processTelegramOutbox,
  updateSignalLifecycleAcrossBars,closedBarsOnly,signalFiltersFromSearchParams,readSignalFilters,
  pruneTickHistory,decideGoldExposure,ensurePerformanceSchema,
  recordProductionPerformanceEvent,recordProductionPerformanceSafely,
  readProductionPerformance,buildPerformanceSummary,signalResultR,
  ensureNewsCalendarSchema,mergeSignalRiskContext,maybeNotifyCalendarEvents,
  persistCalendarEvents,persistNewsEvents
} = worker;
const {
  normalizeCalendarSettings,parseBlsIcs,parseBeaScheduleHtml,parseFedCalendarHtml,
  parseCensusScheduleHtml,parseBlsApiPayload,parseBlsDownloadDataset,buildBlsActualSnapshot,
  applyBlsActuals,parseJoblessClaimsXml,buildJoblessClaimsEvents,buildIsmDerivedSchedule,
  calendarRiskSnapshot,extractOfficialActual
}=await import(economicCalendarUrl);

const now = Date.UTC(2026, 7, 27, 8, 0, 0);
assert.deepEqual(normalizeSignalFilters(),{
  nyFilterOn:true,nyStart:'08:00',nyEnd:'17:00',pivotFilterOn:true,pivotDistance:0.7
});
assert.deepEqual(normalizeSignalFilters({
  nyFilterOn:false,nyStart:'09:15',nyEnd:'16:45',pivotFilterOn:false,pivotDistance:1.25
}),{
  nyFilterOn:false,nyStart:'09:15',nyEnd:'16:45',pivotFilterOn:false,pivotDistance:1.25
});
const parsedFilters=signalFiltersFromSearchParams(new URL('https://example.com/signals?tf=1m&nyFilterOn=0&nyStart=09%3A15&nyEnd=16%3A45&pivotFilterOn=1&pivotDistance=1.25').searchParams);
assert.equal(parsedFilters.ok,true);
assert.deepEqual(parsedFilters.filters,{
  nyFilterOn:false,nyStart:'09:15',nyEnd:'16:45',pivotFilterOn:true,pivotDistance:1.25
});
assert.equal(signalFiltersFromSearchParams(new URL('https://example.com/signals?nyFilterOn=0').searchParams).ok,false);
const seen = '20260827T075500Z';
assert.equal(parseGdeltSeenDate(seen), Date.UTC(2026, 7, 27, 7, 55, 0));

const historyRows=parseTwelveDataTimeSeries({values:[
  {datetime:'2026-08-27 10:01:00',open:'2401',high:'2403',low:'2400',close:'2402',volume:'4'},
  {datetime:'2026-08-27 10:00:00',open:'2400',high:'2402',low:'2399',close:'2401',volume:'3'},
  {datetime:'bad',open:'1',high:'0',low:'2',close:'1'}
]});
assert.equal(historyRows.length,2);
assert.equal(historyRows[0].t,Date.UTC(2026,7,27,10,0));
assert.equal(historyRows[1].provider,'twelve-data');

const signalFrameMinutes={ '1m':1,'5m':5,'15m':15,'30m':30,'60m':60,'240m':240,'1d':1440 };
for (const [tf,minutes] of Object.entries(signalFrameMinutes)) {
  const duration=minutes*60_000;
  const openAt=Date.UTC(2026,7,28,0,0);
  const bars=[
    {t:openAt-duration,o:1,h:2,l:1,c:2,v:1},
    {t:openAt,o:2,h:3,l:2,c:3,v:1}
  ];
  assert.deepEqual(
    closedBarsOnly(bars,tf,openAt+duration-1).map(bar=>bar.t),
    [bars[0].t],
    `${tf}: the current open candle must be excluded from signal inputs`
  );
  assert.equal(
    closedBarsOnly(bars,tf,openAt+duration).length,
    2,
    `${tf}: a candle must become eligible exactly at its close time`
  );
}

const bullish = classifyNewsArticle({
  title: 'Gold rises as Federal Reserve cuts rates and dollar falls',
  url: 'https://www.reuters.com/markets/example-bullish',
  seendate: seen
}, now);
assert.equal(bullish.direction, 'bullish');
assert.equal(bullish.importance, 3);
assert.ok(bullish.confidence >= 65);

const bearish = classifyNewsArticle({
  title: 'Gold falls as Treasury yields rise and dollar strengthens',
  url: 'https://www.reuters.com/markets/example-bearish',
  seendate: seen
}, now);
assert.equal(bearish.direction, 'bearish');
assert.ok(bearish.confidence >= 65);

const brief = buildNewsBrief([{
  title: 'Gold rises as Federal Reserve cuts rates and dollar falls',
  url: 'https://www.reuters.com/markets/example-bullish',
  seendate: seen
}], now);
assert.equal(brief.goldBias.direction, 'bullish');
assert.equal(brief.safety.blockTechnicalSignal, false,'one headline must never block trading');
assert.equal(brief.safety.blockUntil, null);

const untrusted = classifyNewsArticle({
  title: 'Gold surges as war and sanctions trigger safe haven demand',
  url: 'https://example.invalid/speculative-gold-story',
  seendate: seen
}, now);
assert.equal(untrusted.trusted, false);
assert.ok(untrusted.importance < 3, 'untrusted headlines must not become critical');
assert.ok(untrusted.confidence <= 64, 'untrusted confidence must be capped');
const untrustedBrief = buildNewsBrief([{
  title: 'Gold surges as war and sanctions trigger safe haven demand',
  url: 'https://example.invalid/speculative-gold-story',
  seendate: seen
}], now);
assert.equal(untrustedBrief.safety.blockTechnicalSignal, false, 'untrusted news must not block a technical signal');

const persistedBars = [];
const storageValues = new Map();
const fakeStorage = {
  get: async keys => Array.isArray(keys)
    ? new Map(keys.filter(key=>storageValues.has(key)).map(key=>[key,storageValues.get(key)]))
    : storageValues.get(keys),
  getAlarm: async () => null,
  setAlarm: async () => {},
  put: async values => Object.entries(values).forEach(([key,value])=>storageValues.set(key,value)),
  transaction: async callback => callback({
    get:async key=>storageValues.get(key),
    put:async (key,value)=>storageValues.set(key,value)
  })
};
const fakeCtx = {
  storage: fakeStorage,
  blockConcurrencyWhile(fn) { this.ready = fn(); },
  getWebSockets: () => []
};
const fakeDb = {
  exec: async () => {},
  batch: async statements => {
    for (const statement of statements) {
      if (statement.__sql.includes('INSERT INTO bars_v2') && statement.__values[0] === 1) {
        persistedBars.push(statement.__values.slice(1));
      }
    }
  },
  prepare(sql) {
    return {
      all: async () => ({ results:[{ name:'provider' }] }),
      bind: (...values) => ({ __sql:sql, __values:values, run: async () => {} })
    };
  }
};
const feed = new GoldFeed(fakeCtx, { GSX_DB:fakeDb });
await fakeCtx.ready;
await feed.handleProviderMessage(JSON.stringify({ event:'price', symbol:'XAU/USD', price:4600, timestamp:Math.floor(Date.UTC(2026,7,27,9,0,10)/1000) }));
await feed.handleProviderMessage(JSON.stringify({ event:'price', symbol:'XAU/USD', price:4602, timestamp:Math.floor(Date.UTC(2026,7,27,9,0,40)/1000) }));
await feed.handleProviderMessage(JSON.stringify({ event:'price', symbol:'XAU/USD', price:4601, timestamp:Math.floor(Date.UTC(2026,7,27,9,1,1)/1000) }));
assert.equal(persistedBars.length, 1, 'one completed minute must be persisted');
assert.deepEqual(persistedBars[0].slice(1,5), [4600,4602,4600,4602], 'OHLC must be built from the shared live stream');

const tickNow=Date.now();
const recentTicks=[
  {ts:tickNow-20_000,price:4700.1},
  {ts:tickNow-10_000,price:4700.4},
  {ts:tickNow-5_000,price:4700.2}
];
feed.tickHistory=[...recentTicks];
feed.lastSnapshotWriteAt=0;
const providerMinuteTs=Math.floor(tickNow/60_000)*60_000;
const receivedBefore=Date.now();
await feed.handleProviderMessage(JSON.stringify({
  event:'price',symbol:'XAU/USD',price:4700.3,timestamp:providerMinuteTs/1000
}));
const receivedTick=feed.tickHistory.at(-1);
assert.ok(receivedTick.ts>=receivedBefore&&receivedTick.ts<=Date.now(),'tick ordering must use actual Worker receipt time');
assert.notEqual(receivedTick.ts,providerMinuteTs,'provider minute timestamps must not replace actual tick receipt time');
const signalCreatedAt=tickNow-15_000;
const tickWindow=await feed.ticks({from:tickNow-25_000,to:Date.now(),limit:20});
assert.equal(tickWindow.ok,true);
assert.ok(tickWindow.ticks.length>1,'tick history must retain more than only the latest tick');
assert.ok(tickWindow.ticks.every(tick=>Number.isFinite(tick.ts)&&Number.isFinite(tick.price)),'every tick must include timestamp and price');
assert.ok(tickWindow.ticks.some(tick=>tick.ts<signalCreatedAt),'ticks before signal.createdAt must be retrievable');
assert.ok(tickWindow.ticks.some(tick=>tick.ts>=signalCreatedAt),'ticks at or after signal.createdAt must be retrievable');
assert.ok(storageValues.get('tickHistory')?.length>1,'batched Durable Object snapshot must persist tick history');
assert.equal(pruneTickHistory([
  {ts:tickNow-60*60_000-1,price:1},
  {ts:tickNow-1,price:2}
],tickNow).length,1,'ticks older than the retention window must be pruned');
assert.equal(pruneTickHistory(Array.from({length:2405},(_,index)=>({
  ts:tickNow-2405+index,price:4700+index/1000
})),tickNow).length,2400,'tick history must be capped');

const ticksResponse=await worker.default.fetch(new Request(
  `https://example.com/ticks?from=${tickNow-25_000}&to=${tickNow}&limit=20`
),{
  TWELVE_DATA_API_KEY:'configured',
  GOLD_FEED:{getByName:()=>({ticks:options=>feed.ticks(options)})}
},{});
assert.equal(ticksResponse.status,200);
const ticksBody=await ticksResponse.json();
assert.ok(ticksBody.count>1,'the read endpoint must return historical ticks');
assert.equal(ticksBody.retentionMs,60*60_000);
assert.equal(ticksBody.maxTicks,2400);

const exposureNow=Date.UTC(2026,7,31,14,0,0);
const exposureSignal=(id,tf,side,conf,status='')=>({
  id,tf,side,conf,status,signalBarTs:exposureNow-60_000,createdAt:exposureNow-1_000
});
const simultaneous=decideGoldExposure(null,{
  now:exposureNow,officialSignals:[],candidates:[
    exposureSignal('5m-high','5m','sell',90),
    exposureSignal('15m-low','15m','sell',85),
    exposureSignal('30m-opposite','30m','buy',84)
  ]
});
assert.equal(simultaneous.decisions.filter(item=>item.decision==='accepted').length,1,'one cycle must accept only one gold exposure');
assert.equal(simultaneous.decisions.find(item=>item.signalId==='5m-high').decision,'accepted','highest confidence must win');
assert.equal(simultaneous.decisions.find(item=>item.signalId==='15m-low').decision,'confirmation','same direction must become a confirmation');
assert.equal(simultaneous.decisions.find(item=>item.signalId==='30m-opposite').decision,'blocked_opposite','opposite direction must be blocked');

const tied=decideGoldExposure(null,{
  now:exposureNow,officialSignals:[],candidates:[
    exposureSignal('15m-tie','15m','buy',88),
    exposureSignal('60m-tie','60m','buy',88)
  ]
});
assert.equal(tied.decisions.find(item=>item.decision==='accepted').signalId,'60m-tie','higher timeframe must win a confidence tie');

const primary=exposureSignal('5m-primary','5m','sell',88,'active');
const activeState=decideGoldExposure(null,{now:exposureNow,officialSignals:[primary],candidates:[]}).state;
const sameDirection=decideGoldExposure(activeState,{
  now:exposureNow+1_000,officialSignals:[primary],candidates:[exposureSignal('15m-confirm','15m','sell',80)]
});
assert.equal(sameDirection.decisions[0].decision,'confirmation');
assert.equal(sameDirection.state.primarySignalId,primary.id);
const oppositeDirection=decideGoldExposure(activeState,{
  now:exposureNow+1_000,officialSignals:[primary],candidates:[exposureSignal('30m-block','30m','buy',90)]
});
assert.equal(oppositeDirection.decisions[0].decision,'blocked_opposite');
assert.equal(oppositeDirection.state.primarySignalId,primary.id);

const afterTp1=decideGoldExposure(activeState,{
  now:exposureNow+2_000,officialSignals:[{...primary,status:'tp1'}],candidates:[exposureSignal('15m-after-tp1','15m','sell',90)]
});
assert.equal(afterTp1.state.status,'active','TP1 must keep the exposure active');
assert.equal(afterTp1.decisions[0].decision,'confirmation','TP1 must not allow another position');
for (const closedStatus of ['tp2','expired']) {
  const afterClose=decideGoldExposure(activeState,{
    now:exposureNow+3_000,officialSignals:[{...primary,status:closedStatus}],
    candidates:[exposureSignal(`${closedStatus}-new`,'15m','buy',80)]
  });
  assert.equal(afterClose.decisions[0].decision,'accepted',`${closedStatus} must release the exposure`);
  assert.equal(afterClose.state.primarySignalId,`${closedStatus}-new`);
}

const afterSl=decideGoldExposure(activeState,{
  now:exposureNow+4_000,officialSignals:[{...primary,status:'stopped'}],
  candidates:[exposureSignal('sl-blocked','15m','buy',90)]
});
assert.equal(afterSl.state.status,'cooldown');
assert.equal(afterSl.state.cooldownUntil,exposureNow+4_000+30*60_000);
assert.equal(afterSl.decisions[0].decision,'blocked_cooldown','SL must start a global cooldown');
const afterCooldown=decideGoldExposure(afterSl.state,{
  now:afterSl.state.cooldownUntil+1,officialSignals:[],candidates:[exposureSignal('post-cooldown','15m','buy',80)]
});
assert.equal(afterCooldown.decisions[0].decision,'accepted','a signal after the 30-minute cooldown may be accepted');

const oneMinuteOnly=decideGoldExposure(null,{
  now:exposureNow,officialSignals:[],candidates:[exposureSignal('1m-info','1m','sell',99)]
});
assert.equal(oneMinuteOnly.state.status,'flat');
assert.equal(oneMinuteOnly.decisions[0].decision,'informational','1m must never open an exposure');
const oneMinuteConfirmation=decideGoldExposure(activeState,{
  now:exposureNow+2_000,officialSignals:[primary],candidates:[exposureSignal('1m-confirm','1m','sell',99)]
});
assert.equal(oneMinuteConfirmation.decisions[0].decision,'confirmation','1m may only confirm an active exposure');
assert.equal(oneMinuteConfirmation.state.primarySignalId,primary.id);

const bootstrapExposure=decideGoldExposure(null,{
  now:exposureNow,officialSignals:[
    exposureSignal('legacy-5m','5m','sell',88,'tp1'),
    exposureSignal('legacy-15m','15m','sell',72,'active'),
    exposureSignal('legacy-30m','30m','sell',87,'active')
  ],candidates:[]
});
assert.equal(bootstrapExposure.state.primarySignalId,'legacy-5m','existing correlated signals must bootstrap into one primary exposure');
assert.equal(bootstrapExposure.state.confirmations.length,2,'other existing same-direction signals must become linked confirmations');

function makeTransactionalExposureStorage() {
  const values=new Map();
  let tail=Promise.resolve();
  return {
    values,
    get:async keys=>Array.isArray(keys)
      ? new Map(keys.filter(key=>values.has(key)).map(key=>[key,values.get(key)]))
      : values.get(keys),
    put:async (keyOrValues,value)=>{
      if (typeof keyOrValues==='string') values.set(keyOrValues,value);
      else Object.entries(keyOrValues||{}).forEach(([key,item])=>values.set(key,item));
    },
    list:async ({prefix='',limit=1000}={})=>new Map(
      [...values.entries()].filter(([key])=>key.startsWith(prefix)).slice(0,limit)
    ),
    getAlarm:async()=>null,setAlarm:async()=>{},
    transaction(callback) {
      const run=tail.then(()=>callback({
        get:async key=>values.get(key),
        put:async (key,value)=>values.set(key,value)
      }));
      tail=run.catch(()=>{});
      return run;
    }
  };
}
const atomicStorage=makeTransactionalExposureStorage();
const atomicCtx={storage:atomicStorage,blockConcurrencyWhile(fn){this.ready=fn();},getWebSockets:()=>[]};
const atomicFeed=new GoldFeed(atomicCtx,{});
await atomicCtx.ready;
const concurrentResults=await Promise.all([
  atomicFeed.manageGoldExposure({now:exposureNow,officialSignals:[],candidates:[exposureSignal('concurrent-5m','5m','sell',88)]}),
  atomicFeed.manageGoldExposure({now:exposureNow,officialSignals:[],candidates:[exposureSignal('concurrent-15m','15m','sell',87)]})
]);
assert.equal(
  concurrentResults.flatMap(result=>result.decisions).filter(item=>item.decision==='accepted').length,
  1,
  'two concurrent Durable Object admissions must never accept more than one exposure'
);
assert.equal((await atomicFeed.goldExposureStatus()).status,'active');

const allowedOrigin='https://skyeagle123.github.io';
const writeToken='unit-test-write-token';
let unauthorizedWrites=0;
const rejectWritesKv={
  get:async()=>null,
  put:async()=>{ unauthorizedWrites+=1; throw new Error('unauthorized mutation'); }
};
const unauthorizedWriteCases=[
  {path:'/import.csv?tf=1m',body:'time,o,h,l,c,v\n2026-08-28T00:00:00Z,1,2,1,2,1',contentType:'text/csv'},
  {path:'/notify',body:JSON.stringify({tf:'1m',signalId:'not-authorized'}),contentType:'application/json'},
  {path:'/decision',body:JSON.stringify({decision:'not-authorized'}),contentType:'application/json'}
];
for (const testCase of unauthorizedWriteCases) {
  const response=await worker.default.fetch(new Request(`https://example.com${testCase.path}`,{
    method:'POST',
    headers:{Origin:allowedOrigin,'content-type':testCase.contentType},
    body:testCase.body
  }),{
    ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),
    GSX_WRITE_TOKEN:writeToken,
    GSX_KV:rejectWritesKv,
    GSX_DB:fakeDb
  },{});
  assert.equal(response.status,401,`${testCase.path}: missing write token must be rejected`);
  assert.equal((await response.json()).error,'unauthorized');
}
assert.equal(unauthorizedWrites,0,'rejected write requests must not touch KV, D1, or Telegram');

const wrongTokenResponse=await worker.default.fetch(new Request('https://example.com/decision',{
  method:'POST',
  headers:{Origin:allowedOrigin,'content-type':'application/json','x-gsx-write-token':'wrong-token'},
  body:'{}'
}),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_WRITE_TOKEN:writeToken,GSX_KV:rejectWritesKv
},{});
assert.equal(wrongTokenResponse.status,401,'an incorrect write token must be rejected');
assert.equal(unauthorizedWrites,0,'an incorrect token must be rejected before rate-limit storage');

const missingSecretResponse=await worker.default.fetch(new Request('https://example.com/decision',{
  method:'POST',headers:{Origin:allowedOrigin,'content-type':'application/json'},body:'{}'
}),{ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_KV:rejectWritesKv},{});
assert.equal(missingSecretResponse.status,503,'write endpoints must fail closed when the secret is absent');
assert.equal((await missingSecretResponse.json()).error,'write_auth_not_configured');

const authorizedStore=new Map();
const authorizedKv={
  get:async key=>authorizedStore.get(key)??null,
  put:async (key,value)=>authorizedStore.set(key,value)
};
const authorizedDecision=await worker.default.fetch(new Request('https://example.com/decision',{
  method:'POST',
  headers:{Origin:allowedOrigin,'content-type':'application/json','x-gsx-write-token':writeToken},
  body:JSON.stringify({decision:'authorized-test'})
}),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_WRITE_TOKEN:writeToken,GSX_KV:authorizedKv
},{});
assert.equal(authorizedDecision.status,200,'an authorized decision write must succeed');
assert.ok([...authorizedStore.keys()].some(key=>key.startsWith('dec:')),'authorized decision must be persisted');

const authorizedImport=await worker.default.fetch(new Request('https://example.com/import.csv?tf=1m',{
  method:'POST',
  headers:{Origin:allowedOrigin,'content-type':'text/csv','x-gsx-write-token':writeToken},
  body:'time,o,h,l,c,v\n2026-08-28T00:00:00Z,4600,4602,4599,4601,3'
}),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_WRITE_TOKEN:writeToken,GSX_KV:authorizedKv,GSX_DB:fakeDb
},{});
assert.equal(authorizedImport.status,200,'an authorized CSV import must succeed');
assert.equal((await authorizedImport.json()).imported,1);

function makeTrend(direction,count,stepMs,end) {
  const rows=[];
  let price=direction==='up'?2400:2600;
  for (let i=0;i<count;i++) {
    const o=price,c=direction==='up'?o+0.8:o-0.8;
    rows.push({t:end-(count-1-i)*stepMs,o,h:Math.max(o,c)+0.1,l:Math.min(o,c)-0.1,c,v:10});
    price=c+(direction==='up'?0.12:-0.12);
  }
  return rows;
}
const realDateNow=Date.now;
const fixedNow=Date.UTC(2026,7,27,16,0,0);
Date.now=()=>fixedNow;
const signalBars=makeTrend('up',3000,60000,fixedNow-120000);
const signalQuality=evaluateCandleQuality(signalBars,'1m');
assert.equal(signalQuality.ok,true);
const serverSignal=computeServerSignal(signalBars,{
  tf:'1m',
  mtf:[
    {tf:'5m',bars:makeTrend('up',600,300000,fixedNow-600000)},
    {tf:'15m',bars:makeTrend('up',240,900000,fixedNow-1800000)}
  ],
  live:{price:signalBars.at(-1).c,ts:fixedNow-1000,receivedAt:fixedNow-1000,source:'d1'},
  barsSource:'d1',
  dataQuality:signalQuality,
  news:{ok:true,stale:false,goldBias:{direction:'neutral',confidence:0},safety:{blockTechnicalSignal:false}}
});
const outsideNyNow=Date.UTC(2026,7,27,6,0,0);
Date.now=()=>outsideNyNow;
const outsideNyBars=makeTrend('up',3000,60000,outsideNyNow-120000);
const outsideNyOptions={
  tf:'1m',
  mtf:[
    {tf:'5m',bars:makeTrend('up',600,300000,outsideNyNow-600000)},
    {tf:'15m',bars:makeTrend('up',240,900000,outsideNyNow-1800000)}
  ],
  live:{price:outsideNyBars.at(-1).c,ts:outsideNyNow-1000,receivedAt:outsideNyNow-1000,source:'d1'},
  barsSource:'d1',
  dataQuality:evaluateCandleQuality(outsideNyBars,'1m'),
  news:{ok:true,stale:false,goldBias:{direction:'neutral',confidence:0},safety:{blockTechnicalSignal:false}}
};
const nyFilterOn=computeServerSignal(outsideNyBars,{
  ...outsideNyOptions,filters:{nyFilterOn:true,pivotFilterOn:false}
});
assert.equal(nyFilterOn.side,'none','New York filter ON must block an otherwise valid signal outside the configured window');
assert.ok(nyFilterOn.reasons.includes('خارج جلسة نيويورك'));
const nyFilterOff=computeServerSignal(outsideNyBars,{
  ...outsideNyOptions,filters:{nyFilterOn:false,pivotFilterOn:false}
});
assert.equal(nyFilterOff.side,'buy','New York filter OFF must allow an otherwise valid signal outside the configured window');
assert.ok(!nyFilterOff.reasons.includes('خارج جلسة نيويورك'));

function makeNearPivotTrend(count,end) {
  return Array.from({length:count},(_,index)=>{
    const recent=index>=count-45;
    const o=recent?2390+(index-(count-45))*0.8:2400;
    const c=recent?o+0.7:2400;
    return {t:end-(count-1-index)*60000,o,h:Math.max(o,c)+0.1,l:Math.min(o,c)-0.1,c,v:10};
  });
}
const pivotBars=makeNearPivotTrend(3000,outsideNyNow-120000);
const pivotOptions={
  ...outsideNyOptions,
  live:{...outsideNyOptions.live,price:pivotBars.at(-1).c},
  dataQuality:evaluateCandleQuality(pivotBars,'1m'),
  filters:{nyFilterOn:false,pivotFilterOn:false,pivotDistance:50}
};
const pivotFilterOff=computeServerSignal(pivotBars,pivotOptions);
assert.equal(pivotFilterOff.side,'buy','Pivot filter OFF must not veto an otherwise valid signal');
const pivotFilterOn=computeServerSignal(pivotBars,{
  ...pivotOptions,filters:{nyFilterOn:false,pivotFilterOn:true,pivotDistance:50}
});
assert.equal(pivotFilterOn.side,'none','Pivot filter ON must use the configured distance');
assert.ok(pivotFilterOn.reasons.some(reason=>reason.startsWith('السعر قريب من')));
Date.now=realDateNow;
assert.equal(serverSignal.side,'buy','server signal engine must reproduce a strong confirmed buy');
assert.ok(serverSignal.conf>=60);

const parityAt=Date.UTC(2026,7,27,16,0,0);
const parityFrames={
  '1m':makeTrend('up',120,60_000,parityAt-60_000),
  '5m':makeTrend('up',80,300_000,parityAt-300_000),
  '15m':makeTrend('up',60,900_000,parityAt-900_000)
};
const parityFilters={nyFilterOn:false,pivotFilterOn:false};
const parityNews={ok:true,stale:false,goldBias:{direction:'neutral',confidence:0},safety:{blockTechnicalSignal:false}};
const directParity=computeServerSignal(parityFrames['1m'],{
  tf:'1m',
  mtf:[{tf:'5m',bars:parityFrames['5m']},{tf:'15m',bars:parityFrames['15m']}],
  live:{price:parityFrames['1m'].at(-1).c,ts:parityAt,receivedAt:parityAt,source:'backtest'},
  barsSource:'backtest',dataQuality:evaluateCandleQuality(parityFrames['1m'],'1m'),
  filters:parityFilters,news:parityNews,evaluationAt:parityAt
});
assert.equal(directParity.side,'buy','the fixed parity dataset must produce a server signal');

const backtestParity=runServerBacktest({
  tf:'1m',frames:{
    ...parityFrames,
    '1m':[...parityFrames['1m'],{t:parityAt,o:9000,h:9001,l:8990,c:8991,v:1}]
  },
  filters:parityFilters,news:parityNews,startAt:parityAt,endAt:parityAt
});
assert.equal(backtestParity.mode,'simulation');
assert.equal(backtestParity.engine,'computeServerSignal');
assert.equal(backtestParity.trades.length,1);
const simulatedSignal=backtestParity.trades[0].signal;
assert.deepEqual(
  {
    side:simulatedSignal.side,entry:simulatedSignal.entry,tp1:simulatedSignal.tp1,
    tp2:simulatedSignal.tp2,sl:simulatedSignal.sl,createdAt:simulatedSignal.createdAt,
    signalBarTs:simulatedSignal.signalBarTs,score:simulatedSignal.score,
    bull:simulatedSignal.bull,bear:simulatedSignal.bear,mtf:simulatedSignal.mtf,
    regime:simulatedSignal.regime,atr:simulatedSignal.atr,
    lastClose:simulatedSignal.lastClose,livePrice:simulatedSignal.livePrice,
    conf:simulatedSignal.conf,reasons:simulatedSignal.reasons
  },
  {
    side:directParity.side,entry:directParity.entry,tp1:directParity.tp1,
    tp2:directParity.tp2,sl:directParity.sl,createdAt:parityAt,
    signalBarTs:directParity.lastTs,score:directParity.score,
    bull:directParity.bull,bear:directParity.bear,mtf:directParity.mtf,
    regime:directParity.regime,atr:directParity.atr,
    lastClose:directParity.lastClose,livePrice:directParity.livePrice,
    conf:directParity.conf,reasons:directParity.reasons
  },
  'backtest and server must return the same decision and levels for identical closed candles and settings'
);
assert.equal(
  simulatedSignal.entry,parityFrames['1m'].at(-1).c,
  'a still-forming candle must not affect the backtest entry'
);

Date.now=()=>parityAt;
const defaultAsOfBacktest=runServerBacktest({
  tf:'1m',frames:{
    ...parityFrames,
    '1m':[...parityFrames['1m'],{t:parityAt,o:9000,h:9001,l:8990,c:8991,v:1}]
  },
  filters:parityFilters,news:parityNews,startAt:parityAt
});
Date.now=realDateNow;
assert.equal(defaultAsOfBacktest.trades.length,1);
assert.equal(
  defaultAsOfBacktest.trades[0].signal.entry,parityFrames['1m'].at(-1).c,
  'the default backtest as-of time must exclude a currently forming candle'
);

const backtestResponse=await worker.default.fetch(new Request('https://example.com/backtest',{
  method:'POST',headers:{Origin:allowedOrigin,'content-type':'application/json'},
  body:JSON.stringify({
    tf:'1m',frames:parityFrames,
    filters:{nyFilterOn:true,pivotFilterOn:true,pivotDistance:50},
    news:{ok:true,safety:{blockTechnicalSignal:true}},
    startAt:parityAt,endAt:parityAt
  })
}),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),
  GSX_KV:{
    async get(key){
      assert.equal(key,'system:signal-filters');
      return parityFilters;
    },
    async put(){throw new Error('backtest_must_not_write_kv');},
    async delete(){throw new Error('backtest_must_not_delete_kv');}
  },
  get GSX_DB(){throw new Error('backtest_must_not_read_or_write_d1');},
  get GOLD_FEED(){throw new Error('backtest_must_not_touch_exposure_or_ticks');}
},{});
assert.equal(backtestResponse.status,200,'the read-only backtest endpoint must succeed without production bindings');
const backtestPayload=await backtestResponse.json();
assert.equal(backtestPayload.settingsSource,'official-server');
assert.equal(backtestPayload.newsMode,'disabled-historical');
assert.deepEqual(backtestPayload.filters,normalizeSignalFilters(parityFilters));
assert.equal(backtestPayload.trades[0].signal.entry,directParity.entry);
assert.equal(backtestPayload.trades[0].signal.tp1,directParity.tp1);
assert.equal(backtestPayload.trades[0].signal.tp2,directParity.tp2);
assert.equal(backtestPayload.trades[0].signal.sl,directParity.sl);
assert.equal(backtestPayload.trades[0].signal.score,directParity.score);
assert.equal(backtestPayload.trades[0].signal.conf,directParity.conf);

const lifecycleSignal={
  id:'1m:test:buy',tf:'1m',side:'buy',entry:100,tp1:101,tp2:102,sl:99,
  signalBarTs:fixedNow-4*60000,lastProcessedBarTs:fixedNow-4*60000,
  createdAt:fixedNow-4*60000,updatedAt:fixedNow-4*60000,status:'active',tp1Hit:false,lastPrice:100,
  conf:80,reasons:['authorized test signal']
};
const lifecycle=updateSignalLifecycleAcrossBars(lifecycleSignal,[
  {t:fixedNow-3*60000,o:100,h:101.2,l:99.5,c:100.8},
  {t:fixedNow-2*60000,o:100.8,h:100.9,l:100.2,c:100.4}
],100.5,fixedNow);
assert.equal(lifecycle.signal.status,'tp1','an earlier minute TP touch must not be lost when the latest minute no longer touches it');
assert.deepEqual(lifecycle.events.map(item=>item.event),['tp1']);
assert.equal(lifecycle.signal.lastProcessedBarTs,fixedNow-2*60000);

const issuedMidMinute={
  ...lifecycleSignal,
  createdAt:fixedNow-150000,
  updatedAt:fixedNow-150000
};
const ignoresPreSignalTouches=updateSignalLifecycleAcrossBars(issuedMidMinute,[
  {t:fixedNow-3*60000,o:100,h:102.5,l:98.5,c:100.4},
  {t:fixedNow-2*60000,o:100.4,h:100.8,l:99.4,c:100.5}
],100.5,fixedNow);
assert.equal(ignoresPreSignalTouches.signal.status,'active','TP/SL touches in a minute that began before signal issuance must be ignored');
assert.deepEqual(ignoresPreSignalTouches.events,[]);

const tracksPostSignalBar=updateSignalLifecycleAcrossBars(issuedMidMinute,[
  {t:fixedNow-3*60000,o:100,h:102.5,l:98.5,c:100.4},
  {t:fixedNow-2*60000,o:100.4,h:101.2,l:99.4,c:101}
],100.5,fixedNow);
assert.equal(tracksPostSignalBar.signal.status,'tp1','a TP touch in a minute that began after signal issuance must be counted');
assert.deepEqual(tracksPostSignalBar.events.map(item=>item.event),['tp1']);

const tracksPostSignalLivePrice=updateSignalLifecycleAcrossBars(issuedMidMinute,[
  {t:fixedNow-3*60000,o:100,h:102.5,l:98.5,c:100.4}
],102.1,fixedNow);
assert.equal(tracksPostSignalLivePrice.signal.status,'tp2','a live-price touch observed after signal issuance must be counted');

const issuedAtMinuteOpen={...lifecycleSignal,createdAt:fixedNow-2*60000,updatedAt:fixedNow-2*60000};
const exactBoundary=updateSignalLifecycleAcrossBars(issuedAtMinuteOpen,[
  {t:fixedNow-2*60000,o:100,h:101.2,l:99.5,c:101}
],100.5,fixedNow);
assert.equal(exactBoundary.signal.status,'tp1','a candle beginning exactly at signal issuance must be eligible');

const ambiguous=updateSignalLifecycleAcrossBars(lifecycleSignal,[
  {t:fixedNow-3*60000,o:100,h:102.5,l:98.5,c:101}
],101,fixedNow);
assert.equal(ambiguous.signal.status,'stopped','SL must win conservatively when one minute touches target and stop');

assert.equal((await sendTelegramText({},'test')).error,'telegram_not_configured');

function makeTelegramKv() {
  const store=new Map();
  return {
    store,
    get:async (key,type)=>{
      const value=store.get(key);
      return type==='json'&&typeof value==='string'?JSON.parse(value):value??null;
    },
    put:async (key,value)=>store.set(key,value),
    list:async ({prefix})=>({
      keys:[...store.keys()].filter(key=>key.startsWith(prefix)).map(name=>({name})),
      list_complete:true
    })
  };
}

async function makeTelegramHarness() {
  const storage=makeTransactionalExposureStorage();
  const kv=makeTelegramKv();
  const doEnv={GSX_KV:kv,TELEGRAM_TOKEN:'valid',TELEGRAM_CHAT:'chat'};
  const ctx={storage,blockConcurrencyWhile(fn){this.ready=fn();},getWebSockets:()=>[]};
  const feed=new GoldFeed(ctx,doEnv);
  await ctx.ready;
  const binding={getByName:()=>feed};
  return {storage,kv,feed,env:{...doEnv,GOLD_FEED:binding},binding};
}

let telegramNow=fixedNow;
Date.now=()=>telegramNow;
let telegramCalls=[];
globalThis.fetch=async (_url,init)=>{
  telegramCalls.push(JSON.parse(init.body));
  return {ok:true,status:200,json:async()=>({ok:true,result:{message_id:telegramCalls.length}})};
};

const telegramHarness=await makeTelegramHarness();
const officialSignal={...lifecycleSignal,id:'5m:official:buy',tf:'5m',createdAt:telegramNow-1_000,updatedAt:telegramNow-1_000};
const duplicateResults=await Promise.all([
  queueTelegramDelivery(telegramHarness.env,officialSignal,'created','new official signal'),
  queueTelegramDelivery(telegramHarness.env,officialSignal,'created','new official signal')
]);
assert.equal(telegramCalls.length,1,'duplicate official event calls must produce one Telegram send');
assert.equal(duplicateResults[0].eventId,duplicateResults[1].eventId,'eventId must be stable');
assert.equal(duplicateResults[0].status,'sent');

telegramNow+=1_000;
const confirmation={...officialSignal,id:'1m:confirmation:buy',tf:'1m',createdAt:telegramNow,updatedAt:telegramNow,conf:91};
const confirmationDelivery=await queueTelegramDelivery(
  telegramHarness.env,confirmation,'confirmation',signalTelegramText(confirmation,'confirmation',{
    primaryTf:officialSignal.tf,signalCreatedAt:officialSignal.createdAt,eventAt:telegramNow
  }),{rootSignalId:officialSignal.id,signalCreatedAt:officialSignal.createdAt,eventAt:telegramNow}
);
assert.equal(confirmationDelivery.status,'sent');
assert.match(confirmationDelivery.eventId,/confirmation/);
assert.match(telegramCalls.at(-1).text,/ليست صفقة جديدة|ليس صفقة جديدة/,'confirmation must be clearly distinguished from a new trade');
assert.match(telegramCalls.at(-1).text,/وقت الحدث:/,'confirmation must include the real eventAt');

telegramNow+=1_000;
const tp1Signal={...officialSignal,status:'tp1',tp1Hit:true,updatedAt:telegramNow,lastPrice:101};
const tp1Text=signalTelegramText(tp1Signal,'tp1');
const tp1Delivery=await queueTelegramDelivery(telegramHarness.env,tp1Signal,'tp1',tp1Text);
telegramNow+=1_000;
const tp2Signal={...tp1Signal,status:'tp2',updatedAt:telegramNow,closedAt:telegramNow,lastPrice:102};
const tp2Text=signalTelegramText(tp2Signal,'tp2');
const tp2Delivery=await queueTelegramDelivery(telegramHarness.env,tp2Signal,'tp2',tp2Text);
assert.equal(tp1Delivery.status,'sent');
assert.equal(tp2Delivery.status,'sent');
assert.match(telegramCalls.at(-2).text,/تحقق TP1/);
assert.match(telegramCalls.at(-1).text,/تحقق TP2/,'TP1 must be delivered before TP2');
assert.match(telegramCalls.at(-1).text,/وقت الحدث:/);
assert.match(telegramCalls.at(-1).text,/وقت إنشاء الإشارة:/);

const callsBeforeDelayed=telegramCalls.length;
const delayedConfirmation={...confirmation,id:'15m:delayed:buy',tf:'15m'};
const delayedDelivery=await queueTelegramDelivery(
  telegramHarness.env,delayedConfirmation,'confirmation','delayed confirmation',
  {rootSignalId:officialSignal.id,signalCreatedAt:officialSignal.createdAt,eventAt:telegramNow-500}
);
assert.equal(delayedDelivery.status,'dropped');
assert.equal(delayedDelivery.dropReason,'delayed_event');
assert.equal(telegramCalls.length,callsBeforeDelayed,'a delayed event must not be sent after a newer state');

const slHarness=await makeTelegramHarness();
telegramNow+=1_000;
const slPrimary={...officialSignal,id:'15m:official:sell',tf:'15m',side:'sell',createdAt:telegramNow,updatedAt:telegramNow};
await queueTelegramDelivery(slHarness.env,slPrimary,'created','SL trade created');
telegramNow+=1_000;
const slDelivery=await queueTelegramDelivery(
  slHarness.env,{...slPrimary,status:'stopped',closedAt:telegramNow,updatedAt:telegramNow},'stopped','SL'
);
assert.equal(slDelivery.status,'sent');
assert.equal(slDelivery.event,'sl');

const retryHarness=await makeTelegramHarness();
let retryCalls=0;
globalThis.fetch=async ()=>{
  retryCalls+=1;
  return retryCalls===1
    ? {ok:false,status:503,json:async()=>({ok:false,description:'temporarily unavailable'})}
    : {ok:true,status:200,json:async()=>({ok:true,result:{message_id:2}})};
};
telegramNow+=1_000;
const retrySignal={...officialSignal,id:'30m:retry:buy',tf:'30m',createdAt:telegramNow,updatedAt:telegramNow};
retryHarness.kv.store.set('signal:state:30m',JSON.stringify(retrySignal));
const retryStateBefore=retryHarness.kv.store.get('signal:state:30m');
const pendingDelivery=await queueTelegramDelivery(retryHarness.env,retrySignal,'created','retry once');
assert.equal(pendingDelivery.status,'pending');
assert.equal(pendingDelivery.attempts,1);
telegramNow+=60_001;
await retryHarness.feed.processTelegramEvents();
const retryKey=`telegram:event:v2:${pendingDelivery.eventId}`;
const sentDelivery=await retryHarness.storage.get(retryKey);
assert.equal(sentDelivery.status,'sent');
assert.equal(sentDelivery.attempts,2);
assert.equal(sentDelivery.eventAt,retrySignal.createdAt,'retry must preserve the real eventAt');
assert.ok(sentDelivery.sentAt>sentDelivery.eventAt,'Telegram send time must remain separate from eventAt');
await queueTelegramDelivery(retryHarness.env,retrySignal,'created','retry once');
assert.equal(retryCalls,2,'retry plus a repeated cycle must not duplicate a sent event');
assert.equal(retryHarness.kv.store.get('signal:state:30m'),retryStateBefore,'Telegram retry must not mutate signal state');

const staleHarness=await makeTelegramHarness();
let staleCalls=0;
globalThis.fetch=async ()=>{ staleCalls+=1; return {ok:true,status:200,json:async()=>({ok:true})}; };
const staleSignal={...officialSignal,id:'60m:stale:buy',tf:'60m',createdAt:telegramNow-16*60_000,updatedAt:telegramNow-16*60_000};
const staleDelivery=await queueTelegramDelivery(staleHarness.env,staleSignal,'created','old event');
assert.equal(staleDelivery.status,'dropped');
assert.equal(staleDelivery.dropReason,'stale_event');
assert.equal(staleCalls,0,'an old accumulated event must be dropped without a Telegram call');

const legacyKv=makeTelegramKv();
legacyKv.store.set('telegram:delivery:legacy-signal:created',JSON.stringify({
  signalId:'legacy-signal',event:'created',status:'pending',nextAttemptAt:0,text:'legacy message'
}));
await processTelegramOutbox({GSX_KV:legacyKv});
const droppedLegacy=JSON.parse(legacyKv.store.get('telegram:delivery:legacy-signal:created'));
assert.equal(droppedLegacy.status,'dropped');
assert.equal(droppedLegacy.dropReason,'legacy_pre_v2_event');
assert.equal(staleCalls,0,'a legacy pre-deploy outbox record must never be sent');

const restartHarness=await makeTelegramHarness();
const restartEventId='trade:240m:restart:buy:new_signal';
restartHarness.storage.values.set(`telegram:event:v2:${restartEventId}`,{
  schema:2,eventId:restartEventId,rootSignalId:'240m:restart:buy',signalId:'240m:restart:buy',
  tf:'240m',kind:'new_signal',event:'new_signal',eventAt:telegramNow,signalCreatedAt:telegramNow,
  queuedAt:telegramNow,createdAt:telegramNow,updatedAt:telegramNow,queuedVersion:'2026.09.01.3',
  status:'sending',attempts:0,nextAttemptAt:telegramNow,text:'ambiguous restart event'
});
const deploymentEventId='trade:1d:old-deploy:buy:new_signal';
restartHarness.storage.values.set(`telegram:event:v2:${deploymentEventId}`,{
  schema:2,eventId:deploymentEventId,rootSignalId:'1d:old-deploy:buy',signalId:'1d:old-deploy:buy',
  tf:'1d',kind:'new_signal',event:'new_signal',eventAt:telegramNow,signalCreatedAt:telegramNow,
  queuedAt:telegramNow,createdAt:telegramNow,updatedAt:telegramNow,queuedVersion:'2026.08.31.7',
  status:'pending',attempts:0,nextAttemptAt:telegramNow,text:'previous deployment event'
});
await restartHarness.feed.processTelegramEvents();
assert.equal((await restartHarness.storage.get(`telegram:event:v2:${restartEventId}`)).dropReason,'ambiguous_restart');
assert.equal((await restartHarness.storage.get(`telegram:event:v2:${deploymentEventId}`)).dropReason,'previous_deployment');
assert.equal(staleCalls,0,'restart/deploy cleanup must not send ambiguous or previous-version events');

const expiredHarness=await makeTelegramHarness();
globalThis.fetch=async ()=>({ok:true,status:200,json:async()=>({ok:true})});
telegramNow+=1_000;
const expiringSignal={...officialSignal,id:'60m:expired:sell',tf:'60m',side:'sell',createdAt:telegramNow,updatedAt:telegramNow};
await queueTelegramDelivery(expiredHarness.env,expiringSignal,'created','expired trade created');
telegramNow+=1_000;
const expiredDelivery=await queueTelegramDelivery(
  expiredHarness.env,{...expiringSignal,status:'expired',closedAt:telegramNow,updatedAt:telegramNow},'expired','Expired'
);
assert.equal(expiredDelivery.status,'sent');
assert.equal(expiredDelivery.event,'expired');

const notifyHarness=await makeTelegramHarness();
globalThis.fetch=async ()=>({ok:true,status:200,json:async()=>({ok:true})});
notifyHarness.kv.store.set('signal:state:1m',JSON.stringify(lifecycleSignal));
const authorizedNotify=await worker.default.fetch(new Request('https://example.com/notify',{
  method:'POST',
  headers:{Origin:allowedOrigin,'content-type':'application/json','x-gsx-write-token':writeToken},
  body:JSON.stringify({tf:'1m',signalId:lifecycleSignal.id})
}),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_WRITE_TOKEN:writeToken,
  ...notifyHarness.env
},{});
assert.equal(authorizedNotify.status,200,'an authorized official-signal notification must succeed');
assert.equal((await authorizedNotify.json()).ok,true);
Date.now=realDateNow;
globalThis.fetch=originalFetch;

const storedSignal={...serverSignal,id:'test-signal',createdAt:fixedNow,updatedAt:Date.now(),status:'active',tp1Hit:false,lastPrice:serverSignal.entry,signalBarTs:serverSignal.lastTs};
const snapshotResponse=await worker.default.fetch(new Request('https://example.com/signals?tf=1m'),{
  ALLOW_ORIGINS:'["https://skyeagle123.github.io"]',
  GSX_KV:{
    get:async (key,type)=>type==='json'&&key==='signal:state:1m'?storedSignal:null
  }
},{waitUntil:()=>{}});
assert.equal(snapshotResponse.status,200);
const snapshot=await snapshotResponse.json();
assert.equal(snapshot.signals[0].state.id,'test-signal');

const filterStore=new Map([
  ['system:signal-cycle',JSON.stringify({status:'running',startedAt:Date.now()})]
]);
const filterKv={
  get:async (key,type)=>{
    const value=filterStore.get(key);
    return type==='json'&&typeof value==='string'?JSON.parse(value):value??null;
  },
  put:async (key,value)=>filterStore.set(key,value)
};
const syncFiltersResponse=await worker.default.fetch(new Request(
  'https://example.com/signals?tf=5m&nyFilterOn=0&nyStart=08%3A00&nyEnd=17%3A00&pivotFilterOn=1&pivotDistance=1.25',
  {headers:{Origin:allowedOrigin}}
),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_KV:filterKv
},{waitUntil:()=>{}});
assert.equal(syncFiltersResponse.status,200,'an allowed PWA origin must be able to synchronize bounded signal filters');
const syncedPayload=await syncFiltersResponse.json();
assert.deepEqual(syncedPayload.filters,{
  nyFilterOn:false,nyStart:'08:00',nyEnd:'17:00',pivotFilterOn:true,pivotDistance:1.25
});
assert.deepEqual(await readSignalFilters({GSX_KV:filterKv}),syncedPayload.filters);

const beforeRejectedSync=filterStore.get('system:signal-filters');
const rejectedFilterSync=await worker.default.fetch(new Request(
  'https://example.com/signals?tf=5m&nyFilterOn=1&nyStart=08%3A00&nyEnd=17%3A00&pivotFilterOn=0&pivotDistance=0.7',
  {headers:{Origin:'https://attacker.example'}}
),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_KV:filterKv
},{waitUntil:()=>{}});
assert.equal(rejectedFilterSync.status,403,'a disallowed browser origin must not change signal filters');
assert.equal(filterStore.get('system:signal-filters'),beforeRejectedSync);

let aiCalls = 0;
const translated = await enrichNewsBriefArabic({
  AI: {
    run: async (_model, input) => {
      aiCalls += 1;
      assert.equal(input.response_format.type, 'json_object');
      return { response: JSON.stringify({ items: [{
        id: 0,
        titleAr: 'ارتفاع الذهب مع خفض الاحتياطي الفيدرالي للفائدة وتراجع الدولار',
        summaryAr: 'خفض الفائدة وضعف الدولار عاملان داعمان للذهب، مع انتظار تأكيد حركة السعر.'
      }] }) };
    }
  }
}, brief);
assert.equal(aiCalls, 1);
assert.equal(translated.arabicEnrichment, 'workers-ai');
assert.match(translated.items[0].titleAr, /الذهب/);
assert.match(translated.items[0].summaryAr, /داعم/);
assert.equal(translated.items[0].title, brief.items[0].title, 'original title must remain available');

const fallbackArabic = await enrichNewsBriefArabic({}, brief);
assert.equal(fallbackArabic.arabicEnrichment, 'fallback');
assert.equal(fallbackArabic.items[0].titleAr, '');
assert.equal(fallbackArabic.items[0].summaryAr, brief.items[0].reason);

const legacyBrief = { ...brief, updatedAt: Date.now() - 2 * 60 * 60 * 1000 };
let upgradedCache = null;
globalThis.fetch = async () => ({ ok: false, status: 503 });
const upgraded = await getGoldNewsBrief({
  GSX_KV: {
    get: async key => key === 'news:brief' ? legacyBrief : null,
    put: async (key, value) => { upgradedCache = { key, value: JSON.parse(value) }; }
  },
  AI: {
    run: async () => ({ response: JSON.stringify({ items: [{
      id: 0,
      titleAr: 'عنوان عربي من الكاش القديم',
      summaryAr: 'ملخص عربي محافظ للأثر المتوقع على الذهب.'
    }] }) })
  }
});
globalThis.fetch = originalFetch;
assert.equal(upgraded.ok, true);
assert.equal(upgraded.cache, 'legacy-upgraded');
assert.match(upgraded.items[0].titleAr, /عربي/);
assert.equal(upgradedCache.key, 'news:brief:v2');
assert.equal(upgradedCache.value.legacyCache, undefined);

let newsFetchCalls = 0;
globalThis.fetch = async url => {
  newsFetchCalls += 1;
  if (newsFetchCalls === 1) return { ok: true, json: async () => ({ articles: [] }) };
  assert.match(String(url), /query=gold(?:\+|%20)sourcelang/);
  return { ok: true, json: async () => ({ articles: [{
    title: 'Gold rises as the US dollar falls',
    url: 'https://www.reuters.com/markets/gold-fallback',
    seenAt: Date.now() - 5 * 60 * 1000
  }] }) };
};
const focusedFallback = await getGoldNewsBrief({
  AI: { run: async () => ({ response: JSON.stringify({ items: [] }) }) }
});
globalThis.fetch = originalFetch;
assert.equal(newsFetchCalls, 2);
assert.equal(focusedFallback.itemCount, 1);
assert.equal(focusedFallback.items[0].direction, 'bullish');

const stale = classifyNewsArticle({
  title: 'Gold rises as Federal Reserve cuts rates',
  url: 'https://www.reuters.com/markets/example-old',
  seenAt: now - 25 * 60 * 60 * 1000
}, now);
assert.equal(stale, null);

class MemoryD1Statement {
  constructor(database,sql,values=[]) { this.database=database;this.sql=sql;this.values=values; }
  bind(...values) { return new MemoryD1Statement(this.database,this.sql,values); }
  async all() { return {results:this.database.prepare(this.sql).all(...this.values)}; }
  async first() { return this.database.prepare(this.sql).get(...this.values)||null; }
  async run() {
    const result=this.database.prepare(this.sql).run(...this.values);
    return {success:true,meta:{changes:Number(result.changes||0)}};
  }
  runSync() {
    const result=this.database.prepare(this.sql).run(...this.values);
    return {success:true,meta:{changes:Number(result.changes||0)}};
  }
}

class MemoryD1 {
  constructor(database=new DatabaseSync(':memory:')) { this.database=database; }
  async exec(sql) { this.database.exec(sql);return {count:1}; }
  prepare(sql) { return new MemoryD1Statement(this.database,sql); }
  async batch(statements) {
    this.database.exec('BEGIN');
    try {
      const results=statements.map(statement=>statement.runSync());
      this.database.exec('COMMIT');
      return results;
    } catch (error) {
      this.database.exec('ROLLBACK');
      throw error;
    }
  }
}

const performanceDb=new MemoryD1();
await ensurePerformanceSchema({GSX_DB:performanceDb});
const performanceBase=Date.UTC(2026,8,1,8,0,0);
const productionSignal=(id,createdAt,overrides={})=>({
  id,tf:'5m',side:'buy',createdAt,signalBarTs:createdAt-300_000,
  entry:100,tp1:112.5,tp2:121,sl:90,conf:82,score:8.75,
  reasons:['official production reason'],origin:'server',status:'active',
  updatedAt:createdAt,lastPrice:100,...overrides
});

const winner=productionSignal('5m:performance:win',performanceBase);
await Promise.all([
  recordProductionPerformanceEvent({GSX_DB:performanceDb},winner,'created'),
  recordProductionPerformanceEvent({GSX_DB:performanceDb},winner,'created')
]);
await recordProductionPerformanceEvent({GSX_DB:performanceDb},{
  ...winner,status:'tp1',tp1Hit:true,updatedAt:performanceBase+60_000,lastPrice:113
},'tp1');
await recordProductionPerformanceEvent({GSX_DB:performanceDb},{
  ...winner,status:'tp2',tp1Hit:true,updatedAt:performanceBase+120_000,
  closedAt:performanceBase+120_000,lastPrice:122
},'tp2');
await recordProductionPerformanceEvent({GSX_DB:performanceDb},{
  ...winner,status:'tp1',tp1Hit:true,updatedAt:performanceBase+180_000,lastPrice:114
},'tp1');
await recordProductionPerformanceEvent({GSX_DB:performanceDb},{
  ...winner,status:'stopped',updatedAt:performanceBase+240_000,
  closedAt:performanceBase+240_000,lastPrice:89
},'stopped');

for (let index=1;index<=2;index++) {
  const createdAt=performanceBase+index*10*60_000;
  const loser=productionSignal(`15m:performance:loss:${index}`,createdAt,{tf:'15m',side:'sell'});
  await recordProductionPerformanceEvent({GSX_DB:performanceDb},loser,'created');
  await recordProductionPerformanceEvent({GSX_DB:performanceDb},{
    ...loser,status:'stopped',updatedAt:createdAt+60_000,closedAt:createdAt+60_000,lastPrice:91
  },'stopped');
}

const expiring=productionSignal('60m:performance:expired',performanceBase+30*60_000,{tf:'60m'});
await recordProductionPerformanceEvent({GSX_DB:performanceDb},expiring,'created');
await recordProductionPerformanceEvent({GSX_DB:performanceDb},{
  ...expiring,status:'expired',updatedAt:expiring.createdAt+60_000,
  closedAt:expiring.createdAt+60_000,lastPrice:105
},'expired');

assert.equal(signalResultR(winner,'tp2',122),2.1);
assert.equal(signalResultR(winner,'sl',89),-1);
assert.equal(signalResultR(expiring,'expired',105),0.5);
const performance=await readProductionPerformance({GSX_DB:performanceDb},new URLSearchParams('limit=2'));
assert.equal(performance.ok,true);
assert.equal(performance.source,'production-official-signals');
assert.equal(performance.simulation,false);
assert.equal(performance.records.length,2,'performance endpoint must paginate the production ledger');
assert.ok(performance.nextCursor,'a full performance page must expose a stable cursor');
assert.equal(performance.summary.signals,4);
assert.equal(performance.summary.wins,1);
assert.equal(performance.summary.losses,2);
assert.equal(performance.summary.expired,1);
assert.equal(performance.summary.netR,0.6);
assert.equal(performance.summary.averageR,0.15);
assert.equal(performance.summary.maxDrawdownR,2);
assert.equal(performance.summary.bestWinStreak,1);
assert.equal(performance.summary.worstLossStreak,2);
assert.equal(performance.summary.byTimeframe['15m'].losses,2);
assert.equal(performance.summary.byDirection.buy.wins,1);

const winnerRecord=(await readProductionPerformance(
  {GSX_DB:performanceDb},new URLSearchParams('tf=5m&limit=10')
)).records.find(record=>record.signalId===winner.id);
assert.deepEqual(winnerRecord.events.map(event=>event.type),['created','tp1','tp2']);
assert.equal(winnerRecord.finalStatus,'tp2','a later conflicting SL must not replace the first terminal event');
assert.equal(winnerRecord.tp1At,performanceBase+60_000,'a delayed duplicate TP1 must not rewrite the first event time');
assert.equal(winnerRecord.score,8.75);

const duplicateEventCount=performanceDb.database.prepare(
  'SELECT COUNT(*) AS count FROM production_signal_events WHERE signal_id=?'
).get(winner.id).count;
assert.equal(duplicateEventCount,3,'duplicate/retry/restart processing must not duplicate lifecycle events');
const restartedDb=new MemoryD1(performanceDb.database);
await recordProductionPerformanceEvent({GSX_DB:restartedDb},winner,'created');
assert.equal(performanceDb.database.prepare(
  'SELECT COUNT(*) AS count FROM production_signals WHERE signal_id=?'
).get(winner.id).count,1,'the same signal after restart/deploy must remain one trade');

assert.equal((await recordProductionPerformanceEvent(
  {GSX_DB:performanceDb},{...winner,id:'1m:informational',tf:'1m'},'created'
)).error,'official_production_signal_required','1m informational signals must not enter the official performance ledger');
assert.equal((await recordProductionPerformanceEvent(
  {GSX_DB:performanceDb},{...winner,id:'5m:backtest',origin:'backtest'},'created'
)).error,'official_production_signal_required','backtest signals must never enter production performance');
assert.equal((await recordProductionPerformanceSafely(
  {GSX_DB:{batch:async()=>{throw new Error('D1 unavailable');},prepare:performanceDb.prepare.bind(performanceDb)}},
  {...winner,id:'5m:storage-failure'},'created'
)).ok,false,'performance storage failure must be isolated from the signal lifecycle');

const performanceResponse=await worker.default.fetch(new Request('https://example.com/performance?limit=2'),{
  GSX_DB:performanceDb,ALLOW_ORIGINS:JSON.stringify([allowedOrigin])
},{});
assert.equal(performanceResponse.status,200);
const performancePayload=await performanceResponse.json();
assert.equal(performancePayload.records.length,2);
assert.equal(performancePayload.summary.netR,0.6);
assert.equal(performancePayload.storage,'D1');

const emptySummary=buildPerformanceSummary([]);
assert.equal(emptySummary.signals,0);
assert.equal(emptySummary.maxDrawdownR,0);

// Point 8: official calendar parsers, risk policy and context-only news.
const calendarSettings=normalizeCalendarSettings({});
assert.deepEqual(calendarSettings,{
  macroBeforeMinutes:30,macroAfterMinutes:15,fomcBeforeMinutes:60,fomcAfterMinutes:30,
  claimsBeforeMinutes:15,claimsAfterMinutes:10,geopoliticalAfterMinutes:15,localTimeZone:'Asia/Beirut'
});
const blsEvents=parseBlsIcs(`BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:cpi-202609\nDTSTART;TZID=America/New_York:20260910T083000\nSUMMARY:Consumer Price Index\nEND:VEVENT\nBEGIN:VEVENT\nUID:jobs-202609\nDTSTART;TZID=America/New_York:20260904T083000\nSUMMARY:Employment Situation\nEND:VEVENT\nEND:VCALENDAR`,calendarSettings,Date.UTC(2026,8,1));
assert.deepEqual(blsEvents.map(event=>event.type),['cpi','core_cpi','nfp','unemployment_rate']);
assert.equal(blsEvents[0].eventAt,Date.UTC(2026,8,10,12,30),'BLS Eastern release time must convert to UTC');
assert.equal(parseBlsIcs(`BEGIN:VEVENT\nDTSTART;TZID=US-Eastern:20260910T083000\nSUMMARY:Consumer Price Index\nEND:VEVENT`,calendarSettings)[0].eventAt,Date.UTC(2026,8,10,12,30),'the official BLS US-Eastern timezone alias must convert to UTC');
assert.equal(blsEvents[0].forecast,null,'officially unavailable forecasts must stay null');
assert.equal(extractOfficialActual('cpi','The Consumer Price Index increased 0.3 percent in August.'),'0.3%');
assert.equal(extractOfficialActual('core_cpi','The all items less food and energy index rose 0.2 percent.'),'0.2%');
assert.equal(extractOfficialActual('nfp','Total nonfarm payroll employment increased by 145,000 in August.'),'145000');
assert.equal(extractOfficialActual('pce','The PCE price index increased 0.2 percent.'),'0.2%');
assert.equal(extractOfficialActual('ism','PMI was 52.1'),null,'unsupported official values must remain null');

const blsApiSeries=parseBlsApiPayload({status:'REQUEST_SUCCEEDED',Results:{series:[
  {seriesID:'CUSR0000SA0',data:[{year:'2026',period:'M07',value:'102'},{year:'2026',period:'M06',value:'100'},{year:'2026',period:'M05',value:'99'}]},
  {seriesID:'CUSR0000SA0L1E',data:[{year:'2026',period:'M07',value:'201'},{year:'2026',period:'M06',value:'200'},{year:'2026',period:'M05',value:'199'}]},
  {seriesID:'CES0000000001',data:[{year:'2026',period:'M07',value:'159000'},{year:'2026',period:'M06',value:'158800'},{year:'2026',period:'M05',value:'158700'}]},
  {seriesID:'LNS14000000',data:[{year:'2026',period:'M07',value:'4.3'},{year:'2026',period:'M06',value:'4.2'}]}
]}});
const blsSnapshot=buildBlsActualSnapshot(blsApiSeries,Object.fromEntries(Object.keys(blsApiSeries).map(id=>[id,'BLS Public Data API v1'])));
assert.deepEqual({actual:blsSnapshot.cpi.actual,previous:blsSnapshot.cpi.previous}, {actual:'2',previous:'1'});
assert.deepEqual({actual:blsSnapshot.nfp.actual,previous:blsSnapshot.nfp.previous}, {actual:'200',previous:'100'});
assert.deepEqual({actual:blsSnapshot.unemployment_rate.actual,previous:blsSnapshot.unemployment_rate.previous}, {actual:'4.3',previous:'4.2'});
const downloadedBls=parseBlsDownloadDataset('series_id year period value footnote_codes\nCUSR0000SA0 2026 M07 102\nCUSR0000SA0 2026 M06 100\n',['CUSR0000SA0']);
assert.equal(downloadedBls.CUSR0000SA0[0].value,102,'official downloadable BLS datasets must be a usable fallback');
const enrichedBls=applyBlsActuals(blsEvents,blsSnapshot,Date.UTC(2026,8,12));
assert.deepEqual(enrichedBls.map(event=>event.actual),['2','0.5','200','4.3']);
assert.ok(enrichedBls.every(event=>event.metadata.scheduleSource==='fetched-official-ical'&&event.metadata.dataMode==='fetched'));
const upcomingBls=applyBlsActuals(blsEvents,blsSnapshot,Date.UTC(2026,8,1));
assert.deepEqual(upcomingBls.map(event=>event.previous),['2','0.5','200','4.3'],'next BLS releases must expose only the officially known previous values');

const beaEvents=parseBeaScheduleHtml(`<h1>Year 2026</h1><table><tr><td><div class="release-date">September 25, 2026</div><small class="text-muted">8:30 AM</small></td><td class="release-title">Personal Income and Outlays, August 2026</td></tr><tr><td><div class="release-date">September 30, 2026</div><small>8:30 AM</small></td><td class="release-title">Gross Domestic Product, 2nd Quarter 2026</td></tr></table>`,calendarSettings,Date.UTC(2026,8,1));
assert.deepEqual(beaEvents.map(event=>event.type),['pce','core_pce','gdp']);
const fedEvents=parseFedCalendarHtml(`<h1>September 2026</h1><div class="panel"><div class="col-xs-2"><p>2:00 p.m.</p></div><div class="col-xs-7">FOMC Meeting</div><div class="col-xs-3"><p>16</p></div></div><div class="panel"><div class="col-xs-2"><p>10:00 a.m.</p></div><div class="col-xs-7">Speech - Chair Jerome H. Powell on monetary policy and inflation</div><div class="col-xs-3"><p>22</p></div></div>`,`https://www.federalreserve.gov/newsevents/2026-september.htm`,calendarSettings,Date.UTC(2026,8,1));
assert.deepEqual(fedEvents.map(event=>event.type),['fomc_decision','powell_monetary_speech']);
assert.equal(fedEvents[0].riskBeforeMinutes,60);
const censusEvents=parseCensusScheduleHtml(`<table><tr><td>Advance Monthly Sales for Retail and Food Services</td><td>September 16, 2026 8:30 AM</td><td>A202609160001</td></tr></table>`,calendarSettings,Date.UTC(2026,8,1));
assert.equal(censusEvents[0].type,'retail_sales');
const ismEvents=buildIsmDerivedSchedule(calendarSettings,Date.UTC(2026,0,1),Date.UTC(2026,0,1),Date.UTC(2026,11,31));
assert.equal(ismEvents.find(event=>event.type==='ism_manufacturing'&&new Date(event.eventAt).getUTCMonth()===0).eventAt,Date.UTC(2026,0,5,15),'January Manufacturing must use the second NYSE business day');
assert.equal(ismEvents.find(event=>event.type==='ism_services'&&new Date(event.eventAt).getUTCMonth()===0).eventAt,Date.UTC(2026,0,7,15),'January Services must use the fourth NYSE business day');
assert.equal(ismEvents.find(event=>event.type==='ism_services'&&new Date(event.eventAt).getUTCMonth()===3).eventAt,Date.UTC(2026,3,6,14),'NYSE Good Friday must not count as an ISM business day');
assert.equal(ismEvents.find(event=>event.type==='ism_services'&&new Date(event.eventAt).getUTCMonth()===6).eventAt,Date.UTC(2026,6,6,14),'observed Independence Day closure must not count as an ISM business day');
assert.ok(ismEvents.every(event=>event.actual===null&&event.forecast===null&&event.previous===null));
assert.ok(ismEvents.every(event=>event.metadata.scheduleSource==='derived-official-rule'&&event.metadata.dataAvailability==='schedule-only'));
assert.equal(buildIsmDerivedSchedule(calendarSettings,Date.UTC(2029,0,1),Date.UTC(2029,0,1),Date.UTC(2029,0,31)).length,0,'unverified NYSE years must fail closed');

const dolXml=`<r539cyNational rundate="11/25/2026"><week><weekEnded>11/14/2026</weekEnded><InitialClaims><SA>218,000</SA></InitialClaims></week><week><weekEnded>11/21/2026</weekEnded><InitialClaims><SA>225,000</SA></InitialClaims></week></r539cyNational>`;
assert.deepEqual(parseJoblessClaimsXml(dolXml).map(row=>row.actual),[218000,225000]);
const claimsEvents=buildJoblessClaimsEvents(dolXml,calendarSettings,Date.UTC(2026,10,25));
const thanksgivingClaims=claimsEvents.find(event=>event.metadata.weekEnded==='11/21/2026');
assert.equal(thanksgivingClaims.eventAt,Date.UTC(2026,10,25,13,30),'a Thursday federal holiday must move DOL release to preceding Wednesday at 08:30 ET');
assert.equal(thanksgivingClaims.actual,'225000');assert.equal(thanksgivingClaims.previous,'218000');
assert.equal(thanksgivingClaims.metadata.scheduleSource,'derived-official-rule');
assert.equal(thanksgivingClaims.riskBeforeMinutes,15);assert.equal(thanksgivingClaims.riskAfterMinutes,10);

const riskEvent={...blsEvents[0],eventAt:Date.UTC(2026,8,10,12,30)};
assert.equal(calendarRiskSnapshot([riskEvent],riskEvent.eventAt-30*60_000,calendarSettings).active,true);
assert.equal(calendarRiskSnapshot([riskEvent],riskEvent.eventAt+15*60_000,calendarSettings).active,true);
assert.equal(calendarRiskSnapshot([riskEvent],riskEvent.eventAt+15*60_000+1,calendarSettings).active,false);

const geopoliticalAt=Date.UTC(2026,8,1,10,0);
const confirmedNews=buildNewsBrief([
  {title:'Gold surges as war airstrikes escalate safe haven demand',url:'https://www.reuters.com/world/confirmed-a',seenAt:geopoliticalAt-60_000},
  {title:'Gold rises as war missiles escalate safe haven demand',url:'https://apnews.com/article/confirmed-b',seenAt:geopoliticalAt-90_000}
],geopoliticalAt);
assert.equal(confirmedNews.safety.blockTechnicalSignal,true,'two independent trusted sources must confirm exceptional geopolitical risk');
assert.equal(confirmedNews.safety.corroboratedDomains.length,2);
const conflictingNews=buildNewsBrief([
  {title:'Gold surges as war airstrikes escalate safe haven demand',url:'https://www.reuters.com/world/conflict-a',seenAt:geopoliticalAt-60_000},
  {title:'Gold rises as missile escalation drives safe haven demand',url:'https://apnews.com/article/conflict-b',seenAt:geopoliticalAt-70_000},
  {title:'Gold falls as ceasefire agreed while Treasury yields rise and dollar strengthens',url:'https://www.bbc.com/news/conflict-c',seenAt:geopoliticalAt-80_000},
  {title:'Gold drops as ceasefire holds while yields rise and dollar strengthens',url:'https://www.cnbc.com/conflict-d',seenAt:geopoliticalAt-90_000}
],geopoliticalAt);
assert.equal(conflictingNews.goldBias.direction,'mixed');
assert.equal(conflictingNews.safety.classification,'mixed/conflicting');
assert.equal(conflictingNews.safety.blockUntil,geopoliticalAt-60_000+15*60_000,'conflicting-news risk must be capped at 15 minutes after the latest item');

const noNewsScore=computeServerSignal(parityFrames['1m'],{
  tf:'1m',mtf:[{tf:'5m',bars:parityFrames['5m']},{tf:'15m',bars:parityFrames['15m']}],
  live:{price:parityFrames['1m'].at(-1).c,ts:parityAt,receivedAt:parityAt,source:'backtest'},
  barsSource:'backtest',dataQuality:evaluateCandleQuality(parityFrames['1m'],'1m'),filters:parityFilters,
  news:{ok:true,stale:false,goldBias:{direction:'informational',confidence:0},safety:{blockTechnicalSignal:false}},evaluationAt:parityAt
});
const bullishContextScore=computeServerSignal(parityFrames['1m'],{
  tf:'1m',mtf:[{tf:'5m',bars:parityFrames['5m']},{tf:'15m',bars:parityFrames['15m']}],
  live:{price:parityFrames['1m'].at(-1).c,ts:parityAt,receivedAt:parityAt,source:'backtest'},
  barsSource:'backtest',dataQuality:evaluateCandleQuality(parityFrames['1m'],'1m'),filters:parityFilters,
  news:{ok:true,stale:false,goldBias:{direction:'bullish',confidence:92},safety:{blockTechnicalSignal:false}},evaluationAt:parityAt
});
assert.deepEqual(
  {side:bullishContextScore.side,score:bullishContextScore.score,bull:bullishContextScore.bull,bear:bullishContextScore.bear,entry:bullishContextScore.entry,tp1:bullishContextScore.tp1,tp2:bullishContextScore.tp2,sl:bullishContextScore.sl},
  {side:noNewsScore.side,score:noNewsScore.score,bull:noNewsScore.bull,bear:noNewsScore.bear,entry:noNewsScore.entry,tp1:noNewsScore.tp1,tp2:noNewsScore.tp2,sl:noNewsScore.sl},
  'positive news context must not add score or change any official signal level'
);
const blockedContext=mergeSignalRiskContext({ok:true,stale:false,goldBias:{direction:'informational',confidence:0},safety:{blockTechnicalSignal:false}},{ok:true,stale:false,events:[riskEvent]},riskEvent.eventAt);
assert.equal(blockedContext.safety.blockTechnicalSignal,true);
const calendarBlockedSignal=computeServerSignal(parityFrames['1m'],{
  tf:'1m',mtf:[{tf:'5m',bars:parityFrames['5m']},{tf:'15m',bars:parityFrames['15m']}],
  live:{price:parityFrames['1m'].at(-1).c,ts:parityAt,receivedAt:parityAt,source:'backtest'},barsSource:'backtest',
  dataQuality:evaluateCandleQuality(parityFrames['1m'],'1m'),filters:parityFilters,news:blockedContext,evaluationAt:parityAt
});
assert.equal(calendarBlockedSignal.side,'none','calendar risk may only veto an otherwise technical signal');

const calendarDb=new MemoryD1();
await ensureNewsCalendarSchema({GSX_DB:calendarDb});
const storedCalendar={ok:true,events:[riskEvent]};
await persistCalendarEvents({GSX_DB:calendarDb},storedCalendar);
await persistCalendarEvents({GSX_DB:calendarDb},storedCalendar);
assert.equal(calendarDb.database.prepare('SELECT COUNT(*) AS count FROM economic_calendar_events').get().count,1,'calendar persistence must be idempotent');
await persistNewsEvents({GSX_DB:calendarDb},confirmedNews);
await persistNewsEvents({GSX_DB:calendarDb},confirmedNews);
assert.equal(calendarDb.database.prepare('SELECT COUNT(*) AS count FROM news_context_events').get().count,2,'news persistence must deduplicate stable event IDs');

const endpointCache=new Map([
  ['calendar:official:v2',JSON.stringify({ok:true,updatedAt:Date.now(),source:'official-multi-source',events:[riskEvent],sourceStatus:{bls:{ok:true}}})],
  ['news:brief:v2',JSON.stringify({...confirmedNews,updatedAt:Date.now()})]
]);
const endpointKv={get:async(key,type)=>type==='json'?safeJson(endpointCache.get(key)):endpointCache.get(key)??null,put:async(key,value)=>endpointCache.set(key,value)};
function safeJson(value){try{return JSON.parse(value)}catch{return null}}
const calendarGet=await worker.default.fetch(new Request('https://example.com/calendar'),{GSX_KV:endpointKv,ALLOW_ORIGINS:'["*"]'},{});
assert.equal(calendarGet.status,200);
const calendarPayload=await calendarGet.json();
assert.equal(calendarPayload.readOnly,true);assert.equal(calendarPayload.decisionOwner,'worker');assert.equal(calendarPayload.createsOfficialSignals,false);
assert.equal(calendarPayload.events[0].scheduleSource,'fetched-official-ical');
assert.equal(calendarPayload.events[0].scheduleMode,'fetched');
assert.equal((await worker.default.fetch(new Request('https://example.com/calendar',{method:'POST'}),{},{})).status,405);
assert.equal((await worker.default.fetch(new Request('https://example.com/news',{method:'POST'}),{},{})).status,405);

const calendarTelegramNow=Date.UTC(2026,8,10,12,5);
Date.now=()=>calendarTelegramNow;
const calendarHarness=await makeTelegramHarness();
let calendarTelegramCalls=0;
globalThis.fetch=async()=>{calendarTelegramCalls+=1;return {ok:true,status:200,json:async()=>({ok:true})};};
const upcomingCalendar={ok:true,events:[{...riskEvent,eventAt:calendarTelegramNow+20*60_000}]};
await maybeNotifyCalendarEvents(calendarHarness.env,upcomingCalendar,calendarTelegramNow);
await maybeNotifyCalendarEvents(calendarHarness.env,upcomingCalendar,calendarTelegramNow);
assert.equal(calendarTelegramCalls,1,'stable calendar event IDs must prevent duplicate Telegram alerts');
Date.now=realDateNow;globalThis.fetch=originalFetch;

console.log('news intelligence tests passed');
