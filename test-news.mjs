import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

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
const testSource = source.replace(
  "import { DurableObject } from 'cloudflare:workers';",
  'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }'
).replace("'./signal-engine.js'", JSON.stringify(signalEngineUrl));
const worker = await import(`data:text/javascript;base64,${Buffer.from(testSource).toString('base64')}`);
const {
  computeServerSignal,evaluateCandleQuality,normalizeSignalFilters
} = await import(signalEngineUrl);
const {
  GoldFeed,classifyNewsArticle,buildNewsBrief,enrichNewsBriefArabic,getGoldNewsBrief,
  parseGdeltSeenDate,parseTwelveDataTimeSeries,sendTelegramText,queueTelegramDelivery,
  updateSignalLifecycleAcrossBars,closedBarsOnly,signalFiltersFromSearchParams,readSignalFilters,
  pruneTickHistory
} = worker;

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
assert.equal(brief.safety.blockTechnicalSignal, true);
assert.equal(brief.safety.blockUntil, Date.UTC(2026, 7, 27, 8, 10, 0));

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
  put: async values => Object.entries(values).forEach(([key,value])=>storageValues.set(key,value))
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
for (const [index,tick] of recentTicks.entries()) {
  if (index===recentTicks.length-1) feed.lastSnapshotWriteAt=0;
  await feed.handleProviderMessage(JSON.stringify({
    event:'price',symbol:'XAU/USD',price:tick.price,timestamp:Math.floor(tick.ts/1000)
  }));
}
const signalCreatedAt=tickNow-15_000;
const tickWindow=await feed.ticks({from:tickNow-25_000,to:tickNow,limit:20});
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
const telegramStore=new Map();
const telegramKv={
  get:async (key,type)=>{
    const value=telegramStore.get(key);
    return type==='json'&&typeof value==='string'?JSON.parse(value):value??null;
  },
  put:async (key,value)=>telegramStore.set(key,value),
  list:async ({prefix})=>({keys:[...telegramStore.keys()].filter(key=>key.startsWith(prefix)).map(name=>({name})),list_complete:true})
};
globalThis.fetch=async ()=>({ok:false,status:401,json:async()=>({ok:false,description:'Unauthorized'})});
const pendingDelivery=await queueTelegramDelivery({GSX_KV:telegramKv,TELEGRAM_TOKEN:'invalid',TELEGRAM_CHAT:'chat'},lifecycleSignal,'created','test');
assert.equal(pendingDelivery.status,'pending');
assert.equal(pendingDelivery.attempts,1);
assert.equal(pendingDelivery.lastError,'invalid_bot_token');
globalThis.fetch=async ()=>({ok:true,status:200,json:async()=>({ok:true})});
const sentDelivery=await queueTelegramDelivery({GSX_KV:telegramKv,TELEGRAM_TOKEN:'valid',TELEGRAM_CHAT:'chat'},lifecycleSignal,'created','test');
assert.equal(sentDelivery.status,'sent');
assert.equal(sentDelivery.attempts,2);
telegramStore.set('signal:state:1m',JSON.stringify(lifecycleSignal));
const authorizedNotify=await worker.default.fetch(new Request('https://example.com/notify',{
  method:'POST',
  headers:{Origin:allowedOrigin,'content-type':'application/json','x-gsx-write-token':writeToken},
  body:JSON.stringify({tf:'1m',signalId:lifecycleSignal.id})
}),{
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),GSX_WRITE_TOKEN:writeToken,GSX_KV:telegramKv,
  TELEGRAM_TOKEN:'valid',TELEGRAM_CHAT:'chat'
},{});
assert.equal(authorizedNotify.status,200,'an authorized official-signal notification must succeed');
assert.equal((await authorizedNotify.json()).ok,true);
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

console.log('news intelligence tests passed');
