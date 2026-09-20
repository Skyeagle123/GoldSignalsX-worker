import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import {
  calculateRiskBudget,
  createImmutableRiskSnapshot,
  normalizeRiskBudgetLimits,
  normalizeStoredRiskSnapshot,
  summarizeRealizedLossRows,
  utcBudgetPeriodBounds
} from './risk-budget.js';

if (typeof globalThis.crypto.subtle.timingSafeEqual !== 'function') {
  Object.defineProperty(globalThis.crypto.subtle,'timingSafeEqual',{
    value(left,right){
      const a=new Uint8Array(left),b=new Uint8Array(right);
      if (a.byteLength!==b.byteLength) return false;
      let diff=0;
      for (let index=0;index<a.byteLength;index++) diff|=a[index]^b[index];
      return diff===0;
    }
  });
}

const now=Date.UTC(2026,8,16,12,0,0); // Wednesday
const sizing={
  available:true,status:'POSITION_SIZE_AVAILABLE',accountBasis:'equity',
  accountValueUsd:100000,riskPercent:1,riskAmount:1000,suggestedLots:1,
  estimatedLossAtSL:1000,
  signal:{id:'5m:risk-budget:buy',timeframe:'5m',side:'buy',createdAt:now-60_000,entry:2000},
  metadataStatus:{metadata:{contractSize:100,sessionId:'mt5-risk-budget',observedAt:now-1_000}}
};
const built=createImmutableRiskSnapshot(sizing,now);
assert.equal(built.ok,true);
assert.equal(built.snapshot.signalId,sizing.signal.id);
assert.equal(built.snapshot.notionalUsd,200000);
assert.equal(built.snapshot.immutable,true);
assert.equal(built.snapshot.measurementOnly,true);
assert.equal(built.snapshot.decisionUse,false);
assert.equal(createImmutableRiskSnapshot({...sizing,available:false},now).ok,false);
assert.equal(createImmutableRiskSnapshot({
  ...sizing,estimatedLossAtSL:1001,riskAmount:1000
},now).error,'risk_snapshot_loss_exceeds_risk_amount');

const normalizedStored=normalizeStoredRiskSnapshot({
  signal_id:built.snapshot.signalId,schema_version:1,timeframe:'5m',side:'buy',account_basis:'equity',
  account_value_usd:100000,risk_percent:1,risk_amount_usd:1000,
  suggested_lots:1,estimated_loss_at_sl:1000,entry:2000,contract_size:100,
  notional_usd:200000,metadata_session_id:'mt5-risk-budget',
  metadata_observed_at:now-1_000,created_at:now-60_000,recorded_at:now
});
assert.equal(normalizedStored.signalId,built.snapshot.signalId);
assert.equal(normalizeStoredRiskSnapshot({...normalizedStored,notionalUsd:199000}),null);

const periods=utcBudgetPeriodBounds(now);
assert.equal(periods.dayStart,Date.UTC(2026,8,16));
assert.equal(periods.weekStart,Date.UTC(2026,8,14));
assert.deepEqual(normalizeRiskBudgetLimits({
  maxRiskPercent:2,maxDailyLossUsd:2000,maxWeeklyLossUsd:5000,
  maxTotalExposureUsd:500000,timezone:'UTC'
}),{
  maxRiskPercent:2,maxDailyLossUsd:2000,maxWeeklyLossUsd:5000,
  maxTotalExposureUsd:500000,timezone:'UTC'
});

const exposure={status:'active',primarySignalId:built.snapshot.signalId,primaryTf:'5m'};
const limits={
  maxRiskPercent:2,maxDailyLossUsd:2000,maxWeeklyLossUsd:5000,
  maxTotalExposureUsd:500000,timezone:'UTC'
};
const allowed=calculateRiskBudget({
  now,limits,snapshot:built.snapshot,exposure,realizedLossRows:[]
});
assert.equal(allowed.status,'allowed');
assert.equal(allowed.allowed,true);
assert.equal(allowed.measurementOnly,true);
assert.equal(allowed.decisionUse,false);
assert.equal(allowed.executionEnabled,false);
assert.equal(allowed.usage.realizedDailyLossUsd,0);
assert.equal(allowed.usage.activeRiskAtSLUsd,1000);
assert.equal(allowed.usage.projectedDailyLossAtSLUsd,1000);
assert.equal(allowed.remaining.dailyLossUsd,1000);
assert.equal(allowed.usage.activeExposureNotionalUsd,200000);
assert.equal(allowed.remaining.totalExposureUsd,300000);

const priorSl={
  signal_id:'15m:prior-sl',final_status:'sl',closed_at:now-60*60_000,
  estimated_loss_at_sl:1000
};
const dailyLimited=calculateRiskBudget({
  now,limits,snapshot:built.snapshot,exposure,realizedLossRows:[priorSl]
});
assert.equal(dailyLimited.status,'limited');
assert.equal(dailyLimited.allowed,false);
assert.equal(dailyLimited.checks.dailyLoss.atLimit,true);
assert.equal(dailyLimited.usage.realizedDailyLossUsd,1000);
assert.equal(dailyLimited.usage.projectedDailyLossAtSLUsd,2000);
assert.equal(dailyLimited.remaining.dailyLossUsd,0);

const weeklyPriorSl={
  signal_id:'60m:weekly-sl',final_status:'sl',closed_at:periods.weekStart+60_000,
  estimated_loss_at_sl:3500
};
const weeklyLimited=calculateRiskBudget({
  now,limits,snapshot:built.snapshot,exposure,realizedLossRows:[weeklyPriorSl]
});
assert.equal(weeklyLimited.checks.weeklyLoss.status,'allowed');
assert.equal(weeklyLimited.usage.realizedDailyLossUsd,0);
assert.equal(weeklyLimited.usage.realizedWeeklyLossUsd,3500);
assert.equal(weeklyLimited.remaining.weeklyLossUsd,500);

const missingSnapshot=calculateRiskBudget({
  now,limits,snapshot:built.snapshot,exposure,
  realizedLossRows:[{...priorSl,estimated_loss_at_sl:null}]
});
assert.equal(missingSnapshot.status,'unavailable');
assert.equal(missingSnapshot.reason,'historical_risk_snapshot_missing');
assert.deepEqual(missingSnapshot.usage.missingSnapshotSignalIds,['15m:prior-sl']);

const duplicateLoss=summarizeRealizedLossRows([priorSl,{...priorSl}],periods);
assert.equal(duplicateLoss.error,'duplicate_realized_loss_signal');
assert.equal(summarizeRealizedLossRows([{
  signal_id:'expired:neutral',final_status:'expired',closed_at:now,
  estimated_loss_at_sl:500
}],periods).error,'inconsistent_realized_loss_record');
assert.equal(calculateRiskBudget({
  now,limits,snapshot:built.snapshot,exposure,
  realizedLossRows:[{...priorSl,signal_id:built.snapshot.signalId}]
}).reason,'active_exposure_already_realized');

const totalExposureLimited=calculateRiskBudget({
  now,limits:{...limits,maxTotalExposureUsd:200000},
  snapshot:built.snapshot,exposure,realizedLossRows:[]
});
assert.equal(totalExposureLimited.status,'limited');
assert.equal(totalExposureLimited.checks.totalExposure.atLimit,true);
assert.equal(totalExposureLimited.remaining.totalExposureUsd,0);

const atPerTradeLimit=createImmutableRiskSnapshot({
  ...sizing,riskPercent:2,riskAmount:2000,estimatedLossAtSL:2000,
  suggestedLots:2
},now).snapshot;
const tradeLimited=calculateRiskBudget({
  now,limits,snapshot:atPerTradeLimit,
  exposure:{...exposure,primarySignalId:atPerTradeLimit.signalId},realizedLossRows:[]
});
assert.equal(tradeLimited.checks.perTrade.status,'limited');
assert.equal(tradeLimited.checks.perTrade.atLimit,true);

assert.equal(calculateRiskBudget({
  now,limits:{...limits,timezone:'Asia/Beirut'},snapshot:built.snapshot,
  exposure,realizedLossRows:[]
}).reason,'unsupported_risk_budget_timezone');
assert.equal(calculateRiskBudget({
  now,limits:{...limits,maxDailyLossUsd:0},snapshot:built.snapshot,
  exposure,realizedLossRows:[]
}).reason,'risk_budget_limits_unconfigured');
assert.equal(calculateRiskBudget({
  now,limits,snapshot:built.snapshot,
  exposure:{...exposure,primarySignalId:'other'},realizedLossRows:[]
}).reason,'active_exposure_snapshot_mismatch');

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

const workerSource=await fs.readFile(new URL('./goldsignalsx-worker.js',import.meta.url),'utf8');
const generatedWorkerUrl=new URL('./.test-risk-budget-worker.generated.mjs',import.meta.url);
await fs.writeFile(generatedWorkerUrl,workerSource.replace(
  "import { DurableObject } from 'cloudflare:workers';",
  'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }'
));
const worker=await import(`${generatedWorkerUrl.href}?v=${Date.now()}`);
await fs.unlink(generatedWorkerUrl);
const {
  ensurePerformanceSchema,recordProductionPerformanceEvent,
  storeImmutableRiskSnapshot,readRealizedLossRows,calculateOfficialActiveRiskSizing,
  calculateOfficialActiveRiskSizingReadOnly
}=worker;

const integrationDb=new MemoryD1();
await ensurePerformanceSchema({GSX_DB:integrationDb});
const integrationSignal={
  id:'5m:risk-budget:integration',tf:'5m',side:'buy',status:'active',origin:'server',
  createdAt:now,updatedAt:now,entry:2000,sl:1990,tp1:2012.5,tp2:2021,
  signalBarTs:now-300_000,conf:82,score:8.5,reasons:['integration']
};
await recordProductionPerformanceEvent({GSX_DB:integrationDb},integrationSignal,'created');
const integrationExposure={
  symbol:'XAUUSD',status:'active',primarySignalId:integrationSignal.id,primaryTf:'5m'
};
const integrationMetadata={
  source:'mt5',fresh:true,available:true,status:'MT5_METADATA_VERIFIED',
  sessionId:'mt5-risk-budget',observedAt:now,ageMs:0,reason:'',
  metadata:{
    source:'mt5',canonicalSymbol:'XAUUSD',brokerSymbol:'XAUUSDs',
    sessionId:'mt5-risk-budget',observedAt:now,receivedAt:now,
    contractSize:100,tickSize:0.01,tickValue:1,tickValueProfit:1,tickValueLoss:1,
    volumeMin:0.01,volumeMax:100,volumeStep:0.01,volumeLimit:0,
    point:0.01,digits:2,tradeStopsLevel:0,profitCurrency:'USD',accountCurrency:'USD',
    tradeCalcMode:'SYMBOL_CALC_MODE_CFD',tradeMode:4
  }
};
let kvWrites=0,doWrites=0,exposureReads=0,metadataReads=0;
const riskRateLimits=new Map();
const allowedOrigin='https://skyeagle123.github.io';
const writeToken='risk-security-token';
const integrationEnv={
  GSX_DB:integrationDb,RISK_MAX_PERCENT:'2',RISK_MAX_LOTS:'5',
  RISK_METADATA_MAX_AGE_MS:'900000',RISK_MAX_DAILY_LOSS_USD:'2000',
  RISK_MAX_WEEKLY_LOSS_USD:'5000',RISK_MAX_TOTAL_EXPOSURE_USD:'500000',
  RISK_ACCOUNT_BASIS:'equity',RISK_ACCOUNT_VALUE_USD:'100000',RISK_PERCENT:'1',
  RISK_BUDGET_TIMEZONE:'UTC',ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),
  GSX_WRITE_TOKEN:writeToken,WRITE_RL_LIMIT:'20',
  GOLD_FEED:{getByName:()=>({
    goldExposureStatus:async()=>{exposureReads+=1;return integrationExposure;},
    getRiskSizingMetadataStatus:async()=>{metadataReads+=1;return integrationMetadata;},
    ingestMt5Tick:async()=>{doWrites+=1;throw new Error('unexpected DO write');},
    manageGoldExposure:async()=>{doWrites+=1;throw new Error('unexpected DO write');},
    cancelGoldExposureReservation:async()=>{doWrites+=1;throw new Error('unexpected DO write');},
    queueTelegramEvent:async()=>{doWrites+=1;throw new Error('unexpected DO write');}
  })},
  GSX_KV:{
    get:async key=>key==='signal:state:5m'?integrationSignal:riskRateLimits.get(key)??null,
    put:async (key,value)=>{
      if (key.startsWith('rl:write:risk-sizing:')) riskRateLimits.set(key,value);
      else kvWrites+=1;
    }
  }
};
const originalDateNow=Date.now;
Date.now=()=>now;

const riskRequestBody={signalId:integrationSignal.id};
const snapshotCount=()=>integrationDb.database.prepare(
  'SELECT COUNT(*) AS count FROM production_signal_risk_snapshots'
).get().count;
const routeRequest=(body=riskRequestBody,headers={},env=integrationEnv)=>worker.default.fetch(new Request(
  'https://example.com/risk-sizing',{
    method:'POST',headers:{Origin:allowedOrigin,'content-type':'application/json',...headers},
    body:typeof body==='string'?body:JSON.stringify(body)
  }
),env,{});
const verificationRequest=(query=`signalId=${encodeURIComponent(integrationSignal.id)}`,headers={},env=integrationEnv)=>
  worker.default.fetch(new Request(`https://example.com/risk-sizing/verify?${query}`,{
    headers:{Origin:allowedOrigin,...headers}
  }),env,{});

const unauthorizedFirstWrite=await routeRequest();
assert.equal(unauthorizedFirstWrite.status,401,'unauthorized first-write must be rejected');
assert.deepEqual(await unauthorizedFirstWrite.json(),{ok:false,error:'unauthorized'});
assert.equal(snapshotCount(),0);

const wrongTokenWrite=await routeRequest(riskRequestBody,{'x-gsx-write-token':'wrong-token'});
assert.equal(wrongTokenWrite.status,401,'wrong token must be rejected');
assert.equal(snapshotCount(),0);

const untrustedOriginWrite=await worker.default.fetch(new Request(
  'https://example.com/risk-sizing',{
    method:'POST',headers:{
      Origin:'https://attacker.example','content-type':'application/json',
      'x-gsx-write-token':writeToken
    },body:JSON.stringify(riskRequestBody)
  }
),integrationEnv,{});
assert.equal(untrustedOriginWrite.status,403,'untrusted origin must be rejected');
assert.deepEqual(await untrustedOriginWrite.json(),{ok:false,error:'origin_not_allowed'});
assert.equal(snapshotCount(),0);

const unconfiguredAuthWrite=await routeRequest(
  riskRequestBody,{'x-gsx-write-token':writeToken},{...integrationEnv,GSX_WRITE_TOKEN:''}
);
assert.equal(unconfiguredAuthWrite.status,503,'missing write authentication must fail closed');
assert.deepEqual(await unconfiguredAuthWrite.json(),{ok:false,error:'write_auth_not_configured'});
assert.equal(snapshotCount(),0);

const invalidJsonWrite=await routeRequest('{',{'x-gsx-write-token':writeToken});
assert.equal(invalidJsonWrite.status,400);
assert.equal((await invalidJsonWrite.json()).error,'invalid_json');
assert.equal(snapshotCount(),0);

const spoofedInputWrite=await routeRequest(
  {...riskRequestBody,accountBasis:'balance',accountValueUsd:1,riskPercent:0.0001},
  {'x-gsx-write-token':writeToken}
);
assert.equal(spoofedInputWrite.status,400,'client-controlled risk values must be rejected');
assert.deepEqual(await spoofedInputWrite.json(),{ok:false,error:'bad_risk_sizing_input'});
assert.equal(snapshotCount(),0);

const missingAuthorityWrite=await routeRequest(
  riskRequestBody,{'x-gsx-write-token':writeToken},
  {...integrationEnv,RISK_ACCOUNT_VALUE_USD:''}
);
assert.equal(missingAuthorityWrite.status,503,'missing server-side risk authority must fail closed');
assert.deepEqual(await missingAuthorityWrite.json(),{
  ok:false,error:'risk_sizing_authority_not_configured'
});
assert.equal(snapshotCount(),0);

exposureReads=0;
metadataReads=0;
const riskRateLimitEntriesBeforeVerification=riskRateLimits.size;
const unauthorizedVerification=await verificationRequest();
assert.equal(unauthorizedVerification.status,401,'read-only verification must require authentication');
assert.deepEqual(await unauthorizedVerification.json(),{ok:false,error:'unauthorized'});
assert.equal(exposureReads,0,'authentication must run before exposure reads');
assert.equal(metadataReads,0,'authentication must run before metadata reads');
assert.equal(snapshotCount(),0);

const wrongTokenVerification=await verificationRequest(undefined,{'x-gsx-write-token':'wrong-token'});
assert.equal(wrongTokenVerification.status,401);
assert.deepEqual(await wrongTokenVerification.json(),{ok:false,error:'unauthorized'});
assert.equal(exposureReads,0);
assert.equal(metadataReads,0);
assert.equal(snapshotCount(),0);

const untrustedOriginVerification=await worker.default.fetch(new Request(
  `https://example.com/risk-sizing/verify?signalId=${encodeURIComponent(integrationSignal.id)}`,
  {headers:{Origin:'https://attacker.example','x-gsx-write-token':writeToken}}
),integrationEnv,{});
assert.equal(untrustedOriginVerification.status,403,'untrusted verification origin must be rejected');
assert.deepEqual(await untrustedOriginVerification.json(),{ok:false,error:'origin_not_allowed'});
assert.equal(exposureReads,0);
assert.equal(metadataReads,0);
assert.equal(snapshotCount(),0);

const spoofedVerification=await verificationRequest(
  `signalId=${encodeURIComponent(integrationSignal.id)}&accountValueUsd=1&riskPercent=0.0001`,
  {'x-gsx-write-token':writeToken}
);
assert.equal(spoofedVerification.status,400,'client risk authority must be rejected');
assert.deepEqual(await spoofedVerification.json(),{ok:false,error:'bad_risk_sizing_input'});
assert.equal(exposureReads,0);
assert.equal(metadataReads,0);
assert.equal(snapshotCount(),0);

const missingVerificationAuthority=await verificationRequest(
  undefined,{'x-gsx-write-token':writeToken},{...integrationEnv,RISK_PERCENT:''}
);
assert.equal(missingVerificationAuthority.status,503,'read-only verification must fail closed without authority');
assert.deepEqual(await missingVerificationAuthority.json(),{
  ok:false,error:'risk_sizing_authority_not_configured'
});
assert.equal(exposureReads,0);
assert.equal(metadataReads,0);
assert.equal(snapshotCount(),0);

const authorizedVerification=await verificationRequest(
  undefined,{'x-gsx-write-token':writeToken}
);
assert.equal(authorizedVerification.status,200);
const verificationPayload=await authorizedVerification.json();
assert.equal(verificationPayload.available,true);
assert.equal(verificationPayload.status,'POSITION_SIZE_AVAILABLE');
assert.equal(verificationPayload.accountBasis,'equity');
assert.equal(verificationPayload.accountValueUsd,100000);
assert.equal(verificationPayload.riskPercent,1);
assert.equal(verificationPayload.suggestedLots,1);
assert.equal(verificationPayload.estimatedLossAtSL,1000);
assert.equal(verificationPayload.estimatedProfitTP1,1250);
assert.equal(verificationPayload.estimatedProfitTP2,2100);
assert.deepEqual(verificationPayload.verification,{
  readOnly:true,persistence:'disabled',d1Writes:0,kvWrites:0,durableObjectWrites:0
});
assert.equal(snapshotCount(),0,'verification must not create an immutable D1 snapshot');
assert.equal(kvWrites,0,'verification must not write KV');
assert.equal(doWrites,0,'verification must not call a mutating Durable Object method');
assert.equal(riskRateLimits.size,riskRateLimitEntriesBeforeVerification,
  'verification authentication must not use the KV-backed write rate limiter');

const mismatchedSignalVerification=await verificationRequest(
  'signalId=5m%3Aother-official-signal',{'x-gsx-write-token':writeToken}
);
assert.equal(mismatchedSignalVerification.status,200);
assert.equal((await mismatchedSignalVerification.json()).reason,'signal_id_mismatch');
assert.equal(snapshotCount(),0);

const metadataFailureVerification=async reason=>{
  const env={
    ...integrationEnv,
    GOLD_FEED:{getByName:()=>({
      goldExposureStatus:async()=>integrationExposure,
      getRiskSizingMetadataStatus:async()=>({
        source:'mt5',available:false,fresh:false,status:'POSITION_SIZE_UNAVAILABLE',
        reason,sessionId:'mt5-risk-budget'
      })
    })}
  };
  const response=await verificationRequest(undefined,{'x-gsx-write-token':writeToken},env);
  assert.equal(response.status,200);
  const payload=await response.json();
  assert.equal(payload.available,false);
  assert.equal(payload.reason,reason);
  assert.equal(payload.suggestedLots,null);
  assert.equal(snapshotCount(),0);
};
await metadataFailureVerification('metadata_missing');
await metadataFailureVerification('metadata_stale');
await metadataFailureVerification('metadata_session_mismatch');
assert.equal(kvWrites,0);
assert.equal(doWrites,0);

const directReadOnly=await calculateOfficialActiveRiskSizingReadOnly(integrationEnv,{
  signalId:integrationSignal.id,accountBasis:'equity',accountValueUsd:100000,riskPercent:1
});
assert.equal(directReadOnly.available,true);
assert.equal(directReadOnly.verification.readOnly,true);
assert.equal(snapshotCount(),0);

const authorizedInitialization=await routeRequest(
  riskRequestBody,{'x-gsx-write-token':writeToken}
);
assert.equal(authorizedInitialization.status,200);
const authorizedPayload=await authorizedInitialization.json();
assert.equal(authorizedPayload.available,true);
assert.equal(authorizedPayload.riskBudget.status,'allowed');
assert.equal(snapshotCount(),1,'authorized initialization must create one immutable snapshot');

const conflictingInitialization=await routeRequest(
  riskRequestBody,{'x-gsx-write-token':writeToken},
  {...integrationEnv,RISK_ACCOUNT_VALUE_USD:'90000'}
);
assert.equal(conflictingInitialization.status,200);
const conflictingPayload=await conflictingInitialization.json();
assert.equal(conflictingPayload.riskBudget.reason,'risk_snapshot_immutable_mismatch');
assert.equal(integrationDb.database.prepare(
  'SELECT account_value_usd FROM production_signal_risk_snapshots WHERE signal_id=?'
).get(integrationSignal.id).account_value_usd,100000);

const unauthorizedReadAfterInitialization=await routeRequest();
assert.equal(unauthorizedReadAfterInitialization.status,401);
const unauthorizedPayload=await unauthorizedReadAfterInitialization.json();
assert.deepEqual(unauthorizedPayload,{ok:false,error:'unauthorized'});
assert.equal(JSON.stringify(unauthorizedPayload).includes('100000'),false,
  'unauthorized clients must not receive stored account or risk snapshot values');
const wrongTokenReadAfterInitialization=await routeRequest(
  riskRequestBody,{'x-gsx-write-token':'wrong-token'}
);
assert.equal(wrongTokenReadAfterInitialization.status,401);
assert.deepEqual(await wrongTokenReadAfterInitialization.json(),{ok:false,error:'unauthorized'});
const unsupportedReadMethod=await worker.default.fetch(
  new Request('https://example.com/risk-sizing',{headers:{Origin:allowedOrigin}}),
  integrationEnv,{}
);
assert.equal(unsupportedReadMethod.status,405);
assert.equal(JSON.stringify(await unsupportedReadMethod.json()).includes('100000'),false);

exposureReads=0;
metadataReads=0;
const integrated=await calculateOfficialActiveRiskSizing(integrationEnv,{
  signalId:integrationSignal.id,accountBasis:'equity',accountValueUsd:100000,riskPercent:1
});
assert.equal(integrated.available,true);
assert.equal(integrated.riskBudget.status,'allowed');
assert.equal(integrated.riskBudget.usage.activeExposureNotionalUsd,200000);
assert.equal(integrated.riskBudget.remaining.totalExposureUsd,300000);
assert.equal(kvWrites,0,'risk budget must not write KV trading state');
assert.equal(exposureReads,1);
assert.equal(metadataReads,1);
assert.equal(integrationDb.database.prepare(
  'SELECT COUNT(*) AS count FROM production_signal_risk_snapshots'
).get().count,1);

const storageUnavailable=await calculateOfficialActiveRiskSizing({...integrationEnv,GSX_DB:null},{
  signalId:integrationSignal.id,accountBasis:'equity',accountValueUsd:100000,riskPercent:1
});
assert.equal(storageUnavailable.available,true,'risk budget storage failure must not alter Phase 2 sizing');
assert.equal(storageUnavailable.riskBudget.status,'unavailable');
assert.equal(storageUnavailable.riskBudget.reason,'risk_snapshot_storage_unavailable');
assert.equal(kvWrites,0);

await calculateOfficialActiveRiskSizing(integrationEnv,{
  signalId:integrationSignal.id,accountBasis:'equity',accountValueUsd:100000,riskPercent:1
});
assert.equal(integrationDb.database.prepare(
  'SELECT COUNT(*) AS count FROM production_signal_risk_snapshots'
).get().count,1,'snapshot retries must be idempotent');

const changedInputs=await calculateOfficialActiveRiskSizing(integrationEnv,{
  signalId:integrationSignal.id,accountBasis:'equity',accountValueUsd:90000,riskPercent:1
});
assert.equal(changedInputs.available,true,'budget persistence must not alter Phase 2 sizing');
assert.equal(changedInputs.riskBudget.status,'unavailable');
assert.equal(changedInputs.riskBudget.reason,'risk_snapshot_immutable_mismatch');
assert.equal(integrationDb.database.prepare(
  'SELECT account_value_usd FROM production_signal_risk_snapshots WHERE signal_id=?'
).get(integrationSignal.id).account_value_usd,100000);

const priorSlSignal={
  ...integrationSignal,id:'15m:risk-budget:sl',tf:'15m',
  createdAt:now-60*60_000,updatedAt:now-60*60_000
};
await recordProductionPerformanceEvent({GSX_DB:integrationDb},priorSlSignal,'created');
const priorSizing={
  ...integrated,signal:{...integrated.signal,id:priorSlSignal.id,timeframe:'15m',createdAt:priorSlSignal.createdAt}
};
assert.equal((await storeImmutableRiskSnapshot(
  {GSX_DB:integrationDb},priorSizing,now-59*60_000
)).ok,true);
await recordProductionPerformanceEvent({GSX_DB:integrationDb},{
  ...priorSlSignal,status:'stopped',closedAt:now-30*60_000,
  updatedAt:now-30*60_000,lastPrice:priorSlSignal.sl
},'sl');
const storedLosses=await readRealizedLossRows({GSX_DB:integrationDb},{
  weekStart:periods.weekStart,asOf:now
});
assert.equal(storedLosses.rows.length,1);
assert.equal(storedLosses.rows[0].estimated_loss_at_sl,1000);

const legacySlSignal={
  ...integrationSignal,id:'30m:risk-budget:legacy-sl',tf:'30m',
  createdAt:now-2*60*60_000,updatedAt:now-2*60*60_000
};
await recordProductionPerformanceEvent({GSX_DB:integrationDb},legacySlSignal,'created');
await recordProductionPerformanceEvent({GSX_DB:integrationDb},{
  ...legacySlSignal,status:'stopped',closedAt:now-90*60_000,
  updatedAt:now-90*60_000,lastPrice:legacySlSignal.sl
},'sl');
const legacyUnavailable=await calculateOfficialActiveRiskSizing(integrationEnv,{
  signalId:integrationSignal.id,accountBasis:'equity',accountValueUsd:100000,riskPercent:1
});
assert.equal(legacyUnavailable.available,true);
assert.equal(legacyUnavailable.riskBudget.status,'unavailable');
assert.equal(legacyUnavailable.riskBudget.reason,'historical_risk_snapshot_missing');
assert.deepEqual(legacyUnavailable.riskBudget.usage.missingSnapshotSignalIds,[legacySlSignal.id]);
assert.equal(kvWrites,0,'no Signal/Exposure/Confirmation/Telegram state may be written');
Date.now=originalDateNow;

console.log('risk budget tests passed');
