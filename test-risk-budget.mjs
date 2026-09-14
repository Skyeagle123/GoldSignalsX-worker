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
  storeImmutableRiskSnapshot,readRealizedLossRows,calculateOfficialActiveRiskSizing
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
let kvWrites=0,exposureReads=0,metadataReads=0;
const integrationEnv={
  GSX_DB:integrationDb,RISK_MAX_PERCENT:'2',RISK_MAX_LOTS:'5',
  RISK_METADATA_MAX_AGE_MS:'900000',RISK_MAX_DAILY_LOSS_USD:'2000',
  RISK_MAX_WEEKLY_LOSS_USD:'5000',RISK_MAX_TOTAL_EXPOSURE_USD:'500000',
  RISK_BUDGET_TIMEZONE:'UTC',
  GOLD_FEED:{getByName:()=>({
    goldExposureStatus:async()=>{exposureReads+=1;return integrationExposure;},
    getRiskSizingMetadataStatus:async()=>{metadataReads+=1;return integrationMetadata;}
  })},
  GSX_KV:{
    get:async key=>key==='signal:state:5m'?integrationSignal:null,
    put:async()=>{kvWrites+=1;}
  }
};
const originalDateNow=Date.now;
Date.now=()=>now;
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
