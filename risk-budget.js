const RISK_BUDGET_UNAVAILABLE = 'unavailable';
const RISK_BUDGET_ALLOWED = 'allowed';
const RISK_BUDGET_LIMITED = 'limited';
const DAY_MS = 24 * 60 * 60 * 1000;

function finitePositive(value) {
  const number=Number(value);
  return Number.isFinite(number)&&number>0?number:null;
}

function roundMoney(value) {
  return Number.isFinite(Number(value))?Number(Number(value).toFixed(2)):null;
}

function normalizeRiskBudgetLimits(value={}) {
  const timezone=String(value.timezone||'UTC').trim().toUpperCase();
  return {
    timezone,
    maxRiskPercent:finitePositive(value.maxRiskPercent),
    maxDailyLossUsd:finitePositive(value.maxDailyLossUsd),
    maxWeeklyLossUsd:finitePositive(value.maxWeeklyLossUsd),
    maxTotalExposureUsd:finitePositive(value.maxTotalExposureUsd)
  };
}

function utcBudgetPeriodBounds(now=Date.now()) {
  const timestamp=Number(now);
  if (!Number.isFinite(timestamp)) return null;
  const date=new Date(timestamp);
  const dayStart=Date.UTC(date.getUTCFullYear(),date.getUTCMonth(),date.getUTCDate());
  const mondayOffset=(date.getUTCDay()+6)%7;
  return {
    timezone:'UTC',asOf:timestamp,
    dayStart,dayEnd:dayStart+DAY_MS,
    weekStart:dayStart-mondayOffset*DAY_MS,
    weekEnd:dayStart+(7-mondayOffset)*DAY_MS
  };
}

function unavailableRiskBudget(reason,options={}) {
  return {
    ok:true,advisory:true,measurementOnly:true,decisionUse:false,executionEnabled:false,
    available:false,allowed:null,status:RISK_BUDGET_UNAVAILABLE,
    reason:String(reason||'risk_budget_unavailable'),
    timezone:String(options.timezone||'UTC'),
    limits:options.limits||null,periods:options.periods||null,
    snapshot:options.snapshot||null,usage:options.usage||null,
    remaining:options.remaining||null,checks:options.checks||null
  };
}

function createImmutableRiskSnapshot(sizing,recordedAt=Date.now()) {
  const signal=sizing?.signal;
  const metadata=sizing?.metadataStatus?.metadata;
  const timeframe=String(signal?.timeframe||'');
  const side=String(signal?.side||'');
  const values={
    accountValueUsd:Number(sizing?.accountValueUsd),riskPercent:Number(sizing?.riskPercent),
    riskAmountUsd:Number(sizing?.riskAmount),suggestedLots:Number(sizing?.suggestedLots),
    estimatedLossAtSL:Number(sizing?.estimatedLossAtSL),entry:Number(signal?.entry),
    contractSize:Number(metadata?.contractSize),createdAt:Number(signal?.createdAt),
    metadataObservedAt:Number(metadata?.observedAt),recordedAt:Number(recordedAt)
  };
  if (!sizing?.available||!signal?.id||!timeframe||!['buy','sell'].includes(side)||
      !Object.values(values).every(Number.isFinite) ||
      values.accountValueUsd<=0||values.riskPercent<=0||values.riskAmountUsd<=0||
      values.suggestedLots<=0||values.estimatedLossAtSL<=0||values.entry<=0||
      values.contractSize<=0||values.createdAt<=0||values.metadataObservedAt<=0||
      values.recordedAt<=0) {
    return {ok:false,error:'invalid_risk_snapshot_source'};
  }
  if (values.estimatedLossAtSL>values.riskAmountUsd+0.01) {
    return {ok:false,error:'risk_snapshot_loss_exceeds_risk_amount'};
  }
  const metadataSessionId=String(metadata?.sessionId||'');
  if (!metadataSessionId) return {ok:false,error:'risk_snapshot_metadata_session_missing'};
  const notionalUsd=values.suggestedLots*values.contractSize*values.entry;
  if (!Number.isFinite(notionalUsd)||notionalUsd<=0) {
    return {ok:false,error:'risk_snapshot_notional_invalid'};
  }
  return {
    ok:true,
    snapshot:{
      schemaVersion:1,signalId:String(signal.id),timeframe,side,
      accountBasis:String(sizing.accountBasis||'equity'),
      accountValueUsd:roundMoney(values.accountValueUsd),riskPercent:Number(values.riskPercent.toFixed(4)),
      riskAmountUsd:roundMoney(values.riskAmountUsd),suggestedLots:values.suggestedLots,
      estimatedLossAtSL:roundMoney(values.estimatedLossAtSL),entry:values.entry,
      contractSize:values.contractSize,notionalUsd:roundMoney(notionalUsd),
      metadataSessionId,metadataObservedAt:values.metadataObservedAt,
      createdAt:values.createdAt,recordedAt:values.recordedAt,
      immutable:true,measurementOnly:true,decisionUse:false
    }
  };
}

function normalizeStoredRiskSnapshot(value) {
  if (!value||typeof value!=='object') return null;
  const snapshot={
    schemaVersion:Number(value.schemaVersion??value.schema_version),
    signalId:String(value.signalId??value.signal_id??''),
    timeframe:String(value.timeframe||''),side:String(value.side||''),
    accountBasis:String(value.accountBasis??value.account_basis??''),
    accountValueUsd:Number(value.accountValueUsd??value.account_value_usd),
    riskPercent:Number(value.riskPercent??value.risk_percent),
    riskAmountUsd:Number(value.riskAmountUsd??value.risk_amount_usd),
    suggestedLots:Number(value.suggestedLots??value.suggested_lots),
    estimatedLossAtSL:Number(value.estimatedLossAtSL??value.estimated_loss_at_sl),
    entry:Number(value.entry),contractSize:Number(value.contractSize??value.contract_size),
    notionalUsd:Number(value.notionalUsd??value.notional_usd),
    metadataSessionId:String(value.metadataSessionId??value.metadata_session_id??''),
    metadataObservedAt:Number(value.metadataObservedAt??value.metadata_observed_at),
    createdAt:Number(value.createdAt??value.created_at),
    recordedAt:Number(value.recordedAt??value.recorded_at),
    immutable:true,measurementOnly:true,decisionUse:false
  };
  const positive=['accountValueUsd','riskPercent','riskAmountUsd','suggestedLots','estimatedLossAtSL',
    'entry','contractSize','notionalUsd','metadataObservedAt','createdAt','recordedAt'];
  if (snapshot.schemaVersion!==1||!snapshot.signalId||!snapshot.timeframe||
      !['buy','sell'].includes(snapshot.side)||!snapshot.metadataSessionId||
      !positive.every(field=>Number.isFinite(snapshot[field])&&snapshot[field]>0)||
      !['balance','equity'].includes(snapshot.accountBasis)) return null;
  const expectedNotional=snapshot.suggestedLots*snapshot.contractSize*snapshot.entry;
  if (Math.abs(expectedNotional-snapshot.notionalUsd)>0.02||
      snapshot.estimatedLossAtSL>snapshot.riskAmountUsd+0.01) return null;
  return snapshot;
}

function summarizeRealizedLossRows(rows,periods) {
  if (!Array.isArray(rows)||!periods) return {ok:false,error:'realized_loss_data_unavailable'};
  const seen=new Set();
  let dailyLossUsd=0,weeklyLossUsd=0,dailyLossCount=0,weeklyLossCount=0;
  const missingSnapshotSignalIds=[];
  for (const row of rows) {
    const signalId=String(row?.signalId??row?.signal_id??'');
    const finalStatus=String(row?.finalStatus??row?.final_status??'');
    const closedAt=Number(row?.closedAt??row?.closed_at);
    if (!signalId||seen.has(signalId)) return {ok:false,error:'duplicate_realized_loss_signal'};
    seen.add(signalId);
    if (finalStatus!=='sl'||!Number.isFinite(closedAt)||closedAt<periods.weekStart||closedAt>periods.asOf) {
      return {ok:false,error:'inconsistent_realized_loss_record'};
    }
    const lossUsd=Number(row?.estimatedLossAtSL??row?.estimated_loss_at_sl);
    if (!Number.isFinite(lossUsd)||lossUsd<=0) {
      missingSnapshotSignalIds.push(signalId);
      continue;
    }
    weeklyLossUsd+=lossUsd;
    weeklyLossCount+=1;
    if (closedAt>=periods.dayStart) {
      dailyLossUsd+=lossUsd;
      dailyLossCount+=1;
    }
  }
  if (missingSnapshotSignalIds.length) {
    return {ok:false,error:'historical_risk_snapshot_missing',missingSnapshotSignalIds};
  }
  return {
    ok:true,dailyLossUsd:roundMoney(dailyLossUsd),weeklyLossUsd:roundMoney(weeklyLossUsd),
    dailyLossCount,weeklyLossCount,signalIds:[...seen],missingSnapshotSignalIds:[]
  };
}

function budgetCheck(limit,used) {
  const remaining=Math.max(0,Number(limit)-Number(used));
  const limited=Number(used)>=Number(limit)-0.005;
  return {
    status:limited?RISK_BUDGET_LIMITED:RISK_BUDGET_ALLOWED,
    limitUsd:roundMoney(limit),usedUsd:roundMoney(used),remainingUsd:roundMoney(remaining),
    atLimit:Math.abs(Number(used)-Number(limit))<0.005,exceeded:Number(used)>Number(limit)+0.005
  };
}

function calculateRiskBudget(input={}) {
  const limits=normalizeRiskBudgetLimits(input.limits);
  const periods=utcBudgetPeriodBounds(input.now);
  if (limits.timezone!=='UTC') return unavailableRiskBudget('unsupported_risk_budget_timezone',{limits});
  if (!periods) return unavailableRiskBudget('invalid_risk_budget_time',{limits});
  const missingLimits=[];
  for (const field of ['maxRiskPercent','maxDailyLossUsd','maxWeeklyLossUsd','maxTotalExposureUsd']) {
    if (!limits[field]) missingLimits.push(field);
  }
  if (missingLimits.length) {
    return unavailableRiskBudget('risk_budget_limits_unconfigured',{
      limits,periods,usage:{missingLimits}
    });
  }
  const snapshot=normalizeStoredRiskSnapshot(input.snapshot);
  if (!snapshot) return unavailableRiskBudget('risk_snapshot_missing_or_invalid',{limits,periods});
  const exposure=input.exposure&&typeof input.exposure==='object'?input.exposure:null;
  if (!exposure||exposure.status!=='active'||
      String(exposure.primarySignalId||'')!==snapshot.signalId||
      String(exposure.primaryTf||'')!==snapshot.timeframe) {
    return unavailableRiskBudget('active_exposure_snapshot_mismatch',{limits,periods,snapshot});
  }
  const realized=summarizeRealizedLossRows(input.realizedLossRows,periods);
  if (!realized.ok) {
    return unavailableRiskBudget(realized.error,{limits,periods,snapshot,usage:realized});
  }
  if (realized.signalIds.includes(snapshot.signalId)) {
    return unavailableRiskBudget('active_exposure_already_realized',{limits,periods,snapshot,usage:realized});
  }
  const maxTradeRiskUsd=snapshot.accountValueUsd*limits.maxRiskPercent/100;
  const activeRiskAtSLUsd=snapshot.estimatedLossAtSL;
  const projectedDailyLossAtSLUsd=realized.dailyLossUsd+activeRiskAtSLUsd;
  const projectedWeeklyLossAtSLUsd=realized.weeklyLossUsd+activeRiskAtSLUsd;
  const checks={
    perTrade:budgetCheck(maxTradeRiskUsd,activeRiskAtSLUsd),
    dailyLoss:budgetCheck(limits.maxDailyLossUsd,projectedDailyLossAtSLUsd),
    weeklyLoss:budgetCheck(limits.maxWeeklyLossUsd,projectedWeeklyLossAtSLUsd),
    totalExposure:budgetCheck(limits.maxTotalExposureUsd,snapshot.notionalUsd)
  };
  const limitedReasons=Object.entries(checks)
    .filter(([,check])=>check.status===RISK_BUDGET_LIMITED)
    .map(([name])=>`${name}_limit_reached`);
  const status=limitedReasons.length?RISK_BUDGET_LIMITED:RISK_BUDGET_ALLOWED;
  const remaining={
    maxAdditionalRiskUsd:roundMoney(Math.min(
      checks.perTrade.remainingUsd,checks.dailyLoss.remainingUsd,checks.weeklyLoss.remainingUsd
    )),
    dailyLossUsd:checks.dailyLoss.remainingUsd,
    weeklyLossUsd:checks.weeklyLoss.remainingUsd,
    totalExposureUsd:checks.totalExposure.remainingUsd
  };
  return {
    ok:true,advisory:true,measurementOnly:true,decisionUse:false,executionEnabled:false,
    available:true,allowed:status===RISK_BUDGET_ALLOWED,status,
    reason:limitedReasons.join(','),timezone:'UTC',limits,periods,snapshot,
    usage:{
      realizedDailyLossUsd:realized.dailyLossUsd,
      realizedWeeklyLossUsd:realized.weeklyLossUsd,
      activeRiskAtSLUsd:roundMoney(activeRiskAtSLUsd),
      projectedDailyLossAtSLUsd:roundMoney(projectedDailyLossAtSLUsd),
      projectedWeeklyLossAtSLUsd:roundMoney(projectedWeeklyLossAtSLUsd),
      activeExposureNotionalUsd:roundMoney(snapshot.notionalUsd),
      dailyLossCount:realized.dailyLossCount,weeklyLossCount:realized.weeklyLossCount
    },
    remaining,checks
  };
}

export {
  RISK_BUDGET_UNAVAILABLE,RISK_BUDGET_ALLOWED,RISK_BUDGET_LIMITED,
  normalizeRiskBudgetLimits,utcBudgetPeriodBounds,unavailableRiskBudget,
  createImmutableRiskSnapshot,normalizeStoredRiskSnapshot,summarizeRealizedLossRows,
  calculateRiskBudget
};
