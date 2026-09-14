const POSITION_SIZE_UNAVAILABLE = 'POSITION_SIZE_UNAVAILABLE';
const RISK_ESTIMATE_EXCLUSIONS = Object.freeze(['commission','slippage','swap']);
const DEFAULT_RISK_MAX_PERCENT = 2;
const DEFAULT_RISK_MAX_LOTS = 5;
const DEFAULT_METADATA_MAX_AGE_MS = 15 * 60 * 1000;
const DEFAULT_MAX_ACCOUNT_VALUE_USD = 1_000_000_000;
const FUTURE_TOLERANCE_MS = 30 * 1000;
const SUPPORTED_TICK_VALUE_CALC_MODES = Object.freeze([
  'SYMBOL_CALC_MODE_FOREX',
  'SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE',
  'SYMBOL_CALC_MODE_FUTURES',
  'SYMBOL_CALC_MODE_CFD',
  'SYMBOL_CALC_MODE_CFDINDEX',
  'SYMBOL_CALC_MODE_CFDLEVERAGE',
  'SYMBOL_CALC_MODE_EXCH_STOCKS',
  'SYMBOL_CALC_MODE_EXCH_FUTURES',
  'SYMBOL_CALC_MODE_EXCH_FUTURES_FORTS',
  'SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX'
]);


function finitePositive(value) {
  const number=Number(value);
  return Number.isFinite(number)&&number>0?number:NaN;
}

function decimalPlaces(value) {
  const text=String(value);
  if (/e-/i.test(text)) return Math.min(8,Number(text.split(/e-/i)[1])||0);
  const dot=text.indexOf('.');
  return dot<0?0:Math.min(8,text.length-dot-1);
}

function roundTo(value,places=8) {
  if (!Number.isFinite(Number(value))) return null;
  const factor=10**Math.max(0,Math.min(8,Number(places)||0));
  return Math.round((Number(value)+Number.EPSILON)*factor)/factor;
}

function floorToStep(value,step) {
  const amount=Number(value),increment=Number(step);
  if (!Number.isFinite(amount)||!Number.isFinite(increment)||amount<=0||increment<=0) return NaN;
  const places=decimalPlaces(increment);
  const units=Math.floor((amount+increment*1e-9)/increment);
  return roundTo(units*increment,places);
}

function normalizeCurrency(value) {
  const currency=String(value||'').trim().toUpperCase();
  return /^[A-Z]{3,6}$/.test(currency)?currency:'';
}

function normalizeCanonicalSymbol(value) {
  return String(value||'').toUpperCase().replace(/[^A-Z]/g,'');
}

function metadataFieldError(metadata) {
  if (!metadata||typeof metadata!=='object') return 'metadata_missing';
  const positive=[
    'contractSize','tickSize','tickValue','tickValueProfit','tickValueLoss',
    'volumeMin','volumeMax','volumeStep','point'
  ];
  for (const field of positive) {
    if (!Number.isFinite(Number(metadata[field]))||Number(metadata[field])<=0) {
      return 'metadata_invalid_'+field;
    }
  }
  if (!Number.isFinite(Number(metadata.volumeLimit))||Number(metadata.volumeLimit)<0) {
    return 'metadata_invalid_volumeLimit';
  }
  if (!Number.isInteger(Number(metadata.digits))||Number(metadata.digits)<0||Number(metadata.digits)>10) {
    return 'metadata_invalid_digits';
  }
  if (!Number.isInteger(Number(metadata.tradeStopsLevel))||Number(metadata.tradeStopsLevel)<0) {
    return 'metadata_invalid_tradeStopsLevel';
  }
  if (!SUPPORTED_TICK_VALUE_CALC_MODES.includes(String(metadata.tradeCalcMode||''))) {
    return 'metadata_unsupported_trade_calc_mode';
  }
  if (!Number.isInteger(Number(metadata.tradeMode))||![0,1,2,3,4].includes(Number(metadata.tradeMode))) {
    return 'metadata_unsupported_trade_mode';
  }
  if (Number(metadata.volumeMax)<Number(metadata.volumeMin)||
      Number(metadata.volumeStep)>Number(metadata.volumeMax)||
      (Number(metadata.volumeLimit)>0&&Number(metadata.volumeLimit)<Number(metadata.volumeMin))) {
    return 'metadata_inconsistent_volume_limits';
  }
  if (normalizeCanonicalSymbol(metadata.canonicalSymbol)!=='XAUUSD') return 'metadata_bad_canonical_symbol';
  if (!/^[A-Za-z0-9._-]{1,40}$/.test(String(metadata.brokerSymbol||''))) return 'metadata_bad_broker_symbol';
  if (normalizeCurrency(metadata.profitCurrency)!=='USD') return 'metadata_profit_currency_not_usd';
  if (normalizeCurrency(metadata.accountCurrency)!=='USD') return 'metadata_account_currency_not_usd';
  return '';
}

function metadataConsistencyError(metadata) {
  const expected=Number(metadata.contractSize)*Number(metadata.tickSize);
  const values=[Number(metadata.tickValue),Number(metadata.tickValueProfit),Number(metadata.tickValueLoss)];
  if (!Number.isFinite(expected)||expected<=0) return 'metadata_inconsistent_contract_tick';
  for (const value of values) {
    const delta=Math.abs(value-expected)/Math.max(expected,value);
    if (!Number.isFinite(delta)||delta>0.10) return 'metadata_inconsistent_tick_value';
  }
  const tickPointRatio=Number(metadata.tickSize)/Number(metadata.point);
  if (!Number.isFinite(tickPointRatio)||tickPointRatio<1-1e-9) return 'metadata_tick_smaller_than_point';
  return '';
}

function normalizeMt5SymbolMetadata(value,options={}) {
  const receivedAt=Number(options.receivedAt??Date.now());
  const expectedSessionId=String(options.sessionId||'');
  const maxAgeMs=finitePositive(options.maxAgeMs)||DEFAULT_METADATA_MAX_AGE_MS;
  const sessionId=String(value?.sessionId||'').trim();
  const observedAt=Number(value?.observedAt);
  if (!expectedSessionId||sessionId!==expectedSessionId) {
    return {ok:false,error:'metadata_session_mismatch'};
  }
  if (String(value?.source||'').toLowerCase()!=='mt5') return {ok:false,error:'metadata_bad_source'};
  if (!Number.isFinite(receivedAt)||!Number.isFinite(observedAt)) {
    return {ok:false,error:'metadata_bad_timestamp'};
  }
  const ageMs=receivedAt-observedAt;
  if (ageMs<-FUTURE_TOLERANCE_MS||ageMs>maxAgeMs) return {ok:false,error:'metadata_stale'};
  const metadata={
    source:'mt5',
    canonicalSymbol:normalizeCanonicalSymbol(value?.canonicalSymbol),
    brokerSymbol:String(value?.brokerSymbol||'').trim(),
    sessionId,
    observedAt,
    receivedAt,
    contractSize:Number(value?.contractSize),
    tickSize:Number(value?.tickSize),
    tickValue:Number(value?.tickValue),
    tickValueProfit:Number(value?.tickValueProfit),
    tickValueLoss:Number(value?.tickValueLoss),
    volumeMin:Number(value?.volumeMin),
    volumeMax:Number(value?.volumeMax),
    volumeStep:Number(value?.volumeStep),
    volumeLimit:Number(value?.volumeLimit),
    point:Number(value?.point),
    digits:Number(value?.digits),
    tradeStopsLevel:Number(value?.tradeStopsLevel),
    profitCurrency:normalizeCurrency(value?.profitCurrency),
    accountCurrency:normalizeCurrency(value?.accountCurrency),
    tradeCalcMode:String(value?.tradeCalcMode||'').trim().toUpperCase(),
    tradeMode:Number.isFinite(Number(value?.tradeMode))?Number(value.tradeMode):null
  };
  const error=metadataFieldError(metadata)||metadataConsistencyError(metadata);
  return error?{ok:false,error}:{ok:true,metadata};
}

function mt5RiskMetadataStatus(metadata,options={}) {
  const now=Number(options.now??Date.now());
  const sessionId=String(options.sessionId||'');
  const maxAgeMs=finitePositive(options.maxAgeMs)||DEFAULT_METADATA_MAX_AGE_MS;
  const ageMs=now-Number(metadata?.observedAt);
  const base={
    source:'mt5',fresh:false,available:false,status:POSITION_SIZE_UNAVAILABLE,
    sessionId,observedAt:Number(metadata?.observedAt)||null,
    ageMs:Number.isFinite(ageMs)?Math.max(0,ageMs):null
  };
  if (!metadata) return {...base,reason:'metadata_missing'};
  if (!sessionId||String(metadata.sessionId)!==sessionId) return {...base,reason:'metadata_session_mismatch'};
  if (!options.mt5Healthy) return {...base,reason:'mt5_not_healthy'};
  if (!Number.isFinite(ageMs)||ageMs< -FUTURE_TOLERANCE_MS||ageMs>maxAgeMs) {
    return {...base,reason:'metadata_stale'};
  }
  const error=metadataFieldError(metadata)||metadataConsistencyError(metadata);
  if (error) return {...base,reason:error};
  return {
    ...base,available:true,fresh:true,status:'MT5_METADATA_VERIFIED',
    reason:'',metadata:{...metadata}
  };
}

function normalizeRiskSizingLimits(value={}) {
  const maxRiskPercent=finitePositive(value.maxRiskPercent)||DEFAULT_RISK_MAX_PERCENT;
  const maxLots=finitePositive(value.maxLots)||DEFAULT_RISK_MAX_LOTS;
  const metadataMaxAgeMs=finitePositive(value.metadataMaxAgeMs)||DEFAULT_METADATA_MAX_AGE_MS;
  const maxAccountValueUsd=finitePositive(value.maxAccountValueUsd)||DEFAULT_MAX_ACCOUNT_VALUE_USD;
  return {maxRiskPercent,maxLots,metadataMaxAgeMs,maxAccountValueUsd};
}

function baseResult(metadataStatus=null) {
  return {
    ok:true,advisory:true,measurementOnly:true,decisionUse:false,executionEnabled:false,
    estimatesExclude:[...RISK_ESTIMATE_EXCLUSIONS],
    metadataStatus:metadataStatus||{
      source:'mt5',fresh:false,available:false,status:POSITION_SIZE_UNAVAILABLE,reason:'metadata_missing'
    }
  };
}

function positionSizeUnavailable(reason,options={}) {
  return {
    ...baseResult(options.metadataStatus),
    available:false,status:POSITION_SIZE_UNAVAILABLE,reason:String(reason||'unavailable'),
    signal:options.signal||null,
    riskPercent:Number.isFinite(Number(options.riskPercent))?Number(options.riskPercent):null,
    riskAmount:Number.isFinite(Number(options.riskAmount))?roundTo(options.riskAmount,2):null,
    slDistance:Number.isFinite(Number(options.slDistance))?Number(options.slDistance):null,
    suggestedLots:null,estimatedLossAtSL:null,estimatedProfitTP1:null,estimatedProfitTP2:null,
    rrTP1:Number.isFinite(Number(options.rrTP1))?roundTo(options.rrTP1,4):null,
    rrTP2:Number.isFinite(Number(options.rrTP2))?roundTo(options.rrTP2,4):null,
    capApplied:false
  };
}

function calculateRiskSizing(input={}) {
  const signal=input.signal&&typeof input.signal==='object'?input.signal:null;
  const exposure=input.exposure&&typeof input.exposure==='object'?input.exposure:null;
  const requestedSignalId=String(input.signalId||'');
  const limits=normalizeRiskSizingLimits(input.limits);
  const metadataStatus=input.metadataStatus&&typeof input.metadataStatus==='object'
    ?input.metadataStatus:null;
  const signalSummary=signal?{
    id:String(signal.id||''),timeframe:String(signal.tf||''),side:String(signal.side||''),
    status:String(signal.status||''),createdAt:Number(signal.createdAt)||null,
    entry:Number(signal.entry),tp1:Number(signal.tp1),tp2:Number(signal.tp2),sl:Number(signal.sl)
  }:null;

  if (!signal||!exposure||exposure.status!=='active') {
    return positionSizeUnavailable('no_active_official_signal',{metadataStatus,signal:signalSummary});
  }
  if (!requestedSignalId||requestedSignalId!==String(signal.id||'')) {
    return positionSizeUnavailable('signal_id_mismatch',{metadataStatus,signal:signalSummary});
  }
  if (!['active','tp1'].includes(String(signal.status||''))||
      String(exposure.primarySignalId||'')!==String(signal.id||'')||
      String(exposure.primaryTf||'')!==String(signal.tf||'')||
      String(signal.origin||'')!=='server') {
    return positionSizeUnavailable('official_signal_exposure_mismatch',{metadataStatus,signal:signalSummary});
  }
  const side=String(signal.side||''),entry=Number(signal.entry),sl=Number(signal.sl);
  const tp1=Number(signal.tp1),tp2=Number(signal.tp2);
  if (!['buy','sell'].includes(side)||![entry,sl,tp1,tp2].every(Number.isFinite)) {
    return positionSizeUnavailable('invalid_signal_levels',{metadataStatus,signal:signalSummary});
  }
  const logicalLevels=side==='buy'
    ?sl<entry&&tp1>entry&&tp2>=tp1
    :sl>entry&&tp1<entry&&tp2<=tp1;
  if (!logicalLevels) return positionSizeUnavailable('invalid_signal_level_order',{metadataStatus,signal:signalSummary});

  const slDistance=Math.abs(entry-sl);
  const tp1Distance=Math.abs(tp1-entry),tp2Distance=Math.abs(tp2-entry);
  const accountValueUsd=Number(input.accountValueUsd);
  const riskPercent=Number(input.riskPercent);
  const partial={metadataStatus,signal:signalSummary,riskPercent,slDistance};
  if (!Number.isFinite(accountValueUsd)||accountValueUsd<=0||accountValueUsd>limits.maxAccountValueUsd) {
    return positionSizeUnavailable('invalid_account_value',partial);
  }
  if (!Number.isFinite(riskPercent)||riskPercent<=0) {
    return positionSizeUnavailable('invalid_risk_percent',partial);
  }
  if (riskPercent>limits.maxRiskPercent) {
    return positionSizeUnavailable('risk_percent_above_cap',partial);
  }
  const riskAmount=accountValueUsd*riskPercent/100;
  partial.riskAmount=riskAmount;
  if (!metadataStatus?.available||!metadataStatus?.fresh||!metadataStatus?.metadata) {
    return positionSizeUnavailable(metadataStatus?.reason||'metadata_unavailable',partial);
  }
  const metadata=metadataStatus.metadata;
  const metadataError=metadataFieldError(metadata)||metadataConsistencyError(metadata);
  if (metadataError) return positionSizeUnavailable(metadataError,partial);
  const tradeMode=Number(metadata.tradeMode);
  if (tradeMode===0) return positionSizeUnavailable('trade_mode_disabled',partial);
  if (tradeMode===3) return positionSizeUnavailable('trade_mode_close_only',partial);
  if (side==='buy'&&tradeMode===2) return positionSizeUnavailable('trade_mode_buy_not_allowed',partial);
  if (side==='sell'&&tradeMode===1) return positionSizeUnavailable('trade_mode_sell_not_allowed',partial);

  const brokerMinStop=Number(metadata.tradeStopsLevel)*Number(metadata.point);
  if (!Number.isFinite(slDistance)||slDistance<=0||
      slDistance+Number.EPSILON<Number(metadata.tickSize)||
      slDistance+Number.EPSILON<brokerMinStop) {
    return positionSizeUnavailable('sl_distance_below_broker_minimum',partial);
  }

  const slTicks=slDistance/Number(metadata.tickSize);
  const lossPerLot=slTicks*Number(metadata.tickValueLoss);
  if (!Number.isFinite(slTicks)||slTicks<1||!Number.isFinite(lossPerLot)||lossPerLot<=0) {
    return positionSizeUnavailable('invalid_loss_per_lot',partial);
  }
  const rawLots=riskAmount/lossPerLot;
  const brokerLimit=Number(metadata.volumeLimit)>0
    ?Math.min(Number(metadata.volumeMax),Number(metadata.volumeLimit))
    :Number(metadata.volumeMax);
  const effectiveMaxLots=Math.min(brokerLimit,limits.maxLots);
  if (!Number.isFinite(effectiveMaxLots)||effectiveMaxLots<Number(metadata.volumeMin)) {
    return positionSizeUnavailable('volume_caps_below_broker_minimum',partial);
  }
  const capApplied=rawLots>effectiveMaxLots;
  const lotsBeforeRounding=Math.min(rawLots,effectiveMaxLots);
  const suggestedLots=floorToStep(lotsBeforeRounding,Number(metadata.volumeStep));
  if (!Number.isFinite(suggestedLots)||suggestedLots<Number(metadata.volumeMin)) {
    return positionSizeUnavailable('raw_lots_below_volume_min',partial);
  }
  const estimatedLossAtSL=suggestedLots*lossPerLot;
  if (!Number.isFinite(estimatedLossAtSL)||estimatedLossAtSL>riskAmount+0.01) {
    return positionSizeUnavailable('rounded_loss_exceeds_risk_amount',partial);
  }
  const estimatedProfitTP1=suggestedLots*(tp1Distance/Number(metadata.tickSize))*Number(metadata.tickValueProfit);
  const estimatedProfitTP2=suggestedLots*(tp2Distance/Number(metadata.tickSize))*Number(metadata.tickValueProfit);
  const rrTP1=estimatedProfitTP1/estimatedLossAtSL;
  const rrTP2=estimatedProfitTP2/estimatedLossAtSL;

  return {
    ...baseResult(metadataStatus),available:true,status:'POSITION_SIZE_AVAILABLE',reason:'',
    signal:signalSummary,accountBasis:['balance','equity'].includes(String(input.accountBasis))
      ?String(input.accountBasis):'equity',
    accountValueUsd:roundTo(accountValueUsd,2),
    riskPercent:roundTo(riskPercent,4),riskAmount:roundTo(riskAmount,2),
    slDistance:roundTo(slDistance,Number(metadata.digits)),
    slTicks:roundTo(slTicks,4),lossPerLot:roundTo(lossPerLot,2),
    rawLots:roundTo(rawLots,8),suggestedLots,
    estimatedLossAtSL:roundTo(estimatedLossAtSL,2),
    estimatedProfitTP1:roundTo(estimatedProfitTP1,2),
    estimatedProfitTP2:roundTo(estimatedProfitTP2,2),
    rrTP1:roundTo(rrTP1,4),rrTP2:roundTo(rrTP2,4),
    capApplied,rounding:'floor_to_volume_step',
    limits:{...limits,effectiveMaxLots:roundTo(effectiveMaxLots,decimalPlaces(metadata.volumeStep))}
  };
}

export {
  POSITION_SIZE_UNAVAILABLE,RISK_ESTIMATE_EXCLUSIONS,
  normalizeMt5SymbolMetadata,mt5RiskMetadataStatus,normalizeRiskSizingLimits,
  calculateRiskSizing,positionSizeUnavailable
};
