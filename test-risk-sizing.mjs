import assert from 'node:assert/strict';
import {
  calculateRiskSizing,
  mt5RiskMetadataStatus,
  normalizeMt5SymbolMetadata,
  normalizeRiskSizingLimits
} from './risk-sizing.js';

const now=Date.UTC(2026,8,14,12,0,0);
const rawMetadata={
  source:'mt5',canonicalSymbol:'XAUUSD',brokerSymbol:'XAUUSDs',
  sessionId:'risk-session',observedAt:now,
  contractSize:100,tickSize:0.01,tickValue:1,tickValueProfit:1,tickValueLoss:1,
  volumeMin:0.01,volumeMax:100,volumeStep:0.01,volumeLimit:0,
  point:0.01,digits:2,tradeStopsLevel:0,
  profitCurrency:'USD',accountCurrency:'USD',tradeCalcMode:0,tradeMode:4
};
const normalized=normalizeMt5SymbolMetadata(rawMetadata,{
  receivedAt:now,sessionId:'risk-session'
});
assert.equal(normalized.ok,true);
assert.equal(normalized.metadata.brokerSymbol,'XAUUSDs');
assert.equal(normalizeMt5SymbolMetadata(
  {...rawMetadata,sessionId:'other'},{receivedAt:now,sessionId:'risk-session'}
).error,'metadata_session_mismatch');
assert.equal(normalizeMt5SymbolMetadata(
  {...rawMetadata,observedAt:now-15*60_000-1},{receivedAt:now,sessionId:'risk-session'}
).error,'metadata_stale');
assert.equal(normalizeMt5SymbolMetadata(
  {...rawMetadata,tickValueLoss:2},{receivedAt:now,sessionId:'risk-session'}
).error,'metadata_inconsistent_tick_value');

const verifiedMetadata=mt5RiskMetadataStatus(normalized.metadata,{
  now,sessionId:'risk-session',mt5Healthy:true
});
assert.equal(verifiedMetadata.available,true);
assert.equal(mt5RiskMetadataStatus(normalized.metadata,{
  now:now+15*60_000+1,sessionId:'risk-session',mt5Healthy:true
}).reason,'metadata_stale');
assert.equal(mt5RiskMetadataStatus(normalized.metadata,{
  now,sessionId:'risk-session',mt5Healthy:false
}).reason,'mt5_not_healthy');

const buySignal={
  id:'5m:buy-risk',tf:'5m',side:'buy',status:'active',origin:'server',createdAt:now,
  entry:2000,sl:1990,tp1:2012.5,tp2:2021
};
const buyExposure={status:'active',primarySignalId:buySignal.id,primaryTf:'5m'};
const buy=calculateRiskSizing({
  signalId:buySignal.id,signal:buySignal,exposure:buyExposure,
  accountBasis:'equity',accountValueUsd:100000,riskPercent:1,
  metadataStatus:verifiedMetadata
});
assert.equal(buy.available,true);
assert.equal(buy.riskAmount,1000);
assert.equal(buy.slDistance,10);
assert.equal(buy.suggestedLots,1);
assert.equal(buy.estimatedLossAtSL,1000);
assert.equal(buy.estimatedProfitTP1,1250);
assert.equal(buy.estimatedProfitTP2,2100);
assert.equal(buy.rrTP1,1.25);
assert.equal(buy.rrTP2,2.1);
assert.equal(buy.capApplied,false);
assert.deepEqual(buy.estimatesExclude,['commission','slippage','swap']);
assert.equal(buy.measurementOnly,true);
assert.equal(buy.decisionUse,false);
assert.equal(buy.executionEnabled,false);

const sellSignal={
  id:'15m:sell-risk',tf:'15m',side:'sell',status:'tp1',origin:'server',createdAt:now,
  entry:2000,sl:2008,tp1:1990,tp2:1984
};
const sell=calculateRiskSizing({
  signalId:sellSignal.id,signal:sellSignal,
  exposure:{status:'active',primarySignalId:sellSignal.id,primaryTf:'15m'},
  accountBasis:'balance',accountValueUsd:50000,riskPercent:0.5,
  metadataStatus:verifiedMetadata
});
assert.equal(sell.available,true);
assert.equal(sell.riskAmount,250);
assert.equal(sell.suggestedLots,0.31,'lots must round down to volumeStep');
assert.equal(sell.estimatedLossAtSL,248);
assert.equal(sell.estimatedProfitTP1,310);
assert.equal(sell.estimatedProfitTP2,496);
assert.equal(sell.rrTP1,1.25);
assert.equal(sell.rrTP2,2);

const rounded=calculateRiskSizing({
  signalId:buySignal.id,signal:buySignal,exposure:buyExposure,
  accountValueUsd:12345,riskPercent:1,metadataStatus:verifiedMetadata
});
assert.equal(rounded.rawLots,0.12345);
assert.equal(rounded.suggestedLots,0.12);
assert.ok(rounded.estimatedLossAtSL<=rounded.riskAmount);

const capped=calculateRiskSizing({
  signalId:buySignal.id,
  signal:{...buySignal,sl:1999,tp1:2001.25,tp2:2002.1},
  exposure:buyExposure,accountValueUsd:100000,riskPercent:2,
  metadataStatus:verifiedMetadata,limits:{maxRiskPercent:2,maxLots:2}
});
assert.equal(capped.available,true);
assert.equal(capped.rawLots,20);
assert.equal(capped.suggestedLots,2);
assert.equal(capped.capApplied,true);
assert.equal(capped.estimatedLossAtSL,200);

const belowMinimum=calculateRiskSizing({
  signalId:buySignal.id,signal:buySignal,exposure:buyExposure,
  accountValueUsd:500,riskPercent:1,metadataStatus:verifiedMetadata
});
assert.equal(belowMinimum.status,'POSITION_SIZE_UNAVAILABLE');
assert.equal(belowMinimum.reason,'raw_lots_below_volume_min');

assert.equal(calculateRiskSizing({
  signalId:buySignal.id,signal:buySignal,exposure:{status:'flat'},
  accountValueUsd:100000,riskPercent:1,metadataStatus:verifiedMetadata
}).reason,'no_active_official_signal');
assert.equal(calculateRiskSizing({
  signalId:'wrong',signal:buySignal,exposure:buyExposure,
  accountValueUsd:100000,riskPercent:1,metadataStatus:verifiedMetadata
}).reason,'signal_id_mismatch');
assert.equal(calculateRiskSizing({
  signalId:buySignal.id,signal:buySignal,exposure:buyExposure,
  accountValueUsd:100000,riskPercent:3,metadataStatus:verifiedMetadata
}).reason,'risk_percent_above_cap');
assert.equal(calculateRiskSizing({
  signalId:buySignal.id,signal:buySignal,exposure:buyExposure,
  accountValueUsd:100000,riskPercent:1,
  metadataStatus:{available:false,fresh:false,reason:'metadata_stale'}
}).reason,'metadata_stale');

const stoppedMetadata=normalizeMt5SymbolMetadata(
  {...rawMetadata,tradeStopsLevel:200},{receivedAt:now,sessionId:'risk-session'}
);
assert.equal(stoppedMetadata.ok,true);
const stoppedStatus=mt5RiskMetadataStatus(stoppedMetadata.metadata,{
  now,sessionId:'risk-session',mt5Healthy:true
});
assert.equal(calculateRiskSizing({
  signalId:buySignal.id,
  signal:{...buySignal,sl:1999},exposure:buyExposure,
  accountValueUsd:100000,riskPercent:1,metadataStatus:stoppedStatus
}).reason,'sl_distance_below_broker_minimum');

assert.deepEqual(normalizeRiskSizingLimits({maxRiskPercent:1,maxLots:2}),{
  maxRiskPercent:1,maxLots:2,metadataMaxAgeMs:900000,maxAccountValueUsd:1000000000
});

console.log('risk sizing tests passed');
