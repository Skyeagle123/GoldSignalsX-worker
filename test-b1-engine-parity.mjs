import assert from 'node:assert/strict';
import {test} from 'node:test';
import * as before from './test-fixtures/signal-engine-pre-b1.js';
import * as after from './signal-engine.js';
const now=Date.UTC(2026,8,28,15);
const filters={nyFilterOn:false,pivotFilterOn:false};
let seed=731;
const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296;};
function bars(tf,count,drift){let c=4100;return Array.from({length:count},(_,i)=>{const o=c;c+=drift+(random()-.5)*4;return{t:now-(count-i)*before.SIGNAL_TF_MS[tf],o,c,h:Math.max(o,c)+random()*2,l:Math.min(o,c)-random()*2,v:30,provider:'mt5'};});}
let checked=0;
test('old/new engine exact differential: scores, confidence, gates, levels and returns',()=>{
 for(const tf of before.SIGNAL_TIMEFRAMES)for(let iteration=0;iteration<40;iteration++){
  const input=bars(tf,40+iteration*2,(iteration%5-2)*.35);
  const mtf=(before.HIGHER_SIGNAL_TIMEFRAMES[tf]||[]).map(frame=>({tf:frame,bars:bars(frame,80,.3)}));
  const options={tf,mtf,live:{price:input.at(-1).c,ts:now,receivedAt:now,source:'mt5'},barsSource:'d1',evaluationAt:now,filters};
  const evidence={};const original=before.computeServerSignal(input,options),instrumented=after.computeServerSignal(input,options,evidence);
  assert.deepEqual(instrumented,original);assert.deepEqual(after.computeServerSignal(input,options),original);checked++;
  if(evidence.scoring){
   const ops=evidence.ledger.flatMap(x=>x.operations).sort((a,b)=>a.order-b.order);
   assert.equal(ops.filter(x=>x.side==='bull').reduce((sum,x)=>sum+x.operand,0),original.bull);
   assert.equal(ops.filter(x=>x.side==='bear').reduce((sum,x)=>sum+x.operand,0),original.bear);
   assert.equal(evidence.scoring.margin,Math.max(original.bull,original.bear)-Math.min(original.bull,original.bear));
  }
  assert.deepEqual(after.marketProfile(input),before.marketProfile(input));
 }
 assert.equal(checked,280);
});
test('receipt/provider/NaN/candle/News/filter boundary parity',()=>{
 const input=bars('5m',100,.4);
 const base={tf:'5m',mtf:[{tf:'60m',bars:bars('60m',100,.4)}],live:{price:input.at(-1).c,ts:now,receivedAt:now,source:'mt5'},evaluationAt:now,barsSource:'d1',filters};
 const variants=[{dataQuality:{ok:false}},{evaluationAt:now+1e8},{filters:{nyFilterOn:true,pivotFilterOn:true}},...[-1,0,20000,20001].map(age=>({live:{...base.live,receivedAt:now-age}})),...[-30001,-30000,90000,90001].map(age=>({live:{...base.live,ts:now-age}})),{live:{...base.live,price:NaN}},{news:{ok:false,stale:true,safety:{calendarBlockTechnicalSignal:true}}}];
 for(const change of variants){const opts={...base,...change};assert.deepEqual(after.computeServerSignal(input,opts,{}),before.computeServerSignal(input,opts));}
 for(const short of [null,[],input.slice(0,39)])assert.deepEqual(after.computeServerSignal(short,base,{}),before.computeServerSignal(short,base));
});
test('observer exceptions do not alter results or clock-read sequence',()=>{
 const input=bars('5m',100,.4),options={tf:'5m',live:{price:input.at(-1).c,ts:now,receivedAt:now,source:'mt5'},filters,barsSource:'d1'};
 const originalClock=Date.now;let reads=0;Date.now=()=>{reads++;return now;};
 try{
  const expected=before.computeServerSignal(input,options);assert.equal(reads,1);reads=0;
  const broken=new Proxy({},{get(){throw new Error('measurement-only failure');},set(){throw new Error('measurement-only failure');}});
  assert.deepEqual(after.computeServerSignal(input,options,broken),expected);assert.equal(reads,1);
 }finally{Date.now=originalClock;}
});
test('lifecycle, expiry and backtest semantics unchanged',()=>{
 for(const side of ['buy','sell'])for(const tf of ['5m','60m','1d']){
  const signal={id:'parity',side,tf,status:'active',entry:100,tp1:side==='buy'?110:90,tp2:side==='buy'?120:80,sl:side==='buy'?90:110,createdAt:now,lastPrice:100};
  for(const bar of [{t:now,h:115,l:95,c:105},{t:now,h:125,l:75,c:100},{t:now,h:102,l:98,c:100}]){
   assert.deepEqual(after.updateSignalLifecycle(signal,bar,bar.c,now+60000,'mt5'),before.updateSignalLifecycle(signal,bar,bar.c,now+60000,'mt5'));
   assert.deepEqual(after.updateSignalLifecycleAcrossBars(signal,[bar],bar.c,now+60000),before.updateSignalLifecycleAcrossBars(signal,[bar],bar.c,now+60000));
  }
  assert.equal(after.signalExpiryMs(tf),before.signalExpiryMs(tf));
 }
});
test('backtest mechanics exact differential with frozen closed-candle frames',()=>{
 const frames={};for(const tf of before.SIGNAL_TIMEFRAMES)frames[tf]=bars(tf,100,.3);
 for(const tf of ['5m','15m','60m']){
  const options={tf,frames,filters,startAt:now-6*before.SIGNAL_TF_MS[tf],endAt:now,maxEvaluations:30};
  assert.deepEqual(after.runServerBacktest(options),before.runServerBacktest(options));
 }
});
