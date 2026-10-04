// Valid offline fixtures reuse the bounded measurement profiles from d95d804b.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
import * as engine from '../signal-engine.js';
import * as sm from '../signal-measurement.js';
export const BASELINE='d95d804baf447e32be59481b77690bf171e7debb';
export const now=1790867400000;
export const provenance={codeCommit:BASELINE,measurementEffectiveAt:1790608200000};
const session='01c4fef2-36fb-47df-82e1-10288f2e329a';
export async function loadM1Workers(){
 const root=new URL('../',import.meta.url),files=[];
 async function load(source,name){const url=new URL(`.m1-${name}-${process.pid}.mjs`,root);files.push(url);await fs.writeFile(url,source);return import(url.href);}
 try{
  const oldSm=await load(execFileSync('git',['show',`${BASELINE}:signal-measurement.js`],{encoding:'utf8'}),'old-sm');
  const oldSource=execFileSync('git',['show',`${BASELINE}:goldsignalsx-worker.js`],{encoding:'utf8'}).replace("'./signal-measurement.js'",`'./.m1-old-sm-${process.pid}.mjs'`);
  const adapt=source=>source.replace("import { DurableObject } from 'cloudflare:workers';",'class DurableObject {}');
  const before=await load(adapt(oldSource),'before');
  const after=await load(adapt(await fs.readFile(new URL('goldsignalsx-worker.js',root),'utf8')),'after');
  return {before,after,oldSm};
 }finally{await Promise.all(files.map(file=>fs.unlink(file)));}
}
class Binding{
 constructor(){this.database=new DatabaseSync(':memory:');}
 async exec(sql){this.database.exec(sql);return {};}
 prepare(sql){const db=this.database;function statement(values=[]){return {bind(...v){return statement(v);},async all(){return {results:db.prepare(sql).all(...values)};},async first(){return db.prepare(sql).get(...values)||null;},async run(){const result=db.prepare(sql).run(...values);return {meta:{changes:Number(result.changes)}};},runSync(){return db.prepare(sql).run(...values);}};}return statement();}
 async batch(statements){this.database.exec('BEGIN');try{const rows=statements.map(x=>x.runSync());this.database.exec('COMMIT');return rows;}catch(e){this.database.exec('ROLLBACK');throw e;}}
}
export function cycleEnvironment(module,{exposure=null,stale=false,news=null,fullArrays=false,activeSignal=null,telegram=false}={}){
 const db=new Binding();db.database.exec('CREATE TABLE bars_v2(tf INTEGER,t INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,provider TEXT,PRIMARY KEY(tf,t));');
 for(const [tf,step] of Object.entries(engine.SIGNAL_TF_MS)){
  const times=validTimes(module,tf,fullArrays?({'1m':2000,'5m':600,'15m':300,'30m':200,'60m':120,'240m':80,'1d':60})[tf]:100,now),n=times.length;
  const stmt=db.database.prepare('INSERT INTO bars_v2 VALUES(?,?,?,?,?,?,?,?)');
  for(let i=0;i<n;i++){const c=4100+(i-n+1)*.35;stmt.run(step/60000,times[i],c-.3,c+.1,c-.4,c,1,'mt5');}
 }
 const values=new Map();if(activeSignal)values.set(`signal:state:${activeSignal.tf}`,JSON.stringify(activeSignal));
 const writes=[],decisions=[],cancellations=[],telegramEvents=[];let state=exposure;
 const kv={async get(key,type){const value=values.get(key);return type==='json'&&typeof value==='string'?JSON.parse(value):value??null;},async put(key,value){values.set(key,value);writes.push([key,value]);}};
 const feed={async status(){return {latestQuote:{event:'price',price:4100,ts:now,receivedAt:stale?now-21000:now,source:'mt5',sessionId:'synthetic',sequence:11}};},async manageGoldExposure(input){const trace={},previous=state;const result=module.decideGoldExposure(state,input,trace);decisions.push(result);state=result.state;return {...result,measurementBefore:input.measurementEvidence?previous:null,...(input.measurementEvidence?{measurementTrace:trace}:{})};},async cancelGoldExposureReservation(input){cancellations.push(input);},async queueTelegramEvent(record){telegramEvents.push(structuredClone(record));return {ok:true,status:'queued',eventId:record.eventId};}};
 return {env:{GSX_DB:db,GSX_KV:kv,MT5_INGEST_TOKEN:'synthetic-fixture',SIGNAL_ALERTS_ENABLED:telegram?'1':'0',GOLD_FEED:{getByName:()=>feed}},db,values,writes,decisions,cancellations,telegramEvents,news};
}
function validTimes(worker,tf,n,at){const step=engine.SIGNAL_TF_MS[tf],times=[];for(let t=Math.floor(at/step)*step-step;times.length<n;t-=step)if(worker.isGoldMarketOpen(t))times.push(t);return times.reverse();}
export function completeJournal(worker,sm,{heavy=false,active=false,cycleAt=now}={}){
 const now=cycleAt;const frames={};
for(const [tf,n]of Object.entries({'1m':2000,'5m':600,'15m':300,'30m':200,'60m':120,'240m':80,'1d':60})){const step=engine.SIGNAL_TF_MS[tf],end=Math.floor(now/step)*step,times=validTimes(worker,tf,n,now);frames[tf]={tf,provider:'mt5',bars:Array.from({length:n},(_,i)=>{const c=4100+(i-n+1)*.35;return {t:times[i],o:c-.3,h:c+.1,l:c-.4,c,v:1,provider:'mt5'};})};frames[tf].quality=engine.evaluateCandleQuality(frames[tf].bars,tf);}
 const f=structuredClone(frames);for(const q of Object.values(f)){const times=validTimes(worker,q.tf,q.bars.length,now);for(let i=0;i<q.bars.length;i++)q.bars[i].t=times[i];q.quality=engine.evaluateCandleQuality(q.bars,q.tf);}if(heavy)for(const q of Object.values(f))for(const b of q.bars){b.sessionId=session;b.availableAt=b.t+engine.SIGNAL_TF_MS[q.tf]+117;}
 const calendar=heavy?{ok:true,stale:true,cache:'stale-fallback',source:'official-calendar',updatedAt:now-7200000,fetchedAt:now-7199500,refreshError:'calendar_refresh_failed',events:Array.from({length:8},(_,i)=>({id:'us-high-impact-event-'+i,type:i%2?'CONTINUING_JOBLESS_CLAIMS':'INITIAL_JOBLESS_CLAIMS',impact:'high',eventAt:now-600000+i*1000,riskBeforeMinutes:30,riskAfterMinutes:30}))}:{ok:true,stale:false,cache:'hit',source:'official-calendar',updatedAt:now-21000,fetchedAt:now-20500,events:[]};
 const news=worker.mergeSignalRiskContext({ok:true,stale:false,cache:'hit',source:'gdelt',updatedAt:now-31000,safety:{blockTechnicalSignal:false}},calendar,now,{cycleId:'signal-cycle:'+now,cycleStartedAt:now-37});assert.equal(news.safety.blockTechnicalSignal,false);
 const live={price:4100,ts:now-113,receivedAt:now-47,source:'mt5'};sm.rememberDecisionQuote(live,{...live,bid:4099.913,ask:4100.087,midpoint:4100,spread:.174,sessionId:session,sequence:18473});
 const filters={nyFilterOn:false,pivotFilterOn:false},j=sm.beginDecisionCycle(now,f,live,news,filters);j.capturedAt=now+1234;j.candidates=[];j.evaluations=new Map();
 for(const [tf,q]of Object.entries(f)){if(active&&tf==='30m'){sm.observeAttempt(j,{tf,skippedReason:'existing-timeframe-lifecycle',requestedMtf:[],includedMtf:[]});continue;}const trace={},includedMtf=engine.HIGHER_SIGNAL_TIMEFRAMES[tf]||[];const result=engine.computeServerSignal(q.bars,{tf,mtf:includedMtf.map(tf=>f[tf]),live,barsSource:'d1',evaluationAt:now,news,dataQuality:q.quality,filters},trace);sm.observeAttempt(j,{tf,trace,result,requestedMtf:includedMtf,includedMtf,callerGates:[{id:'new-candle',result:'PASS',operands:{signalBarTs:result.lastTs,existingSignalId:null},ordinal:trace.gates.length}]});j.evaluations.set(tf,{...result,provider:q.provider,quality:q.quality,evaluatedAt:now});if(['buy','sell'].includes(result.side)){const id=tf+':'+result.lastTs+':'+result.side;j.candidates.push({id,tf,side:result.side,entry:result.entry,tp1:result.tp1,tp2:result.tp2,sl:result.sl,conf:result.conf,score:result.score,signalBarTs:result.lastTs,createdAt:now,status:'active'});}}
 for(const c of j.candidates)c.mtfAtEntry=worker.createMtfAtEntrySnapshot(c.tf,j.evaluations.get(c.tf).mtf,(engine.HIGHER_SIGNAL_TIMEFRAMES[c.tf]||[]).map(tf=>f[tf]),now);j.broadMatrices=new Map(j.candidates.map(c=>[c.id,worker.buildMtfDirectionMatrix(c,j.evaluations)]));
 assert(j.candidates.length<= (active?6:7));
 const before=heavy?{symbol:'XAUUSD',status:active?'active':'flat',side:active?'buy':'',maxPositions:1,primarySignalId:active?'30m:'+ (now-86400000)+':buy':'',primaryTf:active?'30m':'',openedAt:now-86400000,updatedAt:now-300000,closedAt:active?0:now-300000,closeReason:active?'':'tp2',cooldownUntil:0,source:active?'admission':'post-close'}:null;const trace={},exposure=worker.decideGoldExposure(before,{now,candidates:j.candidates,officialSignals:active?[{id:before.primarySignalId,tf:'30m',side:'buy',createdAt:before.openedAt,status:'tp1'}]:[]},trace);j.exposureResult={...exposure,measurementBefore:before,measurementTrace:trace};j.decisions=new Map(exposure.decisions.map(d=>[d.signalId,d]));j.officialIds=new Set(exposure.decisions.filter(d=>d.decision==='accepted').map(d=>d.signalId));j.confirmationIds=new Set(exposure.decisions.filter(d=>d.decision==='confirmation').map(d=>d.signalId));
 for(const c of j.candidates)if(j.officialIds.has(c.id))sm.rememberOfficialPersistence(c,'created',{ok:true});
 return j;
}
