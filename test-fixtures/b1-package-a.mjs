import {createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import fs from 'node:fs/promises';
import {createMeasurementD1Adapter} from '../measurement-d1-adapter.js';
export async function packageDatabase({file=':memory:',migrations=true,delay=null,clock=()=>0}={}){
 const raw=new DatabaseSync(file);raw.exec('PRAGMA foreign_keys=ON');raw.exec('PRAGMA busy_timeout=1000');
 if(migrations)for(const name of (await fs.readdir(new URL('../migrations/',import.meta.url))).filter(n=>n.endsWith('.sql')).sort())raw.exec(await fs.readFile(new URL('../migrations/'+name,import.meta.url),'utf8'));
 raw.function('pa_test_clock',clock);
 const binding={prepare(sql){sql=sql.replaceAll("CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)",'pa_test_clock()');const bind=(...values)=>({sql,values,async first(){return raw.prepare(sql).get(...values)??null;},async all(){return {results:raw.prepare(sql).all(...values)};}});return {bind,first:()=>bind().first(),all:()=>bind().all()};},async batch(statements){
  raw.exec('BEGIN');let results;try{results=statements.map(({sql,values})=>{const s=raw.prepare(sql);if(s.columns().length)return {results:s.all(...values),meta:{rows_written:0}};const r=s.run(...values);return {results:[],meta:{rows_written:Number(r.changes)}};});raw.exec('COMMIT');}catch(e){raw.exec('ROLLBACK');throw e;}
  const size=raw.prepare('PRAGMA page_size').get().page_size*raw.prepare('PRAGMA page_count').get().page_count;for(const r of results)r.meta={...r.meta,size_after:size};if(delay)await delay(statements,results);return results;
 }};
 return {raw,binding,db:createMeasurementD1Adapter(binding),close:()=>raw.close()};
}
export function fakeBucket(){const etag=wire=>createHash('sha256').update(wire).digest('hex');const objects=new Map(),calls=[],failures=new Map();const bucket={
 async put(key,wire,options){calls.push(['put',key]);if(failures.get('put'))throw Error('fake_put_failure');if(objects.has(key)&&options?.onlyIf?.etagDoesNotMatch==='*')return null;if(options?.onlyIf?.etagMatches&&(!objects.has(key)||etag(objects.get(key))!==options.onlyIf.etagMatches))return null;objects.set(key,typeof wire==='string'?wire:new TextDecoder().decode(wire));return {key,etag:etag(objects.get(key))};},
 async get(key){calls.push(['get',key]);if(failures.get('get'))throw Error('fake_get_failure');if(!objects.has(key))return null;let wire=objects.get(key);if(failures.get('readback'))wire+='corrupt';return {key,etag:etag(wire),size:new TextEncoder().encode(wire).length,async arrayBuffer(){return new TextEncoder().encode(wire).buffer;}};},
 async list({prefix='',cursor,limit=64}={}){const keys=[...objects.keys()].filter(k=>k.startsWith(prefix)).sort();const start=cursor?keys.indexOf(cursor)+1:0,selected=keys.slice(start,start+limit);return {objects:selected.map(key=>({key})),truncated:start+limit<keys.length,cursor:selected.at(-1)};}};return {bucket,objects,calls,failures};}
export const gate=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
export async function deliver(db,packet,now,options={}){const {fragmentMeasurementPacket}=await import('../measurement-transport-fragment.js'),{stageQueueFragment,recoverTransportEvent}=await import('../measurement-queue-consumer.js');for(const wire of await fragmentMeasurementPacket(packet))await stageQueueFragment(db,wire,{now});return recoverTransportEvent(db,packet.envelope.eventId,{now,...options});}
let packetCache;
export async function seedOfficial(options={}){const {cyclePackets,now}=await import('./b1-m2.mjs');packetCache??=await cyclePackets();const x=await packageDatabase(options),at=now+86400000,creation=packetCache.find(p=>p.envelope.kind==='OFFICIAL_CREATION');
 for(const p of [packetCache[0],creation]){const r=await deliver(x.db,p,at);if(r.ingestion?.status!=='ACCEPTED')throw Error(JSON.stringify(r));}
 const {rebuildD1Projection}=await import('../measurement-independent-store.js');const scopeId=creation.envelope.semanticId,snapshot=await rebuildD1Projection(x.db,scopeId,{now:at});return {...x,at,packets:packetCache,creation,scopeId,evaluationId:creation.envelope.payload.evaluationId,snapshot};}
