import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

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

const source=await fs.readFile(new URL('./goldsignalsx-worker.js',import.meta.url),'utf8');
const generatedUrl=new URL('./.test-signals-security-worker.mjs',import.meta.url);
const testSource=source.replace(
  "import { DurableObject } from 'cloudflare:workers';",
  'class DurableObject { constructor(ctx,env) { this.ctx=ctx; this.env=env; } }'
).replace("'./signal-engine.js'",JSON.stringify(new URL('./signal-engine.js',import.meta.url).href))
  .replace("'./economic-calendar.js'",JSON.stringify(new URL('./economic-calendar.js',import.meta.url).href));
await fs.writeFile(generatedUrl,testSource);
const workerModule=await import(`${generatedUrl.href}?v=${Date.now()}`);
await fs.unlink(generatedUrl);
const worker=workerModule.default;

const allowedOrigin='https://skyeagle123.github.io';
const writeToken='test-write-token';
const filtersKey='system:signal-filters';
const initialFilters={
  nyFilterOn:true,nyStart:'08:00',nyEnd:'17:00',pivotFilterOn:true,pivotDistance:0.7
};
const updatedFilters={
  nyFilterOn:false,nyStart:'09:15',nyEnd:'16:45',pivotFilterOn:false,pivotDistance:1.25
};
const store=new Map([[filtersKey,JSON.stringify(initialFilters)]]);
const puts=[];
const kv={
  async get(key,type){
    const value=store.get(key);
    return type==='json'&&typeof value==='string'?JSON.parse(value):value??null;
  },
  async put(key,value){
    puts.push({key,value});
    store.set(key,value);
  }
};
const env={
  ALLOW_ORIGINS:JSON.stringify([allowedOrigin]),
  GSX_WRITE_TOKEN:writeToken,
  GSX_KV:kv
};
let waitUntilCalls=0;
const ctx={waitUntil(){waitUntilCalls++;}};

async function filters(){
  const {updatedAt,...value}=JSON.parse(store.get(filtersKey));
  return value;
}

for (const query of [
  'nyFilterOn=0',
  'pivotFilterOn=0',
  'nyFilterOn=0&nyStart=09%3A15&nyEnd=16%3A45&pivotFilterOn=0&pivotDistance=1.25'
]) {
  const response=await worker.fetch(new Request(`https://example.com/signals?${query}`),env,ctx);
  assert.equal(response.status,200);
  assert.equal(response.headers.get('cache-control')?.includes('no-store'),true);
  const payload=await response.json();
  assert.equal(payload.readOnly,true);
  assert.equal(payload.refreshing,false);
  assert.deepEqual(payload.filters,initialFilters);
  assert.deepEqual(await filters(),initialFilters);
}
assert.equal(puts.length,0,'GET query parameters must never write configuration');
assert.equal(waitUntilCalls,0,'GET /signals must never start a mutating signal cycle');

const noOriginWrite=await worker.fetch(new Request('https://example.com/signals/filters',{
  method:'POST',headers:{'content-type':'application/json','x-gsx-write-token':writeToken},
  body:JSON.stringify(updatedFilters)
}),env,ctx);
assert.equal(noOriginWrite.status,403);
assert.deepEqual(await filters(),initialFilters);

const badTokenWrite=await worker.fetch(new Request('https://example.com/signals/filters',{
  method:'POST',headers:{
    Origin:allowedOrigin,'content-type':'application/json','x-gsx-write-token':'wrong'
  },body:JSON.stringify(updatedFilters)
}),env,ctx);
assert.equal(badTokenWrite.status,401);
assert.deepEqual(await filters(),initialFilters);

const malformedWrite=await worker.fetch(new Request('https://example.com/signals/filters',{
  method:'POST',headers:{
    Origin:allowedOrigin,'content-type':'application/json','x-gsx-write-token':writeToken
  },body:JSON.stringify({nyFilterOn:false,pivotFilterOn:false})
}),env,ctx);
assert.equal(malformedWrite.status,400);
assert.deepEqual(await filters(),initialFilters);

const authorizedWrite=await worker.fetch(new Request('https://example.com/signals/filters',{
  method:'POST',headers:{
    Origin:allowedOrigin,'content-type':'application/json','x-gsx-write-token':writeToken
  },body:JSON.stringify(updatedFilters)
}),env,ctx);
assert.equal(authorizedWrite.status,200);
assert.deepEqual(await authorizedWrite.json(),{ok:true,filters:updatedFilters,changed:true});
assert.deepEqual(await filters(),updatedFilters);

const readAfterWrite=await worker.fetch(new Request('https://example.com/signals'),env,ctx);
assert.equal(readAfterWrite.status,200);
const readPayload=await readAfterWrite.json();
assert.deepEqual(readPayload.filters,updatedFilters);
assert.equal(readPayload.readOnly,true);
assert.equal(waitUntilCalls,0);

console.log('signals read/write security tests passed');
