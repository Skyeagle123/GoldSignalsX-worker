const workerBaseUrl=String(process.env.WORKER_BASE_URL||'').replace(/\/+$/,'');
const pwaUrl=String(process.env.PWA_URL||'').trim();
const retryAttempts=6;
const retryDelayMs=5_000;
const requestTimeoutMs=20_000;

function requireValue(condition,message) {
  if (!condition) throw new Error(message);
}

function delay(ms) {
  return new Promise(resolve=>setTimeout(resolve,ms));
}

async function fetchWithRetry(label,url,accept) {
  let lastError=null;
  for (let attempt=1;attempt<=retryAttempts;attempt++) {
    try {
      const response=await fetch(url,{
        method:'GET',redirect:'follow',headers:{accept},signal:AbortSignal.timeout(requestTimeoutMs)
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response;
    } catch (error) {
      lastError=error;
      if (attempt<retryAttempts) await delay(retryDelayMs);
    }
  }
  throw new Error(`${label} request failed: ${lastError?.message||lastError}`);
}

async function readJson(label,path) {
  const url=`${workerBaseUrl}${path}`;
  const response=await fetchWithRetry(label,url,'application/json');
  const contentType=String(response.headers.get('content-type')||'');
  requireValue(contentType.includes('application/json'),`${label} returned non-JSON content`);
  try {
    return await response.json();
  } catch {
    throw new Error(`${label} returned invalid JSON`);
  }
}

requireValue(workerBaseUrl.startsWith('https://'),'WORKER_BASE_URL must use HTTPS');

const health=await readJson('health','/health');
requireValue(health.ok===true,'health reported ok=false');
requireValue(typeof health.version==='string'&&health.version.length>0,'health version missing');
requireValue(typeof health.selectedPriceFeed==='string'&&health.selectedPriceFeed.length>0,
  'health selectedPriceFeed missing');
requireValue(health.components?.feed&&typeof health.components.feed.state==='string',
  'health feed component missing');
console.log('PASS /health');

const price=await readJson('live price/feed','/price');
requireValue(price.ok===true,'price reported ok=false');
requireValue(Number.isFinite(Number(price.price))&&Number(price.price)>0,'price value invalid');
requireValue(Number.isFinite(Number(price.ts))&&Number(price.ts)>0,'price timestamp invalid');
requireValue(typeof price.source==='string'&&price.source.length>0,'price source missing');
console.log('PASS /price');

const signals=await readJson('Signal Engine read path','/signals?tf=5m');
requireValue(signals.ok===true,'signals reported ok=false');
requireValue(signals.readOnly===true,'signals endpoint is not marked read-only');
requireValue(Array.isArray(signals.signals),'signals payload missing signals array');
requireValue(signals.signals.some(item=>item?.tf==='5m'),'signals payload missing 5m row');
console.log('PASS /signals?tf=5m');

if (pwaUrl) {
  requireValue(pwaUrl.startsWith('https://'),'PWA_URL must use HTTPS');
  const response=await fetchWithRetry('PWA',pwaUrl,'text/html');
  const html=await response.text();
  requireValue(/<!doctype html/i.test(html)&&/<title[\s>]/i.test(html),'PWA returned invalid HTML');
  console.log('PASS PWA');
}
