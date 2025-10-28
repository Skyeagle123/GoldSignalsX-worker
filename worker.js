
export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const origin = req.headers.get('Origin') || '*';
    const allow = parseAllow(env.ALLOW_ORIGINS);
    if (req.method === 'OPTIONS') return cors(new Response('', {status:204}), origin);
    if (origin && allow.length && !allow.includes(origin)) return cors(json({ error:'CORS' }, 403), origin);

    const ip = req.headers.get('CF-Connecting-IP') || '0.0.0.0';
    if (!await rateLimit(ip, env)) return cors(json({ error:'rate_limit' }, 429), origin);

    try {
      if (req.method==='GET' && url.pathname==='/health') return cors(text('ok'), origin);
      if (req.method==='GET' && url.pathname==='/price') return cors(await handlePrice(env), origin);
      if (req.method==='GET' && url.pathname==='/bars') return cors(await handleBars(url, env), origin);
      if (req.method==='GET' && url.pathname==='/export.csv') return cors(await handleExportCSV(url, env), origin);
      if (req.method==='POST' && url.pathname==='/notify') return cors(await handleNotify(req, env), origin);
      if (req.method==='POST' && url.pathname==='/decision') return cors(await handleDecision(req, env), origin);
      return cors(json({ error:'Not found' }, 404), origin);
    } catch (e) {
      return cors(json({ error:String(e) }, 500), origin);
    }
  },
  async scheduled(event, env, ctx) { try{ await rollup(env); }catch(e){ console.log('rollup error',e);} }
}
const TF = { '1m':1,'5m':5,'15m':15,'30m':30,'60m':60,'240m':240,'1d':1440 };
function parseAllow(s){ try{ const a=JSON.parse(s||'[]'); return Array.isArray(a)?a:[]; }catch{return [];} }
async function rateLimit(ip, env){
  const limit=Number(env.RL_LIMIT||180); const key=`rl:${ip}:${Math.floor(Date.now()/60000)}`;
  const cur=Number(await env.GSX_KV.get(key)||0); if(cur>=limit) return false;
  await env.GSX_KV.put(key, String(cur+1), { expirationTtl: 90 }); return true;
}
async function handlePrice(env){
  const r=await fetch(env.UPSTREAM_URL, { cf:{cacheTtl:0} });
  if(!r.ok) throw new Error('upstream '+r.status);
  const j=await r.json(); const ts=j.ts?Number(j.ts):Date.now(); const price=Number(j.price ?? j.close ?? j.last ?? 0);
  if(!price||!isFinite(price)) throw new Error('bad price');
  const day=new Date(ts).toISOString().slice(0,10);
  await env.GSX_KV.put(`ticks:${day}:${ts}`, JSON.stringify({p:price, ts}), { expirationTtl: 7*24*3600 });
  await env.GSX_KV.put(`latest`, JSON.stringify({price, ts}), { expirationTtl: 7*24*3600 });
  return json({ price, ts });
}
function tfToMin(tf){ if(TF[tf]) return TF[tf]; throw new Error('bad tf'); }
function bucket(ts, min){ return Math.floor(ts/(min*60000))*(min*60000); }
async function handleBars(url, env){
  const tf=url.searchParams.get('tf')||'1m'; const limit=Math.min(Number(url.searchParams.get('limit')||1200), 5000);
  const rows = await d1Bars(env, tf, limit); if(rows && rows.length) return json(rows);
  const tfMin=tfToMin(tf); const toMs=Date.now(); const fromMs=toMs - Math.max(tfMin*limit, 24*60)*60000;
  const ticks = await listTicks(env, fromMs, toMs); const bars1m = build1m(ticks); const bars = resample(bars1m, tfMin).slice(-limit);
  return json(bars);
}
async function d1Bars(env, tf, limit){
  if(!env.GSX_DB) return null; const tfMin=tfToMin(tf);
  const q=`SELECT t,o,h,l,c,v FROM bars WHERE tf=? ORDER BY t DESC LIMIT ?`;
  const { results } = await env.GSX_DB.prepare(q).bind(tfMin, limit).all();
  return results.map(r=>({t:r.t,o:r.o,h:r.h,l:r.l,c:r.c,v:r.v})).reverse();
}
async function listTicks(env, fromMs, toMs){
  const out=[]; for (const d of daySpan(fromMs,toMs)){
    let cursor; do{ const r=await env.GSX_KV.list({ prefix:`ticks:${d}:`, limit:1000, cursor });
      for(const k of r.keys){ const ts=Number(k.name.split(':').pop()); if(ts>=fromMs && ts<=toMs){ const v=await env.GSX_KV.get(k.name); if(v){ const o=JSON.parse(v); out.push({ts:o.ts,p:o.p}); } } }
      cursor=r.cursor;
    } while(cursor);
  }
  out.sort((a,b)=>a.ts-b.ts); return out;
}
function daySpan(fromMs,toMs){ const out=[],one=86400000; let d=Date.UTC(new Date(fromMs).getUTCFullYear(), new Date(fromMs).getUTCMonth(), new Date(fromMs).getUTCDate()); const e=Date.UTC(new Date(toMs).getUTCFullYear(), new Date(toMs).getUTCMonth(), new Date(toMs).getUTCDate()); while(d<=e){ out.push(new Date(d).toISOString().slice(0,10)); d+=one; } return out; }
function build1m(ticks){ const bars=[]; let cur=null; for(const t of ticks){ const b=bucket(t.ts,1); if(!cur||b!==cur.t){ if(cur) bars.push(cur); cur={t:b,o:t.p,h:t.p,l:t.p,c:t.p,v:1}; } else { cur.h=Math.max(cur.h,t.p); cur.l=Math.min(cur.l,t.p); cur.c=t.p; cur.v++; } } if(cur) bars.push(cur); return bars; }
function resample(bars1m,toMin){ if(toMin===1) return bars1m; const out=[]; let acc=null, base=null; for(const b of bars1m){ const B=bucket(b.t,toMin); if(base===null){ base=B; acc={t:B,o:b.o,h:b.h,l:b.l,c:b.c,v:b.v}; continue; } if(B===base){ acc.h=Math.max(acc.h,b.h); acc.l=Math.min(acc.l,b.l); acc.c=b.c; acc.v+=b.v; } else { out.push(acc); base=B; acc={t:B,o:b.o,h:b.h,l:b.l,c:b.c,v:b.v}; } } if(acc) out.push(acc); return out; }
function toCSV(bars){ const head='time,o,h,l,c,v\\n'; const rows=bars.map(b=>`${new Date(b.t).toISOString()},${b.o},${b.h},${b.l},${b.c},${b.v}`).join('\\n'); return head+rows+'\\n'; }
async function handleExportCSV(url, env){
  const tf=url.searchParams.get('tf')||'1m'; const rows=await d1Bars(env, tf, 20000);
  const bars = (rows&&rows.length)? rows : await (async()=>{ const toMs=Date.now(), tfMin=tfToMin(tf); const fromMs=toMs-7*24*60*60000; const ticks=await listTicks(env,fromMs,toMs); const bars1m=build1m(ticks); return resample(bars1m, tfMin); })();
  const csv=toCSV(bars); return new Response(csv, { headers:{ 'content-type':'text/csv; charset=utf-8', 'content-disposition':`attachment; filename="XAUUSD_${tf}.csv"` } });
}
async function handleNotify(req, env){
  const { text, chat } = await req.json(); const token=env.TELEGRAM_TOKEN; const chatId=chat ?? env.TELEGRAM_CHAT;
  if(!token||!chatId) return json({ error:'telegram vars missing' }, 500);
  const r=await fetch(`https://api.telegram.org/bot${token}/sendMessage`, { method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({ chat_id:chatId, text }) });
  const j=await r.json(); if(!j.ok) return json({ error:'tg failed', tg:j }, 502); return json({ ok:true });
}
async function handleDecision(req, env){ const body=await req.json(); const ts=Date.now(); await env.GSX_KV.put(`dec:${ts}`, JSON.stringify({ ...body, ts }), { expirationTtl: 7*24*3600 }); return json({ ok:true }); }
async function rollup(env){
  if(!env.GSX_DB) return; const toMs=Date.now(), fromMs=toMs-60*60*1000; const ticks=await listTicks(env, fromMs, toMs); if(!ticks.length) return;
  const bars1m=build1m(ticks); const tfs=[1,5,15,30,60,240,1440];
  for(const tf of tfs){ const bars=tf===1? bars1m : resample(bars1m, tf); for(const b of bars){ await env.GSX_DB.prepare(`INSERT OR REPLACE INTO bars (tf,t,o,h,l,c,v) VALUES (?,?,?,?,?,?,?)`).bind(tf,b.t,b.o,b.h,b.l,b.c,b.v).run(); } }
}
function json(d, status=200){ return new Response(JSON.stringify(d), {status, headers:{'content-type':'application/json; charset=utf-8'}}); }
function text(s, status=200){ return new Response(s, {status}); }
function cors(res, origin){ const h=new Headers(res.headers); h.set('access-control-allow-origin', origin||'*'); h.set('access-control-headers','content-type'); h.set('access-control-allow-methods','GET,POST,OPTIONS'); return new Response(res.body, { status:res.status, headers:h }); }
