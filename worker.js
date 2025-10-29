// GoldSignalsX Worker — CORS fixed + dual binding support (KV/DB)
export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    // --- CORS handling ---
    const reqOrigin = req.headers.get('Origin') || '';
    const allowList = safeParseJSON(env.ALLOW_ORIGINS, []);
    const originAllowed = !reqOrigin || allowList.includes(reqOrigin); // allow if no Origin (direct hit / tools)

    if (req.method === 'OPTIONS') {
      return new Response('', {
        status: 204,
        headers: corsHeaders(originAllowed ? reqOrigin : '*'),
      });
    }
    if (!originAllowed) {
      return json({ error: 'CORS' }, 403, corsHeaders('*'));
    }

    // --- Bindings (support both names) ---
    const DB = env.DB ?? env.GSX_DB;
    const KV = env.KV ?? env.GSX_KV;

    // --- Basic rate limit via KV ---
    const ip = req.headers.get('CF-Connecting-IP') || '0.0.0.0';
    const pass = await rateLimit(KV, env.RL_LIMIT);
    if (!pass) return json({ error: 'rate_limit' }, 429, corsHeaders(reqOrigin || '*'));

    try {
      if (req.method === 'GET' && url.pathname === '/health') {
        return json({ ok: true }, 200, corsHeaders(reqOrigin || '*'));
      }

      if (req.method === 'GET' && url.pathname === '/price') {
        const out = await handlePrice(env, KV);
        return json(out, 200, corsHeaders(reqOrigin || '*'));
      }

      if (req.method === 'GET' && url.pathname === '/bars') {
        const out = await handleBars(url, DB, KV);
        return json(out, 200, corsHeaders(reqOrigin || '*'));
      }

      if (req.method === 'GET' && url.pathname === '/export.csv') {
        const csv = await handleExportCSV(url, DB, KV);
        return new Response(csv, {
          headers: {
            ...corsHeaders(reqOrigin || '*'),
            'content-type': 'text/csv; charset=utf-8',
            'content-disposition': 'attachment; filename="XAUUSD.csv"',
          },
        });
      }

      if (req.method === 'POST' && url.pathname === '/notify') {
        const out = await handleNotify(req, env);
        return json(out, 200, corsHeaders(reqOrigin || '*'));
      }

      if (req.method === 'POST' && url.pathname === '/decision') {
        const out = await handleDecision(req, KV);
        return json(out, 200, corsHeaders(reqOrigin || '*'));
      }

      return json({ error: 'Not found' }, 404, corsHeaders(reqOrigin || '*'));
    } catch (e) {
      return json({ error: String(e?.message || e) }, 500, corsHeaders(reqOrigin || '*'));
    }
  },

  async scheduled(event, env, ctx) {
    try {
      const DB = env.DB ?? env.GSX_DB;
      const KV = env.KV ?? env.GSX_KV;
      await rollup(DB, KV, env);
    } catch (e) {
      console.log('rollup error', e);
    }
  }
};

// -------- helpers --------
function corsHeaders(origin) {
  const o = origin || '*';
  return {
    'access-control-allow-origin': o,
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type',
    'vary': 'Origin'
  };
}

function safeParseJSON(s, fallback) {
  try { const v = JSON.parse(s || '[]'); return Array.isArray(v) ? v : fallback; } catch { return fallback; }
}
function json(d, status=200, extraHeaders={}) {
  return new Response(JSON.stringify(d), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', ...extraHeaders }
  });
}

async function rateLimit(KV, limitStr) {
  try {
    if (!KV) return true;
    const limit = Number(limitStr || 180);
    const key = `rl:${Math.floor(Date.now()/60000)}`;
    const cur = Number(await KV.get(key) || 0);
    if (cur >= limit) return false;
    await KV.put(key, String(cur + 1), { expirationTtl: 90 });
    return true;
  } catch { return true; }
}

// -------- price & storage --------
async function handlePrice(env, KV) {
  const upstream = String(env.UPSTREAM_URL || '').trim();
  if (!upstream) throw new Error('missing UPSTREAM_URL');

  const resp = await fetch(upstream, { cf: { cacheTtl: 0 } });
  if (!resp.ok) throw new Error('upstream ' + resp.status);
  const j = await resp.json();

  const ts = Number(j.ts || Date.now());
  const price = Number(j.price ?? j.close ?? j.last);
  if (!isFinite(price)) throw new Error('bad price');

  if (KV) {
    const day = new Date(ts).toISOString().slice(0,10);
    await KV.put(`ticks:${day}:${ts}`, JSON.stringify({ p: price, ts }), { expirationTtl: 7*24*3600 });
    await KV.put(`latest`, JSON.stringify({ price, ts }), { expirationTtl: 7*24*3600 });
  }
  return { price, ts };
}

// -------- bars & resampling --------
const TF_MIN = { '1m':1,'5m':5,'15m':15,'30m':30,'60m':60,'240m':240,'1d':1440 };
function tfToMin(tf){ const m = TF_MIN[tf]; if (!m) throw new Error('bad tf'); return m; }
function bucket(ts, min){ return Math.floor(ts/(min*60000))*(min*60000); }

async function handleBars(url, DB, KV) {
  const tf = url.searchParams.get('tf') || '1m';
  const limit = Math.min(Number(url.searchParams.get('limit') || 1200), 5000);

  // Try D1 first
  const rows = await d1Bars(DB, tf, limit);
  if (rows && rows.length) return rows;

  // Fallback build from KV ticks
  const tfMin = tfToMin(tf);
  const toMs = Date.now();
  const fromMs = toMs - Math.max(tfMin*limit, 24*60)*60000;
  const ticks = await listTicks(KV, fromMs, toMs);
  const bars1m = build1m(ticks);
  return resample(bars1m, tfMin).slice(-limit);
}

async function d1Bars(DB, tf, limit) {
  if (!DB) return null;
  const tfMin = tfToMin(tf);
  const q = `SELECT t,o,h,l,c,v FROM bars WHERE tf=? ORDER BY t DESC LIMIT ?`;
  const { results } = await DB.prepare(q).bind(tfMin, limit).all();
  return (results || []).map(r => ({ t:r.t,o:r.o,h:r.h,l:r.l,c:r.c,v:r.v })).reverse();
}

async function listTicks(KV, fromMs, toMs) {
  const out = [];
  if (!KV) return out;
  for (const d of daySpan(fromMs, toMs)) {
    let cursor;
    do {
      const r = await KV.list({ prefix:`ticks:${d}:`, limit:1000, cursor });
      for (const k of r.keys) {
        const ts = Number(k.name.split(':').pop());
        if (ts>=fromMs && ts<=toMs) {
          const v = await KV.get(k.name);
          if (v){ const o = JSON.parse(v); out.push({ ts:o.ts, p:o.p }); }
        }
      }
      cursor = r.cursor;
    } while (cursor);
  }
  out.sort((a,b)=>a.ts-b.ts);
  return out;
}

function daySpan(fromMs,toMs){
  const out=[], one=86400000;
  let d = Date.UTC(new Date(fromMs).getUTCFullYear(), new Date(fromMs).getUTCMonth(), new Date(fromMs).getUTCDate());
  const e = Date.UTC(new Date(toMs).getUTCFullYear(), new Date(toMs).getUTCMonth(), new Date(toMs).getUTCDate());
  while (d<=e){ out.push(new Date(d).toISOString().slice(0,10)); d+=one; }
  return out;
}

function build1m(ticks){
  const bars=[]; let cur=null;
  for (const t of ticks){
    const b=bucket(t.ts,1);
    if (!cur || b!==cur.t){ if (cur) bars.push(cur); cur={t:b,o:t.p,h:t.p,l:t.p,c:t.p,v:1}; }
    else { cur.h=Math.max(cur.h,t.p); cur.l=Math.min(cur.l,t.p); cur.c=t.p; cur.v++; }
  }
  if (cur) bars.push(cur);
  return bars;
}

function resample(bars1m,toMin){
  if (toMin===1) return bars1m;
  const out=[]; let acc=null, base=null;
  for (const b of bars1m){
    const B=bucket(b.t,toMin);
    if (base===null){ base=B; acc={t:B,o:b.o,h:b.h,l:b.l,c:b.c,v:b.v}; continue; }
    if (B===base){ acc.h=Math.max(acc.h,b.h); acc.l=Math.min(acc.l,b.l); acc.c=b.c; acc.v+=b.v; }
    else { out.push(acc); base=B; acc={t:B,o:b.o,h:b.h,l:b.l,c:b.c,v:b.v}; }
  }
  if (acc) out.push(acc);
  return out;
}

// -------- CSV export --------
function toCSV(bars){
  const head='time,o,h,l,c,v\\n';
  const rows=bars.map(b=>`${new Date(b.t).toISOString()},${b.o},${b.h},${b.l},${b.c},${b.v}`).join('\\n');
  return head+rows+'\\n';
}

async function handleExportCSV(url, DB, KV){
  const tf=url.searchParams.get('tf')||'1m';
  const rows=await d1Bars(DB, tf, 20000);
  const bars=(rows&&rows.length)? rows : await (async()=>{
    const toMs=Date.now(), tfMin=tfToMin(tf);
    const fromMs=toMs-7*24*60*60000;
    const ticks=await listTicks(KV,fromMs,toMs);
    const bars1m=build1m(ticks);
    return resample(bars1m, tfMin);
  })();
  return toCSV(bars);
}

// -------- Telegram --------
async function handleNotify(req, env){
  const { text, chat } = await req.json();
  const token = env.TELEGRAM_TOKEN;
  const chatId = chat ?? env.TELEGRAM_CHAT;
  if (!token || !chatId) return { error:'telegram vars missing' };

  const r = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method:'POST',
    headers:{'content-type':'application/json'},
    body:JSON.stringify({ chat_id:chatId, text })
  });
  const j = await r.json();
  if (!j.ok) return { error:'tg failed', tg:j };
  return { ok:true };
}

// -------- Decisions store --------
async function handleDecision(req, KV){
  if (!KV) return { error:'no KV' };
  const body = await req.json();
  const ts = Date.now();
  await KV.put(`dec:${ts}`, JSON.stringify({ ...body, ts }), { expirationTtl: 7*24*3600 });
  return { ok:true };
}

// -------- Rollup job --------
async function rollup(DB, KV, env){
  if (!DB) return;
  const toMs=Date.now(), fromMs=toMs-60*60*1000;
  const ticks=await listTicks(KV, fromMs, toMs);
  if (!ticks.length) return;
  const bars1m=build1m(ticks);
  const tfs=[1,5,15,30,60,240,1440];
  for (const tf of tfs){
    const bars = tf===1? bars1m : resample(bars1m, tf);
    for (const b of bars){
      await DB.prepare(
        'INSERT OR REPLACE INTO bars (tf,t,o,h,l,c,v) VALUES (?,?,?,?,?,?,?)'
      ).bind(tf,b.t,b.o,b.h,b.l,b.c,b.v).run();
    }
  }
}
