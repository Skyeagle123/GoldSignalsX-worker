// GoldSignalsX Worker — unified with gold-ticks primary + stooq fallback
// === Indicators: SMA, STD, ATR, BB, ADX/DMI, Market State ===
function sma(arr, p){
  const out=[], q=[]; let sum=0;
  for(let i=0;i<arr.length;i++){
    q.push(arr[i]); sum+=arr[i];
    if(q.length>p) sum-=q.shift();
    out.push(q.length===p ? (sum/p) : null);
  }
  return out;
}
function std(arr, p, ma){
  const out=[], q=[];
  for(let i=0;i<arr.length;i++){
    q.push(arr[i]); if(q.length>p) q.shift();
    if(q.length===p){
      const m = ma[i]; let s=0; for(const v of q) s+=(v-m)*(v-m);
      out.push(Math.sqrt(s/p));
    } else out.push(null);
  }
  return out;
}
function calcATR(bars, period=14){
  const tr=[], out=[]; let atr=null;
  for(let i=0;i<bars.length;i++){
    const b=bars[i], prev=bars[i-1];
    if(!prev){ tr.push(b.h-b.l); continue; }
    const x1=b.h-b.l, x2=Math.abs(b.h-prev.c), x3=Math.abs(b.l-prev.c);
    const t=Math.max(x1,x2,x3);
    tr.push(t);
    if(i===period){ atr = tr.slice(0,period).reduce((a,b)=>a+b,0)/period; out.push(atr); }
    else if(i>period){ atr = ((atr*(period-1)) + t)/period; out.push(atr); }
  }
  while(out.length < bars.length) out.unshift(null);
  return out;
}
function calcBB(closes, period=20, mult=2){
  const ma = sma(closes, period);
  const s  = std(closes, period, ma);
  const upper=[], lower=[];
  for(let i=0;i<closes.length;i++){
    if(ma[i]==null || s[i]==null){ upper.push(null); lower.push(null); continue; }
    upper.push(ma[i] + mult*s[i]);
    lower.push(ma[i] - mult*s[i]);
  }
  return { ma, upper, lower };
}
function calcADX(bars, period=14){
  const len = bars.length;
  const plusDM = Array(len).fill(0), minusDM = Array(len).fill(0), TR = Array(len).fill(0);
  for(let i=1;i<len;i++){
    const up = bars[i].h - bars[i-1].h;
    const dn = bars[i-1].l - bars[i].l;
    plusDM[i]  = (up>dn && up>0) ? up : 0;
    minusDM[i] = (dn>up && dn>0) ? dn : 0;
    const x1=bars[i].h-bars[i].l, x2=Math.abs(bars[i].h-bars[i-1].c), x3=Math.abs(bars[i].l-bars[i-1].c);
    TR[i] = Math.max(x1,x2,x3);
  }
  function wSmooth(src){
    const out = Array(len).fill(null);
    let s = 0; for(let i=1;i<=period;i++) s += src[i]||0;
    out[period] = s;
    for(let i=period+1;i<len;i++) out[i] = out[i-1] - (out[i-1]/period) + (src[i]||0);
    return out;
  }
  const trN=wSmooth(TR), pN=wSmooth(plusDM), mN=wSmooth(minusDM);
  const plusDI=Array(len).fill(null), minusDI=Array(len).fill(null), DX=Array(len).fill(null);
  for(let i=period;i<len;i++){
    if(!trN[i]){ plusDI[i]=minusDI[i]=DX[i]=null; continue; }
    plusDI[i]  = 100*(pN[i]/trN[i]);
    minusDI[i] = 100*(mN[i]/trN[i]);
    const s = plusDI[i] + minusDI[i];
    DX[i] = s ? (100 * Math.abs(plusDI[i]-minusDI[i]) / s) : 0;
  }
  const ADX = Array(len).fill(null);
  let seed=0,count=0,start=-1;
  for(let i=0;i<len;i++){ if(DX[i]!=null){ seed+=DX[i]; count++; if(count===period){ ADX[i]=seed/period; start=i; break;} } }
  for(let i=start+1;i<len;i++){ if(DX[i]!=null) ADX[i] = ((ADX[i-1]*(period-1)) + DX[i]) / period; }
  return { plusDI, minusDI, ADX };
}
function marketStateFromBBATRADX(bars, {bbP=20, bbK=2, atrP=14, adxP=14}={}){
  if(!bars || bars.length<Math.max(bbP, atrP, adxP)+2) return {state:'—'};
  const closes = bars.map(b=>b.c);
  const { ma, upper, lower } = calcBB(closes, bbP, bbK);
  const atrArr = calcATR(bars, atrP);
  const { ADX, plusDI, minusDI } = calcADX(bars, adxP);
  const i = closes.length-1;
  const C = closes[i], U = upper[i], L = lower[i], M = ma[i];
  const ATR = atrArr[i], adx = ADX[i], pdi = plusDI[i], mdi = minusDI[i];
  if(U==null || L==null || !isFinite(C)) return {state:'—'};
  const bandwidthPct = ((U - L) / C) * 100;
  const atrPct = isFinite(ATR) && C>0 ? (ATR / C) * 100 : NaN;
  const Mprev = ma[i-1];
  const slopePct = (Mprev!=null && M!=null && Mprev!==0) ? ((M - Mprev)/Mprev)*100 : 0;
  const pos = (C - L) / Math.max(1e-9, (U - L));
  const BW_TIGHT=1.2, BW_WIDE=1.8, SLOPE_OK=0.03, ATR_OK=0.8, ATR_LOW=0.5, ADX_TREND=22;
  let state='حيادي';
  const trendBias = (pdi!=null && mdi!=null) ? (pdi>mdi ? 'صاعد' : 'هابط') : (slopePct>0?'صاعد':'هابط');
  if (bandwidthPct < BW_TIGHT && (isFinite(atrPct)? atrPct<ATR_LOW : true) && (adx==null || adx<ADX_TREND)){
    state = 'رانج';
  } else if (bandwidthPct > BW_WIDE && Math.abs(slopePct) > SLOPE_OK && (isFinite(atrPct)? atrPct>ATR_OK : true) && (adx==null || adx>=ADX_TREND)){
    state = `ترند ${trendBias}`;
  } else {
    if (adx!=null && adx>=ADX_TREND) state = `ترند ${trendBias}`;
    else state = 'رانج';
  }
  return { state, bandwidthPct, atrPct, adx, plusDI: pdi, minusDI: mdi, slopePct, pos, U, L, M, C };
}

// Endpoints:
//   GET  /                       → صفحة حالة بسيطة (HTML)
//   GET  /health                 → { ok: true }
//   GET  /price                  → { ok, source, symbol, price, close, date, time, isoTime, ts } (مطابق gold-ticks)
//   GET  
// ---- /state endpoint (BB + ATR + ADX) ----
async function handleState(url, env){
  try{
    const tf = url.searchParams.get('tf') || '5m';
    const limit = Math.max(60, Math.min(3000, Number(url.searchParams.get('limit')||600)));
    // Reuse local OHLC if available via function getBars or route /bars
    // Prefer internal /bars handler if present:
    if (typeof getBars === 'function'){
      const bars = await getBars(tf, limit, env);
      const ms = marketStateFromBBATRADX(bars);
      return json({ ok:true, tf, limit, ...ms });
    }
    // Fallback: if we expose /bars routing inside same worker, call it internally
    const barsUrl = new URL(url.origin + `/bars?tf=${encodeURIComponent(tf)}&limit=${encodeURIComponent(limit)}`);
    const r = await fetch(barsUrl, { headers:{ accept:'application/json' } });
    const j = await r.json().catch(()=>null);
    const bars = Array.isArray(j) ? j : (j && Array.isArray(j.data)? j.data : []);
    if(!bars || !bars.length) return json({ ok:false, error:'no bars' }, 502);
    const ms = marketStateFromBBATRADX(bars);
    return json({ ok:true, tf, limit, ...ms });
  }catch(e){
    return json({ ok:false, error:String(e?.message||e) }, 500);
  }
}
/bars?tf=1m&limit=1200  → OHLC JSON (D1 ثم KV fallback)
//   GET  /export.csv?tf=1m       → تنزيل CSV (time,o,h,l,c,v)
//   POST/GET /notify             → Telegram (TELEGRAM_TOKEN/CHAT)
//   POST /decision               → يحفظ قرار/ملخص في KV

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';
    const method = req.method.toUpperCase();

    // ---------- CORS ----------
    const origin = req.headers.get('Origin') || '';
    const allow = parseAllow(env.ALLOW_ORIGINS); // مثال: ["https://skyeagle123.github.io","*"]
    const corsHeaders = makeCorsHeaders(origin, allow);
    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders });

    try {
      // ---------- Routes ----------
      if (path === '/') return htmlHome(corsHeaders);

      if (path === '/health') return json({ ok: true }, corsHeaders);

      if (path === '/price') {
        const out = await getPriceUnified(env); // يُرجع الشكل المطابق gold-ticks
        if (!out.ok) return json({ ok: false, error: 'price_failed', tried: out.tried || [] }, corsHeaders, 502);

        // اختياري: خزن السعر في KV إن متوفر
        if (env.GSX_KV && Number.isFinite(out.price)) {
          const ts = Number(out.ts) || Date.now();
          const price = Number(out.price);
          const day = new Date(ts).toISOString().slice(0, 10);
          await env.GSX_KV.put('latest', JSON.stringify({ price, ts }), { expirationTtl: 7 * 24 * 3600 });
          await env.GSX_KV.put(`ticks:${day}:${ts}`, JSON.stringify({ p: price, ts }), { expirationTtl: 7 * 24 * 3600 });
        }
        return json(out, corsHeaders);
      }

      if (path === '/state') {
      return await handleState(url, env);
    }
    if (path === '/bars') {
        const tf = url.searchParams.get('tf') || '1m';
        const limit = Math.min(Number(url.searchParams.get('limit') || 1200), 5000);
        const rows = await d1Bars(env, tf, limit);
        if (rows && rows.length) return json(rows, corsHeaders);

        // من KV ticks (fallback)
        const tfMin = tfToMin(tf);
        const toMs = Date.now();
        const fromMs = toMs - Math.max(tfMin * limit, 24 * 60) * 60000;
        const ticks = await listTicks(env, fromMs, toMs);
        const bars1m = build1m(ticks);
        const bars = resample(bars1m, tfMin).slice(-limit);
        return json(bars, corsHeaders);
      }

      if (path === '/export.csv') {
        const tf = url.searchParams.get('tf') || '1m';
        let bars = await d1Bars(env, tf, 20000);
        if (!bars || !bars.length) {
          const tfMin = tfToMin(tf);
          const toMs = Date.now();
          const fromMs = toMs - 7 * 24 * 60 * 60000;
          const ticks = await listTicks(env, fromMs, toMs);
          const bars1m = build1m(ticks);
          bars = resample(bars1m, tfMin);
        }
        const csv = toCSV(bars);
        const h = new Headers(corsHeaders);
        h.set('content-type', 'text/csv; charset=utf-8');
        h.set('content-disposition', `attachment; filename="XAUUSD_${tf}.csv"`);
        return new Response(csv, { headers: h });
      }

      if (path === '/notify') {
        const out = await handleNotify(req, env);
        const code = out.ok ? 200 : 502;
        return json(out, corsHeaders, code);
      }

      if (path === '/decision') {
        if (!env.GSX_KV) return json({ ok: false, error: 'no KV' }, corsHeaders, 500);
        const body = await req.json().catch(() => ({}));
        const ts = Date.now();
        await env.GSX_KV.put(`dec:${ts}`, JSON.stringify({ ...body, ts }), { expirationTtl: 7 * 24 * 3600 });
        return json({ ok: true, ts }, corsHeaders);
      }

      return json({ error: 'Not found' }, corsHeaders, 404);
    } catch (e) {
      return json({ ok: false, error: 'exception', message: String(e?.message || e) }, corsHeaders, 500);
    }
  }
};

// ===== Helpers =====
function parseAllow(v) {
  try {
    if (!v) return ['*'];
    if (Array.isArray(v)) return v;
    const a = JSON.parse(v);
    return Array.isArray(a) ? a : ['*'];
  } catch { return ['*']; }
}
function makeCorsHeaders(origin, allowList) {
  const allowAll = allowList.includes('*');
  const allowed = allowAll || allowList.includes(origin) ? (origin || '*') : (allowAll ? '*' : allowList[0] || '*');
  return {
    'access-control-allow-origin': allowed,
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type',
    'content-type': 'application/json; charset=utf-8',
    'vary': 'origin'
  };
}
function json(obj, headers = {}, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers });
}
function htmlHome(headers = {}) {
  const h = new Headers(headers);
  h.set('content-type', 'text/html; charset=utf-8');
  return new Response(`<!doctype html>
<html lang="ar" dir="rtl"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>GoldSignalsX • Worker</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0b0f14;color:#e6e6e6;margin:24px}
.card{max-width:820px;margin:auto;padding:24px;border:1px solid #22303d;border-radius:14px;background:#121a22}
h1{margin:0 0 8px} a{color:#8bd3ff;text-decoration:none} a:hover{text-decoration:underline}
code{background:#0e1620;padding:2px 6px;border-radius:6px}
ul{margin:8px 0 0 1.2em}
</style></head><body>
<div class="card">
<h1>GoldSignalsX Worker</h1>
<p>خدمة موحّدة للسعر الحي، الشموع، الإشعارات والتصدير.</p>
<ul>
<li><a href="/health">/health</a> — فحص سريع</li>
<li><a href="/price">/price</a> — آخر سعر (gold-ticks ← stooq)</li>
<li><a href="/bars?tf=5m">/bars?tf=5m</a> — شموع TF</li>
<li><a href="/export.csv?tf=15m">/export.csv?tf=15m</a> — تنزيل CSV</li>
</ul>
<p class="muted">نسخة: <code>v${new Date().toISOString()}</code></p>
</div></body></html>`, { headers: h });
}

// ---------- Price (gold-ticks primary → stooq fallback) ----------
async function getPriceUnified(env) {
  // 1) gold-ticks (UPSTREAM_URL) إن وجد
  const base = (env.UPSTREAM_URL || '').trim().replace(/\/+$/, '');
  const tried = [];
  if (base) {
    const candidates = [`${base}/price`, `${base}/api/price`, base];
    for (const u of candidates) {
      try {
        const r = await fetch(u, { headers: { accept: 'application/json' }, cf: { cacheTtl: 0 } });
        tried.push({ url: u, status: r.status });
        if (r.ok) {
          const j = await r.json();
          // إذا كان الرد أصلاً على شكل gold-ticks فمرّره بعد التأكد من الحقول
          const maybePrice = Number(j.price ?? j.close ?? j.last);
          if (Number.isFinite(maybePrice)) {
            const symbol = (j.symbol || 'XAUUSD');
            const date   = j.date || (j.isoTime ? j.isoTime.slice(0,10) : undefined);
            const time   = j.time || (j.isoTime ? j.isoTime.slice(11,19).replace('Z','') : undefined);
            const isoTime = j.isoTime || (date && time ? `${date}T${time}Z` : new Date().toISOString());
            const ts = Number(j.ts) || Date.parse(isoTime) || Date.now();
            return {
              ok: true,
              source: 'gold-ticks',
              symbol,
              price: maybePrice,
              close: Number(j.close ?? maybePrice),
              date: date || new Date(ts).toISOString().slice(0,10),
              time: time || new Date(ts).toISOString().slice(11,19).replace('Z',''),
              isoTime,
              ts
            };
          }
        }
      } catch (e) {
        tried.push({ url: u, error: String(e) });
      }
    }
  }

  // 2) Fallback: stooq CSV (نشكّل نفس فوّرمت gold-ticks)
  try {
    const stq = 'https://stooq.com/q/l/?s=xauusd&f=sd2t2ohlcvn&h&e=csv';
    const r = await fetch(stq, { cf: { cacheTtl: 3 } });
    if (!r.ok) throw new Error('stooq ' + r.status);
    const csv = await r.text();
    const out = parseStooqCSV(csv);
    if (!Number.isFinite(out.close)) throw new Error('stooq invalid price');
    const ts = toIsoMs(out.date, out.time) || Date.now();
    const isoTime = new Date(ts).toISOString();
    return {
      ok: true,
      source: 'stooq',
      symbol: out.symbol || 'XAUUSD',
      price: out.close,
      close: out.close,
      date: out.date || isoTime.slice(0,10),
      time: out.time || isoTime.slice(11,19).replace('Z',''),
      isoTime,
      ts
    };
  } catch (e) {
    return { ok: false, tried };
  }
}

function parseStooqCSV(text) {
  // Symbol,Date,Time,Open,High,Low,Close,Volume,Name
  const lines = String(text || '').trim().split(/\r?\n/);
  const row = lines.length > 1 ? lines[1] : lines[0] || '';
  const parts = (row || '').split(',');
  return {
    symbol: (parts[0] || 'XAUUSD').trim(),
    date: (parts[1] || '').trim(),
    time: (parts[2] || '').trim(),
    close: Number((parts[6] || '').trim())
  };
}
function toIsoMs(date, time) {
  if (!date || !time) return null;
  const d = new Date(`${date}T${time}Z`);
  return isNaN(d.getTime()) ? null : d.getTime();
}

// ---------- OHLC / CSV ----------
const TF = { '1m': 1, '5m': 5, '15m': 15, '30m': 30, '60m': 60, '240m': 240, '1d': 1440 };
function tfToMin(tf) { if (TF[tf]) return TF[tf]; throw new Error('bad tf'); }
function bucket(ts, min) { return Math.floor(ts / (min * 60000)) * (min * 60000); }

function build1m(ticks) {
  // ticks: [{ p, ts }]
  const bars = []; let cur = null, base = null;
  for (const t of ticks.sort((a, b) => a.ts - b.ts)) {
    const B = bucket(t.ts, 1);
    if (base === null) { base = B; cur = { t: B, o: t.p, h: t.p, l: t.p, c: t.p, v: 1 }; }
    else if (B === base) { cur.h = Math.max(cur.h, t.p); cur.l = Math.min(cur.l, t.p); cur.c = t.p; cur.v++; }
    else { bars.push(cur); base = B; cur = { t: B, o: t.p, h: t.p, l: t.p, c: t.p, v: 1 }; }
  }
  if (cur) bars.push(cur);
  return bars;
}
function resample(b1m, toMin) {
  if (toMin === 1) return b1m;
  const out = []; let acc = null, base = null;
  for (const b of b1m) {
    const B = bucket(b.t, toMin);
    if (base === null) { base = B; acc = { t: B, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v }; continue; }
    if (B === base) { acc.h = Math.max(acc.h, b.h); acc.l = Math.min(acc.l, b.l); acc.c = b.c; acc.v += b.v; }
    else { out.push(acc); base = B; acc = { t: B, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v }; }
  }
  if (acc) out.push(acc);
  return out;
}
function toCSV(bars) {
  const head = 'time,o,h,l,c,v\n';
  const rows = bars.map(b => `${new Date(b.t).toISOString()},${b.o},${b.h},${b.l},${b.c},${b.v}`).join('\n');
  return head + rows + '\n';
}

// KV ticks & D1 bars
async function listTicks(env, fromMs, toMs) {
  if (!env.GSX_KV) return [];
  const out = [];
  for (const d of daySpan(fromMs, toMs)) {
    let cursor;
    do {
      const r = await env.GSX_KV.list({ prefix: `ticks:${d}:`, cursor });
      cursor = r.cursor;
      for (const k of r.keys) {
        const ts = Number(k.name.split(':').pop());
        if (ts >= fromMs && ts <= toMs) {
          try {
            const j = JSON.parse(await env.GSX_KV.get(k.name));
            if (j && Number.isFinite(j.p)) out.push({ p: j.p, ts: j.ts || ts });
          } catch { }
        }
      }
    } while (cursor);
  }
  return out.sort((a, b) => a.ts - b.ts);
}
function* daySpan(fromMs, toMs) {
  const d = new Date(fromMs); d.setUTCHours(0, 0, 0, 0);
  while (d.getTime() <= toMs) {
    yield d.toISOString().slice(0, 10);
    d.setUTCDate(d.getUTCDate() + 1);
  }
}
async function d1Bars(env, tf, limit) {
  if (!env.GSX_DB) return null;
  const tfMin = tfToMin(tf);
  const q = `SELECT t,o,h,l,c,v FROM bars WHERE tf=? ORDER BY t DESC LIMIT ?`;
  const { results } = await env.GSX_DB.prepare(q).bind(tfMin, limit).all();
  return results.map(r => ({ t: r.t, o: r.o, h: r.h, l: r.l, c: r.c, v: r.v })).reverse();
}

// ---------- Telegram ----------
async function handleNotify(req, env) {
  const token = env.TELEGRAM_TOKEN;
  const chat  = env.TELEGRAM_CHAT;
  if (!token || !chat) return { ok: false, error: 'telegram vars missing' };

  let text = 'GSX alert';
  if (req.method === 'POST') {
    try { const b = await req.json(); if (b && b.text) text = String(b.text); } catch {}
  } else {
    const url = new URL(req.url);
    text = url.searchParams.get('text') || url.searchParams.get('message') || text;
  }

  const r = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chat_id: chat, text })
  });
  const j = await r.json().catch(() => null);
  return { ok: r.ok, status: r.status, tg: j };
}
