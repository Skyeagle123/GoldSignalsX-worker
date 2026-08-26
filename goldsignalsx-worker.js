// GoldSignalsX Worker — OANDA-ready unified XAU/USD price + candles
// Endpoints:
//   GET  /                   → صفحة حالة بسيطة (HTML)
//   GET  /health             → { ok: true }
//   GET  /price              → { ok, price, bid, ask, spread, ts, ageMs, source }
//   GET  /bars?tf=1m&limit=1200   → OHLC JSON (من D1 إن موجود، وإلا من KV ticks)
//   GET  /export.csv?tf=1m        → تنزيل CSV للأعمدة time,o,h,l,c,v
//   POST/GET /notify              → Telegram (TELEGRAM_TOKEN/CHAT)
//   POST /decision                → يحفظ قرار/ملخص في KV
//
// Env:
//   UPSTREAM_URL, ALLOW_ORIGINS
//   OANDA_TOKEN + OANDA_ACCOUNT_ID (secrets), OANDA_ENV, OANDA_INSTRUMENT
//   GSX_KV (اختياري), GSX_DB (اختياري)
//   KV_OFF = "1" لتعطيل القراءة/الكتابة على KV بالكامل
//   TELEGRAM_TOKEN / TELEGRAM_CHAT

const APP_VERSION = '2026.08.26.2';
const MAX_BARS_LIMIT = 5000;
// 700 rows keep a 1m import under D1 Free's per-invocation query and bind limits.
const MAX_IMPORT_ROWS = 700;

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    // ---------- Path (بدون Regex) ----------
    let path = url.pathname || '/';
    while (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
    const method = req.method.toUpperCase();

    // ---------- CORS ----------
    const origin = req.headers.get('Origin') || '';
    const allow = parseAllow(env.ALLOW_ORIGINS);
    const corsHeaders = makeCorsHeaders(origin, allow);
    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders });

    // ---------- Flag لإيقاف KV ----------
    const KV_OFF = String(env.KV_OFF || '').trim() === '1';

    try {
      // ---------- Routes ----------
      if (path === '/') return htmlHome(corsHeaders);

      if (path === '/health') {
        return json({
          ok: true,
          version: APP_VERSION,
          primaryProvider: isOandaConfigured(env) ? 'oanda' : 'fallback',
          oandaConfigured: isOandaConfigured(env)
        }, corsHeaders);
      }

      if (path === '/price') {
        const { ok, data } = await getPriceUnified(env);
        if (!ok) return json({ ok: false, error: 'price_failed' }, corsHeaders, 502);

        // Read-only by design. The 2-second UI poll must never consume KV writes.
        return jsonNoStore({
          ok: true,
          ...data,
          receivedAt: Date.now(),
          ageMs: Math.max(0, Date.now() - data.ts)
        }, corsHeaders);
      }

      // CSV import -> D1 seed fast
      if (path === '/import.csv' && method === 'POST') {
        if (!env.GSX_DB) return json({ ok:false, error:'missing D1 binding GSX_DB' }, corsHeaders, 400);
        const tfRaw = url.searchParams.get('tf') || '1m';
        if (tfRaw !== '1' && tfRaw !== '1m') {
          return json({ ok:false, error:'import supports 1m only' }, corsHeaders, 400);
        }
        await maybeEnsureD1(env);
        const csv = await req.text();
        const lines = csv.trim().split(/\r?\n/);
        const startIdx = (lines[0] || '').toLowerCase().includes('time') ? 1 : 0;
        const rows = [];
        for (let i = startIdx; i < lines.length && rows.length < MAX_IMPORT_ROWS; i++) {
          const parts = lines[i].split(',');
          if (parts.length < 5) continue;
          let [time,o,h,l,c,v] = parts;
          const t = isNaN(Number(time)) ? Date.parse(time) : Number(time);
          if (!Number.isFinite(t)) continue;
          o = parseFloat(o); h = parseFloat(h); l = parseFloat(l); c = parseFloat(c); v = parseFloat(v||'0');
          if (![o, h, l, c, v].every(Number.isFinite) || h < l) continue;
          rows.push({t, o, h, l, c, v});
        }
        if (rows.length === 0) return json({ ok:false, error:'no rows parsed' }, corsHeaders, 400);
        await importBars(env, rows);
        return json({ ok:true, imported: rows.length, tf: 1, truncated: rows.length === MAX_IMPORT_ROWS }, corsHeaders);
      }

      if (path === '/bars') {
        const tf = url.searchParams.get('tf') || '1m';
        if (!TF[tf]) return json({ ok:false, error:'bad tf' }, corsHeaders, 400);
        const limit = parseLimit(url.searchParams.get('limit'), 1200, MAX_BARS_LIMIT);

        // Keep analysis candles on the same provider as the live quote.
        if (isOandaConfigured(env)) {
          try {
            const bars = await getOandaBars(env, tf, limit);
            if (bars.length) return jsonWithSource(bars, 'oanda', corsHeaders);
          } catch (error) {
            logProviderError('oanda-bars', error);
          }
        }

        // من D1 أولاً
        const rows = await d1Bars(env, tf, limit);
        if (rows && rows.length) return jsonWithSource(rows, 'd1', corsHeaders);

        // Fallback إلى KV فقط إذا لم يكن KV_OFF
        if (!KV_OFF) {
          const tfMin = tfToMin(tf);
          const toMs = Date.now();
          const fromMs = toMs - Math.max(tfMin * limit, 24 * 60) * 60000;
          const ticks = await listTicks(env, fromMs, toMs, KV_OFF);
          const bars1m = build1m(ticks);
          const bars = resample(bars1m, tfMin).slice(-limit);
          if (bars.length) return jsonWithSource(bars, 'kv', corsHeaders);
        }

        // إذا لم نجد شيئاً في D1 ولا KV أو كانت النتيجة قليلة، جرّب الـ UPSTREAM /ohlc كـ fallback
        if (env.UPSTREAM_URL) {
          try {
            const tfMap = { '1m':60, '5m':300, '15m':900, '30m':1800, '60m':3600, '1h':3600, '240m':14400, '4h':14400, '1d':86400 };
            const tfSec = tfMap[tf] || 300;
            const lookback = (limit || 600) * tfSec;
            const u = new URL(env.UPSTREAM_URL);
            u.pathname = (u.pathname.replace(/\/+$/,'') + '/ohlc');
            u.search = `?tf=${tfSec}&lookback=${lookback}`;
            const r2 = await fetch(u.toString());
            if (r2.ok) {
              const j = await r2.json();
              const sourceRows = Array.isArray(j) ? j : (Array.isArray(j?.bars) ? j.bars : []);
              if (sourceRows.length) {
                const mapped = sourceRows.map(row => {
                  const t = row.t ?? row.time ?? row[0];
                  const o = row.o ?? row.open ?? row[1];
                  const h = row.h ?? row.high ?? row[2];
                  const l = row.l ?? row.low ?? row[3];
                  const c = row.c ?? row.close ?? row[4];
                  const v = row.v ?? row.volume ?? row[5] ?? 0;
                  const rawTs = typeof t === 'number' ? t : Date.parse(t);
                  const ts = Number.isFinite(rawTs) && rawTs < 1e12 ? rawTs * 1000 : rawTs;
                  return { t: ts, o: Number(o), h: Number(h), l: Number(l), c: Number(c), v: Number(v) };
                }).filter(b => Number.isFinite(b.t) && Number.isFinite(b.o) && Number.isFinite(b.c));
                if (mapped.length) return jsonWithSource(mapped.slice(-limit), 'gold-ticks', corsHeaders);
              }
            }
          } catch (error) {
            logProviderError('gold-ticks-bars', error);
          }
        }

        // إذا لم نجد أي بيانات على الإطلاق → رجّع مصفوفة فاضية
        return json([], corsHeaders);
      }

      if (path === '/export.csv') {
        const tf = url.searchParams.get('tf') || '1m';
        if (!TF[tf]) return json({ ok:false, error:'bad tf' }, corsHeaders, 400);
        await maybeEnsureD1(env);
        let bars = await d1Bars(env, tf, 20000);
        if ((!bars || !bars.length) && !KV_OFF) {
          const tfMin = tfToMin(tf);
          const toMs = Date.now();
          const fromMs = toMs - 7 * 24 * 60 * 60000;
          const ticks = await listTicks(env, fromMs, toMs, KV_OFF);
          const bars1m = build1m(ticks);
          bars = resample(bars1m, tfMin);
        }
        const csv = toCSV(bars || []);
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
        if (!env.GSX_KV || KV_OFF) return json({ ok: false, error: 'no KV' }, corsHeaders, 500);
        const body = await req.json().catch(() => ({}));
        const ts = Date.now();
        await env.GSX_KV.put(`dec:${ts}`, JSON.stringify({ ...body, ts }), { expirationTtl: 7 * 24 * 3600 });
        return json({ ok: true, ts }, corsHeaders);
      }

      return json({ error: 'Not found' }, corsHeaders, 404);

    } catch (e) {
      return json({ ok: false, error: 'exception', message: String(e?.message || e) }, corsHeaders, 500);
    }
  },

  async scheduled(_controller, env, ctx) {
    // One D1 snapshot every five minutes; never writes price ticks to KV.
    if (env.GSX_DB) ctx.waitUntil(refreshStoredPrice(env));
  }
};

// ===== Helpers =====
function parseAllow(v) {
  try {
    if (!v) return [];
    if (Array.isArray(v)) return v;
    const a = JSON.parse(v);
    return Array.isArray(a) ? a : [];
  } catch { return []; }
}
function makeCorsHeaders(origin, allowList) {
  const allowAll = allowList.includes('*');
  const allowed = !origin ? '*' : (allowAll ? origin : (allowList.includes(origin) ? origin : ''));
  const headers = {
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type',
    'access-control-expose-headers': 'x-gsx-source',
    'access-control-max-age': '86400',
    'content-type': 'application/json; charset=utf-8',
    'vary': 'origin'
  };
  if (allowed) headers['access-control-allow-origin'] = allowed;
  return headers;
}
function json(obj, headers = {}, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers });
}
function jsonNoStore(obj, headers = {}, status = 200) {
  const h = new Headers(headers);
  h.set('cache-control', 'no-store, no-cache, must-revalidate, max-age=0');
  h.set('pragma', 'no-cache');
  h.set('expires', '0');
  return new Response(JSON.stringify(obj), { status, headers: h });
}
function jsonWithSource(obj, source, headers = {}, status = 200) {
  const h = new Headers(headers);
  h.set('x-gsx-source', source);
  return new Response(JSON.stringify(obj), { status, headers: h });
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
<li><a href="/price">/price</a> — آخر سعر (OANDA عند تفعيله، ثم fallback)</li>
<li><a href="/bars?tf=5m">/bars?tf=5m</a> — شموع TF</li>
<li><a href="/export.csv?tf=15m">/export.csv?tf=15m</a> — تنزيل CSV</li>
</ul>
<p class="muted">نسخة: <code>v${APP_VERSION}</code></p>
</div></body></html>`, { headers: h });
}

// ---------- OANDA (primary when configured) ----------
function isOandaConfigured(env) {
  return String(env.OANDA_ENABLED || '1').trim() !== '0' &&
    Boolean(String(env.OANDA_TOKEN || '').trim()) &&
    Boolean(String(env.OANDA_ACCOUNT_ID || '').trim());
}
function oandaBase(env) {
  return String(env.OANDA_ENV || 'practice').trim().toLowerCase() === 'live'
    ? 'https://api-fxtrade.oanda.com'
    : 'https://api-fxpractice.oanda.com';
}
function oandaInstrument(env) {
  return String(env.OANDA_INSTRUMENT || 'XAU_USD').trim().toUpperCase();
}
function oandaHeaders(env) {
  return {
    accept: 'application/json',
    authorization: `Bearer ${String(env.OANDA_TOKEN || '').trim()}`,
    'accept-datetime-format': 'RFC3339'
  };
}
async function fetchOandaJson(env, path, query = {}) {
  const url = new URL(path, oandaBase(env));
  for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));
  const response = await fetch(url, { headers: oandaHeaders(env) });
  if (!response.ok) {
    const requestId = response.headers.get('requestid') || '';
    throw new Error(`OANDA ${response.status}${requestId ? ` (${requestId})` : ''}`);
  }
  return response.json();
}
async function getOandaPrice(env) {
  const account = encodeURIComponent(String(env.OANDA_ACCOUNT_ID).trim());
  const instrument = oandaInstrument(env);
  const payload = await fetchOandaJson(env, `/v3/accounts/${account}/pricing`, {
    instruments: instrument,
    includeUnitsAvailable: 'false',
    includeHomeConversions: 'false'
  });
  const quote = Array.isArray(payload.prices) ? payload.prices[0] : null;
  const bid = Number(quote?.bids?.[0]?.price ?? quote?.closeoutBid);
  const ask = Number(quote?.asks?.[0]?.price ?? quote?.closeoutAsk);
  if (!Number.isFinite(bid) || !Number.isFinite(ask)) throw new Error('OANDA quote has no bid/ask');
  const ts = Date.parse(quote.time);
  if (!Number.isFinite(ts)) throw new Error('OANDA quote has invalid time');
  return {
    source: 'oanda',
    instrument,
    status: quote.status || 'unknown',
    price: (bid + ask) / 2,
    bid,
    ask,
    spread: ask - bid,
    ts
  };
}
const OANDA_GRANULARITY = {
  '1m': 'M1', '5m': 'M5', '15m': 'M15', '30m': 'M30',
  '60m': 'H1', '1h': 'H1', '240m': 'H4', '4h': 'H4', '1d': 'D'
};
async function getOandaBars(env, tf, limit) {
  const account = encodeURIComponent(String(env.OANDA_ACCOUNT_ID).trim());
  const instrument = encodeURIComponent(oandaInstrument(env));
  const payload = await fetchOandaJson(env, `/v3/accounts/${account}/instruments/${instrument}/candles`, {
    price: 'M',
    granularity: OANDA_GRANULARITY[tf],
    count: Math.min(limit + 1, MAX_BARS_LIMIT),
    smooth: 'false'
  });
  return (Array.isArray(payload.candles) ? payload.candles : [])
    .filter(candle => candle?.complete === true && candle?.mid)
    .map(candle => ({
      t: Date.parse(candle.time),
      o: Number(candle.mid.o),
      h: Number(candle.mid.h),
      l: Number(candle.mid.l),
      c: Number(candle.mid.c),
      v: Number(candle.volume || 0),
      complete: true
    }))
    .filter(bar => [bar.t, bar.o, bar.h, bar.l, bar.c, bar.v].every(Number.isFinite) && bar.h >= bar.l)
    .slice(-limit);
}
function logProviderError(provider, error) {
  console.error(JSON.stringify({
    message: 'market data provider failed',
    provider,
    error: error instanceof Error ? error.message : String(error)
  }));
}

// ---------- Price (OANDA → direct public quote → legacy gold-ticks → Stooq) ----------
async function getPriceUnified(env) {
  const base = (env.UPSTREAM_URL || '').trim().replace(/\/+$/g, '');
  const tried = [];
  if (isOandaConfigured(env)) {
    try {
      return { ok: true, data: await getOandaPrice(env) };
    } catch (error) {
      tried.push({ source: 'oanda', error: error instanceof Error ? error.message : String(error) });
      logProviderError('oanda-price', error);
    }
  }
  // Public no-key quote. Keep it ahead of the legacy gold-ticks Worker because
  // that service can be healthy while its in-memory snapshot is still empty.
  try {
    const quoteUrl = `https://api.gold-api.com/price/XAU?_=${Date.now()}`;
    const r = await fetch(quoteUrl, {
      headers: { accept: 'application/json', 'cache-control': 'no-cache' },
      cache: 'no-store',
      cf: { cacheTtl: 0, cacheEverything: false }
    });
    tried.push({ source: 'gold-api', status: r.status });
    const data = await priceFromResponse(r, 'gold-api');
    if (data) return { ok: true, data };
  } catch (e) {
    tried.push({ source: 'gold-api', error: String(e) });
  }

  if (env.TICKS && typeof env.TICKS.fetch === 'function') {
    try {
      const r = await env.TICKS.fetch(new Request('https://gold-ticks.internal/price', {
        headers: { accept: 'application/json' }
      }));
      tried.push({ source: 'service-binding', status: r.status });
      const data = await priceFromResponse(r, 'gold-ticks-binding');
      if (data) return { ok: true, data };
    } catch (e) {
      tried.push({ source: 'service-binding', error: String(e) });
    }
  }
  if (base) {
    const candidates = [`${base}/price`, `${base}/api/price`, base];
    for (const u of candidates) {
      try {
        const r = await fetch(u, { headers: { accept: 'application/json' }, cf: { cacheTtl: 0 } });
        tried.push({ url: u, status: r.status });
        const data = await priceFromResponse(r, 'gold-ticks');
        if (data) return { ok: true, data };
      } catch (e) {
        tried.push({ url: u, error: String(e) });
      }
    }
  }

  try {
    const stq = "https://stooq.com/q/l/?s=xauusd&f=sd2t2ohlcvn&h&e=csv";
    const r = await fetch(stq, { cf: { cacheTtl: 3 } });
    if (!r.ok) throw new Error('stooq ' + r.status);
    const csv = await r.text();
    const out = parseStooqCSV(csv);
    if (!Number.isFinite(out.close)) throw new Error('stooq invalid price');
    const ts = toIsoMs(out.date, out.time) || Date.now();
    return { ok: true, data: { source: 'stooq', price: out.close, ts } };
  } catch (e) {
    tried.push({ source: 'stooq', error: String(e) });
    return { ok: false, tried };
  }
}
async function priceFromResponse(response, source) {
  if (!response.ok) return null;
  const j = await response.json();
  const price = Number(j.price ?? j.close ?? j.last);
  if (!Number.isFinite(price)) return null;
  const rawTs = j.ts ?? j.time ?? j.timestamp ?? j.updatedAt ?? j.updated_at;
  let ts = typeof rawTs === 'string' ? Date.parse(rawTs) : Number(rawTs);
  if (!Number.isFinite(ts)) ts = Date.now();
  if (ts < 1e12) ts *= 1000;
  const bid = Number(j.bid);
  const ask = Number(j.ask);
  return {
    source,
    price,
    ts,
    ...(Number.isFinite(bid) ? { bid } : {}),
    ...(Number.isFinite(ask) ? { ask } : {}),
    ...(Number.isFinite(bid) && Number.isFinite(ask) ? { spread: ask - bid } : {})
  };
}
function parseStooqCSV(text) {
  const lines = String(text || '').trim().split(/\r?\n/);
  const row = lines.length > 1 ? lines[1] : lines[0] || '';
  const parts = (row || '').split(',');
  return { symbol: (parts[0] || 'XAUUSD').trim(), date: (parts[1] || '').trim(), time: (parts[2] || '').trim(), close: Number((parts[6] || '').trim()) };
}
function toIsoMs(date, time) { if (!date || !time) return null; const d = new Date(`${date}T${time}Z`); return isNaN(d.getTime()) ? null : d.getTime(); }

// ---------- OHLC / CSV ----------
const TF = { '1m': 1, '5m': 5, '15m': 15, '30m': 30, '60m': 60, '1h': 60, '240m': 240, '4h': 240, '1d': 1440 };
function tfToMin(tf) { if (TF[tf]) return TF[tf]; throw new Error('bad tf'); }
function bucket(ts, min) { return Math.floor(ts / (min * 60000)) * (min * 60000); }
function parseLimit(value, fallback, max) {
  const n = Number.parseInt(value || '', 10);
  return Number.isFinite(n) ? Math.min(Math.max(n, 1), max) : fallback;
}

function build1m(ticks) {
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
  const rows = (bars || []).map(b => `${new Date(b.t).toISOString()},${b.o},${b.h},${b.l},${b.c},${b.v}`).join('\n');
  return head + rows + '\n';
}

// KV ticks & D1 bars
async function listTicks(env, fromMs, toMs, KV_OFF) {
  if (KV_OFF || !env.GSX_KV) return [];
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
          } catch {}
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

// ===== دالة D1 المعدّلة لدعم TFs متعددة من 1m =====
async function d1Bars(env, tf, limit) {
  if (!env.GSX_DB) return null;

  const tfMin = tfToMin(tf);     // مثلًا 1 أو 5 أو 15 ...
  const baseTf = 1;              // نحن نخزن فقط 1m في D1
  const q = `SELECT t,o,h,l,c,v FROM bars WHERE tf=? ORDER BY t DESC LIMIT ?`;

  // لو الطلب 1m → رجّع مباشرة من D1
  if (tfMin === baseTf) {
    const { results } = await env.GSX_DB
      .prepare(q)
      .bind(baseTf, limit)
      .all();

    return results
      .map(r => ({ t: r.t, o: r.o, h: r.h, l: r.l, c: r.c, v: r.v }))
      .reverse();
  }

  // TF أكبر (5m, 15m, 30m, 60m, 240m, 1d...)
  // منجيب عدد أكبر من شموع 1m وبنركّب منها TF المطلوب
  const factor = Math.max(1, Math.round(tfMin / baseTf)); // مثلًا 5 أو 15...
  const need = limit * factor + 10;                        // زيادة صغيرة احتياط

  const { results } = await env.GSX_DB
    .prepare(q)
    .bind(baseTf, need)
    .all();

  const b1m = results
    .map(r => ({ t: r.t, o: r.o, h: r.h, l: r.l, c: r.c, v: r.v }))
    .reverse();

  if (!b1m.length) return [];

  // نستخدم نفس resample الموجودة فوق
  return resample(b1m, tfMin).slice(-limit);
}

// ---------- Telegram ----------
async function handleNotify(req, env) {
  const token = env.TELEGRAM_TOKEN;
  const chat = env.TELEGRAM_CHAT;
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

// ---------- D1 ensure ----------
async function maybeEnsureD1(env) {
  if (!env.GSX_DB) return;
  // Only import/scheduled persistence call this; no module-level I/O promise.
  await env.GSX_DB.exec('CREATE TABLE IF NOT EXISTS bars (tf INTEGER NOT NULL DEFAULT 1, t INTEGER PRIMARY KEY, o REAL NOT NULL, h REAL NOT NULL, l REAL NOT NULL, c REAL NOT NULL, v REAL NOT NULL DEFAULT 0);');
  await env.GSX_DB.exec('CREATE UNIQUE INDEX IF NOT EXISTS ux_bars_t ON bars(t);');
  await env.GSX_DB.exec('CREATE INDEX IF NOT EXISTS idx_bars_tf_t ON bars(tf, t);');
}

async function refreshStoredPrice(env) {
  const { ok, data } = await getPriceUnified(env);
  if (!ok) throw new Error('price_failed');
  await persistPriceToD1(env, data);
}

async function persistPriceToD1(env, data) {
  if (!env.GSX_DB || !data || !Number.isFinite(Number(data.price))) return;
  const price = Number(data.price);
  const ts = Number(data.ts) || Date.now();
  await maybeEnsureD1(env);
  const t = bucket(ts, 1);
  await env.GSX_DB.prepare(`
    INSERT INTO bars (tf,t,o,h,l,c,v) VALUES (1,?1,?2,?2,?2,?2,1)
    ON CONFLICT(t) DO UPDATE SET
      tf=1,
      h=MAX(bars.h, excluded.h),
      l=MIN(bars.l, excluded.l),
      c=excluded.c,
      v=COALESCE(bars.v, 0) + 1
  `).bind(t, price).run();
}

async function importBars(env, rows) {
  const statements = [];
  // D1 permits at most 100 bound parameters; each row uses six.
  const chunkSize = 16;
  for (let start = 0; start < rows.length; start += chunkSize) {
    const chunk = rows.slice(start, start + chunkSize);
    const placeholders = chunk.map(() => '(1,?,?,?,?,?,?)').join(',');
    const values = chunk.flatMap(r => [r.t, r.o, r.h, r.l, r.c, r.v]);
    statements.push(env.GSX_DB.prepare(`
      INSERT INTO bars (tf,t,o,h,l,c,v) VALUES ${placeholders}
      ON CONFLICT(t) DO UPDATE SET
        tf=1, o=excluded.o, h=excluded.h, l=excluded.l, c=excluded.c, v=excluded.v
    `).bind(...values));
  }
  await env.GSX_DB.batch(statements);
}
