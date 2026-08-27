// GoldSignalsX Worker — OANDA-ready unified XAU/USD price + candles
// Endpoints:
//   GET  /                   → صفحة حالة بسيطة (HTML)
//   GET  /health             → { ok: true }
//   GET  /price              → { ok, price, bid, ask, spread, ts, ageMs, source }
//   GET  /stream             → WebSocket live XAU/USD stream (Twelve Data)
//   GET  /bars?tf=1m&limit=1200   → OHLC JSON (من D1 إن موجود، وإلا من KV ticks)
//   GET  /news                → أهم الأخبار المؤثرة على الذهب + الميل الإخباري
//   GET  /export.csv?tf=1m        → تنزيل CSV للأعمدة time,o,h,l,c,v
//   POST/GET /notify              → Telegram (TELEGRAM_TOKEN/CHAT)
//   POST /decision                → يحفظ قرار/ملخص في KV
//
// Env:
//   UPSTREAM_URL, ALLOW_ORIGINS
//   OANDA_TOKEN + OANDA_ACCOUNT_ID (secrets), OANDA_ENV, OANDA_INSTRUMENT
//   TWELVE_DATA_API_KEY (secret)
//   GSX_KV (اختياري), GSX_DB (اختياري)
//   KV_OFF = "1" لتعطيل القراءة/الكتابة على KV بالكامل
//   TELEGRAM_TOKEN / TELEGRAM_CHAT

const APP_VERSION = '2026.08.27.1';
const MAX_BARS_LIMIT = 5000;
// 700 rows keep a 1m import under D1 Free's per-invocation query and bind limits.
const MAX_IMPORT_ROWS = 700;
const NEWS_CACHE_MS = 15 * 60 * 1000;
const NEWS_MAX_AGE_MS = 12 * 60 * 60 * 1000;
const NEWS_ALERT_MAX_AGE_MS = 45 * 60 * 1000;

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
          oandaConfigured: isOandaConfigured(env),
          twelveDataConfigured: isTwelveDataConfigured(env)
        }, corsHeaders);
      }

      if (path === '/stream') {
        return handleTwelveDataStream(req, env, corsHeaders);
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

      if (path === '/news') {
        const brief = await getGoldNewsBrief(env, { notify: false });
        return jsonNoStore(brief, corsHeaders, brief.ok ? 200 : 502);
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
    // Keep price persistence and news refresh off the request path.
    ctx.waitUntil(runScheduledTasks(env));
  }
};

async function runScheduledTasks(env) {
  const tasks = [];
  if (env.GSX_DB) tasks.push(refreshStoredPrice(env));
  tasks.push(getGoldNewsBrief(env, { notify: true }));
  const results = await Promise.allSettled(tasks);
  results.filter(result => result.status === 'rejected').forEach(result => {
    console.error(JSON.stringify({
      message: 'scheduled task failed',
      error: result.reason instanceof Error ? result.reason.message : String(result.reason)
    }));
  });
}

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
<li><code>/stream</code> — بث سعر XAU/USD الحي من Twelve Data</li>
<li><a href="/bars?tf=5m">/bars?tf=5m</a> — شموع TF</li>
<li><a href="/export.csv?tf=15m">/export.csv?tf=15m</a> — تنزيل CSV</li>
</ul>
<p class="muted">نسخة: <code>v${APP_VERSION}</code></p>
</div></body></html>`, { headers: h });
}

// ---------- Twelve Data live WebSocket ----------
function isTwelveDataConfigured(env) {
  return Boolean(String(env.TWELVE_DATA_API_KEY || '').trim());
}

function closeSocket(socket, code = 1000, reason = 'closed') {
  try {
    if (socket && socket.readyState < 2) socket.close(code, reason);
  } catch {}
}

function normalizeTwelveDataMessage(raw) {
  let payload;
  try {
    payload = JSON.parse(typeof raw === 'string' ? raw : String(raw));
  } catch {
    return null;
  }

  if (payload?.event === 'price') {
    const price = Number(payload.price);
    let ts = Number(payload.timestamp);
    if (!Number.isFinite(price)) return null;
    if (!Number.isFinite(ts)) ts = Date.now();
    else if (ts < 1e12) ts *= 1000;
    return {
      event: 'price',
      symbol: String(payload.symbol || 'XAU/USD'),
      price,
      ts,
      receivedAt: Date.now(),
      source: 'twelve-data'
    };
  }

  if (payload?.event === 'status' || payload?.status || payload?.code) {
    return {
      event: payload.event || 'status',
      status: payload.status || 'error',
      code: payload.code || '',
      message: String(payload.message || 'Twelve Data status')
    };
  }
  return null;
}

function handleTwelveDataStream(req, env, corsHeaders = {}) {
  if (req.method.toUpperCase() !== 'GET') {
    return json({ ok: false, error: 'method_not_allowed' }, corsHeaders, 405);
  }
  const origin = req.headers.get('Origin') || '';
  const allowedOrigins = parseAllow(env.ALLOW_ORIGINS);
  if (!origin || (!allowedOrigins.includes('*') && !allowedOrigins.includes(origin))) {
    return json({ ok: false, error: 'origin_not_allowed' }, corsHeaders, 403);
  }
  if (!isTwelveDataConfigured(env)) {
    return json({ ok: false, error: 'twelve_data_not_configured' }, corsHeaders, 503);
  }
  if ((req.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
    return json({ ok: false, error: 'websocket_upgrade_required' }, corsHeaders, 426);
  }

  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  // This Worker is a WebSocket proxy, so keep each side half-open long enough
  // to coordinate provider/client close frames explicitly.
  server.accept({ allowHalfOpen: true });

  const apiKey = String(env.TWELVE_DATA_API_KEY).trim();
  const upstream = new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${encodeURIComponent(apiKey)}`);

  upstream.addEventListener('open', () => {
    upstream.send(JSON.stringify({ action: 'subscribe', params: { symbols: 'XAU/USD' } }));
  });
  upstream.addEventListener('message', (event) => {
    const message = normalizeTwelveDataMessage(event.data);
    if (!message || server.readyState !== 1) return;
    server.send(JSON.stringify(message));
  });
  upstream.addEventListener('error', () => {
    console.error(JSON.stringify({ message: 'live provider websocket error', provider: 'twelve-data' }));
    if (server.readyState === 1) {
      server.send(JSON.stringify({ event: 'error', message: 'live_provider_error' }));
    }
  });
  upstream.addEventListener('close', (event) => {
    console.log(JSON.stringify({
      message: 'live provider websocket closed',
      provider: 'twelve-data',
      code: event.code,
      wasClean: event.wasClean
    }));
    closeSocket(server, event.code === 1000 ? 1000 : 1011, 'live provider closed');
  });
  server.addEventListener('close', (event) => {
    closeSocket(upstream, 1000, 'client closed');
    // Required with allowHalfOpen: finish the client-side close handshake.
    try { server.close(event.code || 1000, 'client closed'); } catch {}
  });
  server.addEventListener('error', () => closeSocket(upstream, 1011, 'client error'));

  return new Response(null, { status: 101, webSocket: client });
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
    const r = await fetch('https://api.gold-api.com/price/XAU', {
      headers: { accept: 'application/json' },
      cf: { cacheTtl: 1 }
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

// ---------- Gold news intelligence ----------
const NEWS_QUERY = [
  'gold', 'bullion', 'XAUUSD', '"Federal Reserve"', 'FOMC', 'inflation',
  '"interest rates"', '"Treasury yields"', '"US dollar"', 'war',
  'sanctions', 'tariffs', '"central bank"'
].join(' OR ');

const TRUSTED_NEWS_DOMAINS = [
  'federalreserve.gov', 'bls.gov', 'bea.gov', 'treasury.gov', 'ecb.europa.eu',
  'reuters.com', 'apnews.com', 'bloomberg.com', 'ft.com', 'wsj.com',
  'cnbc.com', 'bbc.com'
];

const BULLISH_NEWS_RULES = [
  { phrases:['rate cut','cuts rates','cut interest rates','dovish','monetary easing'], weight:3, reason:'خفض الفائدة أو لهجة تيسيرية تدعم الذهب' },
  { phrases:['dollar falls','dollar weakens','weaker dollar','dollar slides'], weight:2.5, reason:'ضعف الدولار يدعم الذهب' },
  { phrases:['yields fall','yields drop','lower yields','bond yields decline'], weight:2.5, reason:'تراجع العوائد يقلل كلفة الاحتفاظ بالذهب' },
  { phrases:['war','airstrike','missile','invasion','escalation','escalates','geopolitical tension'], weight:2.2, reason:'تصاعد المخاطر السياسية يدعم طلب الملاذ الآمن' },
  { phrases:['sanctions','trade war','tariff escalation'], weight:1.8, reason:'العقوبات والتوتر التجاري يرفعان طلب التحوط' },
  { phrases:['recession','bank crisis','market selloff','risk-off'], weight:2.2, reason:'مخاطر الركود أو الأسواق تدعم الملاذ الآمن' },
  { phrases:['central bank buying','gold reserves rise','gold purchases','etf inflows','safe-haven demand'], weight:2.8, reason:'ارتفاع الطلب المؤسسي أو الاحتياطي يدعم الذهب' },
  { phrases:['inflation accelerates','inflation rises','hot inflation','prices surge'], weight:1.3, reason:'ارتفاع التضخم قد يزيد طلب التحوط' }
];

const BEARISH_NEWS_RULES = [
  { phrases:['rate hike','raises rates','hikes rates','hawkish','higher for longer','monetary tightening'], weight:3, reason:'رفع الفائدة أو لهجة متشددة يضغطان على الذهب' },
  { phrases:['dollar rises','dollar strengthens','stronger dollar','dollar jumps'], weight:2.5, reason:'قوة الدولار تضغط على الذهب' },
  { phrases:['yields rise','yields jump','higher yields','bond yields climb'], weight:2.5, reason:'ارتفاع العوائد يزيد كلفة الاحتفاظ بالذهب' },
  { phrases:['ceasefire','peace deal','de-escalation','truce agreement'], weight:2, reason:'انحسار المخاطر يقلل طلب الملاذ الآمن' },
  { phrases:['strong jobs','jobs beat','payrolls beat','unemployment falls','robust economy'], weight:1.8, reason:'قوة الاقتصاد قد تؤخر خفض الفائدة' },
  { phrases:['inflation cools','inflation falls','inflation slows','prices ease'], weight:1.2, reason:'تراجع التضخم يقلل طلب التحوط' },
  { phrases:['central bank selling','gold reserves fall','etf outflows'], weight:2.8, reason:'تراجع الطلب المؤسسي يضغط على الذهب' }
];

function normalizeHeadline(value) {
  return ` ${String(value || '').toLowerCase().replace(/[^a-z0-9%]+/g, ' ').replace(/\s+/g, ' ').trim()} `;
}

function hasAnyPhrase(text, phrases) {
  return phrases.some(phrase => text.includes(` ${phrase.toLowerCase()} `));
}

function parseGdeltSeenDate(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value < 1e12 ? value * 1000 : value;
  const raw = String(value || '').trim();
  if (/^\d{10,13}$/.test(raw)) {
    const numeric = Number(raw);
    return raw.length <= 10 ? numeric * 1000 : numeric;
  }
  const match = raw.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/);
  if (match) return Date.UTC(+match[1], +match[2] - 1, +match[3], +match[4], +match[5], +match[6]);
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : Date.now();
}

function safeNewsUrl(value) {
  try {
    const url = new URL(String(value || ''));
    return url.protocol === 'https:' || url.protocol === 'http:' ? url.toString() : '';
  } catch { return ''; }
}

function newsDomain(value, fallback = '') {
  try { return new URL(String(value || '')).hostname.replace(/^www\./, '').toLowerCase(); }
  catch { return String(fallback || '').replace(/^www\./, '').toLowerCase(); }
}

function trustedNewsDomain(domain) {
  return TRUSTED_NEWS_DOMAINS.some(item => domain === item || domain.endsWith(`.${item}`));
}

function scoreNewsRules(text, rules) {
  let score = 0;
  const reasons = [];
  for (const rule of rules) {
    if (!hasAnyPhrase(text, rule.phrases)) continue;
    score += rule.weight;
    reasons.push(rule.reason);
  }
  return { score, reasons };
}

function classifyNewsArticle(article, now = Date.now()) {
  const title = String(article?.title || '').trim().replace(/\s+/g, ' ');
  const url = safeNewsUrl(article?.url);
  if (!title || !url) return null;
  const text = normalizeHeadline(title);
  const domain = newsDomain(url, article?.domain);
  const trusted = trustedNewsDomain(domain);
  const seenAt = parseGdeltSeenDate(article?.seendate || article?.seenAt);
  const ageMs = Math.max(0, now - seenAt);
  if (ageMs > NEWS_MAX_AGE_MS) return null;

  const goldTerms = ['gold','bullion','xauusd','xau usd','precious metal','safe haven'];
  const macroTerms = ['federal reserve','fomc','interest rate','inflation','cpi','pce','payrolls','jobs report','us dollar','treasury yields','central bank','ecb'];
  const geopoliticalTerms = ['war','airstrike','missile','invasion','attack','sanctions','tariffs','trade war','ceasefire','geopolitical tension','nato','ukraine','russia','iran','israel','china','taiwan'];
  const goldRelated = hasAnyPhrase(text, goldTerms);
  const macroRelated = hasAnyPhrase(text, macroTerms);
  const geopolitical = hasAnyPhrase(text, geopoliticalTerms);
  const relevance = (goldRelated ? 5 : 0) + (macroRelated ? 2 : 0) + (geopolitical ? 2 : 0) + (trusted ? 1 : 0);
  if (relevance < 3) return null;

  const bull = scoreNewsRules(text, BULLISH_NEWS_RULES);
  const bear = scoreNewsRules(text, BEARISH_NEWS_RULES);
  const delta = bull.score - bear.score;
  const direction = delta >= 0.75 ? 'bullish' : delta <= -0.75 ? 'bearish' : 'neutral';
  const criticalMacro = hasAnyPhrase(text, ['fomc','federal reserve','rate cut','rate hike','cpi','pce','payrolls','jobs report']);
  const criticalRisk = hasAnyPhrase(text, ['war','airstrike','missile','invasion','sanctions','trade war','bank crisis']);
  const importance = (criticalMacro && trusted) || (goldRelated && criticalRisk) ? 3 : (goldRelated || macroRelated || trusted ? 2 : 1);
  const confidence = direction === 'neutral'
    ? 50
    : Math.min(92, Math.round(52 + Math.abs(delta) * 8 + importance * 4 + (trusted ? 5 : 0)));
  const reasons = [...new Set(direction === 'bullish' ? bull.reasons : direction === 'bearish' ? bear.reasons : [])];

  return {
    id: '', title, url, domain,
    source: domain || String(article?.sourcecountry || 'news'),
    seenAt, ageMs, importance, direction, confidence,
    reason: reasons[0] || 'الأثر غير محسوم ويحتاج تأكيداً من حركة السعر',
    trusted
  };
}

function uniqueNewsArticles(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = item.title.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function buildNewsBrief(rawArticles, now = Date.now()) {
  const items = uniqueNewsArticles((Array.isArray(rawArticles) ? rawArticles : [])
    .map(article => classifyNewsArticle(article, now))
    .filter(Boolean))
    .sort((a, b) => b.importance - a.importance || b.seenAt - a.seenAt)
    .slice(0, 24);

  let bullWeight = 0;
  let bearWeight = 0;
  const biasReasons = [];
  for (const item of items) {
    const freshness = Math.max(0.25, 1 - item.ageMs / NEWS_MAX_AGE_MS);
    const weight = item.importance * (item.confidence / 100) * freshness;
    if (item.direction === 'bullish') bullWeight += weight;
    if (item.direction === 'bearish') bearWeight += weight;
    if (item.direction !== 'neutral' && item.reason) biasReasons.push(item.reason);
  }
  const total = bullWeight + bearWeight;
  const score = total ? (bullWeight - bearWeight) / total : 0;
  const direction = score >= 0.18 ? 'bullish' : score <= -0.18 ? 'bearish' : 'neutral';
  const confidence = total ? Math.min(90, Math.round(52 + Math.abs(score) * 36 + Math.min(8, total))) : 0;
  const freshCritical = items.find(item => item.importance === 3 && item.ageMs <= 15 * 60 * 1000);
  const hasBullCritical = items.some(item => item.importance === 3 && item.direction === 'bullish');
  const hasBearCritical = items.some(item => item.importance === 3 && item.direction === 'bearish');
  const conflictingCritical = hasBullCritical && hasBearCritical;
  const blockTechnicalSignal = Boolean(freshCritical || conflictingCritical);
  const blockReason = freshCritical
    ? 'خبر شديد التأثير صدر خلال آخر 15 دقيقة؛ ننتظر استقرار السعر قبل الدخول'
    : conflictingCritical ? 'الأخبار الشديدة متعارضة؛ تم تعليق الدخول حتى تتضح الحركة' : '';
  const advice = blockTechnicalSignal
    ? 'انتظار — لا دخول أثناء صدمة الخبر'
    : direction === 'bullish' ? 'ميل شرائي مشروط بتأكيد الشارت'
      : direction === 'bearish' ? 'ميل بيعي مشروط بتأكيد الشارت'
        : 'حيادي — الخبر لا يعطي اتجاهاً واضحاً';

  return {
    ok: true,
    updatedAt: now,
    source: 'gdelt',
    itemCount: items.length,
    items,
    goldBias: {
      direction,
      score: Number(score.toFixed(3)),
      confidence,
      advice,
      reasons: [...new Set(biasReasons)].slice(0, 4)
    },
    safety: {
      blockTechnicalSignal,
      reason: blockReason,
      blockUntil: freshCritical ? freshCritical.seenAt + 15 * 60 * 1000 : null
    }
  };
}

async function fetchGdeltGoldNews() {
  const url = new URL('https://api.gdeltproject.org/api/v2/doc/doc');
  url.searchParams.set('query', `(${NEWS_QUERY}) sourcelang:english`);
  url.searchParams.set('mode', 'artlist');
  url.searchParams.set('maxrecords', '75');
  url.searchParams.set('format', 'json');
  url.searchParams.set('sort', 'datedesc');
  url.searchParams.set('timespan', '12h');
  const response = await fetch(url.toString(), {
    headers: { accept: 'application/json', 'user-agent': 'GoldSignalsX/1.0' },
    cf: { cacheTtl: 300, cacheEverything: true }
  });
  if (!response.ok) throw new Error(`GDELT ${response.status}`);
  const payload = await response.json();
  return Array.isArray(payload?.articles) ? payload.articles : [];
}

async function readNewsCache(env) {
  if (!env.GSX_KV) return null;
  try {
    const cached = await env.GSX_KV.get('news:brief', 'json');
    return cached && typeof cached === 'object' ? cached : null;
  } catch { return null; }
}

async function writeNewsCache(env, brief) {
  if (!env.GSX_KV) return;
  await env.GSX_KV.put('news:brief', JSON.stringify(brief), { expirationTtl: 6 * 60 * 60 });
}

async function getGoldNewsBrief(env, { notify = false } = {}) {
  const now = Date.now();
  const cached = await readNewsCache(env);
  if (cached && Number.isFinite(Number(cached.updatedAt)) && now - Number(cached.updatedAt) < NEWS_CACHE_MS) {
    const result = { ...cached, stale: false, ageMs: now - Number(cached.updatedAt), cache: 'hit' };
    if (notify) await maybeNotifyHighImpactNews(env, result);
    return result;
  }
  try {
    const articles = await fetchGdeltGoldNews();
    const brief = buildNewsBrief(articles, now);
    await writeNewsCache(env, brief);
    if (notify) await maybeNotifyHighImpactNews(env, brief);
    return { ...brief, stale: false, ageMs: 0, cache: 'refreshed' };
  } catch (error) {
    console.error(JSON.stringify({ message:'gold news refresh failed', error:error instanceof Error ? error.message : String(error) }));
    if (cached) {
      const ageMs = now - Number(cached.updatedAt || 0);
      return { ...cached, stale: ageMs > 60 * 60 * 1000, ageMs, cache: 'stale-fallback', refreshError: 'news_refresh_failed' };
    }
    return { ok:false, stale:true, ageMs:null, updatedAt:null, source:'gdelt', itemCount:0, items:[], goldBias:{direction:'neutral',score:0,confidence:0,advice:'الأخبار غير متاحة مؤقتاً',reasons:[]}, safety:{blockTechnicalSignal:false,reason:'',blockUntil:null}, error:'news_unavailable' };
  }
}

async function hashNewsId(value) {
  const bytes = new TextEncoder().encode(String(value || ''));
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return Array.from(new Uint8Array(digest)).slice(0, 12).map(byte => byte.toString(16).padStart(2, '0')).join('');
}

function newsDirectionArabic(direction) {
  if (direction === 'bullish') return 'داعم للذهب';
  if (direction === 'bearish') return 'ضاغط على الذهب';
  return 'الأثر غير محسوم';
}

async function maybeNotifyHighImpactNews(env, brief) {
  if (String(env.NEWS_ALERTS_ENABLED || '1').trim() === '0' || !env.GSX_KV) return;
  const item = (brief?.items || []).find(candidate =>
    candidate.importance === 3 && candidate.direction !== 'neutral' &&
    candidate.confidence >= 65 && Date.now() - candidate.seenAt <= NEWS_ALERT_MAX_AGE_MS
  );
  if (!item) return;
  const id = await hashNewsId(item.url || item.title);
  const key = `news:sent:${id}`;
  if (await env.GSX_KV.get(key)) return;
  const text = [
    '🚨 خبر مهم للذهب — GoldSignalsX',
    item.title,
    `التأثير المتوقع: ${newsDirectionArabic(item.direction)} (${item.confidence}%)`,
    `السبب: ${item.reason}`,
    brief?.safety?.blockTechnicalSignal ? 'النصيحة: انتظار 15 دقيقة ومراقبة تأكيد الشارت' : `النصيحة: ${brief?.goldBias?.advice || 'مراقبة الشارت'}`,
    `المصدر: ${item.source}`,
    item.url,
    'تنبيه: الخبر لا يُستخدم وحده كإشارة تداول.'
  ].join('\n');
  const sent = await sendTelegramText(env, text);
  if (sent.ok) await env.GSX_KV.put(key, String(Date.now()), { expirationTtl: 24 * 60 * 60 });
}

// ---------- Telegram ----------
async function sendTelegramText(env, text) {
  const token = env.TELEGRAM_TOKEN;
  const chat = env.TELEGRAM_CHAT;
  if (!token || !chat) return { ok:false, error:'telegram vars missing' };
  const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method:'POST',
    headers:{'content-type':'application/json'},
    body:JSON.stringify({ chat_id:chat, text:String(text || 'GSX alert') })
  });
  const tg = await response.json().catch(() => null);
  return { ok:response.ok, status:response.status, tg };
}

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

  return sendTelegramText(env, text);
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

export { buildNewsBrief, classifyNewsArticle, parseGdeltSeenDate };
