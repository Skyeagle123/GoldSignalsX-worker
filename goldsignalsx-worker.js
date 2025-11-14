// GoldSignalsX Worker — unified with gold-ticks primary + stooq fallback
// Endpoints:
//   GET  /                   → صفحة حالة بسيطة (HTML)
//   GET  /health             → { ok: true }
//   GET  /price              → { ok, price, ts, source }
//   GET  /bars?tf=1m&limit=1200   → OHLC JSON (من D1 إن موجود، وإلا من KV ticks)
//   GET  /export.csv?tf=1m        → تنزيل CSV للأعمدة time,o,h,l,c,v
//   POST/GET /notify              → Telegram (TELEGRAM_TOKEN/CHAT)
//   POST /decision                → يحفظ قرار/ملخص في KV
//
// Env:
//   UPSTREAM_URL, ALLOW_ORIGINS
//   GSX_KV (اختياري), GSX_DB (اختياري)
//   KV_OFF = "1" لتعطيل القراءة/الكتابة على KV بالكامل
//   TELEGRAM_TOKEN / TELEGRAM_CHAT

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
      await maybeEnsureD1(env);

      // ---------- Routes ----------
      if (path === '/') return htmlHome(corsHeaders);

      if (path === '/health') return json({ ok: true }, corsHeaders);

      if (path === '/price') {
        const { ok, data } = await getPriceUnified(env);
        if (!ok) return json({ ok: false, error: 'price_failed' }, corsHeaders, 502);

        // كتابة KV (اختياري) لكن مع احترام KV_OFF
        if (!KV_OFF && env.GSX_KV && data && Number.isFinite(data.price)) {
          const ts = Number(data.ts) || Date.now();
          const price = Number(data.price);
          const day = new Date(ts).toISOString().slice(0, 10);
          await env.GSX_KV.put(
            `latest`,
            JSON.stringify({ price, ts }),
            { expirationTtl: 7 * 24 * 3600 }
          ).catch(() => {});
          await env.GSX_KV.put(
            `ticks:${day}:${ts}`,
            JSON.stringify({ p: price, ts }),
            { expirationTtl: 7 * 24 * 3600 }
          ).catch(() => {});
        }

        // كتابة شمعة 1 دقيقة في D1
        if (env.GSX_DB && data && Number.isFinite(data.price)) {
          try {
            const price = Number(data.price);
            const ts = Number(data.ts) || Date.now();
            const tfMin = 1; // 1m
            const bucketTs = Math.floor(ts / 60000) * 60000; // نقرّب للدقيقة

            await env.GSX_DB.prepare(
              'INSERT OR REPLACE INTO bars (t, tf, o, h, l, c, v) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)'
            )
            .bind(bucketTs, tfMin, price, price, price, price, 1)
            .run();
          } catch (e) {
            // best-effort فقط: لا نكسر /price إذا فشل الإدخال
          }
        }

        return json({ ok: true, ...data }, corsHeaders);
      }

      // CSV import -> D1 seed fast
      if (path === '/import.csv' && method === 'POST') {
        if (!env.GSX_DB) return json({ ok:false, error:'missing D1 binding GSX_DB' }, 400);
        const url2 = new URL(req.url);
        const tf = parseInt(url2.searchParams.get('tf') || '1', 10);
        const csv = await req.text();
        const lines = csv.trim().split(/\r?\n/);
        const startIdx = lines[0].toLowerCase().includes('time') ? 1 : 0;
        const rows = [];
        for (let i = startIdx; i < lines.length; i++) {
          const parts = lines[i].split(',');
          if (parts.length < 5) continue;
          let [time,o,h,l,c,v] = parts;
          const t = isNaN(Number(time)) ? Date.parse(time) : Number(time);
          if (!Number.isFinite(t)) continue;
          o = parseFloat(o); h = parseFloat(h); l = parseFloat(l); c = parseFloat(c); v = parseFloat(v||'0');
          rows.push({t, o, h, l, c, v});
        }
        await initD1(env);
        if (rows.length === 0) return json({ ok:false, error:'no rows parsed' }, 400);
        const stmt = env.GSX_DB.prepare('INSERT OR REPLACE INTO bars (t,o,h,l,c,v,tf) VALUES (?1,?2,?3,?4,?5,?6,?7)');
        const batch = rows.map(r => stmt.bind(r.t, r.o, r.h, r.l, r.c, r.v, tf));
        await env.GSX_DB.batch(batch);
        return json({ ok:true, imported: rows.length, tf }, corsHeaders);
      }

      if (path === '/bars') {
        const tf = url.searchParams.get('tf') || '1m';
        const limit = Math.min(Number(url.searchParams.get('limit') || 1200), 5000);

        // من D1 أولاً
        const rows = await d1Bars(env, tf, limit);
        if (rows && rows.length) return json(rows, corsHeaders);

        // Fallback إلى KV فقط إذا لم يكن KV_OFF
        if (!KV_OFF) {
          const tfMin = tfToMin(tf);
          const toMs = Date.now();
          const fromMs = toMs - Math.max(tfMin * limit, 24 * 60) * 60000;
          const ticks = await listTicks(env, fromMs, toMs, KV_OFF);
          const bars1m = build1m(ticks);
          const bars = resample(bars1m, tfMin).slice(-limit);
          return json(bars, corsHeaders);
        }

        // إذا لم نجد شيئاً في D1 ولا KV أو كانت النتيجة قليلة، جرّب الـ UPSTREAM /ohlc كـ fallback
        if (env.UPSTREAM_URL) {
          try {
            const tfMap = { '1m':60, '3m':180, '5m':300, '15m':900, '30m':1800, '1h':3600, '4h':14400, '1d':86400 };
            const tfSec = tfMap[tf] || 300;
            const lookback = (limit || 600) * tfSec;
            const u = new URL(env.UPSTREAM_URL);
            u.pathname = (u.pathname.replace(/\/+$/,'') + '/ohlc');
            u.search = `?tf=${tfSec}&lookback=${lookback}`;
            const r2 = await fetch(u.toString());
            if (r2.ok) {
              const j = await r2.json();
              if (Array.isArray(j) && j.length) {
                const mapped = j.map(row => {
                  const t = row.t ?? row.time ?? row[0];
                  const o = row.o ?? row.open ?? row[1];
                  const h = row.h ?? row.high ?? row[2];
                  const l = row.l ?? row.low ?? row[3];
                  const c = row.c ?? row.close ?? row[4];
                  const v = row.v ?? row.volume ?? row[5] ?? 0;
                  const ts = typeof t === 'number' ? t : Date.parse(t);
                  return { t: ts, o: Number(o), h: Number(h), l: Number(l), c: Number(c), v: Number(v) };
                }).filter(b => Number.isFinite(b.t) && Number.isFinite(b.o) && Number.isFinite(b.c));
                if (mapped.length) return json(mapped.slice(-limit), corsHeaders);
              }
            }
          } catch (e) {
            // ignore upstream fallback errors
          }
        }

        // إذا لم نجد أي بيانات على الإطلاق → رجّع مصفوفة فاضية
        return json([], corsHeaders);
      }

      if (path === '/export.csv') {
        const tf = url.searchParams.get('tf') || '1m';
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
  const base = (env.UPSTREAM_URL || '').trim().replace(/\/+$/g, '');
  const tried = [];
  if (base) {
    const candidates = [`${base}/price`, `${base}/api/price`, base];
    for (const u of candidates) {
      try {
        const r = await fetch(u, { headers: { accept: 'application/json' }, cf: { cacheTtl: 0 } });
        tried.push({ url: u, status: r.status });
        if (r.ok) {
          const j = await r.json();
          const price = Number(j.price ?? j.close ?? j.last);
          if (Number.isFinite(price)) {
            const ts = Number(j.ts) || Date.now();
            return { ok: true, data: { source: 'gold-ticks', price, ts } };
          }
        }
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
    return { ok: false, tried };
  }
}
function parseStooqCSV(text) {
  const lines = String(text || '').trim().split(/\r?\n/);
  const row = lines.length > 1 ? lines[1] : lines[0] || '';
  const parts = (row || '').split(',');
  return { symbol: (parts[0] || 'XAUUSD').trim(), date: (parts[1] || '').trim(), time: (parts[2] || '').trim(), close: Number((parts[6] || '').trim()) };
}
function toIsoMs(date, time) { if (!date || !time) return null; const d = new Date(`${date}T${time}Z`); return isNaN(d.getTime()) ? null : d.getTime(); }

// ---------- OHLC / CSV ----------
const TF = { '1m': 1, '5m': 5, '15m': 15, '30m': 30, '60m': 60, '240m': 240, '1d': 1440 };
function tfToMin(tf) { if (TF[tf]) return TF[tf]; throw new Error('bad tf'); }
function bucket(ts, min) { return Math.floor(ts / (min * 60000)) * (min * 60000); }

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
  try {
    await env.GSX_DB.exec(`
      CREATE TABLE IF NOT EXISTS bars (
        t INTEGER NOT NULL,
        tf INTEGER NOT NULL,
        o REAL NOT NULL,
        h REAL NOT NULL,
        l REAL NOT NULL,
        c REAL NOT NULL,
        v INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_bars_tf_t ON bars(tf, t);
    `);
  } catch (_) {}
}

// ملاحظة: initD1 مستخدمة قبل batch في /import.csv
async function initD1(env) { /* الجدول يُنشأ في maybeEnsureD1 */ }

// ===== GSX: auto-upsert 1m bar into D1 on each /price =====
async function upsertD1Minute(env, data) {
  try {
    if (!env.GSX_DB) return;
    if (!data || !Number.isFinite(Number(data.price))) return;
    const price = Number(data.price);
    const t = Math.floor((Number(data.ts) || Date.now()) / 60000) * 60000;
    await env.GSX_DB.exec(`
      CREATE TABLE IF NOT EXISTS bars (
        t INTEGER NOT NULL,
        tf INTEGER NOT NULL,
        o REAL NOT NULL,
        h REAL NOT NULL,
        l REAL NOT NULL,
        c REAL NOT NULL,
        v INTEGER NOT NULL,
        PRIMARY KEY (t, tf)
      );
      CREATE INDEX IF NOT EXISTS idx_bars_tf_t ON bars(tf, t);
    `);
    const tf = 1;
    const stmt = env.GSX_DB.prepare('INSERT OR REPLACE INTO bars (t,o,h,l,c,v,tf) VALUES (?1,?2,?3,?4,?5,?6,?7)');
    await stmt.bind(t, price, price, price, price, 1, tf).run();
  } catch (e) {
    // best-effort only
  }
}
