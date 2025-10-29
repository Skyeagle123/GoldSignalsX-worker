// GoldSignalsX Worker — hardened upstream + CORS + dual bindings
export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const origin = req.headers.get('Origin') || '';

    // --- CORS gate (allow if no Origin, or if in list) ---
    const allow = parseAllow(env.ALLOW_ORIGINS);
    const originAllowed = !origin || allow.includes(origin);

    if (req.method === 'OPTIONS') {
      return new Response('', { status: 204, headers: corsHeaders(originAllowed ? origin : '*') });
    }
    if (!originAllowed) {
      return json({ error: 'CORS' }, 403, corsHeaders('*'));
    }

    // --- bindings (support both names) ---
    const DB = env.DB ?? env.GSX_DB;
    const KV = env.KV ?? env.GSX_KV;

    try {
      if (url.pathname === '/health') {
        return json({ ok: true }, 200, corsHeaders(origin || '*'));
      }
      if (url.pathname === '/__debug') {
        const u = normalizeUpstream(String(env.UPSTREAM_URL || ''));
        return json({
          allow,
          origin,
          originAllowed,
          upstream_raw: String(env.UPSTREAM_URL || ''),
          upstream_norm: u,
          hasDB: !!DB,
          hasKV: !!KV
        }, 200, corsHeaders(origin || '*'));
      }
      if (url.pathname === '/price') {
        const data = await handlePrice(env);
        return json(data, 200, corsHeaders(origin || '*'));
      }
      if (url.pathname === '/bars') {
        const out = await handleBars(url, DB, KV);
        return json(out, 200, corsHeaders(origin || '*'));
      }
      if (url.pathname === '/export.csv') {
        const csv = await handleExportCSV(url, DB, KV);
        return new Response(csv, { headers: { ...corsHeaders(origin || '*'), 'content-type': 'text/csv; charset=utf-8' } });
      }
      if (url.pathname === '/notify' && req.method === 'POST') {
        const out = await handleNotify(req, env);
        return json(out, 200, corsHeaders(origin || '*'));
      }
      if (url.pathname === '/decision' && req.method === 'POST') {
        const out = await handleDecision(req, KV);
        return json(out, 200, corsHeaders(origin || '*'));
      }
      return json({ error: 'Not found' }, 404, corsHeaders(origin || '*'));
    } catch (e) {
      return json({ error: String(e?.message || e) }, 500, corsHeaders(origin || '*'));
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

// ---------- CORS helpers ----------
function parseAllow(s) { try { const a = JSON.parse(s || '[]'); return Array.isArray(a) ? a : []; } catch { return []; } }
function corsHeaders(origin) {
  return {
    'access-control-allow-origin': origin,
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type',
    'vary': 'Origin'
  };
}
function json(d, status=200, extra={}) {
  return new Response(JSON.stringify(d), { status, headers: { 'content-type': 'application/json; charset=utf-8', ...extra } });
}

// ---------- Upstream normalization ----------
function normalizeUpstream(raw) {
  // trim + remove any invisible/whitespace
  let s = (raw || '').trim().replace(/\s+/g, '');
  if (!s) return s;
  // If it's an origin only (no path), append /price
  try {
    const u = new URL(s);
    // If path is empty or just '/', append /price
    if (u.pathname === '' || u.pathname === '/') {
      u.pathname = '/price';
      s = u.toString();
    }
  } catch {
    // Not a valid URL; leave as-is (fetch will throw)
  }
  // Ensure single slash ending for /price (avoid double slashes)
  s = s.replace(/\/+$/,''); // strip trailing slashes
  if (!/\/price$/.test(s)) s = s + '/price';
  return s;
}

// ---------- Rate limit via KV (simple per-minute) ----------
async function rateLimit(KV, env, req) {
  if (!KV) return true;
  const ip = req.headers.get('CF-Connecting-IP') || '0.0.0.0';
  const limit = Number(env.RL_LIMIT || 180);
  const key = `rl:${ip}:${Math.floor(Date.now()/60000)}`;
  const cur = Number(await KV.get(key) || 0);
  if (cur >= limit) return false;
  await KV.put(key, String(cur + 1), { expirationTtl: 90 });
  return true;
}

// ---------- /price ----------
async function handlePrice(env) {
  const upstream = normalizeUpstream(String(env.UPSTREAM_URL || ''));
  if (!upstream) throw new Error('missing UPSTREAM_URL');

  const r = await fetch(upstream, { headers: { 'accept': 'application/json' }, cf: { cacheTtl: 0 } });
  const text = await r.text();
  if (!r.ok) throw new Error('upstream ' + r.status + ' ' + text.slice(0,120));

  // Try parse and pick a numeric price
  let j;
  try { j = JSON.parse(text); } catch { throw new Error('upstream json parse'); }
  const price = Number(j.price ?? j.close ?? j.last ?? j.value ?? j.bid ?? j.ask);
  const ts = Number(j.ts || Date.now());
  if (!isFinite(price)) throw new Error('bad price');

  return { price, ts };
}

// ---------- bars helpers (minimal stubs to keep your API) ----------
const TF_MIN = { '1m':1,'5m':5,'15m':15,'30m':30,'60m':60,'240m':240,'1d':1440 };
function tfToMin(tf){ const m = TF_MIN[tf]; if (!m) throw new Error('bad tf'); return m; }
function bucket(ts, min){ return Math.floor(ts/(min*60000))*(min*60000); }

async function handleBars(url, DB, KV){
  const tf = url.searchParams.get('tf') || '1m';
  const limit = Math.min(Number(url.searchParams.get('limit') || 1200), 5000);
  // If you have D1 schema ready, query here; else return empty for now
  return [];
}

async function handleExportCSV(url, DB, KV){
  return 'time,o,h,l,c,v\n';
}

// ---------- Telegram ----------
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

async function handleDecision(req, KV){
  if (!KV) return { error:'no KV' };
  const body = await req.json();
  const ts = Date.now();
  await KV.put(`dec:${ts}`, JSON.stringify({ ...body, ts }), { expirationTtl: 7*24*3600 });
  return { ok:true };
}

// (Optional) rollup stub — keep your existing logic if you had one
async function rollup(DB, KV, env){ /* no-op here */ }
