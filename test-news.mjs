import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

const source = await fs.readFile(new URL('./goldsignalsx-worker.js', import.meta.url), 'utf8');
const worker = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
const { classifyNewsArticle, buildNewsBrief, enrichNewsBriefArabic, parseGdeltSeenDate } = worker;

const now = Date.UTC(2026, 7, 27, 8, 0, 0);
const seen = '20260827T075500Z';
assert.equal(parseGdeltSeenDate(seen), Date.UTC(2026, 7, 27, 7, 55, 0));

const bullish = classifyNewsArticle({
  title: 'Gold rises as Federal Reserve cuts rates and dollar falls',
  url: 'https://www.reuters.com/markets/example-bullish',
  seendate: seen
}, now);
assert.equal(bullish.direction, 'bullish');
assert.equal(bullish.importance, 3);
assert.ok(bullish.confidence >= 65);

const bearish = classifyNewsArticle({
  title: 'Gold falls as Treasury yields rise and dollar strengthens',
  url: 'https://www.reuters.com/markets/example-bearish',
  seendate: seen
}, now);
assert.equal(bearish.direction, 'bearish');
assert.ok(bearish.confidence >= 65);

const brief = buildNewsBrief([{
  title: 'Gold rises as Federal Reserve cuts rates and dollar falls',
  url: 'https://www.reuters.com/markets/example-bullish',
  seendate: seen
}], now);
assert.equal(brief.goldBias.direction, 'bullish');
assert.equal(brief.safety.blockTechnicalSignal, true);
assert.equal(brief.safety.blockUntil, Date.UTC(2026, 7, 27, 8, 10, 0));

let aiCalls = 0;
const translated = await enrichNewsBriefArabic({
  AI: {
    run: async (_model, input) => {
      aiCalls += 1;
      assert.equal(input.response_format.type, 'json_object');
      return { response: JSON.stringify({ items: [{
        id: 0,
        titleAr: 'ارتفاع الذهب مع خفض الاحتياطي الفيدرالي للفائدة وتراجع الدولار',
        summaryAr: 'خفض الفائدة وضعف الدولار عاملان داعمان للذهب، مع انتظار تأكيد حركة السعر.'
      }] }) };
    }
  }
}, brief);
assert.equal(aiCalls, 1);
assert.equal(translated.arabicEnrichment, 'workers-ai');
assert.match(translated.items[0].titleAr, /الذهب/);
assert.match(translated.items[0].summaryAr, /داعم/);
assert.equal(translated.items[0].title, brief.items[0].title, 'original title must remain available');

const fallbackArabic = await enrichNewsBriefArabic({}, brief);
assert.equal(fallbackArabic.arabicEnrichment, 'fallback');
assert.equal(fallbackArabic.items[0].titleAr, '');
assert.equal(fallbackArabic.items[0].summaryAr, brief.items[0].reason);

const stale = classifyNewsArticle({
  title: 'Gold rises as Federal Reserve cuts rates',
  url: 'https://www.reuters.com/markets/example-old',
  seenAt: now - 13 * 60 * 60 * 1000
}, now);
assert.equal(stale, null);

console.log('news intelligence tests passed');
