import fs from 'node:fs/promises';

const URLS={
  ical:'https://www.bls.gov/schedule/news_release/bls.ics?download=1',
  api:'https://api.bls.gov/publicAPI/v1/timeseries/data/',
  cpi:'https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems?download=1',
  payroll:'https://download.bls.gov/pub/time.series/ce/ce.data.00a.TotalNonfarm.Employment?download=1',
  labor:'https://download.bls.gov/pub/time.series/ln/ln.data.1.AllData?download=1'
};
const SERIES=['CUSR0000SA0','CUSR0000SA0L1E','CES0000000001','LNS14000000'];
const headers={'user-agent':'GoldSignalsX BLS snapshot updater (+https://github.com/Skyeagle123/GoldSignalsX-worker)','accept':'text/calendar,application/json,text/plain,*/*'};

async function text(url,options={}) {
  const response=await fetch(url,{...options,headers:{...headers,...options.headers}});
  if (!response.ok) throw new Error(`${new URL(url).hostname} ${response.status}`);
  return response.text();
}

function rows(input,wanted) {
  const found={};
  for (const line of String(input||'').split(/\r?\n/)) {
    const cells=line.trim().split(/\s+/),id=cells[0];
    if (!wanted.includes(id)||cells.length<4||!/^M(?:0[1-9]|1[0-2])$/.test(cells[2])||!Number.isFinite(Number(cells[3]))) continue;
    (found[id]||=[]).push({year:cells[1],period:cells[2],value:cells[3]});
  }
  return found;
}

const generatedAt=new Date().toISOString(),year=new Date().getUTCFullYear();
const ical=await text(URLS.ical);
const scheduleIcs=[...ical.replace(/\r?\n[ \t]/g,'').matchAll(/BEGIN:VEVENT[\s\S]*?END:VEVENT/g)]
  .map(match=>match[0]).filter(block=>/SUMMARY:(?:Consumer Price Index|Employment Situation)(?:\r?\n|$)/i.test(block)).join('\n');
if (!scheduleIcs||!new RegExp(`DTSTART[^:]*:${year}`).test(scheduleIcs)) throw new Error('BLS iCal did not contain the current-year required releases');

const series={};
try {
  const payload=JSON.parse(await text(URLS.api,{method:'POST',headers:{'content-type':'application/json'},
    body:JSON.stringify({seriesid:SERIES,startyear:String(year-1),endyear:String(year)})}));
  if (payload.status!=='REQUEST_SUCCEEDED') throw new Error(`BLS API ${payload.status||'failed'}`);
  for (const item of payload.Results?.series||[]) series[item.seriesID]=(item.data||[]).filter(row=>/^M(?:0[1-9]|1[0-2])$/.test(row.period)&&Number.isFinite(Number(row.value))).slice(0,24);
} catch (apiError) {
  const groups=[[URLS.cpi,SERIES.slice(0,2)],[URLS.payroll,[SERIES[2]]],[URLS.labor,[SERIES[3]]]];
  for (const [url,ids] of groups) Object.assign(series,rows(await text(url),ids));
}
for (const id of SERIES) if (!series[id]?.length) throw new Error(`missing BLS snapshot series ${id}`);

const snapshot={schema:1,generatedAt,scheduleIcs,series,sources:{
  schedule:{name:'BLS official release calendar/iCal',url:URLS.ical.replace('?download=1','')},
  actuals:{name:'BLS Public Data API v1',url:URLS.api},
  downloadableFallbacks:[URLS.cpi,URLS.payroll,URLS.labor]
}};
const target=new URL('../bls-official-snapshot.js',import.meta.url);
let prior=null;
try { prior=(await import(`${target.href}?v=${Date.now()}`)).BLS_OFFICIAL_SNAPSHOT; } catch {}
const unchanged=prior&&prior.scheduleIcs===snapshot.scheduleIcs&&JSON.stringify(prior.series)===JSON.stringify(snapshot.series);
if (unchanged) console.log('official BLS snapshot data is unchanged');
else {
  await fs.writeFile(target,`// Generated only from the official BLS sources recorded below.\nexport const BLS_OFFICIAL_SNAPSHOT = ${JSON.stringify(snapshot,null,2)};\n`);
  console.log(`updated BLS official snapshot at ${generatedAt}`);
}
