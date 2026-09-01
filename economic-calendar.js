import { BLS_OFFICIAL_SNAPSHOT } from './bls-official-snapshot.js';

const SOURCE_URLS = Object.freeze({
  bls:'https://www.bls.gov/schedule/news_release/bls.ics',
  blsApi:'https://api.bls.gov/publicAPI/v1/timeseries/data/',
  blsCpiDownload:'https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems',
  blsCesDownload:'https://download.bls.gov/pub/time.series/ce/ce.data.00a.TotalNonfarm.Employment',
  blsLaborDownload:'https://download.bls.gov/pub/time.series/ln/ln.data.1.AllData',
  blsApiDocs:'https://www.bls.gov/developers/api_signature_v1.htm',
  blsIcalDocs:'https://www.bls.gov/help/hlpical.htm',
  bea:'https://www.bea.gov/news/schedule',
  beaRss:'https://apps.bea.gov/rss/rss.xml',
  census:'https://www.census.gov/economic-indicators/calendar-listview.html',
  dol:'https://oui.doleta.gov/unemploy/wkclaims/report.asp',
  dolClaimsPage:'https://oui.doleta.gov/unemploy/claims.asp',
  dolScheduleRule:'https://www.dol.gov/newsroom/newsletter/archive/2010/20101021-3',
  opmHolidays:'https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/',
  ism:'https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/',
  ismManufacturingRule:'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/june/',
  ismServicesRule:'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/services/january/',
  nyseHolidays:'https://www.nyse.com/trade/hours-calendars',
  fedBase:'https://www.federalreserve.gov/newsevents/',
  fedMonetary:'https://www.federalreserve.gov/feeds/press_monetary.xml',
  fedSpeeches:'https://www.federalreserve.gov/feeds/speeches.xml'
});

const BLS_SERIES = Object.freeze({
  cpi:'CUSR0000SA0',core_cpi:'CUSR0000SA0L1E',nfp:'CES0000000001',unemployment_rate:'LNS14000000'
});

// These dates are copied from the explicitly published OPM schedules. Years not
// listed here deliberately fail closed instead of extrapolating a holiday rule.
const OPM_FEDERAL_HOLIDAYS = Object.freeze({
  2026:['2026-01-01','2026-01-19','2026-02-16','2026-05-25','2026-06-19','2026-07-03','2026-09-07','2026-10-12','2026-11-11','2026-11-26','2026-12-25'],
  2027:['2027-01-01','2027-01-18','2027-02-15','2027-05-31','2027-06-18','2027-07-05','2027-09-06','2027-10-11','2027-11-11','2027-11-25','2027-12-24'],
  2028:['2027-12-31','2028-01-17','2028-02-21','2028-05-29','2028-06-19','2028-07-04','2028-09-04','2028-10-09','2028-11-10','2028-11-23','2028-12-25'],
  2029:['2029-01-01','2029-01-15','2029-02-19','2029-05-28','2029-06-19','2029-07-04','2029-09-03','2029-10-08','2029-11-12','2029-11-22','2029-12-25'],
  2030:['2030-01-01','2030-01-21','2030-02-18','2030-05-27','2030-06-19','2030-07-04','2030-09-02','2030-10-14','2030-11-11','2030-11-28','2030-12-25']
});

// NYSE publishes this exact closed-day table for 2026-2028. Only those years
// are eligible for derived ISM dates; later years remain unavailable.
const NYSE_HOLIDAYS = Object.freeze({
  2026:['2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25','2026-06-19','2026-07-03','2026-09-07','2026-11-26','2026-12-25'],
  2027:['2027-01-01','2027-01-18','2027-02-15','2027-03-26','2027-05-31','2027-06-18','2027-07-05','2027-09-06','2027-11-25','2027-12-24'],
  2028:['2028-01-17','2028-02-21','2028-04-14','2028-05-29','2028-06-19','2028-07-04','2028-09-04','2028-11-23','2028-12-25']
});

const MONTHS = Object.freeze({
  january:0,february:1,march:2,april:3,may:4,june:5,
  july:6,august:7,september:8,october:9,november:10,december:11
});

const DEFAULT_RISK_SETTINGS = Object.freeze({
  macroBeforeMinutes:30,macroAfterMinutes:15,
  fomcBeforeMinutes:60,fomcAfterMinutes:30,
  claimsBeforeMinutes:15,claimsAfterMinutes:10,
  geopoliticalAfterMinutes:15,localTimeZone:'Asia/Beirut'
});

function boundedInteger(value,fallback,min=0,max=180) {
  const parsed=Number.parseInt(String(value??''),10);
  return Number.isFinite(parsed)?Math.min(max,Math.max(min,parsed)):fallback;
}

export function normalizeCalendarSettings(env={}) {
  const localTimeZone=String(env.CALENDAR_LOCAL_TIMEZONE||DEFAULT_RISK_SETTINGS.localTimeZone).trim();
  try { new Intl.DateTimeFormat('en',{timeZone:localTimeZone}).format(new Date()); }
  catch { return {...normalizeCalendarSettings({...env,CALENDAR_LOCAL_TIMEZONE:DEFAULT_RISK_SETTINGS.localTimeZone})}; }
  return {
    macroBeforeMinutes:boundedInteger(env.CALENDAR_MACRO_BEFORE_MIN,30),
    macroAfterMinutes:boundedInteger(env.CALENDAR_MACRO_AFTER_MIN,15),
    fomcBeforeMinutes:boundedInteger(env.CALENDAR_FOMC_BEFORE_MIN,60),
    fomcAfterMinutes:boundedInteger(env.CALENDAR_FOMC_AFTER_MIN,30),
    claimsBeforeMinutes:boundedInteger(env.CALENDAR_CLAIMS_BEFORE_MIN,15),
    claimsAfterMinutes:boundedInteger(env.CALENDAR_CLAIMS_AFTER_MIN,10),
    geopoliticalAfterMinutes:boundedInteger(env.NEWS_GEOPOLITICAL_AFTER_MIN,15),
    localTimeZone
  };
}

function decodeHtml(value) {
  return String(value||'')
    .replace(/&nbsp;|&#160;/gi,' ')
    .replace(/&amp;/gi,'&').replace(/&quot;/gi,'"').replace(/&#39;|&apos;/gi,"'")
    .replace(/&lt;/gi,'<').replace(/&gt;/gi,'>')
    .replace(/&#(\d+);/g,(_,n)=>String.fromCodePoint(Number(n)))
    .replace(/\s+/g,' ').trim();
}

function stripHtml(value) {
  return decodeHtml(String(value||'').replace(/<script\b[\s\S]*?<\/script>/gi,' ')
    .replace(/<style\b[\s\S]*?<\/style>/gi,' ').replace(/<[^>]*>/g,' '));
}

function slug(value) {
  return String(value||'').toLowerCase().normalize('NFKD')
    .replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'').slice(0,100);
}

function easternLocalToUtc(year,monthIndex,day,hour,minute) {
  const guess=Date.UTC(year,monthIndex,day,hour,minute,0);
  const parts=new Intl.DateTimeFormat('en-US',{
    timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',
    hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false
  }).formatToParts(new Date(guess));
  const get=type=>Number(parts.find(part=>part.type===type)?.value||0);
  const represented=Date.UTC(get('year'),get('month')-1,get('day'),get('hour')%24,get('minute'),get('second'));
  return guess-(represented-guess);
}

function parseClock(value) {
  const text=stripHtml(value).toLowerCase().replace(/\./g,'');
  const match=text.match(/(\d{1,2}):(\d{2})\s*(am|pm)/i);
  if (!match) return null;
  let hour=Number(match[1])%12;
  if (match[3].toLowerCase()==='pm') hour+=12;
  return {hour,minute:Number(match[2])};
}

function eventRisk(type,settings=DEFAULT_RISK_SETTINGS) {
  if (['fomc_decision','fomc_press_conference','powell_monetary_speech'].includes(type)) {
    return {before:Number(settings.fomcBeforeMinutes),after:Number(settings.fomcAfterMinutes)};
  }
  if (type==='jobless_claims') {
    return {before:Number(settings.claimsBeforeMinutes),after:Number(settings.claimsAfterMinutes)};
  }
  if (['cpi','core_cpi','pce','core_pce','nfp','unemployment_rate','gdp','retail_sales','ism_manufacturing','ism_services'].includes(type)) {
    return {before:Number(settings.macroBeforeMinutes),after:Number(settings.macroAfterMinutes)};
  }
  return {before:0,after:0};
}

function makeEvent({id,type,name,eventAt,source,sourceUrl,updatedAt=Date.now(),actual=null,forecast=null,previous=null,impact='high',metadata={}},settings=DEFAULT_RISK_SETTINGS) {
  const risk=eventRisk(type,settings);
  return {
    id:String(id),type:String(type),name:String(name),eventAt:Number(eventAt),
    eventAtUtc:new Date(Number(eventAt)).toISOString(),country:'United States',currency:'USD',impact,
    actual:actual==null?null:String(actual),forecast:forecast==null?null:String(forecast),
    previous:previous==null?null:String(previous),source:String(source),sourceUrl:String(sourceUrl),
    lastUpdated:Number(updatedAt),riskBeforeMinutes:risk.before,riskAfterMinutes:risk.after,metadata
  };
}

function parseIcsDate(property,value) {
  const raw=String(value||'').trim();
  const match=raw.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})?(Z)?$/);
  if (!match) return NaN;
  const parts=match.slice(1,7).map(Number),[year,month,day,hour,minute,second=0]=parts;
  if (match[7]==='Z') return Date.UTC(year,month-1,day,hour,minute,second);
  if (/TZID=(?:America\/New_York|US-Eastern)/i.test(property)) return easternLocalToUtc(year,month-1,day,hour,minute)+second*1000;
  return Date.UTC(year,month-1,day,hour,minute,second);
}

export function parseBlsIcs(ics,settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  const unfolded=String(ics||'').replace(/\r?\n[ \t]/g,'');
  const entries=[...unfolded.matchAll(/BEGIN:VEVENT([\s\S]*?)END:VEVENT/g)].map(match=>match[1]);
  const out=[];
  for (const entry of entries) {
    const fields={};
    for (const line of entry.split(/\r?\n/)) {
      const colon=line.indexOf(':');
      if (colon<0) continue;
      const property=line.slice(0,colon),key=property.split(';')[0].toUpperCase();
      fields[key]={property,value:line.slice(colon+1).replace(/\\,/g,',').replace(/\\n/gi,' ')};
    }
    const title=String(fields.SUMMARY?.value||'');
    const eventAt=parseIcsDate(fields.DTSTART?.property,fields.DTSTART?.value);
    if (!Number.isFinite(eventAt)) continue;
    const base=fields.UID?.value||`${slug(title)}:${eventAt}`;
    const common={eventAt,source:'U.S. Bureau of Labor Statistics',sourceUrl:SOURCE_URLS.bls,updatedAt,settings,metadata:{
      release:title,uid:fields.UID?.value||'',scheduleSource:'fetched-official-ical',
      scheduleSourceUrl:SOURCE_URLS.bls,scheduleDocumentationUrl:SOURCE_URLS.blsIcalDocs,
      scheduleMode:'fetched',dataAvailability:'schedule-and-official-actuals'
    }};
    if (/consumer price index/i.test(title)) {
      out.push(makeEvent({...common,id:`bls:${base}:cpi`,type:'cpi',name:'Consumer Price Index (CPI)'},settings));
      out.push(makeEvent({...common,id:`bls:${base}:core-cpi`,type:'core_cpi',name:'Core Consumer Price Index (Core CPI)'},settings));
    }
    if (/employment situation/i.test(title)) {
      out.push(makeEvent({...common,id:`bls:${base}:nfp`,type:'nfp',name:'Nonfarm Payrolls (NFP)'},settings));
      out.push(makeEvent({...common,id:`bls:${base}:unemployment`,type:'unemployment_rate',name:'U.S. Unemployment Rate'},settings));
    }
  }
  return out;
}

function validBlsRows(rows=[]) {
  return rows.filter(row=>/^M(?:0[1-9]|1[0-2])$/.test(String(row.period||''))&&Number.isFinite(Number(row.value)))
    .map(row=>({...row,year:Number(row.year),month:Number(String(row.period).slice(1)),value:Number(row.value)}))
    .sort((a,b)=>b.year-a.year||b.month-a.month);
}

export function parseBlsApiPayload(payload) {
  if (String(payload?.status||'')!=='REQUEST_SUCCEEDED') return {};
  return Object.fromEntries((payload?.Results?.series||[]).map(series=>[
    String(series.seriesID||''),validBlsRows(series.data||[])
  ]).filter(([seriesId,rows])=>seriesId&&rows.length));
}

export function parseBlsDownloadDataset(text,seriesIds=Object.values(BLS_SERIES)) {
  const wanted=new Set(seriesIds),out={};
  for (const line of String(text||'').split(/\r?\n/)) {
    const cells=line.trim().split(/\s+/),seriesId=cells[0];
    if (!wanted.has(seriesId)||cells.length<4) continue;
    const [year,period,value]=cells.slice(1,4),row={year,period,value};
    if (!out[seriesId]) out[seriesId]=[];
    out[seriesId].push(row);
  }
  return Object.fromEntries(Object.entries(out).map(([seriesId,rows])=>[seriesId,validBlsRows(rows)]).filter(([,rows])=>rows.length));
}

function rounded(value,digits=1) {
  if (!Number.isFinite(value)) return null;
  return Number(value.toFixed(digits));
}

export function buildBlsActualSnapshot(seriesRows={},sourceBySeries={}) {
  const snapshot={};
  for (const [type,seriesId] of Object.entries(BLS_SERIES)) {
    const rows=validBlsRows(seriesRows[seriesId]||[]),latest=rows[0],prior=rows[1],beforePrior=rows[2];
    if (!latest) continue;
    let actual=null,previous=null,unit='';
    if (type==='cpi'||type==='core_cpi') {
      actual=prior?rounded((latest.value/prior.value-1)*100,1):null;
      previous=prior&&beforePrior?rounded((prior.value/beforePrior.value-1)*100,1):null;
      unit='% MoM';
    } else if (type==='nfp') {
      actual=prior?Math.round(latest.value-prior.value):null;
      previous=prior&&beforePrior?Math.round(prior.value-beforePrior.value):null;
      unit='thousands, monthly change';
    } else if (type==='unemployment_rate') {
      actual=rounded(latest.value,1);previous=prior?rounded(prior.value,1):null;unit='%';
    }
    if (actual==null) continue;
    snapshot[type]={actual:String(actual),previous:previous==null?null:String(previous),forecast:null,
      referencePeriod:`${latest.year}-${String(latest.month).padStart(2,'0')}`,seriesId,unit,
      dataSource:sourceBySeries[seriesId]||'BLS Public Data API v1',
      dataSourceUrl:sourceBySeries[seriesId]?.includes('download')?downloadUrlForSeries(seriesId):SOURCE_URLS.blsApi,
      dataMode:'fetched'};
  }
  return snapshot;
}

function downloadUrlForSeries(seriesId) {
  if ([BLS_SERIES.cpi,BLS_SERIES.core_cpi].includes(seriesId)) return SOURCE_URLS.blsCpiDownload;
  if (seriesId===BLS_SERIES.nfp) return SOURCE_URLS.blsCesDownload;
  return SOURCE_URLS.blsLaborDownload;
}

export function applyBlsActuals(events,snapshot={},now=Date.now()) {
  const latestPast=new Map(),nextFuture=new Map();
  for (const event of events||[]) {
    if (!BLS_SERIES[event.type]) continue;
    if (event.eventAt<=now&&(!latestPast.has(event.type)||event.eventAt>latestPast.get(event.type).eventAt)) latestPast.set(event.type,event);
    if (event.eventAt>now&&(!nextFuture.has(event.type)||event.eventAt<nextFuture.get(event.type).eventAt)) nextFuture.set(event.type,event);
  }
  return (events||[]).map(event=>{
    const isLatestPast=latestPast.get(event.type)?.id===event.id,isNextFuture=nextFuture.get(event.type)?.id===event.id;
    if ((!isLatestPast&&!isNextFuture)||!snapshot[event.type]) return event;
    const data=snapshot[event.type];
    return {...event,actual:isLatestPast?data.actual:null,forecast:null,previous:isLatestPast?data.previous:data.actual,metadata:{...event.metadata,
      seriesId:data.seriesId,referencePeriod:data.referencePeriod,unit:data.unit,
      dataSource:data.dataSource,dataSourceUrl:data.dataSourceUrl,dataMode:data.dataMode,
      ...(data.snapshotGeneratedAt?{dataSnapshotGeneratedAt:data.snapshotGeneratedAt}:{}),
      ...(isNextFuture?{previousReferencePeriod:data.referencePeriod}:{}),
      actualMethod:event.type==='nfp'?'latest SA level minus prior SA level':
        (event.type==='unemployment_rate'?'latest SA rate':'latest SA index versus prior SA index')
    }};
  });
}

export function readBundledBlsFallback(settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  if (Number(BLS_OFFICIAL_SNAPSHOT?.schema)!==1||!BLS_OFFICIAL_SNAPSHOT?.scheduleIcs) return {ok:false,events:[],snapshot:{}};
  const events=parseBlsIcs(BLS_OFFICIAL_SNAPSHOT.scheduleIcs,settings,updatedAt).map(event=>({...event,metadata:{...event.metadata,
    scheduleSource:'official-ical-snapshot-fallback',scheduleMode:'official-snapshot-fallback',
    scheduleSnapshotGeneratedAt:BLS_OFFICIAL_SNAPSHOT.generatedAt,
    scheduleSourceUrl:BLS_OFFICIAL_SNAPSHOT.sources?.schedule?.url||SOURCE_URLS.bls
  }}));
  const sourceBySeries=Object.fromEntries(Object.values(BLS_SERIES).map(seriesId=>[
    seriesId,'BLS Public Data API v1 via verified repository snapshot'
  ]));
  const snapshot=buildBlsActualSnapshot(BLS_OFFICIAL_SNAPSHOT.series||{},sourceBySeries);
  for (const data of Object.values(snapshot)) {
    data.dataMode='official-snapshot-fallback';data.snapshotGeneratedAt=BLS_OFFICIAL_SNAPSHOT.generatedAt;
    data.dataSourceUrl=BLS_OFFICIAL_SNAPSHOT.sources?.actuals?.url||SOURCE_URLS.blsApi;
  }
  return {ok:events.length>0&&Object.keys(snapshot).length===Object.keys(BLS_SERIES).length,events,snapshot,
    generatedAt:BLS_OFFICIAL_SNAPSHOT.generatedAt};
}

function parseMonthDay(value,year,time) {
  const match=stripHtml(value).match(/([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?/);
  const month=MONTHS[String(match?.[1]||'').toLowerCase()],clock=parseClock(time);
  if (month==null||!match||!clock) return NaN;
  return easternLocalToUtc(Number(match[3]||year),month,Number(match[2]),clock.hour,clock.minute);
}

export function parseBeaScheduleHtml(html,settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  const year=Number(String(html||'').match(/Year\s+(20\d{2})/i)?.[1]||new Date(updatedAt).getUTCFullYear());
  const rows=[...String(html||'').matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)],out=[];
  for (const row of rows) {
    const body=row[1];
    const date=body.match(/class=["'][^"']*release-date[^"']*["'][^>]*>([\s\S]*?)<\/div>/i)?.[1];
    const time=body.match(/<small\b[^>]*>([\s\S]*?)<\/small>/i)?.[1];
    const title=stripHtml(body.match(/class=["'][^"']*release-title[^"']*["'][^>]*>([\s\S]*?)<\/td>/i)?.[1]);
    const eventAt=parseMonthDay(date,year,time);
    if (!title||!Number.isFinite(eventAt)) continue;
    const common={eventAt,source:'U.S. Bureau of Economic Analysis',sourceUrl:SOURCE_URLS.bea,updatedAt,metadata:{release:title}};
    const key=`bea:${eventAt}:${slug(title)}`;
    if (/\bGDP\b|gross domestic product/i.test(title)) out.push(makeEvent({...common,id:`${key}:gdp`,type:'gdp',name:`GDP — ${title}`},settings));
    if (/personal income and outlays/i.test(title)) {
      out.push(makeEvent({...common,id:`${key}:pce`,type:'pce',name:'Personal Consumption Expenditures Price Index (PCE)'},settings));
      out.push(makeEvent({...common,id:`${key}:core-pce`,type:'core_pce',name:'Core PCE Price Index'},settings));
    }
  }
  return out;
}

function pageYearMonth(html,sourceUrl,updatedAt) {
  const text=stripHtml(String(html||'').slice(0,25000));
  const match=text.match(/(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})/i);
  if (match) return {year:Number(match[2]),month:MONTHS[match[1].toLowerCase()]};
  const urlMatch=String(sourceUrl||'').match(/(20\d{2})-([a-z]+)\.htm/i);
  return {year:Number(urlMatch?.[1]||new Date(updatedAt).getUTCFullYear()),month:MONTHS[String(urlMatch?.[2]||'').toLowerCase()]};
}

export function parseFedCalendarHtml(html,sourceUrl,settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  const {year,month}=pageYearMonth(html,sourceUrl,updatedAt),out=[];
  if (!Number.isFinite(year)||month==null) return out;
  const blocks=String(html||'').split(/<div\s+class=["'][^"']*panel\b/gi).slice(1);
  for (const block of blocks) {
    const time=parseClock(block.match(/col-xs-2[\s\S]*?<p[^>]*>([\s\S]*?)<\/p>/i)?.[1]);
    const day=Number(stripHtml(block.match(/col-xs-3[\s\S]*?<p[^>]*>([\s\S]*?)<\/p>/i)?.[1]));
    const middle=stripHtml(block.match(/col-xs-7[\s\S]*?(?=<div\s+class=["'][^"']*col-xs-3)/i)?.[0]);
    if (!time||!Number.isFinite(day)||day<1||day>31||!middle) continue;
    const eventAt=easternLocalToUtc(year,month,day,time.hour,time.minute);
    let type='',name='',impact='high';
    if (/FOMC Meeting/i.test(middle)) { type='fomc_decision';name='Federal Reserve FOMC Rate Decision'; }
    else if (/FOMC Press Conference/i.test(middle)) { type='fomc_press_conference';name='FOMC Press Conference'; }
    else if (/Speech\s*-\s*(?:Chair|Governor)\s+Jerome\s+H\.\s+Powell/i.test(middle)&&/monetary|economic outlook|inflation|price stability|interest rate|central bank/i.test(middle)) {
      type='powell_monetary_speech';name=`Powell monetary-policy speech — ${middle.slice(0,180)}`;
    } else if (/Speech\s*-/i.test(middle)&&/monetary policy|economic outlook|inflation|price stability|interest rate/i.test(middle)) {
      type='fed_speech';name=`Fed speech — ${middle.slice(0,180)}`;impact='medium';
    }
    if (!type) continue;
    out.push(makeEvent({id:`fed:${type}:${eventAt}`,type,name,eventAt,source:'Board of Governors of the Federal Reserve System',sourceUrl,updatedAt,impact,metadata:{calendarText:middle}},settings));
  }
  return out;
}

export function parseCensusScheduleHtml(html,settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  const rows=[...String(html||'').matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)],out=[];
  for (const row of rows) {
    const text=stripHtml(row[1]);
    if (!/Advance Monthly Sales for Retail and Food Services/i.test(text)) continue;
    const dateMatch=text.match(/(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(20\d{2})/i);
    const clock=parseClock(text),month=MONTHS[String(dateMatch?.[1]||'').toLowerCase()];
    if (!dateMatch||!clock||month==null) continue;
    const eventAt=easternLocalToUtc(Number(dateMatch[3]),month,Number(dateMatch[2]),clock.hour,clock.minute);
    const officialId=text.match(/\bA(20\d{10})\b/)?.[0]||String(eventAt);
    out.push(makeEvent({id:`census:retail-sales:${officialId}`,type:'retail_sales',name:'U.S. Advance Retail Sales',eventAt,source:'U.S. Census Bureau',sourceUrl:SOURCE_URLS.census,updatedAt,metadata:{release:text.slice(0,240),officialId}},settings));
  }
  return out;
}

function isoDate(year,monthIndex,day) {
  return `${year}-${String(monthIndex+1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
}

function nthNyseBusinessDay(year,monthIndex,ordinal) {
  const holidays=NYSE_HOLIDAYS[year];
  if (!holidays) return null;
  const closed=new Set(holidays),daysInMonth=new Date(Date.UTC(year,monthIndex+1,0)).getUTCDate();
  let count=0;
  for (let day=1;day<=daysInMonth;day++) {
    const weekday=new Date(Date.UTC(year,monthIndex,day)).getUTCDay();
    if (weekday===0||weekday===6||closed.has(isoDate(year,monthIndex,day))) continue;
    if (++count===ordinal) return day;
  }
  return null;
}

export function buildIsmDerivedSchedule(settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now(),from=updatedAt-45*24*60*60_000,to=updatedAt+370*24*60*60_000) {
  const out=[],cursor=new Date(Date.UTC(new Date(from).getUTCFullYear(),new Date(from).getUTCMonth(),1));
  const last=new Date(Date.UTC(new Date(to).getUTCFullYear(),new Date(to).getUTCMonth(),1));
  while (cursor<=last) {
    const year=cursor.getUTCFullYear(),month=cursor.getUTCMonth();
    if (NYSE_HOLIDAYS[year]) {
      const definitions=[
        {type:'ism_manufacturing',name:'ISM Manufacturing PMI release',ordinal:month===0?2:1,ruleUrl:SOURCE_URLS.ismManufacturingRule,rule:'first NYSE business day; January second NYSE business day'},
        {type:'ism_services',name:'ISM Services PMI release',ordinal:month===0?4:3,ruleUrl:SOURCE_URLS.ismServicesRule,rule:'third NYSE business day; January fourth NYSE business day'}
      ];
      for (const definition of definitions) {
        const day=nthNyseBusinessDay(year,month,definition.ordinal);
        if (!day) continue;
        const eventAt=easternLocalToUtc(year,month,day,10,0);
        if (eventAt<from||eventAt>to) continue;
        out.push(makeEvent({id:`ism:${definition.type}:${eventAt}`,type:definition.type,name:definition.name,eventAt,
          source:'Institute for Supply Management',sourceUrl:definition.ruleUrl,updatedAt,
          actual:null,forecast:null,previous:null,metadata:{
            scheduleSource:'derived-official-rule',scheduleSourceUrl:definition.ruleUrl,
            scheduleRule:definition.rule,holidayCalendarSource:'NYSE official holidays',
            holidayCalendarUrl:SOURCE_URLS.nyseHolidays,scheduleMode:'derived',
            dataAvailability:'schedule-only',dataSource:'ISM official published release rule',
            dataSourceUrl:definition.ruleUrl,redistribution:'schedule only; no ISM PMI values extracted or redistributed'
          }},settings));
      }
    }
    cursor.setUTCMonth(cursor.getUTCMonth()+1);
  }
  return out.sort((a,b)=>a.eventAt-b.eventAt||a.id.localeCompare(b.id));
}

export function parseRssItems(xml) {
  return [...String(xml||'').matchAll(/<item\b[^>]*>([\s\S]*?)<\/item>/gi)].map(match=>{
    const body=match[1],field=name=>{
      const value=body.match(new RegExp(`<${name}\\b[^>]*>([\\s\\S]*?)<\\/${name}>`,'i'))?.[1]||'';
      return stripHtml(value.replace(/^<!\[CDATA\[|\]\]>$/g,''));
    };
    const link=field('link'),guid=field('guid'),publishedAt=Date.parse(field('pubDate')||field('dc:date'));
    return {title:field('title'),description:field('description'),url:link||guid,publishedAt:Number.isFinite(publishedAt)?publishedAt:0,guid};
  }).filter(item=>item.title);
}

function latestRssSummary(items,pattern,eventAt,maxAgeMs=45*24*60*60*1000) {
  const match=(items||[]).filter(item=>pattern.test(item.title)).sort((a,b)=>b.publishedAt-a.publishedAt)[0];
  if (!match||!match.publishedAt||eventAt-match.publishedAt>maxAgeMs) return null;
  return {summary:(match.description||match.title).slice(0,500),actualAt:match.publishedAt,actualUrl:match.url};
}

function firstMatch(text,patterns,suffix='') {
  for (const pattern of patterns) {
    const match=String(text||'').match(pattern);
    if (match?.[1]) return `${match[1].replace(/,/g,'')}${suffix}`;
  }
  return null;
}

export function extractOfficialActual(type,summary) {
  const text=stripHtml(summary);
  if (type==='core_cpi') return firstMatch(text,[/all items less food and energy[^.]*?(?:rose|increased|fell|declined)\s+([+-]?\d+(?:\.\d+)?)\s*percent/i],'%');
  if (type==='cpi') return firstMatch(text,[/consumer price index[^.]*?(?:rose|increased|fell|declined)\s+([+-]?\d+(?:\.\d+)?)\s*percent/i,/all items index[^.]*?(?:rose|increased|fell|declined)\s+([+-]?\d+(?:\.\d+)?)\s*percent/i],'%');
  if (type==='nfp') return firstMatch(text,[/nonfarm payroll employment[^.]*?(?:rose|increased|fell|declined)\s+(?:by\s+)?([+-]?[\d,]+)/i],'');
  if (type==='unemployment_rate') return firstMatch(text,[/unemployment rate[^.]*?(?:at|to)\s+([+-]?\d+(?:\.\d+)?)\s*percent/i],'%');
  if (type==='gdp') return firstMatch(text,[/gross domestic product[^.]*?(?:increased|decreased)\s+(?:at an annual rate of\s+)?([+-]?\d+(?:\.\d+)?)\s*percent/i],'%');
  if (type==='core_pce') return firstMatch(text,[/excluding food and energy[^.]*?(?:increased|decreased|rose|fell)\s+([+-]?\d+(?:\.\d+)?)\s*percent/i],'%');
  if (type==='pce') return firstMatch(text,[/PCE price index[^.]*?(?:increased|decreased|rose|fell)\s+([+-]?\d+(?:\.\d+)?)\s*percent/i],'%');
  if (type==='jobless_claims') return firstMatch(text,[/initial claims[^.]*?(?:were|was|at|to)\s+([\d,]+)/i],'');
  if (type==='fomc_decision') {
    const range=text.match(/target range[^.]*?([\d.]+)\s*(?:to|-)\s*([\d.]+)\s*percent/i);
    if (range) return `${range[1]}%–${range[2]}%`;
  }
  return null;
}

export function enrichCalendarActuals(events,feeds={}) {
  const bea=parseRssItems(feeds.bea),fed=parseRssItems(`${feeds.fedMonetary||''}${feeds.fedSpeeches||''}`);
  return (events||[]).map(event=>{
    let update=null;
    if (event.type==='gdp') update=latestRssSummary(bea,/gross domestic product|\bGDP\b/i,event.eventAt);
    else if (['pce','core_pce'].includes(event.type)) update=latestRssSummary(bea,/personal income and outlays|personal consumption expenditures/i,event.eventAt);
    else if (event.type.startsWith('fomc_')) update=latestRssSummary(fed,/federal reserve|FOMC|monetary policy/i,event.eventAt,7*24*60*60*1000);
    else if (event.type==='powell_monetary_speech') update=latestRssSummary(fed,/Powell/i,event.eventAt,7*24*60*60*1000);
    if (!update||update.actualAt<event.eventAt-5*60_000) return event;
    const actual=extractOfficialActual(event.type,update.summary);
    return {...event,actual,lastUpdated:Math.max(event.lastUpdated,update.actualAt),metadata:{...event.metadata,actualAt:update.actualAt,actualUrl:update.actualUrl,officialReleaseSummary:update.summary}};
  });
}

function parseUsDate(value) {
  const match=String(value||'').match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  return match?Date.UTC(Number(match[3]),Number(match[1])-1,Number(match[2])):NaN;
}

function numericXmlValue(value) {
  const clean=decodeHtml(value).replace(/,/g,'').trim();
  return /^-?\d+(?:\.\d+)?$/.test(clean)?Number(clean):null;
}

export function parseJoblessClaimsXml(xml) {
  return [...String(xml||'').matchAll(/<week>([\s\S]*?)<\/week>/gi)].map(match=>{
    const body=match[1],weekEndedText=decodeHtml(body.match(/<weekEnded>([\s\S]*?)<\/weekEnded>/i)?.[1]);
    const initial=body.match(/<InitialClaims>([\s\S]*?)<\/InitialClaims>/i)?.[1]||'';
    const actual=numericXmlValue(initial.match(/<SA>([\s\S]*?)<\/SA>/i)?.[1]);
    return {weekEndedText,weekEnded:parseUsDate(weekEndedText),actual};
  }).filter(row=>Number.isFinite(row.weekEnded)).sort((a,b)=>a.weekEnded-b.weekEnded);
}

function dolReleaseAt(weekEnded) {
  const saturday=new Date(weekEnded);
  if (saturday.getUTCDay()!==6) return null;
  const thursday=new Date(Date.UTC(saturday.getUTCFullYear(),saturday.getUTCMonth(),saturday.getUTCDate()+5));
  const year=thursday.getUTCFullYear(),holidays=OPM_FEDERAL_HOLIDAYS[year];
  if (!holidays) return null;
  const shift=holidays.includes(isoDate(year,thursday.getUTCMonth(),thursday.getUTCDate()))?-1:0;
  return easternLocalToUtc(year,thursday.getUTCMonth(),thursday.getUTCDate()+shift,8,30);
}

export function buildJoblessClaimsEvents(dolXml,settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  const rows=parseJoblessClaimsXml(dolXml),out=[];
  let lastActual=null,upcomingPreviousAssigned=false;
  for (let index=0;index<rows.length;index++) {
    const row=rows[index],eventAt=dolReleaseAt(row.weekEnded);
    if (!Number.isFinite(eventAt)) continue;
    let previous=null;
    if (row.actual!=null) { previous=lastActual;lastActual=row.actual; }
    else if (eventAt>=updatedAt&&!upcomingPreviousAssigned) { previous=lastActual;upcomingPreviousAssigned=true; }
    out.push(makeEvent({id:`dol:jobless-claims:${row.weekEndedText.replaceAll('/','-')}`,type:'jobless_claims',
      name:'U.S. Initial Jobless Claims',eventAt,source:'U.S. Department of Labor — ETA/OUI',
      sourceUrl:SOURCE_URLS.dol,updatedAt,actual:row.actual,forecast:null,previous,
      metadata:{
        weekEnded:row.weekEndedText,scheduleSource:'derived-official-rule',
        scheduleSourceUrl:SOURCE_URLS.dolScheduleRule,
        scheduleRule:'normally Thursday at 08:30 ET; preceding Wednesday when a federal holiday conflicts',
        holidayCalendarSource:'U.S. Office of Personnel Management published federal holiday schedule',
        holidayCalendarUrl:SOURCE_URLS.opmHolidays,scheduleMode:'derived',
        dataAvailability:'official-actuals',dataSource:'DOL ETA/OUI official XML weekly claims data',
        dataSourceUrl:SOURCE_URLS.dol,dataMode:'fetched'
      }},settings));
  }
  return out;
}

export const buildJoblessClaimsEvent=buildJoblessClaimsEvents;

function monthPageUrl(date) {
  const month=Object.keys(MONTHS).find(name=>MONTHS[name]===date.getUTCMonth());
  return `${SOURCE_URLS.fedBase}${date.getUTCFullYear()}-${month}.htm`;
}

function nextMonth(date) { return new Date(Date.UTC(date.getUTCFullYear(),date.getUTCMonth()+1,1)); }

async function fetchText(url,options={}) {
  const method=String(options.method||'GET').toUpperCase();
  const response=await fetch(url,{method,body:options.body,headers:{
    accept:'text/html,application/xml,text/calendar,application/json;q=0.9,*/*;q=0.5',
    'user-agent':'GoldSignalsX/1.0 (+official-calendar; contact: https://github.com/Skyeagle123/GoldSignalsX-worker)',
    ...(options.headers||{})
  },signal:AbortSignal.timeout(Number(options.timeoutMs||15_000)),
  ...(method==='GET'?{cf:{cacheTtl:300,cacheEverything:true}}:{})});
  if (!response.ok) throw new Error(`${new URL(url).hostname} ${response.status}`);
  return response.text();
}

async function fetchBlsActualSnapshot(now) {
  const year=new Date(now).getUTCFullYear(),seriesIds=Object.values(BLS_SERIES),seriesRows={},sourceBySeries={};
  let apiError='';
  try {
    const text=await fetchText(SOURCE_URLS.blsApi,{method:'POST',headers:{'content-type':'application/json'},
      body:JSON.stringify({seriesid:seriesIds,startyear:String(year-1),endyear:String(year)})});
    Object.assign(seriesRows,parseBlsApiPayload(JSON.parse(text)));
    for (const seriesId of Object.keys(seriesRows)) sourceBySeries[seriesId]='BLS Public Data API v1';
  } catch (error) { apiError=String(error?.message||error); }

  const missing=seriesIds.filter(seriesId=>!seriesRows[seriesId]?.length),downloadErrors=[];
  const groups=[
    {url:SOURCE_URLS.blsCpiDownload,ids:[BLS_SERIES.cpi,BLS_SERIES.core_cpi]},
    {url:SOURCE_URLS.blsCesDownload,ids:[BLS_SERIES.nfp]},
    {url:SOURCE_URLS.blsLaborDownload,ids:[BLS_SERIES.unemployment_rate],timeoutMs:30_000}
  ].filter(group=>group.ids.some(seriesId=>missing.includes(seriesId)));
  for (const group of groups) {
    try {
      const text=await fetchText(group.url,{timeoutMs:group.timeoutMs||20_000});
      const parsed=parseBlsDownloadDataset(text,group.ids);
      for (const [seriesId,rows] of Object.entries(parsed)) {
        if (!seriesRows[seriesId]?.length) {
          seriesRows[seriesId]=rows;sourceBySeries[seriesId]='BLS official downloadable time-series dataset';
        }
      }
    } catch (error) { downloadErrors.push(`${new URL(group.url).pathname}: ${String(error?.message||error)}`); }
  }
  const snapshot=buildBlsActualSnapshot(seriesRows,sourceBySeries),missingAfter=Object.keys(BLS_SERIES).filter(type=>!snapshot[type]);
  return {ok:missingAfter.length===0,snapshot,sourceBySeries,missing:missingAfter,
    api:{ok:!apiError,...(apiError?{error:apiError}:{})},
    downloads:{attempted:groups.length,ok:downloadErrors.length===0,...(downloadErrors.length?{errors:downloadErrors}:{})}};
}

export async function fetchOfficialCalendar(settings=DEFAULT_RISK_SETTINGS,now=Date.now()) {
  const current=new Date(now),fedUrls=[monthPageUrl(current),monthPageUrl(nextMonth(current))];
  const dolBody=new URLSearchParams({level:'us',strtdate:String(current.getUTCFullYear()),enddate:String(current.getUTCFullYear()+1),filetype:'xml',submit:'Submit'}).toString();
  const requests={
    bls:fetchText(SOURCE_URLS.bls),bea:fetchText(SOURCE_URLS.bea),census:fetchText(SOURCE_URLS.census),
    dol:fetchText(SOURCE_URLS.dol,{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded'},body:dolBody,timeoutMs:30_000}),
    blsActuals:fetchBlsActualSnapshot(now),fedCurrent:fetchText(fedUrls[0]),fedNext:fetchText(fedUrls[1]),
    beaRss:fetchText(SOURCE_URLS.beaRss),fedMonetary:fetchText(SOURCE_URLS.fedMonetary),fedSpeeches:fetchText(SOURCE_URLS.fedSpeeches)
  };
  const entries=await Promise.all(Object.entries(requests).map(async([name,promise])=>{
    try {
      const value=await promise;
      return [name,name==='blsActuals'?value:{ok:true,text:value}];
    }
    catch (error) { return [name,{ok:false,error:String(error?.message||error),text:''}]; }
  }));
  const result=Object.fromEntries(entries),events=[],bundledBls=readBundledBlsFallback(settings,now);
  let blsEvents=result.bls.ok?parseBlsIcs(result.bls.text,settings,now):[];
  const blsScheduleFallback=blsEvents.length===0&&bundledBls.ok;
  if (blsScheduleFallback) blsEvents=bundledBls.events;
  events.push(...blsEvents);
  if (result.bea.ok) events.push(...parseBeaScheduleHtml(result.bea.text,settings,now));
  if (result.census.ok) events.push(...parseCensusScheduleHtml(result.census.text,settings,now));
  const ismEvents=buildIsmDerivedSchedule(settings,now,now-45*24*60*60_000,now+370*24*60*60_000);
  events.push(...ismEvents);
  if (result.dol.ok) events.push(...buildJoblessClaimsEvents(result.dol.text,settings,now));
  if (result.fedCurrent.ok) events.push(...parseFedCalendarHtml(result.fedCurrent.text,fedUrls[0],settings,now));
  if (result.fedNext.ok) events.push(...parseFedCalendarHtml(result.fedNext.text,fedUrls[1],settings,now));
  const feeds={
    bea:result.beaRss.text,fedMonetary:result.fedMonetary.text,fedSpeeches:result.fedSpeeches.text
  };
  const liveBlsActuals=result.blsActuals.snapshot||{},effectiveBlsActuals={...bundledBls.snapshot,...liveBlsActuals};
  const blsActualFallbackTypes=Object.keys(bundledBls.snapshot||{}).filter(type=>!liveBlsActuals[type]);
  const withBls=applyBlsActuals(events,effectiveBlsActuals,now);
  const unique=[...new Map(enrichCalendarActuals(withBls,feeds).map(event=>[event.id,event])).values()]
    .filter(event=>Number.isFinite(event.eventAt)&&event.eventAt>=now-45*24*60*60*1000&&event.eventAt<=now+370*24*60*60*1000)
    .sort((a,b)=>a.eventAt-b.eventAt||a.id.localeCompare(b.id));
  const sourceStatus=Object.fromEntries(Object.entries(result).filter(([name])=>!['beaRss','fedMonetary','fedSpeeches'].includes(name)).map(([name,value])=>[name,
    name==='blsActuals'?{ok:value.ok,api:value.api,downloads:value.downloads,missing:value.missing,
      seriesSources:value.sourceBySeries}:{ok:value.ok,...(!value.ok?{error:value.error}:{})}
  ]));
  sourceStatus.bls={ok:blsEvents.length>0,mode:blsScheduleFallback?'official-ical-snapshot-fallback':'fetched-official-ical',
    liveFetch:{ok:result.bls.ok,...(!result.bls.ok?{error:result.bls.error}:{})},
    ...(blsScheduleFallback?{snapshotGeneratedAt:bundledBls.generatedAt}:{})};
  sourceStatus.blsActuals={ok:Object.keys(effectiveBlsActuals).length===Object.keys(BLS_SERIES).length,
    mode:blsActualFallbackTypes.length?'official-snapshot-fallback':'fetched',
    live:{api:result.blsActuals.api,downloads:result.blsActuals.downloads,missing:result.blsActuals.missing},
    fallbackTypes:blsActualFallbackTypes,...(blsActualFallbackTypes.length?{snapshotGeneratedAt:bundledBls.generatedAt}:{})};
  sourceStatus.ism={ok:ismEvents.length>0,scheduleSource:'derived-official-rule',dataAvailability:'schedule-only',
    supportedHolidayYears:Object.keys(NYSE_HOLIDAYS).map(Number),...(!ismEvents.length?{error:'no_verified_nyse_holiday_year_in_requested_window'}:{})};
  return {ok:unique.length>0,updatedAt:now,source:'official-multi-source',events:unique,sourceStatus};
}

export function calendarRiskSnapshot(events,now=Date.now(),settings=DEFAULT_RISK_SETTINGS) {
  const active=(Array.isArray(events)?events:[]).filter(event=>{
    if (event.impact!=='high') return false;
    const start=event.eventAt-Number(event.riskBeforeMinutes??eventRisk(event.type,settings).before)*60_000;
    const end=event.eventAt+Number(event.riskAfterMinutes??eventRisk(event.type,settings).after)*60_000;
    return now>=start&&now<=end;
  }).sort((a,b)=>a.eventAt-b.eventAt);
  const upcoming=(Array.isArray(events)?events:[]).filter(event=>event.eventAt>=now).sort((a,b)=>a.eventAt-b.eventAt)[0]||null;
  const blockUntil=active.length?Math.max(...active.map(event=>event.eventAt+Number(event.riskAfterMinutes||0)*60_000)):null;
  return {
    active:active.length>0,blockTechnicalSignal:active.length>0,
    reason:active.length?`High-impact calendar risk: ${active.map(event=>event.name).join(' / ')}`:'',
    blockUntil,activeEvents:active,upcoming
  };
}

export function formatCalendarEvent(event,localTimeZone='Asia/Beirut') {
  const metadata=event?.metadata||{};
  return {
    ...event,
    dataSource:event.dataSource||metadata.dataSource||event.source,
    scheduleSource:event.scheduleSource||metadata.scheduleSource||null,
    dataMode:event.dataMode||metadata.dataMode||null,
    scheduleMode:event.scheduleMode||metadata.scheduleMode||null,
    eventAtUtc:new Date(Number(event.eventAt)).toISOString(),
    eventAtLocal:new Intl.DateTimeFormat('en-GB',{
      timeZone:localTimeZone,year:'numeric',month:'2-digit',day:'2-digit',
      hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false,timeZoneName:'short'
    }).format(new Date(Number(event.eventAt))),
    localTimeZone
  };
}

export { DEFAULT_RISK_SETTINGS, SOURCE_URLS };
