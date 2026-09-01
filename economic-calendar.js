const SOURCE_URLS = Object.freeze({
  bls:'https://www.bls.gov/schedule/news_release/bls.ics',
  blsCpi:'https://www.bls.gov/feed/cpi_latest.rss',
  blsCes:'https://www.bls.gov/feed/ces_latest.rss',
  blsCps:'https://www.bls.gov/feed/cps_latest.rss',
  bea:'https://www.bea.gov/news/schedule',
  beaRss:'https://apps.bea.gov/rss/rss.xml',
  census:'https://www.census.gov/economic-indicators/calendar-listview.html',
  dol:'https://www.dol.gov/rss/releases.xml',
  ism:'https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/',
  fedBase:'https://www.federalreserve.gov/newsevents/',
  fedMonetary:'https://www.federalreserve.gov/feeds/press_monetary.xml',
  fedSpeeches:'https://www.federalreserve.gov/feeds/speeches.xml'
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
  if (/TZID=America\/New_York/i.test(property)) return easternLocalToUtc(year,month-1,day,hour,minute)+second*1000;
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
    const common={eventAt,source:'U.S. Bureau of Labor Statistics',sourceUrl:SOURCE_URLS.bls,updatedAt,settings,metadata:{release:title,uid:fields.UID?.value||''}};
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

export function parseIsmCalendarHtml(html,settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  const rows=[...String(html||'').matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)],out=[];
  for (const row of rows) {
    const cells=[...row[1].matchAll(/<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi)].map(match=>stripHtml(match[1]));
    const monthMatch=String(cells[0]||'').match(/([A-Za-z]+)\s+(20\d{2})/);
    const month=MONTHS[String(monthMatch?.[1]||'').toLowerCase()],year=Number(monthMatch?.[2]);
    if (month==null||!Number.isFinite(year)) continue;
    for (const [index,type,name] of [[1,'ism_manufacturing','ISM Manufacturing PMI release'],[2,'ism_services','ISM Services PMI release']]) {
      const day=Number(String(cells[index]||'').match(/\d{1,2}/)?.[0]);
      if (!Number.isFinite(day)||day<1||day>31) continue;
      const eventAt=easternLocalToUtc(year,month,day,10,0);
      out.push(makeEvent({id:`ism:${type}:${eventAt}`,type,name,eventAt,source:'Institute for Supply Management',sourceUrl:SOURCE_URLS.ism,updatedAt,actual:null,forecast:null,previous:null,metadata:{redistribution:'schedule-only; PMI values require ISM permission'}},settings));
    }
  }
  return out;
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
  const cpi=parseRssItems(feeds.blsCpi),ces=parseRssItems(feeds.blsCes),cps=parseRssItems(feeds.blsCps);
  const bea=parseRssItems(feeds.bea),fed=parseRssItems(`${feeds.fedMonetary||''}${feeds.fedSpeeches||''}`),dol=parseRssItems(feeds.dol);
  return (events||[]).map(event=>{
    let update=null;
    if (['cpi','core_cpi'].includes(event.type)) update=latestRssSummary(cpi,/consumer price index/i,event.eventAt);
    else if (event.type==='nfp') update=latestRssSummary(ces,/employment|payroll/i,event.eventAt);
    else if (event.type==='unemployment_rate') update=latestRssSummary(cps,/labor force|unemployment/i,event.eventAt);
    else if (event.type==='gdp') update=latestRssSummary(bea,/gross domestic product|\bGDP\b/i,event.eventAt);
    else if (['pce','core_pce'].includes(event.type)) update=latestRssSummary(bea,/personal income and outlays|personal consumption expenditures/i,event.eventAt);
    else if (event.type.startsWith('fomc_')) update=latestRssSummary(fed,/federal reserve|FOMC|monetary policy/i,event.eventAt,7*24*60*60*1000);
    else if (event.type==='powell_monetary_speech') update=latestRssSummary(fed,/Powell/i,event.eventAt,7*24*60*60*1000);
    else if (event.type==='jobless_claims') update=latestRssSummary(dol,/Unemployment Insurance Weekly Claims/i,event.eventAt,14*24*60*60*1000);
    if (!update||update.actualAt<event.eventAt-5*60_000) return event;
    const actual=extractOfficialActual(event.type,update.summary);
    return {...event,actual,lastUpdated:Math.max(event.lastUpdated,update.actualAt),metadata:{...event.metadata,actualAt:update.actualAt,actualUrl:update.actualUrl,officialReleaseSummary:update.summary}};
  });
}

function nextThursdayAfter(timestamp) {
  const date=new Date(timestamp+12*60*60*1000);
  const day=date.getUTCDay(),add=((4-day+7)%7)||7;
  const target=new Date(Date.UTC(date.getUTCFullYear(),date.getUTCMonth(),date.getUTCDate()+add));
  return easternLocalToUtc(target.getUTCFullYear(),target.getUTCMonth(),target.getUTCDate(),8,30);
}

export function buildJoblessClaimsEvent(dolXml,settings=DEFAULT_RISK_SETTINGS,updatedAt=Date.now()) {
  const latest=parseRssItems(dolXml).filter(item=>/Unemployment Insurance Weekly Claims/i.test(item.title)).sort((a,b)=>b.publishedAt-a.publishedAt)[0];
  if (!latest?.publishedAt||updatedAt-latest.publishedAt>14*24*60*60*1000) return [];
  const eventAt=nextThursdayAfter(latest.publishedAt);
  return [makeEvent({id:`dol:jobless-claims:${eventAt}`,type:'jobless_claims',name:'U.S. Initial Jobless Claims',eventAt,source:'U.S. Department of Labor',sourceUrl:SOURCE_URLS.dol,updatedAt,metadata:{derivedFromOfficialWeeklyFeed:true,lastOfficialReleaseAt:latest.publishedAt}},settings)];
}

function monthPageUrl(date) {
  const month=Object.keys(MONTHS).find(name=>MONTHS[name]===date.getUTCMonth());
  return `${SOURCE_URLS.fedBase}${date.getUTCFullYear()}-${month}.htm`;
}

function nextMonth(date) { return new Date(Date.UTC(date.getUTCFullYear(),date.getUTCMonth()+1,1)); }

async function fetchText(url) {
  const response=await fetch(url,{headers:{accept:'text/html,application/xml,text/calendar;q=0.9,*/*;q=0.5','user-agent':'GoldSignalsX/1.0 (+official-calendar)'},signal:AbortSignal.timeout(15_000),cf:{cacheTtl:300,cacheEverything:true}});
  if (!response.ok) throw new Error(`${new URL(url).hostname} ${response.status}`);
  return response.text();
}

export async function fetchOfficialCalendar(settings=DEFAULT_RISK_SETTINGS,now=Date.now()) {
  const current=new Date(now),fedUrls=[monthPageUrl(current),monthPageUrl(nextMonth(current))];
  const requests={
    bls:fetchText(SOURCE_URLS.bls),bea:fetchText(SOURCE_URLS.bea),census:fetchText(SOURCE_URLS.census),
    dol:fetchText(SOURCE_URLS.dol),ism:fetchText(SOURCE_URLS.ism),fedCurrent:fetchText(fedUrls[0]),fedNext:fetchText(fedUrls[1]),
    blsCpi:fetchText(SOURCE_URLS.blsCpi),blsCes:fetchText(SOURCE_URLS.blsCes),blsCps:fetchText(SOURCE_URLS.blsCps),
    beaRss:fetchText(SOURCE_URLS.beaRss),fedMonetary:fetchText(SOURCE_URLS.fedMonetary),fedSpeeches:fetchText(SOURCE_URLS.fedSpeeches)
  };
  const entries=await Promise.all(Object.entries(requests).map(async([name,promise])=>{
    try { return [name,{ok:true,text:await promise}]; }
    catch (error) { return [name,{ok:false,error:String(error?.message||error),text:''}]; }
  }));
  const result=Object.fromEntries(entries),events=[];
  if (result.bls.ok) events.push(...parseBlsIcs(result.bls.text,settings,now));
  if (result.bea.ok) events.push(...parseBeaScheduleHtml(result.bea.text,settings,now));
  if (result.census.ok) events.push(...parseCensusScheduleHtml(result.census.text,settings,now));
  if (result.ism.ok) events.push(...parseIsmCalendarHtml(result.ism.text,settings,now));
  if (result.dol.ok) events.push(...buildJoblessClaimsEvent(result.dol.text,settings,now));
  if (result.fedCurrent.ok) events.push(...parseFedCalendarHtml(result.fedCurrent.text,fedUrls[0],settings,now));
  if (result.fedNext.ok) events.push(...parseFedCalendarHtml(result.fedNext.text,fedUrls[1],settings,now));
  const feeds={
    blsCpi:result.blsCpi.text,blsCes:result.blsCes.text,blsCps:result.blsCps.text,
    bea:result.beaRss.text,fedMonetary:result.fedMonetary.text,fedSpeeches:result.fedSpeeches.text,dol:result.dol.text
  };
  const unique=[...new Map(enrichCalendarActuals(events,feeds).map(event=>[event.id,event])).values()]
    .filter(event=>Number.isFinite(event.eventAt)&&event.eventAt>=now-45*24*60*60*1000&&event.eventAt<=now+370*24*60*60*1000)
    .sort((a,b)=>a.eventAt-b.eventAt||a.id.localeCompare(b.id));
  const sourceStatus=Object.fromEntries(Object.entries(result).filter(([name])=>!name.startsWith('blsC')&&!['beaRss','fedMonetary','fedSpeeches'].includes(name)).map(([name,value])=>[name,{ok:value.ok,...(!value.ok?{error:value.error}:{})}]));
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
  return {
    ...event,
    eventAtUtc:new Date(Number(event.eventAt)).toISOString(),
    eventAtLocal:new Intl.DateTimeFormat('en-GB',{
      timeZone:localTimeZone,year:'numeric',month:'2-digit',day:'2-digit',
      hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false,timeZoneName:'short'
    }).format(new Date(Number(event.eventAt))),
    localTimeZone
  };
}

export { DEFAULT_RISK_SETTINGS, SOURCE_URLS };
