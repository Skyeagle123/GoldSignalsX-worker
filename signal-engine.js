export const SIGNAL_TIMEFRAMES = ['1m','5m','15m','30m','60m','240m','1d'];
export const HIGHER_SIGNAL_TIMEFRAMES = Object.freeze({
  '1m':['5m','15m'], '5m':['15m','60m'], '15m':['60m','240m'],
  '30m':['60m','240m'], '60m':['240m'], '240m':[], '1d':[]
});
export const SIGNAL_TF_MS = Object.freeze({
  '1m':60000, '5m':300000, '15m':900000, '30m':1800000,
  '60m':3600000, '240m':14400000, '1d':86400000
});
const HIGHER_TF = HIGHER_SIGNAL_TIMEFRAMES;
const TF_MS = SIGNAL_TF_MS;

export const DEFAULT_SIGNAL_FILTERS = Object.freeze({
  nyFilterOn:true,nyStart:'08:00',nyEnd:'17:00',pivotFilterOn:true,pivotDistance:0.7
});
function normalizedClock(value,fallback){const match=/^(\d{2}):(\d{2})$/.exec(String(value||''));if(!match)return fallback;const hour=Number(match[1]),minute=Number(match[2]);return hour>=0&&hour<=23&&minute>=0&&minute<=59?`${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')}`:fallback}
export function normalizeSignalFilters(filters={}){const distance=Number(filters?.pivotDistance);return{nyFilterOn:typeof filters?.nyFilterOn==='boolean'?filters.nyFilterOn:DEFAULT_SIGNAL_FILTERS.nyFilterOn,nyStart:normalizedClock(filters?.nyStart,DEFAULT_SIGNAL_FILTERS.nyStart),nyEnd:normalizedClock(filters?.nyEnd,DEFAULT_SIGNAL_FILTERS.nyEnd),pivotFilterOn:typeof filters?.pivotFilterOn==='boolean'?filters.pivotFilterOn:DEFAULT_SIGNAL_FILTERS.pivotFilterOn,pivotDistance:Number.isFinite(distance)?Math.max(0,Math.min(50,distance)):DEFAULT_SIGNAL_FILTERS.pivotDistance}}

function newYorkClockParts(ts) {
  const parts=new Intl.DateTimeFormat('en-US',{
    timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hour12:false
  }).formatToParts(new Date(ts));
  const get=type=>parts.find(part=>part.type===type)?.value||'';
  return {weekday:get('weekday'),minute:(Number(get('hour'))%24)*60+Number(get('minute'))};
}

function expectedMarketClosure(previousTs,nextTs,duration) {
  const gap=nextTs-previousTs;
  if (!Number.isFinite(gap)||gap<=duration*1.5) return false;
  if (duration>=86400000&&gap>=2*86400000&&gap<=4*86400000) return true;
  if (gap>=30*60*60*1000&&gap<=80*60*60*1000) {
    const previousDay=newYorkClockParts(previousTs+duration).weekday;
    const nextDay=newYorkClockParts(nextTs).weekday;
    if (previousDay==='Fri'&&(nextDay==='Sun'||nextDay==='Mon')) return true;
  }
  if (gap<=Math.max(3*60*60*1000,duration*3)) {
    const previousEnd=newYorkClockParts(previousTs+duration).minute;
    const nextStart=newYorkClockParts(nextTs).minute;
    return previousEnd>=16*60+30&&previousEnd<=17*60+30&&
      nextStart>=17*60+30&&nextStart<=18*60+30;
  }
  return false;
}

export function evaluateCandleQuality(bars,tf) {
  const duration=TF_MS[tf]||60000;
  const ordered=(Array.isArray(bars)?bars:[])
    .filter(bar=>[bar?.t,bar?.o,bar?.h,bar?.l,bar?.c].every(Number.isFinite)&&bar.h>=bar.l)
    .sort((a,b)=>a.t-b.t);
  const duplicateTimes=new Set();
  for (let i=1;i<ordered.length;i++) if (ordered[i].t===ordered[i-1].t) duplicateTimes.add(ordered[i].t);
  const recent=ordered.slice(-Math.min(120,ordered.length));
  const recentStart=Number(recent[0]?.t)||Infinity;
  const duplicates=[...duplicateTimes].filter(ts=>ts>=recentStart).length;
  let gaps=0;
  for (let i=1;i<recent.length;i++) {
    const delta=recent[i].t-recent[i-1].t;
    if (delta>duration*1.5&&!expectedMarketClosure(recent[i-1].t,recent[i].t,duration)) gaps++;
  }
  const ok=ordered.length>=40&&duplicates===0&&gaps===0;
  const reasons=[];
  if (ordered.length<40) reasons.push('أقل من 40 شمعة مكتملة');
  if (duplicates) reasons.push(`${duplicates} توقيت مكرر`);
  if (gaps) reasons.push(`${gaps} فجوة غير طبيعية`);
  return {ok,gaps,duplicates,count:ordered.length,reason:ok?'جودة الشموع سليمة':`جودة الشموع غير سليمة: ${reasons.join('، ')}`};
}

const ema=(arr,p)=>{if(!arr.length)return[];const k=2/(p+1),out=[arr[0]];let e=arr[0];for(let i=1;i<arr.length;i++){e=(arr[i]-e)*k+e;out.push(e)}return out};
const sma=(arr,p)=>{const out=[],q=[];let sum=0;for(const v of arr){q.push(v);sum+=v;if(q.length>p)sum-=q.shift();out.push(q.length===p?sum/p:null)}return out};
const std=(arr,p,ma)=>{const out=[],q=[];for(let i=0;i<arr.length;i++){q.push(arr[i]);if(q.length>p)q.shift();if(q.length===p){let s=0;for(const v of q)s+=(v-ma[i])**2;out.push(Math.sqrt(s/p))}else out.push(null)}return out};
function calcBB(closes,p=20,mult=2){const ma=sma(closes,p),s=std(closes,p,ma),upper=[],lower=[];for(let i=0;i<closes.length;i++){if(ma[i]==null||s[i]==null){upper.push(null);lower.push(null)}else{upper.push(ma[i]+mult*s[i]);lower.push(ma[i]-mult*s[i])}}return{ma,upper,lower}}
function calcATR(bars,p=14){if(bars.length<2)return[];const trs=[];for(let i=0;i<bars.length;i++){if(!i)trs.push(bars[i].h-bars[i].l);else trs.push(Math.max(bars[i].h-bars[i].l,Math.abs(bars[i].h-bars[i-1].c),Math.abs(bars[i].l-bars[i-1].c)))}const out=[];let atr=trs.slice(0,p).reduce((a,b)=>a+b,0)/p;for(let i=0;i<bars.length;i++){if(i<p)out.push(null);else if(i===p)out.push(atr);else{atr=(atr*(p-1)+trs[i])/p;out.push(atr)}}return out}
function calcRSI(closes,p=14){if(closes.length<=p)return[];const gains=[],losses=[];for(let i=1;i<closes.length;i++){const d=closes[i]-closes[i-1];gains.push(Math.max(d,0));losses.push(Math.max(-d,0))}let ag=gains.slice(0,p).reduce((a,b)=>a+b,0)/p,al=losses.slice(0,p).reduce((a,b)=>a+b,0)/p;const out=Array(p).fill(null);for(let i=p;i<gains.length;i++){ag=(ag*(p-1)+gains[i])/p;al=(al*(p-1)+losses[i])/p;out.push(al===0?100:100-100/(1+ag/al))}return out}
function calcStoch(closes,highs,lows,p=14){if(closes.length<p)return[];const out=[];for(let i=p-1;i<closes.length;i++){let h=-Infinity,l=Infinity;for(let j=i-p+1;j<=i;j++){h=Math.max(h,highs[j]);l=Math.min(l,lows[j])}out.push((closes[i]-l)/(h-l||1)*100)}while(out.length<closes.length)out.unshift(null);return out}
function calcMACD(closes,fast=12,slow=26,signal=9){const f=ema(closes,fast),s=ema(closes,slow),line=f.map((v,i)=>v-s[i]),sig=ema(line,signal);return{macdLine:line,signalLine:sig,hist:line.map((v,i)=>v-(sig[i]??0))}}
function calcADX(bars,p=14){const len=bars.length;if(len<p+2)return{plusDI:[],minusDI:[],ADX:[]};const pd=Array(len).fill(0),md=Array(len).fill(0),tr=Array(len).fill(0);for(let i=1;i<len;i++){const up=bars[i].h-bars[i-1].h,down=bars[i-1].l-bars[i].l;pd[i]=up>down&&up>0?up:0;md[i]=down>up&&down>0?down:0;tr[i]=Math.max(bars[i].h-bars[i].l,Math.abs(bars[i].h-bars[i-1].c),Math.abs(bars[i].l-bars[i-1].c))}const smooth=src=>{const out=Array(len).fill(null);let s=0;for(let i=1;i<=p;i++)s+=src[i]||0;out[p]=s;for(let i=p+1;i<len;i++)out[i]=out[i-1]-out[i-1]/p+(src[i]||0);return out};const tn=smooth(tr),pn=smooth(pd),mn=smooth(md),plusDI=Array(len).fill(null),minusDI=Array(len).fill(null),dx=Array(len).fill(null);for(let i=p;i<len;i++){if(!tn[i])continue;plusDI[i]=100*pn[i]/tn[i];minusDI[i]=100*mn[i]/tn[i];const s=plusDI[i]+minusDI[i];dx[i]=s?100*Math.abs(plusDI[i]-minusDI[i])/s:0}const ADX=Array(len).fill(null);let seed=0,count=0,start=-1;for(let i=0;i<len;i++)if(dx[i]!=null){seed+=dx[i];if(++count===p){ADX[i]=seed/p;start=i;break}}for(let i=start+1;i<len;i++)if(dx[i]!=null)ADX[i]=(ADX[i-1]*(p-1)+dx[i])/p;return{plusDI,minusDI,ADX}}

function detectPattern(bars){const none={name:'لا يوجد نمط واضح',detail:'',direction:'neutral',strength:0,all:[]};if(bars.length<3)return none;const n=bars.length,a=bars[n-3],b=bars[n-2],c=bars[n-1],body=x=>Math.abs(x.c-x.o),range=x=>Math.max(x.h-x.l,Number.EPSILON),bull=x=>x.c>x.o,bear=x=>x.c<x.o,bodyPct=x=>body(x)/range(x),upper=x=>x.h-Math.max(x.o,x.c),lower=x=>Math.min(x.o,x.c)-x.l,prior=bars.slice(Math.max(0,n-9),n-1),swingLow=c.l<=Math.min(...prior.map(x=>x.l)),swingHigh=c.h>=Math.max(...prior.map(x=>x.h)),priorDown=b.c<bars[Math.max(0,n-5)].c,priorUp=b.c>bars[Math.max(0,n-5)].c,patterns=[],add=(name,direction,strength,detail)=>patterns.push({name,direction,strength,detail});
if(bear(b)&&bull(c)&&c.c>=b.o&&c.o<=b.c&&bodyPct(c)>=.55)add('Bullish Engulfing','bullish',.82,'ابتلاع شرائي يؤكد انتقال السيطرة للمشترين.');if(bull(b)&&bear(c)&&c.o>=b.c&&c.c<=b.o&&bodyPct(c)>=.55)add('Bearish Engulfing','bearish',.82,'ابتلاع بيعي يؤكد انتقال السيطرة للبائعين.');const smallB=bodyPct(b)<=.3;if(bear(a)&&bodyPct(a)>=.45&&smallB&&bull(c)&&c.c>=(a.o+a.c)/2)add('Morning Star','bullish',.92,'نجمة صباحية من ثلاث شمعات؛ انعكاس صعودي قوي.');if(bull(a)&&bodyPct(a)>=.45&&smallB&&bear(c)&&c.c<=(a.o+a.c)/2)add('Evening Star','bearish',.92,'نجمة مسائية من ثلاث شمعات؛ انعكاس هبوطي قوي.');const hammer=bodyPct(c)<=.38&&lower(c)>=Math.max(body(c)*2,range(c)*.48)&&upper(c)<=range(c)*.18,star=bodyPct(c)<=.38&&upper(c)>=Math.max(body(c)*2,range(c)*.48)&&lower(c)<=range(c)*.18;if(hammer&&(swingLow||priorDown))add('Hammer','bullish',.68,'مطرقة عند قاع/هبوط قريب؛ تحتاج تأكيد اتجاه.');if(star&&(swingHigh||priorUp))add('Shooting Star','bearish',.68,'نجم ساقط عند قمة/صعود قريب؛ يحتاج تأكيد اتجاه.');if(bear(b)&&bull(c)&&c.o<b.c&&c.c>(b.o+b.c)/2&&c.c<b.o)add('Piercing Line','bullish',.72,'اختراق شرائي لأكثر من نصف جسم الشمعة السابقة.');if(bull(b)&&bear(c)&&c.o>b.c&&c.c<(b.o+b.c)/2&&c.c>b.o)add('Dark Cloud Cover','bearish',.72,'غطاء سحابي داكن؛ ضغط بيعي بعد صعود.');const inside=Math.max(c.o,c.c)<=Math.max(b.o,b.c)&&Math.min(c.o,c.c)>=Math.min(b.o,b.c);if(inside&&body(c)<=body(b)*.5&&bear(b)&&bull(c))add('Bullish Harami','bullish',.56,'هارامي شرائي؛ إشارة انعكاس متوسطة.');if(inside&&body(c)<=body(b)*.5&&bull(b)&&bear(c))add('Bearish Harami','bearish',.56,'هارامي بيعي؛ إشارة انعكاس متوسطة.');const longBull=x=>bull(x)&&bodyPct(x)>=.5,longBear=x=>bear(x)&&bodyPct(x)>=.5;if(longBull(a)&&longBull(b)&&longBull(c)&&a.c<b.c&&b.c<c.c)add('Three White Soldiers','bullish',.88,'ثلاثة جنود بيض؛ استمرار/انعكاس صعودي قوي.');if(longBear(a)&&longBear(b)&&longBear(c)&&a.c>b.c&&b.c>c.c)add('Three Black Crows','bearish',.88,'ثلاثة غربان سود؛ استمرار/انعكاس هبوطي قوي.');if(bodyPct(c)<=.1)add('Doji','neutral',.35,'دوجي؛ تردد ولا تُستخدم وحدها للدخول.');if(!patterns.length)return none;patterns.sort((x,y)=>y.strength-x.strength);return{...patterns[0],all:patterns}}

function timeframeBias(bars){if(bars.length<40)return{direction:'neutral',strength:0};const closes=bars.map(b=>b.c),fast=ema(closes,10).at(-1),slow=ema(closes,34).at(-1),hist=calcMACD(closes).hist.at(-1),{plusDI,minusDI,ADX}=calcADX(bars),adx=ADX.at(-1),pdi=plusDI.at(-1),mdi=minusDI.at(-1);let bull=0,bear=0;if(fast>slow&&closes.at(-1)>fast)bull+=2;if(fast<slow&&closes.at(-1)<fast)bear+=2;if(Number.isFinite(hist))hist>0?bull++:hist<0?bear++:0;if(Number.isFinite(adx)&&adx>=18&&Number.isFinite(pdi)&&Number.isFinite(mdi))pdi>mdi?bull++:bear++;const d=bull-bear;return{direction:d>=1.5?'bullish':d<=-1.5?'bearish':'neutral',strength:Math.min(1,Math.abs(d)/4)}}
function nyClockMinutes(ts){const parts=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',hour12:false}).formatToParts(new Date(ts)),h=Number(parts.find(p=>p.type==='hour')?.value),m=Number(parts.find(p=>p.type==='minute')?.value);return Number.isFinite(h)&&Number.isFinite(m)?(h%24)*60+m:NaN}
function clockMinutes(value){const [hour,minute]=String(value).split(':').map(Number);return hour*60+minute}
function inNyTradingWindow(ts,start='08:00',end='17:00'){const now=nyClockMinutes(ts),from=clockMinutes(start),to=clockMinutes(end);return from<=to?now>=from&&now<=to:now>=from||now<=to}
function nyDateKey(ts){const p=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(ts)),g=t=>p.find(x=>x.type===t)?.value||'';return`${g('year')}-${g('month')}-${g('day')}`}
function calculateDailyPivots(bars){const days=[];let cur=null;for(const b of bars){const {t,h,l,c}=b;if(![t,h,l,c].every(Number.isFinite))continue;const key=nyDateKey(t);if(!cur||cur.key!==key){cur={key,high:h,low:l,close:c};days.push(cur)}else{cur.high=Math.max(cur.high,h);cur.low=Math.min(cur.low,l);cur.close=c}}if(days.length<2)return null;const d=days.at(-2),P=(d.high+d.low+d.close)/3,r=d.high-d.low;return{P,R1:2*P-d.low,S1:2*P-d.high,R2:P+r,S2:P-r,R3:d.high+2*(P-d.low),S3:d.low-2*(d.high-P)}}
function nearestPivot(price,p){if(!p)return null;return['P','R1','R2','R3','S1','S2','S3'].map(label=>({label,value:p[label],distance:Math.abs(price-p[label])})).sort((a,b)=>a.distance-b.distance)[0]}
export function marketProfile(bars){const closes=bars.map(b=>b.c),{ma,upper,lower}=calcBB(closes),atr=calcATR(bars).at(-1),{ADX,plusDI,minusDI}=calcADX(bars),i=closes.length-1,C=closes[i],U=upper[i],L=lower[i],M=ma[i],adx=ADX[i],pdi=plusDI[i],mdi=minusDI[i],bw=(U-L)/C*100,atrPct=atr/C*100,slope=ma[i-1]&&M?((M-ma[i-1])/ma[i-1])*100:0,bias=pdi!=null&&mdi!=null?(pdi>mdi?'up':'down'):(slope>0?'up':'down');let state;if(bw<1.2&&atrPct<.5&&(adx==null||adx<22))state='range';else if(bw>1.8&&Math.abs(slope)>.03&&atrPct>.8&&(adx==null||adx>=22))state=`trend-${bias}`;else state=adx!=null&&adx>=22?`trend-${bias}`:'range';return{state,atr,adx,use:{ema:state.startsWith('trend'),macd:state.startsWith('trend'),rsi:true,stoch:!state.startsWith('trend'),bb:!state.startsWith('trend')}}}

export function computeServerSignal(bars,{tf,mtf=[],live={},barsSource,news,dataQuality,filters:signalFilters={},evaluationAt=Date.now()}={}){const now=Number(evaluationAt);const filters=normalizeSignalFilters(signalFilters);if(!Array.isArray(bars)||bars.length<40)return{side:'none',tf,reasons:['نحتاج 40 شمعة مكتملة على الأقل']};if(dataQuality?.ok===false)return{side:'none',tf,reasons:[dataQuality.reason||'جودة الشموع غير سليمة']};const closes=bars.map(b=>b.c),highs=bars.map(b=>b.h),lows=bars.map(b=>b.l),prof=marketProfile(bars),use=prof.use,eF=ema(closes,10),eS=ema(closes,34),rsi=calcRSI(closes).at(-1),stA=calcStoch(closes,highs,lows),st=stA.at(-1),stPrev=stA.at(-2),macd=calcMACD(closes),hist=macd.hist.at(-1),histPrev=macd.hist.at(-2),{ma,upper,lower}=calcBB(closes),atr=prof.atr,{ADX,plusDI,minusDI}=calcADX(bars),C=closes.at(-1),ef=eF.at(-1),es=eS.at(-1),adx=ADX.at(-1),pdi=plusDI.at(-1),mdi=minusDI.at(-1),M=ma.at(-1),U=upper.at(-1),L=lower.at(-1),pat=detectPattern(bars),reasons=[],neutral=[];if(!Number.isFinite(atr)||atr<=0)return{side:'none',tf,reasons:['ATR غير صالح']};const lastTs=bars.at(-1).t,maxAge=Math.max(TF_MS[tf]*3,420000);if(now-lastTs>maxAge)return{side:'none',tf,reasons:['بيانات الشموع قديمة'],lastTs,age:now-lastTs};const quoteTs=Number(live.ts),recv=Number(live.receivedAt||quoteTs),providerAge=now-quoteTs,receiptAge=now-recv;if(receiptAge<0||receiptAge>20000)return{side:'none',tf,reasons:['وصول السعر متوقف/قديم'],receiptAge};if(providerAge < -30000||providerAge>90000)return{side:'none',tf,reasons:['توقيت السوق متأخر'],providerAge};let bull=0,bear=0;const br=[],sr=[];const trendUp=ef>es&&C>ef,trendDown=ef<es&&C<ef;if(use.ema&&trendUp){bull+=2.5;br.push('EMA تؤكد اتجاهاً صاعداً')}if(use.ema&&trendDown){bear+=2.5;sr.push('EMA تؤكد اتجاهاً هابطاً')}if(use.macd&&Number.isFinite(hist)){if(hist>0){bull+=1.25;br.push('زخم MACD موجب')}if(hist<0){bear+=1.25;sr.push('زخم MACD سالب')}if(Number.isFinite(histPrev)&&hist>histPrev)bull+=.35;if(Number.isFinite(histPrev)&&hist<histPrev)bear+=.35}if(Number.isFinite(adx)&&adx>=18&&Number.isFinite(pdi)&&Number.isFinite(mdi)){if(pdi>mdi){bull+=1;br.push(`+DI يتفوّق مع ADX ${adx.toFixed(1)}`)}else{bear+=1;sr.push(`-DI يتفوّق مع ADX ${adx.toFixed(1)}`)}}else neutral.push('ADX ضعيف');if(use.rsi&&Number.isFinite(rsi)){if(rsi>=52&&rsi<70)bull+=.8;else if(rsi<=48&&rsi>30)bear+=.8;else if(rsi>=70||rsi<=30)neutral.push('RSI متطرف')}if(use.stoch&&Number.isFinite(st)&&Number.isFinite(stPrev)){if(st<40&&st>stPrev)bull+=.55;if(st>60&&st<stPrev)bear+=.55}if(use.bb&&Number.isFinite(M))C>M?bull+=.45:bear+=.45;if(use.bb&&Number.isFinite(U)&&C>U)bull+=.35;if(use.bb&&Number.isFinite(L)&&C<L)bear+=.35;if(pat.direction==='bullish'){bull+=pat.strength*2.5;br.push(`${pat.name}: ${pat.detail}`)}else if(pat.direction==='bearish'){bear+=pat.strength*2.5;sr.push(`${pat.name}: ${pat.detail}`)}const last=bars.at(-1),prev=bars.at(-2),range=Math.max(last.h-last.l,Number.EPSILON),body=Math.abs(last.c-last.o),bullC=(last.c>last.o&&body/range>=.52&&last.c>=last.h-range*.2)||last.c>prev.h||pat.direction==='bullish',bearC=(last.c<last.o&&body/range>=.52&&last.c<=last.l+range*.2)||last.c<prev.l||pat.direction==='bearish';if(bullC){bull+=1.15;br.push('إغلاق الشمعة يؤكد ضغطاً شرائياً')}if(bearC){bear+=1.15;sr.push('إغلاق الشمعة يؤكد ضغطاً بيعياً')}let mtfBull=0,mtfBear=0,mtfNeutral=0;for(const f of mtf){const b=timeframeBias(f.bars);if(b.direction==='bullish'){mtfBull++;bull+=1.6+.6*b.strength;br.push(`الإطار ${f.tf} صاعد`)}else if(b.direction==='bearish'){mtfBear++;bear+=1.6+.6*b.strength;sr.push(`الإطار ${f.tf} هابط`)}else mtfNeutral++}let newsBlocked=false;if(news?.ok&&!news.stale){if(news.safety?.blockTechnicalSignal){newsBlocked=true;neutral.unshift(news.safety.reason||'خبر شديد التأثير')}const dir=news.goldBias?.direction,conf=Math.max(0,Math.min(100,Number(news.goldBias?.confidence)||0)),w=Math.min(.9,Math.max(0,(conf-50)/40*.9));if(dir==='bullish'&&w>0){bull+=w;br.push(`الأخبار داعمة للذهب (${conf.toFixed(0)}%)`)}else if(dir==='bearish'&&w>0){bear+=w;sr.push(`الأخبار ضاغطة على الذهب (${conf.toFixed(0)}%)`)}}
const leader=bull>=bear?'buy':'sell',score=Math.max(bull,bear),opp=Math.min(bull,bear),margin=score-opp,confirm=leader==='buy'?mtfBull:mtfBear,oppositions=leader==='buy'?mtfBear:mtfBull,candle=leader==='buy'?bullC:bearC,required=Math.min(1,HIGHER_TF[tf].length),sideReasons=leader==='buy'?br:sr;if(score<7.4)neutral.unshift(`النقاط ${score.toFixed(1)} أقل من 7.4`);if(margin<2)neutral.unshift('تعارض واضح');if(!candle)neutral.unshift('لا يوجد إغلاق شمعة مؤكِّد');if(confirm<required)neutral.unshift(`تأكيد MTF غير كافٍ (${confirm}/${required})`);if(oppositions>confirm&&mtf.length)neutral.unshift('الأطر الأعلى تعاكس الإشارة');const nyBlocked=filters.nyFilterOn&&!inNyTradingWindow(live.ts,filters.nyStart,filters.nyEnd);if(nyBlocked)neutral.unshift('خارج جلسة نيويورك');const piv=calculateDailyPivots(bars),near=nearestPivot(C,piv),pivotBlocked=filters.pivotFilterOn&&(!piv||(near&&near.distance<filters.pivotDistance));if(pivotBlocked)neutral.unshift(!piv?'Pivot غير متوفر':`السعر قريب من ${near.label}`);let side=(score>=7.4&&margin>=2&&candle&&confirm>=required&&!(oppositions>confirm&&mtf.length)&&!nyBlocked&&!pivotBlocked&&!newsBlocked)?leader:'none';const livePrice=Number(live.price),gap=Math.abs(livePrice-C),aligned=gap<=Math.max(atr*.75,C*.002),stored=['d1','kv'].includes(barsSource),same=!live.source||!barsSource||live.source===barsSource||(String(live.source).startsWith('gold-ticks')&&barsSource==='gold-ticks'),consistent=same||(stored&&aligned);if(!consistent)neutral.unshift('السعر والشموع غير متوافقين');if(!aligned)neutral.unshift(`فرق السعر الحي عن آخر شمعة كبير (${gap.toFixed(2)}$)`);if(!consistent||!aligned)side='none';if(side==='none')return{side,tf,regime:prof.state,score,bull,bear,mtf:{bull:mtfBull,bear:mtfBear,neutral:mtfNeutral},reasons:[...neutral,...sideReasons.slice(0,3)],lastClose:C,atr,livePrice,lastTs};const entry=livePrice,recent=bars.slice(-6),struct=side==='buy'?Math.min(...recent.map(x=>x.l)):Math.max(...recent.map(x=>x.h)),risk=Math.min(atr*1.8,Math.max(atr,side==='buy'?entry-(struct-atr*.15):(struct+atr*.15)-entry)),sl=side==='buy'?entry-risk:entry+risk,tp1=side==='buy'?entry+risk*1.25:entry-risk*1.25,tp2=side==='buy'?entry+risk*2.1:entry-risk*2.1;let conf=55+(score-7.4)*6+confirm*5+4-Math.max(0,opp-2)*1.5;conf=Math.max(55,Math.min(88,conf));return{side,tf,regime:prof.state,score,bull,bear,mtf:{bull:mtfBull,bear:mtfBear,neutral:mtfNeutral},entry,tp1,tp2,sl,conf,reasons:[...sideReasons,...neutral].slice(0,8),lastClose:C,atr,livePrice,lastTs}}

export function signalExpiryMs(tf) {
  const duration=TF_MS[tf];
  return Math.min(7*24*60*60*1000,Math.max(30*60*1000,duration*12));
}

export function updateSignalLifecycle(signal,bar,livePrice,now) {
  if (!signal||!['active','tp1'].includes(signal.status)) return signal;
  const barHigh=Number(bar?.h),barLow=Number(bar?.l),price=Number(livePrice);
  const high=Math.max(Number.isFinite(barHigh)?barHigh:-Infinity,Number.isFinite(price)?price:-Infinity);
  const low=Math.min(Number.isFinite(barLow)?barLow:Infinity,Number.isFinite(price)?price:Infinity);
  let status=signal.status;
  if (signal.side==='buy') {
    if (low<=signal.sl) status='stopped';
    else if (high>=signal.tp2) status='tp2';
    else if (high>=signal.tp1) status='tp1';
  } else {
    if (high>=signal.sl) status='stopped';
    else if (low<=signal.tp2) status='tp2';
    else if (low<=signal.tp1) status='tp1';
  }
  if (status===signal.status&&now-signal.createdAt>signalExpiryMs(signal.tf)) status='expired';
  return {
    ...signal,status,tp1Hit:signal.tp1Hit||status==='tp1'||status==='tp2',
    lastPrice:Number.isFinite(price)?price:Number(signal.lastPrice),updatedAt:now,
    ...(['tp2','stopped','expired'].includes(status)?{closedAt:now,closedBarTs:Number(bar?.t||now)}:{})
  };
}

export function updateSignalLifecycleAcrossBars(signal,bars,livePrice,now,barDurationMs=60_000) {
  let current={...signal};
  const events=[];
  const after=Number(current.lastProcessedBarTs||current.signalBarTs||0);
  const trackingStartedAt=Number(current.createdAt||0);
  const pending=(Array.isArray(bars)?bars:[])
    .filter(bar=>Number(bar?.t)>after&&Number(bar.t)>=trackingStartedAt)
    .sort((a,b)=>Number(a.t)-Number(b.t));
  for (const bar of pending) {
    const previous=current.status;
    const eventAt=Math.min(now,Number(bar.t)+barDurationMs);
    current=updateSignalLifecycle(current,bar,Number(bar.c),eventAt);
    current.lastProcessedBarTs=Number(bar.t);
    if (current.status!==previous) events.push({event:current.status,signal:{...current}});
    if (!['active','tp1'].includes(current.status)) break;
  }
  if (['active','tp1'].includes(current.status)) {
    const previous=current.status;
    current=updateSignalLifecycle(current,null,livePrice,now);
    if (current.status!==previous) events.push({event:current.status,signal:{...current}});
  }
  return {signal:current,events};
}

function normalizeBacktestBars(value) {
  return (Array.isArray(value)?value:[]).map(row=>({
    t:Number(row?.t),o:Number(row?.o),h:Number(row?.h),l:Number(row?.l),c:Number(row?.c),v:Number(row?.v||0)
  })).filter(row=>[row.t,row.o,row.h,row.l,row.c].every(Number.isFinite)&&row.h>=row.l)
    .sort((left,right)=>left.t-right.t);
}

function closedBacktestBars(bars,tf,evaluationAt) {
  const duration=TF_MS[tf];
  return bars.filter(bar=>bar.t+duration<=evaluationAt);
}

export function runServerBacktest({tf,frames={},filters={},news=null,startAt=-Infinity,endAt=Date.now(),maxEvaluations=2000}={}) {
  if (!SIGNAL_TIMEFRAMES.includes(tf)) throw new Error('bad_backtest_tf');
  const normalizedFrames=Object.fromEntries(
    SIGNAL_TIMEFRAMES.map(frame=>[frame,normalizeBacktestBars(frames?.[frame])])
  );
  const primary=normalizedFrames[tf];
  if (primary.length<40) throw new Error('insufficient_backtest_bars');
  const trackingTf=normalizedFrames['1m'].length?'1m':tf;
  const trackingBars=normalizedFrames[trackingTf];
  const normalizedFilters=normalizeSignalFilters(filters);
  const trades=[];
  let active=null,activeIndex=-1,evaluations=0;

  for (let index=39;index<primary.length&&evaluations<Math.max(1,Number(maxEvaluations)||1);index++) {
    const signalBar=primary[index];
    const evaluationAt=signalBar.t+TF_MS[tf];
    if (evaluationAt<Number(startAt)||evaluationAt>Number(endAt)) continue;
    evaluations++;

    if (active&&['active','tp1'].includes(active.status)) {
      const eligibleTracking=closedBacktestBars(trackingBars,trackingTf,evaluationAt);
      const lifecycle=updateSignalLifecycleAcrossBars(
        active,eligibleTracking,NaN,evaluationAt,TF_MS[trackingTf]
      );
      active=lifecycle.signal;
      const record=trades[activeIndex];
      record.outcome={status:active.status,updatedAt:active.updatedAt,closedAt:Number(active.closedAt||0)};
      record.events.push(...lifecycle.events.map(item=>({event:item.event,eventAt:item.signal.updatedAt,price:item.signal.lastPrice})));
      if (['active','tp1'].includes(active.status)) continue;
      active=null;
      activeIndex=-1;
      continue;
    }

    const bars=closedBacktestBars(primary,tf,evaluationAt);
    const mtf=(HIGHER_TF[tf]||[]).map(frame=>({
      tf:frame,bars:closedBacktestBars(normalizedFrames[frame],frame,evaluationAt)
    })).filter(frame=>evaluateCandleQuality(frame.bars,frame.tf).ok);
    const live={price:Number(signalBar.c),ts:evaluationAt,receivedAt:evaluationAt,source:'backtest'};
    const result=computeServerSignal(bars,{
      tf,mtf,live,barsSource:'backtest',news,
      dataQuality:evaluateCandleQuality(bars,tf),filters:normalizedFilters,evaluationAt
    });
    if (!['buy','sell'].includes(result.side)) continue;
    active={
      id:`${tf}:${result.lastTs}:${result.side}`,tf,side:result.side,
      entry:Number(result.entry),tp1:Number(result.tp1),tp2:Number(result.tp2),sl:Number(result.sl),
      score:Number(result.score),bull:Number(result.bull),bear:Number(result.bear),
      mtf:result.mtf?{...result.mtf}:null,regime:String(result.regime||''),
      atr:Number(result.atr),lastClose:Number(result.lastClose),livePrice:Number(result.livePrice),
      conf:Number(result.conf),reasons:Array.isArray(result.reasons)?result.reasons.slice(0,8):[],
      signalBarTs:Number(result.lastTs),lastProcessedBarTs:Number(result.lastTs),
      createdAt:evaluationAt,updatedAt:evaluationAt,status:'active',tp1Hit:false,
      lastPrice:Number(result.entry),origin:'backtest'
    };
    trades.push({signal:{...active},outcome:{status:'active',updatedAt:evaluationAt,closedAt:0},events:[]});
    activeIndex=trades.length-1;
  }

  return {
    ok:true,mode:'simulation',engine:'computeServerSignal',tf,filters:normalizedFilters,
    evaluations,trades,
    summary:trades.reduce((summary,trade)=>{
      summary.total++;
      summary[trade.outcome.status]=(summary[trade.outcome.status]||0)+1;
      return summary;
    },{total:0})
  };
}

export { calculateDailyPivots, timeframeBias };
