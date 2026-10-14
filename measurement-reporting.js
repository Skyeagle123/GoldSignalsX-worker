import {serialize,hash,identity,time,need,LIMITS,bytes,parseEvidence} from './measurement-independent-contract.js';
import {readArchiveEntry,verifyRootCheckpoint} from './measurement-archive.js';
import {cataloguePage,catalogueLookup} from './measurement-archive-catalog.js';
import {readRecord} from './measurement-independent-store.js';
const enc=new TextEncoder();
const b64=b=>btoa(String.fromCharCode(...b)).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');
const unb64=s=>Uint8Array.from(atob(s.replaceAll('-','+').replaceAll('_','/')),c=>c.charCodeAt(0));
export async function createIndependentReporting(db,{archive=null,tokenKey,clock=()=>Date.now()}={}){
 need(typeof tokenKey==='string'&&tokenKey.length>=32,'pa_reporting_key');const key=await crypto.subtle.importKey('raw',enc.encode(tokenKey),{name:'HMAC',hash:'SHA-256'},false,['sign','verify']);
 const sign=async value=>{const wire=serialize(value,8192);return b64(enc.encode(wire))+'.'+b64(new Uint8Array(await crypto.subtle.sign('HMAC',key,enc.encode(wire))));};
 const verify=async token=>{need(typeof token==='string'&&token.length<=12000,'pa_report_token');const parts=token.split('.');need(parts.length===2,'pa_report_token');const data=unb64(parts[0]);need(await crypto.subtle.verify('HMAC',key,unb64(parts[1]),data),'pa_report_token');const value=JSON.parse(new TextDecoder().decode(data));need(value.version==='measurement-report-v1'&&value.expiresAt>clock(),'pa_report_token_expired');return value;};
 async function query(input,{cursor=null,limit=32,maxObjects=128,maxBytes=8388608,maxWorkMs=1000}={}){
  need(input.definition==='engine-v1','pa_report_unsupported_definition');need(input.asOf<=clock(),'pa_report_future_asof');const view=input.view??'ALL';need(['ALL','CENSUS','RESULTS'].includes(view),'pa_report_view');const allowedKinds=view==='CENSUS'?['CENSUS','CENSUS_CAPTURE','COVERAGE']:view==='RESULTS'?['RESULT','CORRECTION_ADMISSION','CORRECTION_STATE']:null;identity(input.cohort);identity(input.definition);time(input.from);time(input.to);time(input.asOf);need(input.from<=input.to&&limit>=1&&limit<=LIMITS.reportRecords&&maxObjects>=1&&maxObjects<=LIMITS.reportObjects&&maxBytes>=1&&maxBytes<=LIMITS.reportBytes&&maxWorkMs>0&&maxWorkMs<=30000,'pa_report_bound');
  const meta=await db.first('SELECT epoch,revision,live_floor,root_key,catalogue_generation FROM pa_meta'),queryIdentity=await hash(input);
  let pin=cursor?await verify(cursor):{version:'measurement-report-v1',queryIdentity,cohort:input.cohort,definition:input.definition,asOf:input.asOf,epoch:meta.epoch,liveRevision:meta.revision,catalogueGeneration:meta.catalogue_generation,rootKey:meta.root_key,after:'',expiresAt:clock()+900000};
  if(pin.rootKey)limit=Math.min(limit,8);
  need(pin.queryIdentity===queryIdentity&&pin.cohort===input.cohort&&pin.definition===input.definition&&pin.epoch===meta.epoch,'pa_report_scope');
  if(pin.liveRevision<meta.live_floor)return {status:'UNAVAILABLE',reason:'pinned_live_snapshot_evicted',coverage:'UNKNOWN_CAPTURE',complete:false,snapshot:pin};
  const live=await db.all('SELECT * FROM pa_records WHERE ready=1 AND cohort=? AND definition=? AND created_at BETWEEN ? AND ? AND available_at<=? AND revision<=? AND order_key>? AND (? IS NULL OR kind IN(SELECT value FROM json_each(?))) ORDER BY order_key LIMIT ?',input.cohort,input.definition,input.from,input.to,input.asOf,pin.liveRevision,pin.after,allowedKinds?serialize(allowedKinds):null,serialize(allowedKinds??[]),limit+1);
  const coldConflicts=new Set(),merged=new Map();let objects=0,total=0,complete=true,reason=null,frontier=null,archivePage=null,indexRoot=null;const started=performance.now(),readCache=new Map();
  const countedRead=async key=>{if(readCache.has(key))return readCache.get(key);need(objects<maxObjects&&performance.now()-started<=maxWorkMs,'pa_report_budget');objects++;const wire=await archive.read(key);total+=bytes(wire);need(total<=maxBytes,'pa_report_budget');readCache.set(key,wire);return wire;};
  try{
   if(pin.rootKey){need(archive,'pa_archive_unavailable');const root=JSON.parse(await countedRead(pin.rootKey));need(root.format==='measurement-root-v1'&&root.generation<=pin.catalogueGeneration,'pa_report_root');
    await verifyRootCheckpoint(archive,root,pin.rootKey,{read:countedRead});const receipt=JSON.parse(await countedRead(root.receiptKey));need(receipt.publication===root.publication&&receipt.readback===true&&receipt.manifestKey===root.manifestKey,'pa_report_root_receipt');
    indexRoot=root.indexRoot;const countedArchive={...archive,read:countedRead};archivePage=await cataloguePage(countedArchive,root.indexRoot,{after:pin.after,maxObjects:Math.min(64,Math.max(1,maxObjects-objects)),limit:limit+1});
    if(!archivePage.complete)frontier=archivePage.next;
    const integrityScopes=[...new Set([...live.map(r=>r.scope_id),...archivePage.entries.filter(e=>!e.lookupType).map(e=>e.scope_id)].filter(Boolean))];for(const scopeId of integrityScopes){const scope=await catalogueLookup(countedArchive,indexRoot,'scope:'+scopeId);if(scope?.firstConflictAt!=null&&scope.firstConflictAt<=input.asOf)coldConflicts.add(scopeId);}
    for(const entry of archivePage.entries){if(entry.lookupType||allowedKinds&&!allowedKinds.includes(entry.kind)||entry.cohort!==input.cohort||entry.definition!==input.definition||entry.created_at<input.from||entry.created_at>input.to||entry.available_at>input.asOf)continue;
     try{const r=await readArchiveEntry(archive,entry,{read:countedRead});merged.set(r.id,{...r,storage:'ARCHIVED'});}catch(error){if(!/budget/.test(error.message))throw error;frontier=archivePage.entries.slice(0,archivePage.entries.indexOf(entry)).at(-1)?.order_key??pin.after;archivePage={...archivePage,complete:false,next:frontier};reason='archive_read_budget';break;}
    }
   }
   for(const row of live){if(frontier&&row.order_key>frontier)continue;const r=await readRecord(db,row.id);need(r,'pa_live_snapshot_changed');total+=r.bytes;need(total<=maxBytes,'pa_report_budget');const old=merged.get(r.id);need(!old||old.digest===r.digest,'pa_report_integrity_conflict');merged.set(r.id,{...r,storage:old?'MIXED':'LIVE'});}
  }catch(error){return {status:/budget/.test(error.message)?'PENDING':'UNAVAILABLE',reason:error.message,complete:false,coverage:'UNKNOWN_CAPTURE',records:[],snapshot:pin,cursor:await sign(pin),traversal:{objects,bytes:total},retryBudget:{suggestedLimit:1,maximumObjects:LIMITS.reportObjects,maximumWorkMs:30000}};}
  // Captured census and analytically admitted census are two availability
  // boundaries for one evaluation, never two denominator entries.
  const census=new Map(),others=[];try{for(const r of merged.values()){if(!['CENSUS','CENSUS_CAPTURE'].includes(r.kind)){others.push(r);continue;}const p=parseEvidence(r.wire),evaluationId=p.evaluationId,decisionDigest=r.kind==='CENSUS'?r.digest:p.decisionDigest;
   if(r.kind==='CENSUS_CAPTURE'){let canonical=await db.first('SELECT digest,available_at FROM pa_records WHERE id=? AND ready=1 AND revision<=? AND available_at<=?','decision:'+evaluationId,pin.liveRevision,input.asOf);
    if(!canonical&&indexRoot){const entry=await catalogueLookup({...archive,read:countedRead},indexRoot,'decision:'+evaluationId);if(entry&&entry.available_at<=input.asOf)canonical=entry;}
    if(canonical){need(canonical.digest===decisionDigest,'pa_report_census_conflict');continue;}
   }
   const prior=census.get(evaluationId);
   if(prior){const oldDigest=prior.kind==='CENSUS'?prior.digest:parseEvidence(prior.wire).decisionDigest;need(oldDigest===decisionDigest,'pa_report_census_conflict');const chosen=prior.kind==='CENSUS'?prior:r;chosen.captureAvailableAt=Math.min(prior.captureAvailableAt??prior.available_at,r.available_at);census.set(evaluationId,chosen);}else census.set(evaluationId,{...r,captureAvailableAt:r.available_at});}
  }catch(error){return {status:/budget/.test(error.message)?'PENDING':'UNAVAILABLE',reason:error.message,complete:false,coverage:'UNKNOWN_CAPTURE',records:[],snapshot:pin,cursor:await sign(pin),traversal:{objects,bytes:total},retryBudget:{suggestedLimit:1,maximumObjects:LIMITS.reportObjects,maximumWorkMs:30000}};}
  const ordered=[...others,...census.values()].sort((a,b)=>a.order_key.localeCompare(b.order_key)),page=ordered.slice(0,limit);
  const coverageRows=await db.all('SELECT status,from_at,to_at,known_missing,record_id FROM pa_coverage WHERE cohort=? AND from_at<=? AND to_at>=? LIMIT 129',input.cohort,input.to,input.from);
  let coverage='UNKNOWN_CAPTURE';const archivedCoverage=[...merged.values()].filter(r=>r.kind==='COVERAGE').map(r=>parseEvidence(r.wire));if(archivedCoverage.some(p=>p.status==='KNOWN_GAPS'))coverage='KNOWN_GAPS';if(coverageRows.some(r=>r.status==='KNOWN_GAPS'))coverage='KNOWN_GAPS';else if(Array.isArray(input.captureScope)&&input.captureScope.length>0){for(const r of coverageRows.filter(r=>r.status==='COMPLETE_FOR_SCOPE'&&r.from_at<=input.from&&r.to_at>=input.to)){const proof=await readRecord(db,r.record_id);if(proof&&serialize(JSON.parse(proof.wire).captureScope?.slice().sort())===serialize(input.captureScope.slice().sort())){coverage='COMPLETE_FOR_SCOPE';break;}}}
  if(ordered.length>limit||live.length>limit||archivePage&&!archivePage.complete){complete=false;reason=reason??'pagination';}
  const liveFrontier=live.length>limit?live.at(-1)?.order_key:null,scanFrontier=frontier&&liveFrontier?(frontier<liveFrontier?frontier:liveFrontier):frontier??liveFrontier;
  const next=!complete?await sign({...pin,after:page.length>=limit?page.at(-1).order_key:scanFrontier??page.at(-1)?.order_key??pin.after}):null;
  const scopeIds=[...new Set(page.map(r=>r.scope_id).filter(Boolean))];
  const conflicted=new Set((await db.all("SELECT DISTINCT scope_id FROM pa_records WHERE kind='CONFLICT' AND ready=1 AND available_at<=? AND revision<=? AND scope_id IN(SELECT value FROM json_each(?)) LIMIT 129",input.asOf,pin.liveRevision,serialize(scopeIds))).map(r=>r.scope_id));
  for(const id of coldConflicts)conflicted.add(id);
  // Recheck only establishes whether we can still serve the pinned snapshot;
  // it does not advertise unqualified response-time CURRENT.
  const after=await db.first('SELECT epoch,live_floor FROM pa_meta');if(after.epoch!==pin.epoch||after.live_floor>pin.liveRevision)return {status:'UNAVAILABLE',reason:'pinned_live_snapshot_evicted',complete:false,records:[],snapshot:pin};
  return {status:complete?'COMPLETE':'PENDING',complete,reason,coverage,view,historicalTruth:'IMMUTABLE_VERSIONS_AT_PINNED_SNAPSHOT',pageRecordBudget:limit,cursor:next,snapshot:pin,traversal:{objects,bytes:total},
   censusDenominator:page.filter(r=>['CENSUS','CENSUS_CAPTURE'].includes(r.kind)).length,skippedAttempts:page.filter(r=>['CENSUS','CENSUS_CAPTURE'].includes(r.kind)&&(r.kind==='CENSUS'?parseEvidence(r.wire):parseEvidence(r.wire).decisionEvidence).outcome==='SKIPPED').length,actualEngineEvaluations:page.filter(r=>['CENSUS','CENSUS_CAPTURE'].includes(r.kind)&&(r.kind==='CENSUS'?parseEvidence(r.wire):parseEvidence(r.wire).decisionEvidence).evidenceCompleteness?.entry==='CAPTURED').length,denominatorScope:complete?'QUERY':'PAGE_ONLY',records:page.map(r=>({id:r.id,semanticIdentity:['CENSUS','CENSUS_CAPTURE'].includes(r.kind)?'evaluation:'+parseEvidence(r.wire).evaluationId:r.semantic_key,digest:r.digest,kind:r.kind,createdAt:r.created_at,availableAt:r.available_at,captureAvailableAt:r.captureAvailableAt??null,analyticalAvailableAt:r.kind==='CENSUS'?r.available_at:null,payload:parseEvidence(r.wire),
    dimensions:{storage:r.storage,availability:r.kind==='CENSUS_CAPTURE'?'PENDING':'AVAILABLE',integrity:conflicted.has(r.scope_id)?'CONFLICTED':'VERIFIED',retention:r.category==='CENSUS'||r.category==='DEFINITION'||r.retention>=input.asOf?'RETAINED':'POLICY_EXPIRED',coverage}}))};
 }
 // This capability deliberately exposes no database, migration, writer or hydration method.
 return Object.freeze({query});
}
