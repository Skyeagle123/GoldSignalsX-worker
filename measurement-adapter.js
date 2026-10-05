// M2 offline admission and transaction ownership. No Worker/env or remote DB.
const adapters=new WeakMap(),databases=new WeakMap(),statements=new WeakMap(),leases=new WeakMap();
const required=db=>{const r=adapters.get(db);if(!r)throw new Error('measurement_commit_adapter_required');return r;};
export function measurementAdapterState(db){return required(db).root.view;}
export function assertMeasurementAdapter(db){required(db);}
export function measurementAdapterMetrics(db){const r=required(db).root;return {...r.metrics,pendingSubjects:r.state.pending.size,pendingEvaluations:r.state.pendingEvaluations.size,generationTokens:r.state.generations.size};}
const thenable=v=>v&&typeof v.then==='function';
const copyValue=v=>ArrayBuffer.isView(v)||v instanceof ArrayBuffer?structuredClone(v):v;

// The sole admitted engine is native, synchronous SQLite. Import is deferred:
// importing the trading/shared modules does not load any Node or DB capability.
export async function createOfflineSqliteMeasurementAdapter(database,{calls=[]}={}){
 const sqliteModule='node:sqlite';const {DatabaseSync}=await import(sqliteModule);
 if(!(database instanceof DatabaseSync))throw new Error('measurement_native_sqlite_required');
 let root=databases.get(database);
 if(!root){
  const nativePrepare=DatabaseSync.prototype.prepare,nativeExec=DatabaseSync.prototype.exec;
  // Native methods perform their own receiver brand checks; copied prototypes
  // or caller replacements cannot supply the transaction implementation.
  nativePrepare.call(database,'SELECT 1').get();
  root={database,prepare:sql=>nativePrepare.call(database,sql),exec:sql=>nativeExec.call(database,sql),busy:false,
   metrics:{protectedTransactions:0,subjectChecks:0,mutationStatements:0,instrumentationCallbacks:0},
   state:{pending:new Set(),pendingEvaluations:new Set(),generations:new Map(),exhausted:false}};
  const readonlySet=set=>Object.freeze({has:id=>set.has(id),get size(){return set.size;}});
  root.view=Object.freeze({pending:readonlySet(root.state.pending),pendingEvaluations:readonlySet(root.state.pendingEvaluations),get exhausted(){return root.state.exhausted;}});
  databases.set(database,root);
 }
 return construct(root,{calls});
}
function construct(root,options){
 options=Object.freeze({...options});const record={root,options};
 const basePrepare=sql=>{
  options.calls?.push(sql);
  const bind=(...inputValues)=>{const values=Object.freeze(inputValues.map(copyValue));
   const native=()=>root.prepare(sql);
   const statement={async run(){return (await execute(record,[statement],null))[0];},
    async first(){return native().get(...values)??null;},async all(){return {results:native().all(...values)};}};
   statements.set(statement,{root,sql,values});return Object.freeze(statement);
  };
  return {bind,first:()=>bind().first(),all:()=>bind().all()};
 };
 const prepare=sql=>{
  if(!options.prepare)return basePrepare(sql);
  const decorated=options.prepare(sql);
  return {...decorated,bind(...values){const s=decorated.bind(...values);const bound={...s,async run(){return (await execute(record,[bound],null))[0];}};statements.set(bound,{root,sql,values:Object.freeze(values.map(copyValue))});return Object.freeze(bound);}};
 };
 const adapter={prepare,batch:ss=>execute(record,ss,null)};
 Object.freeze(adapter);adapters.set(adapter,record);return adapter;
}
// Explicit instrumentation/decorators may affect reads and ordinary ingress,
// but cannot replace the protected transaction implementation or its checks.
export function wrapOfflineMeasurementAdapter(base,options={}){
 const registered=required(base);if(options.batch&&options.batch!==base.batch)throw new Error('measurement_commit_implementation_substitution');
 return construct(registered.root,options);
}
export function suspendMeasurementLifecycle(db,signalId,evaluationId=null){
 const c=required(db).root.state;
 for(const [set,id,key]of [[c.pending,signalId,`signal:${signalId}`],[c.pendingEvaluations,evaluationId,`evaluation:${evaluationId}`]]){
  if(!id)continue;if(set.size>=4032&&!set.has(id))c.exhausted=true;else set.add(id);
  const generation=c.generations.get(key);if(generation)generation.valid=false;
 }
}
export function clearMeasurementLifecycle(db,signalId,evaluationId=null){
 const c=required(db).root.state;if(signalId){c.pending.delete(signalId);c.generations.delete(`signal:${signalId}`);}if(evaluationId){c.pendingEvaluations.delete(evaluationId);c.generations.delete(`evaluation:${evaluationId}`);}
}
export function assertMeasurementLifecycle(db,signalId,evaluationId=null){const c=required(db).root.state;if(c.exhausted||c.pending.has(signalId)||evaluationId&&c.pendingEvaluations.has(evaluationId))throw new Error('measurement_quarantine_capability_pending');}
export function acquireMeasurementCommitCheck(db,signalId,evaluationId=null){
 const root=required(db).root,c=root.state,keys=[`signal:${signalId}`,...(evaluationId?[`evaluation:${evaluationId}`]:[])],tokens=[];
 try{for(const key of keys){let token=c.generations.get(key);if(!token){if(c.generations.size>=8064){c.exhausted=true;throw new Error('measurement_quarantine_capability_pending');}token={valid:true,users:0};c.generations.set(key,token);}token.users++;tokens.push([key,token]);}}
 catch(error){for(const [key,t]of tokens)if(--t.users===0&&c.generations.get(key)===t)c.generations.delete(key);throw error;}
 let released=false;
 const check=()=>{root.metrics.subjectChecks++;if(released||c.exhausted||c.pending.has(signalId)||evaluationId&&c.pendingEvaluations.has(evaluationId)||tokens.some(([,t])=>!t.valid))throw new Error('measurement_quarantine_capability_pending');};
 check.release=()=>{if(released)return;released=true;for(const [key,t]of tokens)if(--t.users===0&&c.generations.get(key)===t)c.generations.delete(key);};
 leases.set(check,{root,check});return Object.freeze(check);
}
export async function commitMeasurement(db,ss,check){
 const r=required(db),registered=(Array.isArray(check)?check:[check]).map(c=>leases.get(c));
 if(!registered.length||registered.some(lease=>!lease||lease.root!==r.root))throw new Error('measurement_commit_check_required');
 return execute(r,ss,()=>{for(const lease of registered)lease.check();});
}
async function execute(record,ss,check){
 const {root,options}=record;
 const work=ss.map(s=>{const d=statements.get(s);if(!d||d.root!==root)throw new Error('measurement_statement_capability_required');return d;});
 const publicWork=(options.beforeBatch||options.beforeStatement||options.afterStatement||options.beforeCommit||options.afterCommit)?Object.freeze(work.map(d=>Object.freeze({sql:d.sql,values:Object.freeze(d.values.map(copyValue))}))):null;
 if(options.beforeBatch){root.metrics.instrumentationCallbacks++;const v=options.beforeBatch(publicWork,!!check);if(thenable(v))await v;}
 check?.();
 if(root.busy)throw new Error('measurement_transaction_busy');
 root.exec('BEGIN');root.busy=true;let ownsTransaction=true;if(check)root.metrics.protectedTransactions++;
 try{
  const results=[];
  for(let i=0;i<work.length;i++){const d=work[i];
   if(options.beforeStatement){root.metrics.instrumentationCallbacks++;const v=options.beforeStatement(publicWork[i],!!check);if(thenable(v))await v;}
   check?.();const r=root.prepare(d.sql).run(...d.values);root.metrics.mutationStatements++;results.push({meta:{rows_written:Number(r.changes)}});
   if(options.afterStatement){root.metrics.instrumentationCallbacks++;const v=options.afterStatement(publicWork[i],!!check);if(thenable(v))await v;}
  }
  // A hook may pause after SQL/ordinary checks. It is never the final check.
  if(options.beforeCommit){root.metrics.instrumentationCallbacks++;const v=options.beforeCommit(publicWork,!!check);if(thenable(v))await v;}
  check?.();root.exec('COMMIT');root.busy=false;ownsTransaction=false;
  if(options.afterCommit){root.metrics.instrumentationCallbacks++;const v=options.afterCommit(publicWork,!!check);if(thenable(v))await v;}return results;
 }catch(error){if(ownsTransaction)root.exec('ROLLBACK');throw error;}finally{if(ownsTransaction)root.busy=false;}
}
