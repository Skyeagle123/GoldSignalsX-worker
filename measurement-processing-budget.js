// One meter belongs to the outer opportunity, never to a processing subject.
import {need,parseEvidence} from './measurement-independent-contract.js';
import {validateMarketEvidence} from './measurement-market-envelope.js';
export const validatedMarketPacket=Symbol('invocation-validated-market-packet');
export async function validateDecodedMarket(record,budget=null,parsed=null){
 const p=parsed??parseEvidence(record.wire);
 if(p?.version==='measurement-market-v1'&&record.kind!=='MARKET_EXPORT'){budget?.unexpectedMarket();need(false,'pa_processing_market_identity');}
 if(record.kind==='MARKET_EXPORT'&&!record[validatedMarketPacket])record[validatedMarketPacket]=await validateMarketEvidence(record.wire);return record;
}
export const PROCESSING_BUDGET_VERSION='pa-processing-budget-v1';
export class ProcessingYield extends Error{constructor(phase){super('pa_processing_budget_pending');this.phase=phase;}}
export function processingBudget(db){
 const start=db.metrics().statements;
 const used={marketDecodes:0,recordDecodes:0,archiveOperations:0,archiveBytes:0,catalogueReads:0,decodedBytes:0};
 let phase='LOOKUP',committed=false;const checkpoints=[];
 const sqlRemaining=()=>Math.min(db.remainingStatements(),900-(db.metrics().statements-start));
 const trace=()=>({version:PROCESSING_BUDGET_VERSION,...used,sqlStatements:db.metrics().statements-start,reservedSqlRemaining:sqlRemaining(),reservedArchiveRemaining:1024-used.archiveOperations,phase,durableProgressCommitted:committed,checkpoints:[...checkpoints]});
 const b={invocation:true,sqlRemaining,archiveRemaining:()=>Infinity,
  beforeCheckpoint(){const t={sqlRemaining:sqlRemaining(),archiveRemaining:1024-used.archiveOperations,phase};need(t.sqlRemaining>=128&&t.archiveRemaining>=8,'pa_processing_checkpoint_reserve');if(checkpoints.length<16)checkpoints.push(t);},
  phase(p){phase=p;},trace,committed(){committed=true;},
  can({objects=0,bytes=0,records=0,market=0,catalogue=0,decodedBytes=0}={}){return used.archiveOperations+objects<=1016&&b.archiveRemaining()>=objects+8&&used.decodedBytes+decodedBytes<=16000000&&used.archiveBytes+bytes<=31820000&&used.recordDecodes+records<=128&&used.marketDecodes+market<=8&&used.catalogueReads+catalogue<=64&&sqlRemaining()>=160;},
  reserve(unit){if(!b.can(unit))throw new ProcessingYield(phase);},
  operation(key=''){need(used.archiveOperations<1024,'pa_processing_archive_operations');used.archiveOperations++;if(key.includes('/catalogue/')){need(used.catalogueReads<64,'pa_processing_catalogue_reads');used.catalogueReads++;}},
  readBytes(n){need(Number.isSafeInteger(n)&&n>=0&&used.archiveBytes+n<=32000000,'pa_processing_archive_bytes');used.archiveBytes+=n;},
  unexpectedMarket(){need(used.marketDecodes<8,'pa_processing_market_decodes');used.marketDecodes++;},
  // Even a declared non-market record needs one unused market allowance until
  // its bytes prove that classification. This prevents metadata relabelling
  // from smuggling a ninth market payload through a non-market decoder.
  record(r){need(Number.isSafeInteger(r.bytes??0)&&(r.bytes??0)>=0&&(r.bytes??0)<=2097152,'pa_processing_record_geometry');b.reserve({records:1,market:1,decodedBytes:r.bytes??0});used.decodedBytes+=r.bytes??0;used.recordDecodes++;if(r.kind==='MARKET_EXPORT')used.marketDecodes++;},
  get remaining(){return Math.min(8-used.marketDecodes,128-used.recordDecodes);}
 };
 return b;
}
