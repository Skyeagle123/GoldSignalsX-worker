// Measurement-only limits. Never imported by Signal Engine or Exposure Manager.
export const OUTCOME_PLANNING_BYTES=3840;
export const OUTCOME_REVIEW_BYTES=8192;
// SQLite record payload: serial types + header varints + exact column bytes.
// Page/index allocation is deliberately separate from this logical record size.
const varintBytes=n=>{let k=1;while(n>=128&&k<9){n=Math.floor(n/128);k++;}return k;};
export function sqliteRecordPayloadBytes(values){
 const columns=values.map(v=>{if(v==null)return [0,0];if(v instanceof Uint8Array)return [12+2*v.length,v.length];if(typeof v==='string'){const n=new TextEncoder().encode(v).length;return [13+2*n,n];}if(typeof v!=='number')throw new Error('measurement_record_type');if(!Number.isSafeInteger(v))return [7,8];if(v===0||v===1)return [8+v,0];const n=v>=-128&&v<=127?1:v>=-32768&&v<=32767?2:v>=-8388608&&v<=8388607?3:v>=-2147483648&&v<=2147483647?4:v>=-140737488355328&&v<=140737488355327?6:8;return [n===6?5:n===8?6:n,n];});
 const types=columns.reduce((n,[type])=>n+varintBytes(type),0);let header=types+1;while(header!==types+varintBytes(header))header=types+varintBytes(header);
 return header+columns.reduce((n,[_type,bytes])=>n+bytes,0);
}
export function outcomeProfileBudget(profiles){
 if(!Array.isArray(profiles)||!profiles.length||profiles.some(p=>!Number.isFinite(p.bytes)||p.bytes<0))throw new Error('measurement_profile_invalid');
 const average=profiles.reduce((n,p)=>n+p.bytes,0)/profiles.length;
 return {testMatrixAverage:average,planningAveragePass:average<=OUTCOME_PLANNING_BYTES,reviewAlarm:profiles.some(p=>p.bytes>OUTCOME_REVIEW_BYTES),planningAllowance:OUTCOME_PLANNING_BYTES,reviewCeiling:OUTCOME_REVIEW_BYTES,populationDistribution:'NOT_PROVEN; TEST_MATRIX_IS_NOT_PRODUCTION'};
}
