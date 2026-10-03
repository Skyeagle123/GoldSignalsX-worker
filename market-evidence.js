import {canonicalSerialize,digestPayload,parseEvidence} from './signal-evidence.js';
export async function buildMarketManifest(tf,bars,availableAt,{blockSize=128,maxBytes=65536}={}){
  if(!Array.isArray(bars)||bars.length>2000)throw new Error('measurement_market_bound');
  const blocks=[],references=[];
  const duration=({'1m':60000,'1m-research':60000,'5m':300000,'15m':900000,'30m':1800000,'60m':3600000,'240m':14400000,'1d':86400000})[tf];
  if(!duration||!Number.isInteger(blockSize)||blockSize<1||blockSize>128)throw new Error('measurement_market_bound');
  const groups=[];let group=[],bucket=null;
  for(const bar of bars){const next=Math.floor(bar.t/(duration*blockSize));if(group.length&&(next!==bucket||group.length>=blockSize)){groups.push(group);group=[];}bucket=next;group.push(bar);}
  if(group.length)groups.push(group);
  for(const group of groups){
    const rows=group.map(b=>({t:b.t,o:b.o,h:b.h,l:b.l,c:b.c,v:b.v,
      provider:b.provider??null,sessionId:b.sessionId??null,sourceAvailableAt:b.availableAt??null}));
    const payload=canonicalSerialize({schema:1,tf,rows,ordering:'AS_CONSUMED'},maxBytes),digest=await digestPayload(payload);
    blocks.push({blockId:`market:${digest}`,digest,payload,tf,from:rows[0]?.t??null,to:rows.at(-1)?.t??null});
    references.push({blockId:`market:${digest}`,count:rows.length,availableAt,sourceAvailability:'UNKNOWN_UNLESS_ROW_PROVIDES_IT'});
  }
  return {manifest:{tf,count:bars.length,references,ordering:'AS_CONSUMED'},blocks};
}
export function replayMarketManifest(manifest,blocks){
  const byId=new Map(blocks.map(b=>[b.blockId,b]));
  return manifest.references.flatMap(ref=>{
    const block=byId.get(ref.blockId);if(!block)throw new Error('measurement_market_missing');
    const data=parseEvidence(block.payload);if(data.tf!==manifest.tf||data.rows.length!==ref.count)throw new Error('measurement_manifest_conflict');
    return data.rows;
  });
}
