// M1-only in-memory transport. Acceptance is NEVER durable delivery.
export function createOfflineTransport({deliver=null,maxPackets=128,maxBytes=16*1024*1024}={}){
 if(!Number.isInteger(maxPackets)||maxPackets<1||maxPackets>128||!Number.isInteger(maxBytes)||maxBytes<1||maxBytes>16*1024*1024)throw new Error('measurement_offline_bound_invalid');
 const identities=new Map(),events=new Map(),pending=new Map();let usedBytes=0;
 return Object.freeze({mode:'OFFLINE',
  async send(packet,{signal}={}){
   if(signal?.aborted)return {status:'ABORTED',durable:false};
   if(!Object.isFrozen(packet)||!Object.isFrozen(packet.envelope)||typeof packet.wire!=='string')throw new Error('measurement_transport_packet_invalid');
   const e=packet.envelope,prior=identities.get(e.semanticKey);
   if(prior&&prior!==e.payloadDigest)return {status:'INTEGRITY_CONFLICT',durable:false};
   if(events.has(e.eventId))return {status:'DUPLICATE',durable:false};
   if(pending.has(e.eventId)){const result=await pending.get(e.eventId);return result.status==='OFFLINE_ACCEPTED'?{status:'DUPLICATE',durable:false}:result;}
   if(events.size+pending.size>=maxPackets||usedBytes+packet.wireBytes>maxBytes)return {status:'OFFLINE_CAPACITY_EXCEEDED',durable:false};
   usedBytes+=packet.wireBytes;identities.set(e.semanticKey,e.payloadDigest);
   const work=Promise.resolve().then(async()=>{
    if(deliver)await deliver(packet,{signal});
    if(signal?.aborted)return {status:'ABORTED',durable:false};
    events.set(e.eventId,packet);return {status:'OFFLINE_ACCEPTED',durable:false};
   });pending.set(e.eventId,work);
   try{return await work;}finally{pending.delete(e.eventId);if(!events.has(e.eventId)){usedBytes-=packet.wireBytes;identities.delete(e.semanticKey);}}
  },
  packets:()=>Object.freeze([...events.values()])
 });
}
