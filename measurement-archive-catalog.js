// Immutable bounded radix catalogue. Only the latest root is needed in hot D1.
// Semantic identities, never local SQL row IDs, select portable entries.
import {need,hash,serialize,bytes,LIMITS} from './measurement-independent-contract.js';
const VERSION='measurement-catalogue-v1',MAX_DEPTH=32,MAX_LEAF=8;
async function load(archive,key){if(!key)return {format:VERSION,type:'LEAF',entries:[]};const node=JSON.parse(await archive.read(key));need(node.format===VERSION&&['LEAF','BRANCH'].includes(node.type),'pa_catalogue_format');
 if(node.type==='LEAF')need(Array.isArray(node.entries)&&node.entries.length<=MAX_LEAF,'pa_catalogue_leaf');else need(node.children&&Object.keys(node.children).length<=16&&Object.keys(node.children).every(k=>/^[a-f0-9]$/.test(k)),'pa_catalogue_branch');return node;}
export async function catalogueInsert(archive,rootKey,entry,depth=0){need(depth<MAX_DEPTH,'pa_catalogue_depth');const node=await load(archive,rootKey),key=entry.order_key??await hash(entry.id);need(/^[a-f0-9]{64}$/.test(key),'pa_catalogue_identity');entry={...entry,order_key:key};
 if(node.type==='LEAF'){const existing=node.entries.find(e=>e.id===entry.id);need(!existing||existing.digest===entry.digest&&(entry.lookupType==='SAMPLE'||['available_at','created_at','cohort','kind','semantic_key'].every(k=>existing[k]===entry[k])),'pa_catalogue_semantic_conflict');if(existing&&entry.lookupType==='SAMPLE')return rootKey;const entries=node.entries.filter(e=>e.id!==entry.id);entries.push(entry);entries.sort((a,b)=>a.order_key.localeCompare(b.order_key));
  if(entries.length<=MAX_LEAF)return (await archive.write('catalogue',{format:VERSION,type:'LEAF',entries})).key;
  const children={};for(const e of entries){const nibble=e.order_key[depth];children[nibble]=await catalogueInsert(archive,children[nibble]??null,e,depth+1);}return (await archive.write('catalogue',{format:VERSION,type:'BRANCH',children})).key;
 }
 const nibble=key[depth],children={...node.children};children[nibble]=await catalogueInsert(archive,children[nibble]??null,entry,depth+1);return (await archive.write('catalogue',{format:VERSION,type:'BRANCH',children})).key;
}
export async function catalogueLookup(archive,rootKey,id,{maxObjects=34}={}){const digest=await hash(id);let key=rootKey;for(let depth=0;key&&depth<MAX_DEPTH;depth++){need(depth<maxObjects,'pa_catalogue_lookup_budget');const node=await load(archive,key);if(node.type==='LEAF')return node.entries.find(e=>e.id===id)??null;key=node.children[digest[depth]]??null;}need(!key,'pa_catalogue_depth');return null;}
export async function cataloguePage(archive,rootKey,{after='',maxObjects=32,limit=64}={}){need(limit>=1&&limit<=129&&maxObjects>=1&&maxObjects<=64,'pa_catalogue_page_bound');let objects=0;const entries=[],stack=rootKey?[{key:rootKey,prefix:''}]:[];
 while(stack.length){const nodeRef=stack.pop();if(after&&nodeRef.prefix&&nodeRef.prefix<after.slice(0,nodeRef.prefix.length))continue;
  if(objects>=maxObjects)return {entries,complete:false,next:entries.at(-1)?.order_key??after,reason:'catalogue_budget',objects};const node=await load(archive,nodeRef.key);objects++;
  if(node.type==='LEAF'){for(const e of node.entries){if(e.order_key<=after)continue;entries.push(e);if(entries.length>=limit)return {entries,complete:false,next:e.order_key,objects};}}
  else for(const nibble of Object.keys(node.children).sort().reverse())stack.push({key:node.children[nibble],prefix:nodeRef.prefix+nibble});
 }
 return {entries,complete:true,next:entries.at(-1)?.order_key??after,objects};}
