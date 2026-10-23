'use strict';
// Deliberately narrow canonical JSON domain: safe integers, strings, booleans,
// null, arrays and objects. No duplicate keys, floats, trailing data or surrogates.
function parse(text,{integersOnly=true}={}) {
 if(typeof text!=='string'||Buffer.byteLength(text)>1048576)throw Error('JSON size/type');
 let i=0;function ws(){while(/[ \t\r\n]/.test(text[i]||'')&&i<text.length)i++;}
 function val(depth=0){if(depth>40)throw Error('JSON depth');ws();let c=text[i];
 if(c==='{'||c==='['){i++;let obj=c==='{'?Object.create(null):[],seen=new Set(),end=c==='{'?'}':']';ws();if(text[i]===end){i++;return obj;}while(true){if(c==='{'){ws();if(text[i]!=='"')throw Error('JSON key');let k=str();if(seen.has(k)||['__proto__','constructor','prototype'].includes(k))throw Error('JSON duplicate/unsafe key');seen.add(k);ws();if(text[i++]!==':')throw Error('JSON colon');obj[k]=val(depth+1);}else obj.push(val(depth+1));ws();if(text[i]===end){i++;return obj;}if(text[i++]!==',')throw Error('JSON delimiter');}}
 if(c==='"')return str();for(let [token,v]of [['true',true],['false',false],['null',null]])if(text.slice(i,i+token.length)===token){i+=token.length;return v;}
 let m=/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(text.slice(i));if(!m)throw Error('JSON value');i+=m[0].length;let n=Number(m[0]);if(!Number.isFinite(n)||(integersOnly&&(!Number.isSafeInteger(n)||/[.eE]/.test(m[0])))||Object.is(n,-0))throw Error('JSON number');return n;
 }
 function str(){let a=i++;while(i<text.length){if(text[i]==='\\'){i+=2;continue;}if(text[i++]==='"'){let s=JSON.parse(text.slice(a,i));for(let j=0;j<s.length;j++){let n=s.charCodeAt(j);if(n>=0xd800&&n<=0xdbff){let b=s.charCodeAt(++j);if(!(b>=0xdc00&&b<=0xdfff))throw Error('JSON surrogate');}else if(n>=0xdc00&&n<=0xdfff)throw Error('JSON surrogate');}return s;}}throw Error('JSON string');}
 let r=val();ws();if(i!==text.length)throw Error('JSON trailing');return r;
}
function canonical(v){if(v===null||typeof v!=='object')return JSON.stringify(v);if(Array.isArray(v))return '['+v.map(canonical).join(',')+']';return '{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')+'}';}
module.exports={parse,canonical};
