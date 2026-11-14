// Real core module with controlled browser timers: no external dependencies.
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';
const source=await readFile(new URL('../web_ui/static/core.js',import.meta.url),'utf8');
const original={setTimeout,clearTimeout,fetch:globalThis.fetch,document:globalThis.document,window:globalThis.window,crypto:globalThis.crypto};
const cryptoDescriptor=Object.getOwnPropertyDescriptor(globalThis,'crypto');
const timers=new Map(),listeners=new Map(),elements=new Map();let clock=0;
const response=data=>({status:200,ok:true,json:async()=>data});
const flush=async()=>{for(let i=0;i<12;i++)await Promise.resolve();};
const tick=async()=>{const first=timers.entries().next().value;assert.ok(first,'A future refresh must remain scheduled');timers.delete(first[0]);first[1]();await flush();};
try{
 Object.defineProperty(globalThis,'crypto',{value:{randomUUID},configurable:true});
 globalThis.setTimeout=callback=>{timers.set(++clock,callback);return clock;};
 globalThis.clearTimeout=id=>timers.delete(id);
 globalThis.document={hidden:false,activeElement:null,body:{},addEventListener(){},querySelector(selector){
  if(!elements.has(selector))elements.set(selector,{content:'synthetic-csrf',textContent:'',hidden:true});return elements.get(selector);
 },querySelectorAll(){return [];}};
 globalThis.window={addEventListener(name,fn){if(!listeners.has(name))listeners.set(name,[]);listeners.get(name).push(fn);}};
 globalThis.fetch=async()=>response({saved:true});
 const core=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
 let refreshes=0;core.poll(async()=>{refreshes++;},5000);await flush();assert.equal(refreshes,1);
 let resume;
 globalThis.fetch=()=>new Promise(resolve=>{resume=()=>resolve(response({saved:true}));});
 const saving=core.action('/api/profile',{name:'Synthetic diner'},null,null);
 await tick();assert.equal(refreshes,1);assert.ok(timers.size,'Saving must defer, not terminate, live refresh');
 resume();await saving;await tick();assert.equal(refreshes,2);
 document.hidden=true;await tick();assert.equal(refreshes,2);assert.ok(timers.size);
 document.hidden=false;await tick();assert.equal(refreshes,3);
 console.log('Live polling resumes after overlapping saves and hidden intervals: PASS');
 for(const fn of listeners.get('beforeunload')||[])fn();
 const requests=[],resolvers=[];
 globalThis.fetch=(path,options)=>new Promise(resolve=>{requests.push(JSON.parse(options.body));resolvers.push(()=>resolve(response({saved:true})));});
 const first=core.action('/api/cart',{restaurant_id:'paper',item_id:'bowl',quantity:1},null,null);
 const second=core.action('/api/cart',{restaurant_id:'paper',item_id:'tea',quantity:1},null,null);
 assert.deepEqual(requests.map(r=>r.item_id),['bowl','tea']);assert.notEqual(requests[0].operation_id,requests[1].operation_id);
 resolvers.forEach(resolve=>resolve());await Promise.all([first,second]);
 console.log('Concurrent cart actions preserve independent item submissions: PASS');
 const paths=[];globalThis.fetch=async path=>{paths.push(path);return response(path.includes('after=')?{rows:['second'],next_after:null}:{rows:['first'],next_after:'cursor one'});};
 const result=await core.paged('/records?recipient=courier','rows',3);
 assert.deepEqual(result.rows,['first','second']);assert.equal(result.next_after,null);
 assert.deepEqual(paths,['/records?recipient=courier','/records?recipient=courier&after=cursor%20one']);
 console.log('Live history refresh retains loaded pages and scopes cursor requests: PASS');
}finally{
 globalThis.setTimeout=original.setTimeout;globalThis.clearTimeout=original.clearTimeout;
 for(const name of ['fetch','document','window']){if(original[name]===undefined)delete globalThis[name];else globalThis[name]=original[name];}
 if(cryptoDescriptor)Object.defineProperty(globalThis,'crypto',cryptoDescriptor);else delete globalThis.crypto;
}
