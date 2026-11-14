import http from 'k6/http';
import {check,sleep} from 'k6';
import {Rate} from 'k6/metrics';
const failures=new Rate('business_errors');
const customer=__ENV.CUSTOMER_URL||'http://customer-service:8083';
const order=__ENV.ORDER_URL||'http://order-service:8081';
const restaurant=__ENV.RESTAURANT_URL||'http://restaurant-api:8084';
export const options={
 scenarios:{steady:{executor:'ramping-vus',startVUs:0,stages:[{duration:__ENV.RAMP||'1m',target:Number(__ENV.VUS||60)},{duration:__ENV.HOLD||'8m',target:Number(__ENV.VUS||60)},{duration:'30s',target:0}],gracefulRampDown:'20s'}},
 thresholds:{http_req_failed:['rate<0.01'],http_req_duration:['p(95)<2000'],business_errors:['rate<0.01']},
 discardResponseBodies:false,
};
function post(base,path,value,key){
 const response=http.post(base+path,JSON.stringify(value),{headers:{'Content-Type':'application/json',...(key?{'Idempotency-Key':key}:{})}});
 if(!check(response,{'fixture persisted':r=>r.status===201}))throw Error('Synthetic load fixture creation failed.');
 return response.json();
}
export function setup(){
 const run=Date.now().toString(36);
 const c=post(customer,'/customers',{name:'Synthetic Load Customer',email:'load-'+run+'@example.test'}).customer;
 const a=post(customer,'/customers/'+c.id+'/addresses',{label:'Load test',line1:'Synthetic block',city:'Demo City',region:'Demo',postal_code:'000000',country_code:'IN'}).address;
 const r=post(restaurant,'/restaurants',{name:'Synthetic Load Kitchen',description:'Isolated load fixture',currency:'INR',is_open:true,estimated_prep_time_mins:10});
 const rid=r.restaurant?.restaurant_id||r.restaurant_id;
 const menu=http.put(restaurant+'/restaurants/'+rid+'/menu/meal',JSON.stringify({name:'Load meal',price_minor:10000,available:true}),{headers:{'Content-Type':'application/json'}});
 if(menu.status!==200&&menu.status!==201)throw Error('Load menu unavailable.');
 const o=post(order,'/orders',{customer_id:c.id,address_id:a.id,restaurant_id:rid,items:[{item_id:'meal',quantity:1}]},'load-'+run);
 return {customer_id:c.id,order_id:o.order_id};
}
export default function(data){
 const results=http.batch([
  ['GET',customer+'/customers/'+data.customer_id,null,{tags:{name:'customer-profile'}}],
  ['GET',customer+'/customers/'+data.customer_id+'/addresses',null,{tags:{name:'customer-addresses'}}],
  ['GET',order+'/orders/'+data.order_id,null,{tags:{name:'order-read'}}],
  ['GET',order+'/orders/'+data.order_id+'/events',null,{tags:{name:'order-history'}}],
 ]);
 for(const response of results)failures.add(!check(response,{'actual service returned 200':r=>r.status===200}));
 sleep(Number(__ENV.PAUSE||0.1));
}

export function handleSummary(data){
 const names=['http_reqs','http_req_failed','http_req_duration','business_errors','checks','iterations'];
 const metrics=Object.fromEntries(names.map(name=>[name,data.metrics[name]?.values]));
 return {stdout:'K6_SUMMARY '+JSON.stringify({state:data.state,metrics})+'\\n'};
}
