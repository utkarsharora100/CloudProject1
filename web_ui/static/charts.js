import {$,$$,esc,icon,money,short,human,date,time,utcDate,duration,badge,empty,api,poll,workspaceRequired} from './core.js';
const svg=(body,label)=>'<svg viewBox="0 0 520 245" role="group" aria-label="'+esc(label)+'">'+body+'</svg>';
const text=(x,y,value,attrs='')=>'<text x="'+x+'" y="'+y+'" '+attrs+'>'+esc(value)+'</text>';
const emptyChart=(title,copy)=>'<div class="chart-empty">'+icon('chart')+'<p>'+esc(title)+'</p><span>'+esc(copy)+'</span></div>';
export async function operations(ctx){
  if(!workspaceRequired(ctx))return;
  let latest=null,bucket=null,stage='',hours='24';
  $('#run-scope').textContent='Workspace run '+short(ctx.workspace.run_id);
  function table(){
    if(!latest)return;
    const rows=latest.orders.filter(row=>(!stage||row.status===stage)&&(!bucket||(Date.parse(row.created_at)>=Date.parse(bucket)&&Date.parse(row.created_at)<Date.parse(bucket)+3600000)));
    $('#operations-orders').innerHTML=rows.length?'<table><thead><tr><th>Order</th><th>Created · local time</th><th>Confirmed stage</th><th>Total</th><th></th></tr></thead><tbody>'+rows.slice().reverse().map(row=>'<tr><td class="order-code">'+short(row.order_id)+'</td><td>'+esc(date(row.created_at))+'</td><td>'+badge(row.status)+'</td><td class="amount">'+money(row.total_minor)+'</td><td><a class="text-link" href="/operations/orders/'+row.order_id+'">View execution ↗</a></td></tr>').join('')+'</tbody></table>':empty('No orders in this view.','Choose another filter or place a real local order in this workspace.','/','Choose a meal','bag');
    $('#clear-chart-filter').hidden=!bucket;
  }
  function arrivals(){
    const data=latest.arrivals;
    if(!latest.order_count){$('#arrivals-chart').innerHTML=emptyChart('The first order sets the rhythm.','This chart uses actual order creation times.');return;}
    const max=Math.max(1,...data.map(row=>row.count)),width=448,height=170,left=42,top=17,step=width/data.length;
    let body='';
    const ticks=Math.min(4,max);for(let i=0;i<=ticks;i++){const y=top+height-height*i/ticks;body+='<line class="gridline" x1="42" y1="'+y+'" x2="490" y2="'+y+'"/>'+text(30,y+4,Math.round(max*i/ticks),'text-anchor="end"');}
    body+='<line class="axis" x1="42" y1="187" x2="490" y2="187"/>';
    data.forEach((row,i)=>{
      const x=left+i*step+step*.17,h=height*row.count/max;
      body+='<g role="button" tabindex="0" data-bucket="'+esc(row.start)+'" aria-label="'+esc(utcDate(row.start))+', '+row.count+' orders. Select to filter."><rect x="'+x+'" y="'+(top+height-Math.max(h,1))+'" width="'+(step*.66)+'" height="'+Math.max(h,1)+'" class="bar '+(bucket===row.start?'selected':'')+'"/><rect x="'+(left+i*step)+'" y="15" width="'+step+'" height="188" fill="transparent"/><title>'+esc(utcDate(row.start))+': '+row.count+' orders</title></g>';
      if(i%Math.max(1,Math.floor(data.length/6))===0)body+=text(x+step*.3,211,row.label,'text-anchor="middle"');
    });
    body+=text(265,236,'Hour of creation · UTC','text-anchor="middle"');
    $('#arrivals-chart').innerHTML=svg(body,'Order arrivals by UTC hour. Bars filter the order list.');
    $('#arrivals-chart').insertAdjacentHTML('afterend','');
  }
  function durations(){
    const data=latest.durations;
    const points=data.flatMap((row,i)=>[['preparation_seconds','#8da77a','Preparation'],['delivery_seconds','#d0a669','Delivery']].filter(([key])=>row[key]!=null).map(([key,color,label])=>({i,key,color,label,value:row[key],order:row.order_id,at:row.created_at})));
    if(!points.length){$('#duration-chart').innerHTML=emptyChart('Let a stage finish first.','Unfinished durations are missing, never zero.');return;}
    const max=Math.max(60,...points.map(row=>row.value)),left=43,top=17,width=448,height=170;
    const x=i=>left+(data.length<=1?width/2:width*i/(data.length-1)),y=value=>top+height-height*value/max;
    let body='';
    for(let i=0;i<=4;i++){const yy=top+height-height*i/4;body+='<line class="gridline" x1="43" y1="'+yy+'" x2="491" y2="'+yy+'"/>'+text(32,yy+4,(max*i/4/60).toFixed(1),'text-anchor="end"');}
    for(const [key,color] of [['preparation_seconds','#8da77a'],['delivery_seconds','#d0a669']]){
      let segment=[];
      const flush=()=>{if(segment.length>1)body+='<polyline fill="none" stroke="'+color+'" stroke-width="2" points="'+segment.join(' ')+'"/>';segment=[];};
      data.forEach((row,i)=>{if(row[key]==null){flush();return;}segment.push(x(i)+','+y(row[key]));});flush();
    }
    points.forEach(row=>{body+='<g role="button" tabindex="0" data-point="'+row.order+'" data-label="'+esc(row.label)+'" data-seconds="'+row.value+'" aria-label="'+row.label+' '+duration(row.value)+', order '+short(row.order)+'. Open execution."><circle class="chart-point" cx="'+x(row.i)+'" cy="'+y(row.value)+'" r="4.7" fill="'+row.color+'"/><circle cx="'+x(row.i)+'" cy="'+y(row.value)+'" r="12" fill="transparent"/><title>'+row.label+' · '+duration(row.value)+' · '+short(row.order)+'</title></g>';});
    const ticks=data.length<=1?[0]:[0,Math.floor((data.length-1)/2),data.length-1];
    [...new Set(ticks)].forEach(i=>body+=text(x(i),211,short(data[i].order_id),'text-anchor="middle"'));
    body+=text(266,236,'Order · by creation time','text-anchor="middle"');
    $('#duration-chart').innerHTML=svg(body,'Measured preparation and delivery durations in minutes. Select a point to open its order.');
  }
  function dataTables(){
    $('#arrivals-chart').parentElement.querySelector('details')?.remove();
    $('#duration-chart').parentElement.querySelector('details')?.remove();
    $('#arrivals-chart').parentElement.insertAdjacentHTML('beforeend','<details class="data-table"><summary>Read the arrival data as a table</summary><table><thead><tr><th>UTC hour</th><th>Orders</th></tr></thead><tbody>'+latest.arrivals.map(row=>'<tr><td>'+esc(utcDate(row.start))+' UTC bucket</td><td>'+row.count+'</td></tr>').join('')+'</tbody></table></details>');
    $('#duration-chart').parentElement.insertAdjacentHTML('beforeend','<details class="data-table"><summary>Read the duration data as a table</summary><table><thead><tr><th>Order</th><th>Preparation</th><th>Delivery</th></tr></thead><tbody>'+latest.durations.map(row=>'<tr><td><a href="/operations/orders/'+row.order_id+'">'+short(row.order_id)+'</a></td><td>'+duration(row.preparation_seconds)+'</td><td>'+duration(row.delivery_seconds)+'</td></tr>').join('')+'</tbody></table></details>');
  }
  async function refresh(){
    const detailsState=$$('.chart-panel details').map(row=>row.open);
    latest=await api('/api/operations?hours='+hours);
    const metric=(label,value,copy)=>'<div class="metric-card"><span class="eyebrow">'+label+'</span><div class="metric-value">'+value+'</div><p>'+copy+'</p></div>';
    $('#operations-metrics').innerHTML=metric('Orders in this window',latest.order_count,latest.delivered_count+' confirmed delivered')+
      metric('Mean preparation',latest.preparation.mean_seconds==null?'—':(latest.preparation.mean_seconds/60).toFixed(1)+'<small>min</small>',latest.preparation.sample_count+' completed preparation samples')+
      metric('Mean delivery',latest.delivery.mean_seconds==null?'—':(latest.delivery.mean_seconds/60).toFixed(1)+'<small>min</small>',latest.delivery.sample_count+' completed delivery samples');
    const currentStage=$('#stage-filter').value;
    $('#stage-filter').innerHTML='<option value="">Every stage</option>'+Object.keys(latest.stage_counts).sort().map(value=>'<option value="'+value+'">'+esc(human(value))+' ('+latest.stage_counts[value]+')</option>').join('');
    $('#stage-filter').value=currentStage;stage=$('#stage-filter').value;
    arrivals();durations();dataTables();$$('.chart-panel details').forEach((row,i)=>row.open=!!detailsState[i]);table();
    $('#metrics-scope-note').textContent='Scope: this workspace’s recorded orders · '+hours+'-hour window ending '+date(latest.as_of)+' · UTC chart buckets include partial edge hours. '+(latest.truncated?'The bounded source page is incomplete; totals do not represent all workspace orders. ':'')+(latest.history_unavailable_count?latest.history_unavailable_count+' order histories are unavailable; duration samples exclude them.':'Only confirmed start/end pairs contribute to duration means.');
  }
  const reload=poll(refresh,7000);
  $('#metrics-window').addEventListener('change',event=>{hours=event.target.value;bucket=null;reload();});
  $('#stage-filter').addEventListener('change',event=>{stage=event.target.value;table();});
  $('#clear-chart-filter').addEventListener('click',()=>{bucket=null;arrivals();table();$('#arrivals-detail').textContent='Select a bar to filter the order list.';});
  function inspectBar(event,activate=false){
    const row=event.target.closest('[data-bucket]');if(!row)return;
    const point=latest.arrivals.find(v=>v.start===row.dataset.bucket);
    $('#arrivals-detail').textContent=utcDate(point.start)+' UTC bucket · '+point.count+' orders'+(activate?' · filter applied':'');
    if(activate){bucket=bucket===point.start?null:point.start;arrivals();table();}
  }
  $('#arrivals-chart').addEventListener('pointerover',e=>inspectBar(e));$('#arrivals-chart').addEventListener('focusin',e=>inspectBar(e));
  $('#arrivals-chart').addEventListener('click',e=>inspectBar(e,true));
  $('#arrivals-chart').addEventListener('keydown',e=>{if(['Enter',' '].includes(e.key)){e.preventDefault();inspectBar(e,true);}});
  function inspectPoint(event,activate=false){const row=event.target.closest('[data-point]');if(!row)return;$('#duration-detail').textContent=row.dataset.label+' · '+duration(Number(row.dataset.seconds))+' · order '+short(row.dataset.point);if(activate)location.href='/operations/orders/'+row.dataset.point;}
  $('#duration-chart').addEventListener('pointerover',e=>inspectPoint(e));$('#duration-chart').addEventListener('focusin',e=>inspectPoint(e));
  $('#duration-chart').addEventListener('click',e=>inspectPoint(e,true));
  $('#duration-chart').addEventListener('keydown',e=>{if(['Enter',' '].includes(e.key)){e.preventDefault();inspectPoint(e,true);}});
}
