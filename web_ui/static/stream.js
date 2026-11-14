import {$,esc,api,poll,workspaceRequired,empty} from './core.js';
const utcDate=value=>new Date(value).toISOString().replace('T',' ').replace('.000Z','');
export async function stream(ctx){
 if(!workspaceRequired(ctx))return;
 let rows=[],selected=null;
 function render(){
  const latest=rows.at(-1),metric=(label,value,copy)=>'<div class="metric-card"><span class="eyebrow">'+label+'</span><div class="metric-value">'+value+'</div><p>'+esc(copy)+'</p></div>';
  $('#stream-metrics').innerHTML=metric('Latest active orders',latest?.unique_order_count??'—','Distinct orders in the latest finalized window')+metric('Latest source events',latest?.event_count??'—','Duplicate event identities counted once')+metric('Window length',latest?latest.window_seconds+'<small>sec</small>':'—','Event time · finalized by a source watermark');
  if(!rows.length){
   $('#stream-chart').innerHTML=empty('A window is taking shape.','Finalized Flink results appear after source activity advances the watermark. Create and progress a real local order; no results are fabricated.',null,null,'chart');
   $('#stream-table').innerHTML='';return;
  }
  const max=Math.max(1,...rows.map(r=>r.unique_order_count)),left=43,top=15,height=170,step=448/rows.length;
  let svg='<svg viewBox="0 0 520 245" role="group" aria-label="Flink finalized unique active orders by UTC window">';
  const ticks=Math.min(4,max);
  for(let i=0;i<=ticks;i++){const y=top+height-height*i/ticks;svg+='<line class="gridline" x1="43" y1="'+y+'" x2="491" y2="'+y+'"/><text x="32" y="'+(y+4)+'" text-anchor="end">'+Math.round(max*i/ticks)+'</text>';}
  rows.forEach((r,i)=>{const x=left+i*step,h=height*r.unique_order_count/max;svg+='<g role="button" tabindex="0" data-window="'+i+'" aria-label="'+esc(utcDate(r.window_start))+' UTC, '+r.unique_order_count+' active orders, '+r.event_count+' events"><rect class="bar '+(selected===r.window_start?'selected':'')+'" x="'+(x+step*.15)+'" y="'+(top+height-Math.max(1,h))+'" width="'+step*.7+'" height="'+Math.max(1,h)+'"/><rect x="'+x+'" y="10" width="'+step+'" height="180" fill="transparent"/><title>'+esc(utcDate(r.window_start))+' UTC · '+r.unique_order_count+' active orders</title></g>';if(i%Math.max(1,Math.ceil(rows.length/5))===0)svg+='<text x="'+(x+step/2)+'" y="210" text-anchor="middle">'+new Date(r.window_start).toISOString().slice(11,16)+'</text>';});
  svg+='<text x="266" y="237" text-anchor="middle">Window start · UTC</text></svg>';$('#stream-chart').innerHTML=svg;
  const visible=rows.filter((r,i)=>selected==null||r.window_start===selected);
  $('#stream-table').innerHTML='<table><thead><tr><th>UTC window</th><th>Unique active orders</th><th>Source events</th><th>Event stages</th></tr></thead><tbody>'+visible.slice().reverse().map(r=>'<tr><td>'+esc(utcDate(r.window_start))+' → '+esc(utcDate(r.window_end))+'</td><td>'+r.unique_order_count+'</td><td>'+r.event_count+'</td><td>'+Object.entries(r.stage_counts).map(([k,v])=>esc(k.toLowerCase().replaceAll('_',' '))+': '+v).join(' · ')+'</td></tr>').join('')+'</tbody></table>';
  $('#stream-clear').hidden=selected==null;
 }
 poll(async()=>{const value=await api('/api/operations/stream');rows=value.windows;if(selected!=null&&!rows.some(r=>r.window_start===selected))selected=null;render();},7000);
 function inspect(event,activate=false){const node=event.target.closest('[data-window]');if(!node)return;const i=Number(node.dataset.window),r=rows[i];$('#stream-detail').textContent=utcDate(r.window_start)+' → '+utcDate(r.window_end)+' UTC · '+r.unique_order_count+' unique active orders · '+r.event_count+' distinct source events';if(activate){selected=selected===r.window_start?null:r.window_start;render();}}
 $('#stream-chart').addEventListener('pointerover',e=>inspect(e));$('#stream-chart').addEventListener('focusin',e=>inspect(e));$('#stream-chart').addEventListener('click',e=>inspect(e,true));
 $('#stream-chart').addEventListener('keydown',e=>{if(['Enter',' '].includes(e.key)){e.preventDefault();inspect(e,true);}});
 $('#stream-clear').addEventListener('click',()=>{selected=null;render();});
}
