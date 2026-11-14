import {$,$$,api,esc,short,human,date,time,duration,badge,stateClass,poll,empty,workspaceRequired} from './core.js';
const WIDTH=210,HEIGHT=82;
const fixed={payment:[300,30],kitchen_accept:[300,158],prepare:[300,286],assign:[300,414],pickup:[300,542],deliver:[300,670],notify_paid:[565,158],notify_ready:[565,414],notify_delivered:[565,798],refund:[35,158]};
const labels={payment:'Payment',kitchen_accept:'Kitchen acceptance',prepare:'Food preparation',assign:'Courier assignment',pickup:'Food pickup',deliver:'Delivery',notify_paid:'Payment note',notify_ready:'Ready note',notify_delivered:'Delivery note',refund:'Refund'};
const reasons={dependencies:'A required predecessor has not succeeded.',branch_condition:'This conditional branch waits for a confirmed refund request.',branch_not_taken:'This branch was not needed for the confirmed outcome.',prerequisite_failed:'A prerequisite failed or was cancelled. No downstream success is assumed.',payment_request_required:'A simulated payment has not been requested.',payment_retry_required:'The declined payment needs an explicit customer retry.',refund_request_required:'A refund request has not been recorded.',kitchen_action_required:'The kitchen must explicitly request the next action.',delivery_request_required:'Ready food needs an explicit dispatch request.',courier_unavailable:'No eligible courier is available in the selected zone.',older_order_waiting:'An older ready order has priority in this zone.',courier_action_required:'The assigned courier must confirm the physical handoff.',courier_presence_required:'The assigned courier needs fresh online presence.',notification_event_required:'No subscribed progress message exists for this stage.',notification_ordering:'An earlier progress note is still pending.',notification_backoff:'The next notification retry time has not arrived.',notification_failure:'The notification attempt budget was exhausted. An explicit retry is available in the inbox.',worker_unavailable:'Prerequisites are satisfied; waiting for a capable worker.',worker_selected:'A worker was selected. It must start a valid lease before doing work.',lease_started:'A worker owns the recorded execution lease.',worker_lost:'Worker ownership was lost; the attempt is recoverable within its budget.',lease_expired:'The previous execution lease expired.',order_confirmed:'The business outcome was confirmed by persisted Order history.',notification_confirmed:'The simulated notification transport receipt confirms delivery.',worker_completed:'The owning worker confirmed the required outcome.',resource_wait:'The task is waiting for an action, resource or safe outcome reconciliation.',backoff:'The task is waiting for its next permitted attempt.',notification_subscription_enabled:'A late subscription enabled this previously skipped notification.',notification_retry_requested:'A new explicit notification retry round was acknowledged.'};
const reason=node=>reasons[node.reason_code]||human(node.reason_code||node.state);
export function layout(nodes,edges){
  const values=new Map(nodes.map(node=>[node.task_id,{...node,x:0,y:0}]));
  const rank=new Map();const visiting=new Set();
  function depth(id){if(rank.has(id))return rank.get(id);if(visiting.has(id))return 0;visiting.add(id);const parents=edges.filter(edge=>edge.to===id).map(edge=>depth(edge.from));const value=parents.length?Math.max(...parents)+1:0;rank.set(id,value);visiting.delete(id);return value;}
  const rows=new Map();
  for(const node of values.values()){
    if(fixed[node.key]){[node.x,node.y]=fixed[node.key];continue;}
    const row=depth(node.task_id),index=rows.get(row)||0;rows.set(row,index+1);
    node.x=35+index*265;node.y=30+row*128;
  }
  return values;
}
function edgePath(a,b){
  const ax=a.x+WIDTH/2,ay=a.y+HEIGHT,bx=b.x+WIDTH/2,by=b.y;
  if(a.x===b.x&&b.y-a.y<=150)return 'M'+ax+' '+ay+' V'+by;
  if(a.x===b.x){const side=a.x-35;return 'M'+a.x+' '+(a.y+HEIGHT/2)+' H'+side+' V'+(b.y+HEIGHT/2)+' H'+b.x;}
  const right=b.x>a.x;const startX=right?a.x+WIDTH:a.x,endX=right?b.x:b.x+WIDTH;
  const middle=(startX+endX)/2;
  return 'M'+startX+' '+(a.y+HEIGHT/2)+' H'+middle+' V'+(b.y+HEIGHT/2)+' H'+endX;
}
export function relations(selected,edges,recursive=false){
  const result=new Set([selected]);let changed=true;
  if(!recursive){edges.forEach(edge=>{if(edge.from===selected)result.add(edge.to);if(edge.to===selected)result.add(edge.from);});return result;}
  const ancestors=new Set([selected]),descendants=new Set([selected]);
  while(changed){changed=false;for(const edge of edges){if(ancestors.has(edge.to)&&!ancestors.has(edge.from)){ancestors.add(edge.from);changed=true;}if(descendants.has(edge.from)&&!descendants.has(edge.to)){descendants.add(edge.to);changed=true;}}}
  return new Set([...ancestors,...descendants]);
}
export async function execution(ctx){
  if(!workspaceRequired(ctx))return;
  const oid=document.body.dataset.record,svg=$('#task-graph');
  let latest=null,nodes=new Map(),selected=null,chain=false,view={x:0,y:0,w:810,h:910},drag=null;
  function viewport(){svg.setAttribute('viewBox',[view.x,view.y,view.w,view.h].join(' '));}
  function fit(initial=false){const rows=[...nodes.values()];const width=Math.max(810,...rows.map(n=>n.x+WIDTH+35)),height=Math.max(300,...rows.map(n=>n.y+HEIGHT+35));view={x:0,y:0,w:width,h:height};if(initial&&svg.clientWidth<480){view={x:255,y:0,w:300,h:300*svg.clientHeight/Math.max(1,svg.clientWidth)};}viewport();}
  function select(id,focus=false){if(!nodes.has(id))return;selected=id;renderGraph();inspector();if(focus)$$('[data-task]',svg).find(n=>n.dataset.task===id)?.focus();}
  function renderGraph(){
    const graph=latest.graph;if(!graph)return;
    const related=selected?relations(selected,graph.edges,chain):new Set();
    const focused=document.activeElement?.dataset?.task,focusedList=document.activeElement?.dataset?.selectTask;
    $('#graph-edges').innerHTML=graph.edges.filter(edge=>nodes.has(edge.from)&&nodes.has(edge.to)).map(edge=>{
      const parent=nodes.get(edge.from),child=nodes.get(edge.to);
      const highlight=chain?related.has(edge.from)&&related.has(edge.to):edge.from===selected||edge.to===selected;
      return '<path class="graph-edge '+(highlight?'highlight':'')+'" d="'+edgePath(parent,child)+'" marker-end="url(#edge-arrow)" tabindex="0" role="button" data-edge="'+edge.from+':'+edge.to+'" aria-label="'+esc(labels[child.key]||child.label)+' requires '+esc(labels[parent.key]||parent.label)+'. Every prerequisite must succeed."><title>'+esc(labels[child.key]||child.label)+' requires '+esc(labels[parent.key]||parent.label)+' · AND dependency</title></path>';
    }).join('');
    $('#graph-nodes').innerHTML=[...nodes.values()].map(node=>{
      const label=labels[node.key]||node.label,expired=node.lease_deadline_expired&&node.state==='RUNNING';
      return '<g class="graph-node '+stateClass(expired||node.state==='READY'?'RETRY_WAIT':node.state)+(node.task_id===selected?' selected':related.has(node.task_id)?' related':'')+'" transform="translate('+node.x+' '+node.y+')" tabindex="0" role="button" data-task="'+node.task_id+'" aria-label="'+esc(label)+', '+esc(human(node.state))+'. '+esc(reason(node))+'"><rect class="node-body" width="'+WIDTH+'" height="'+HEIGHT+'" rx="7"/><text class="node-label" x="16" y="27">'+esc(label)+'</text><text class="node-state" x="16" y="48">'+esc(human(node.state))+(expired?' · lease expired':'')+'</text><text class="node-meta" x="16" y="67">'+esc(node.condition==='refund_requested'?'Conditional · refund requested':node.prerequisites.length>1?'AND · '+node.prerequisites_complete+' / '+node.prerequisites.length+' prerequisites':'Attempt '+node.attempt_count+' · generation '+node.generation)+'</text>'+(node.prerequisites.length>1?'<rect x="84" y="-21" width="42" height="15" rx="3" fill="#f7f2e7" stroke="#cabda6"/><text class="and-label" x="105" y="-10" text-anchor="middle">AND</text>':'')+'</g>';
    }).join('');
    if(focused)$$('[data-task]',svg).find(n=>n.dataset.task===focused)?.focus({preventScroll:true});
    $('#task-list').innerHTML=graph.nodes.map(node=>'<button class="task-list-button '+(node.task_id===selected?'active':'')+'" data-select-task="'+node.task_id+'"><strong>'+esc(labels[node.key]||node.label)+'</strong>'+badge(node.state==='READY'?'PENDING':node.state,human(node.state))+'<p>'+esc(reason(node))+'</p><p>Prerequisites: '+(node.prerequisites.length?node.prerequisites.map(p=>esc(labels[p.key]||human(p.key))+(p.satisfied?' ✓':' —')).join(' · '):'None')+'</p></button>').join('');
    if(focusedList)$$('[data-select-task]').find(n=>n.dataset.selectTask===focusedList)?.focus({preventScroll:true});
  }
  function inspector(){
    const node=nodes.get(selected);if(!node)return;
    const previous=$$('#task-inspector details').map(d=>d.open);
    const attempts=node.attempts||[];
    $('#task-inspector').innerHTML='<span class="eyebrow">A CLOSER LOOK</span><h2>'+esc(labels[node.key]||node.label)+'</h2>'+badge(node.state==='READY'?'PENDING':node.state,human(node.state))+
      '<div class="reason-box">'+esc(reason(node))+'</div><span class="eyebrow">PREREQUISITES · '+node.prerequisites_complete+' / '+node.prerequisites.length+'</span>'+
      (node.prerequisites.length?'<ul class="checklist">'+node.prerequisites.map(p=>'<li><span class="tick '+(p.satisfied?'done':'')+'">'+(p.satisfied?'✓':'—')+'</span><span>'+esc(labels[p.key]||human(p.key))+'<br><small>'+esc(human(p.state))+'</small></span></li>').join('')+'</ul>':'<p class="caption">No predecessor task. Domain input and ownership gates still apply.</p>')+
      '<dl class="detail-grid"><div><dt>First ready</dt><dd>'+time(node.ready_at)+'</dd></div><div><dt>First started</dt><dd>'+time(node.started_at)+'</dd></div><div><dt>Outcome confirmed</dt><dd>'+time(node.confirmed_at)+'</dd></div><div><dt>Next eligible</dt><dd>'+time(node.available_at)+'</dd></div><div><dt>Attempts</dt><dd>'+node.attempt_count+'</dd></div><div><dt>Worker failures</dt><dd>'+node.failure_count+' / '+node.max_failures+'</dd></div></dl>'+
      '<p class="caption">Completion source: '+esc(node.completion_source?human(node.completion_source):'Not confirmed')+'. '+(node.completion_source==='order_history'?'History confirmation does not invent worker execution time.':'')+'</p>'+
      '<details><summary>Attempt history ('+attempts.length+')</summary>'+(
        attempts.length?attempts.map(a=>'<div class="attempt-row"><strong>Generation '+a.generation+' · '+esc(human(a.outcome))+'</strong><p>'+esc(a.worker_name)+' · '+time(a.started_at)+' → '+time(a.finished_at)+'</p><p>'+(a.finished_at?duration((Date.parse(a.finished_at)-Date.parse(a.started_at))/1000):'Open attempt · not a completed duration')+'</p></div>').join(''):'<p class="caption">No worker attempt was recorded.</p>')+'</details>'+
      '<details><summary>Ownership & proof</summary><dl class="detail-grid"><div><dt>Recorded owner</dt><dd>'+esc(node.owner?.name||'No active owner')+'</dd></div><div><dt>Lease deadline</dt><dd>'+time(node.lease_until)+'</dd></div><div><dt>Generation</dt><dd>'+node.generation+'</dd></div><div><dt>Lease expired</dt><dd>'+String(node.lease_deadline_expired)+'</dd></div></dl><p class="caption">Task '+short(node.task_id)+' · confirmation event '+short(node.confirmed_event_id)+'. Private session and lease tokens are never exposed.</p></details>';
    $$('#task-inspector details').forEach((d,i)=>d.open=!!previous[i]);
  }
  function timeline(){
    const events=latest.history,times=latest.metrics.stage_times;
    const start=Date.parse(events[0]?.occurred_at),last=Math.max(...events.map(e=>Date.parse(e.occurred_at)),...(['DELIVERED','CANCELLED','REFUNDED'].includes(latest.order.status)?[]:[Date.now()])),total=Math.max(1,(last-start)/1000);
    const rows=[['Payment wait','PENDING_PAYMENT','PAID'],['Kitchen response','PAID','KITCHEN_ACCEPTED'],['Preparation','PREPARING','READY'],['Courier wait','READY','COURIER_ASSIGNED'],['Pickup wait','COURIER_ASSIGNED','PICKED_UP'],['Delivery','PICKED_UP','DELIVERED']];
    $('#stage-timeline').innerHTML=rows.map(([label,from,to])=>{
      const a=times[from],b=times[to];
      if(!a)return '<div class="timeline-row"><strong>'+label+'</strong><div class="timeline-track"></div><span>Not started</span></div>';
      const offset=(Date.parse(a)-start)/1000/total*100,elapsed=((b?Date.parse(b):last)-Date.parse(a))/1000,bar=Math.max(.2,elapsed/total*100);
      return '<div class="timeline-row"><strong>'+label+'</strong><div class="timeline-track"><svg viewBox="0 0 100 20" preserveAspectRatio="none" aria-label="'+esc(label)+' '+esc(b?duration(elapsed):'unfinished')+'"><rect class="'+(b?'closed-interval':'open-interval')+'" x="'+offset+'" y="1" width="'+bar+'" height="18" rx="1"/></svg></div><span>'+esc(b?duration(elapsed):'Unfinished')+'</span></div>';
    }).join('')+'<div class="timeline-axis"><span>'+time(events[0]?.occurred_at)+'</span><span>'+time(new Date(last).toISOString())+'</span></div><p class="fineprint">Actual Order event times. Open intervals extend to this observation and are not included in completed-duration averages. Waiting and execution are shown separately.</p>';
  }
  async function refresh(){
    latest=await api('/api/operations/orders/'+oid);
    $('#execution-ref').textContent='ORDER '+short(oid)+' · LIVE SIMULATION';
    $('#execution-state').innerHTML=badge(latest.order.status);
    $('#graph-freshness').textContent=latest.graph?(latest.graph.coordination_available?'Observed '+time(latest.observed_at):'Coordination unavailable · recorded state retained'):'Graph is being materialized';
    if(!latest.graph){$('#task-inspector').innerHTML=empty('The graph is taking shape.','The elected scheduler has not materialized this order yet. Its actual records will appear here.',null,null,'layers');return;}
    nodes=layout(latest.graph.nodes,latest.graph.edges);
    if(!selected||!nodes.has(selected)){
      selected=latest.graph.nodes.find(n=>n.state==='RUNNING')?.task_id||latest.graph.nodes.find(n=>n.state==='READY')?.task_id||latest.graph.nodes.find(n=>n.state==='BLOCKED')?.task_id||latest.graph.nodes[0]?.task_id;
      fit(true);
    }
    renderGraph();inspector();timeline();
    $('#execution-notifications').innerHTML=latest.notifications.length?latest.notifications.map(n=>'<article class="activity-card"><span class="eyebrow">'+esc(human(n.recipient_type))+' · '+esc(human(n.channel))+'</span><h3>'+esc(n.title)+'</h3>'+badge(n.state)+'<p>'+n.attempt_count+' transport attempts · round '+n.retry_round+'</p><p>'+esc(n.sent_at?'Confirmed '+time(n.sent_at):'No confirmed transport receipt yet')+'</p>'+(n.workflow_id?'<p class="order-code">Workflow '+short(n.workflow_id)+'</p>':'')+'</article>').join(''):empty('No notification activity yet.','A subscription and a committed stage create a message. Independent branches never hold up the meal.',null,null,'bell');
  }
  poll(refresh,4500);
  function zoom(factor){const cx=view.x+view.w/2,cy=view.y+view.h/2;view.w=Math.max(260,Math.min(2200,view.w*factor));view.h=Math.max(290,Math.min(2500,view.h*factor));view.x=cx-view.w/2;view.y=cy-view.h/2;viewport();}
  $('#graph-zoom-in').addEventListener('click',()=>zoom(.82));$('#graph-zoom-out').addEventListener('click',()=>zoom(1.22));$('#graph-fit').addEventListener('click',()=>fit());
  $('#graph-chain').addEventListener('click',event=>{chain=!chain;event.currentTarget.setAttribute('aria-pressed',chain);renderGraph();});
  $('#task-list').addEventListener('click',event=>{const b=event.target.closest('[data-select-task]');if(b){select(b.dataset.selectTask);$('#task-inspector').scrollIntoView({block:'nearest',behavior:'smooth'});}});
  function click(event){
    const node=event.target.closest('[data-task]');
    if(node){select(node.dataset.task);return;}
    const edge=event.target.closest('[data-edge]');
    if(edge){const [from,to]=edge.dataset.edge.split(':');select(to);$('#task-inspector .reason-box').textContent=(labels[nodes.get(to).key]||nodes.get(to).label)+' requires '+(labels[nodes.get(from).key]||nodes.get(from).label)+'. All prerequisites must succeed; selecting this edge does not change execution.';}
  }
  svg.addEventListener('click',event=>{if(drag?.moved)return;click(event);});
  svg.addEventListener('keydown',event=>{if(['Enter',' '].includes(event.key)){event.preventDefault();click(event);}if(event.key==='+')zoom(.82);if(event.key==='-')zoom(1.22);});
  svg.addEventListener('pointerdown',event=>{if(event.button!==0)return;drag={x:event.clientX,y:event.clientY,view:{...view},moved:false,target:event.target};svg.setPointerCapture(event.pointerId);});
  svg.addEventListener('pointermove',event=>{if(!drag)return;const dx=event.clientX-drag.x,dy=event.clientY-drag.y;if(Math.abs(dx)+Math.abs(dy)>5)drag.moved=true;if(drag.moved){const rect=svg.getBoundingClientRect();view.x=drag.view.x-dx*view.w/rect.width;view.y=drag.view.y-dy*view.h/rect.height;viewport();}});
  svg.addEventListener('pointerup',event=>{if(drag&&!drag.moved)click({target:drag.target});setTimeout(()=>drag=null,0);});
  svg.addEventListener('pointercancel',()=>drag=null);
}
