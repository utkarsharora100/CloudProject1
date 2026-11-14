import {$,$$,api,action,esc,icon,money,short,human,date,time,badge,empty,mealArt,poll,toast,workspaceRequired,confirmAction,paged} from './core.js';

export async function kitchen(ctx){
  if(!workspaceRequired(ctx))return;
  let rows=[],next=null,deliveries=[],pageCount=1;
  const menu=await api('/api/restaurants');
  const options=menu.restaurants;
  if(!options.some(r=>r.restaurant_id===ctx.kitchen_restaurant_id)){
    try{const m=await api('/api/restaurants/'+ctx.kitchen_restaurant_id+'/menu');options.unshift({restaurant_id:m.restaurant_id,name:m.name});}catch{}
  }
  $('#kitchen-select').innerHTML=options.map(row=>'<option value="'+esc(row.restaurant_id)+'" '+(row.restaurant_id===ctx.kitchen_restaurant_id?'selected':'')+'>'+esc(row.name)+'</option>').join('');
  $('#kitchen-name').textContent=options.find(r=>r.restaurant_id===ctx.kitchen_restaurant_id)?.name||'Live kitchen queue';
  $('#kitchen-select').addEventListener('change',async event=>{try{await action('/api/context',{restaurant_id:event.target.value},null,'Kitchen selected.');location.reload();}catch{}});
  function render(){
    const states=[['WAITING','Incoming'],['ACCEPTED','Accepted'],['PREPARING','Preparing'],['READY','Ready & handed over']];
    $('#kitchen-summary').innerHTML=states.map(([state,label])=>'<div class="small-metric"><strong>'+rows.filter(r=>r.state===state).length+'</strong><span>'+label+'</span></div>').join('');
    $('#kitchen-board').innerHTML=states.map(([state,label])=>{
      const items=rows.filter(row=>row.state===state);
      return '<section class="board-column"><div class="column-heading"><h2>'+label+'</h2><span>'+items.length+'</span></div>'+(
        items.length?items.map(row=>{
          const pending=(row.commands||[]).find(cmd=>cmd.state==='PENDING');
          const job=deliveries.find(d=>d.order_id===row.order_id);
          const final=['COURIER_ASSIGNED','PICKED_UP','DELIVERED','CANCELLED','REFUND_PENDING','REFUNDED'].includes(row.order_status);
          let buttons='';
          if(state==='WAITING')buttons='<button class="button primary" data-order="'+row.order_id+'" data-action="accept">Accept order ↗</button><button class="text-button" data-order="'+row.order_id+'" data-action="reject">Reject & request refund</button>';
          if(state==='ACCEPTED')buttons='<button class="button primary" data-order="'+row.order_id+'" data-action="start_preparation">Start preparation ↗</button><button class="text-button" data-order="'+row.order_id+'" data-action="reject">Reject before cooking</button>';
          if(state==='PREPARING')buttons='<button class="button primary" data-order="'+row.order_id+'" data-action="ready">Mark food ready ✓</button>';
          if(state==='READY'&&!job&&!final)buttons='<button class="button primary" data-order="'+row.order_id+'" data-action="dispatch">Request a courier ↗</button>';
          if(pending)buttons='<div class="pending-request">'+esc(human(pending.action))+' requested · waiting for confirmation</div>';
          else if(job)buttons='<div class="pending-request">'+(job.state==='WAITING'?'Courier requested · '+esc(human(job.wait_reason||'waiting for capacity')):esc(human(job.state)))+'</div>';
          else if(final)buttons='<div class="pending-request">'+esc(human(row.order_status))+'</div>';
          if(row.details_unavailable)buttons='<div class="notice">Ticket details are temporarily unavailable. Actions resume after the owning service confirms them.</div>';
          return '<article class="ticket"><span class="eyebrow">ORDER '+short(row.order_id)+'</span><h3>'+row.estimated_prep_time_mins+' min estimate</h3>'+
            (row.items||[]).map(item=>'<div class="ticket-item"><strong>'+item.quantity+'×</strong><span>'+esc(item.name)+'</span></div>').join('')+
            '<p class="caption">Paid '+time(row.paid_at)+' · updated '+time(row.updated_at)+'</p>'+buttons+
            '<div class="ticket-links"><a class="text-link" href="/operations/orders/'+row.order_id+'">Execution ↗</a><span>'+esc(human(row.order_status))+'</span></div></article>';
        }).join(''):'<div class="column-empty">No '+label.toLowerCase()+' orders.</div>')+'</section>';
    }).join('');
    const terminal=rows.filter(row=>['REJECTED','CANCELLED'].includes(row.state));
    if(terminal.length)$('#kitchen-board').insertAdjacentHTML('beforeend','<section class="board-column"><div class="column-heading"><h2>Closed without cooking</h2><span>'+terminal.length+'</span></div>'+terminal.map(row=>'<article class="ticket"><span class="eyebrow">ORDER '+short(row.order_id)+'</span><h3>'+human(row.state)+'</h3>'+badge(row.order_status)+'<p class="caption">'+esc(human(row.rejection_reason||row.order_status))+'</p></article>').join('')+'</section>');
    $('#load-kitchen').hidden=!next;
  }
  async function refresh(){
    const [value,fleet]=await Promise.all([paged('/api/kitchen','tickets',pageCount),api('/api/fleet')]);
    rows=value.tickets;next=value.next_after;deliveries=fleet.deliveries;render();
  }
  const reload=poll(refresh,4500);
  $('#kitchen-board').addEventListener('click',async event=>{
    const button=event.target.closest('[data-action]');if(!button)return;
    const row=rows.find(item=>item.order_id===button.dataset.order),kind=button.dataset.action;
    if(kind==='reject'&&!await confirmAction('Reject this order?','This records a human kitchen decision and requests a full simulated refund through the Payment Service.','Reject & request refund'))return;
    try{
      await action('/api/kitchen/'+row.order_id+(kind==='dispatch'?'/dispatch':'/actions'),
        {expected_version:row.order_version,...(kind!=='dispatch'?{action:kind}:{}),...(kind==='reject'?{reason:'capacity'}:{})},button);
      await reload();
    }catch{await reload();}
  });
  $('#load-kitchen').addEventListener('click',async()=>{try{const value=await api('/api/kitchen?after='+next);rows.push(...value.tickets);next=value.next_after;pageCount++;render();}catch(e){toast(e.message,true);}});
}
export async function menuEditor(ctx){
  if(!workspaceRequired(ctx))return;
  let menu=null;const f=$('#restaurant-form'),itemForm=$('#item-form');
  async function refresh(){
    menu=await api('/api/kitchen/menu');
    if(!f.contains(document.activeElement)){f.elements.name.value=menu.name;f.elements.is_open.checked=menu.is_open;f.elements.estimated_prep_time_mins.value=menu.estimated_prep_time_mins;}
    $('#menu-editor-list').innerHTML=menu.items.length?menu.items.map(item=>'<article class="editor-card"><div class="meal-thumb">'+mealArt(item)+'</div><div><h3>'+esc(item.name)+'</h3><p>'+esc(item.description||'No description saved.')+'</p><strong>'+money(item.price_minor)+'</strong> '+badge(item.available?'AVAILABLE':'OFFLINE',item.available?'Available':'Unavailable')+'<div class="editor-buttons"><button class="button small-button" data-item="'+esc(item.item_id)+'" data-action="edit">Edit</button><button class="button small-button" data-item="'+esc(item.item_id)+'" data-action="availability">'+(item.available?'Pause item':'Make available')+'</button><button class="text-button" data-item="'+esc(item.item_id)+'" data-action="delete">Remove</button></div></div></article>').join(''):empty('Your menu starts here.','Add a meal with an integer-paise price. Customer orders keep their original saved prices when menus change.',null,null,'chef');
  }
  f.addEventListener('submit',async event=>{
    event.preventDefault();try{await action('/api/kitchen/menu',{action:'profile',version:menu.version,profile:{name:f.elements.name.value,is_open:f.elements.is_open.checked,estimated_prep_time_mins:Number(f.elements.estimated_prep_time_mins.value)}},$('button',f),'Kitchen settings saved.');await refresh();}catch{await refresh();}
  });
  function edit(item=null){
    itemForm.reset();itemForm.hidden=false;$('#item-form-title').textContent=item?'A little refinement.':'A new favourite.';
    itemForm.elements.item_id.value=item?.item_id||'';itemForm.elements.item_id.readOnly=!!item;
    itemForm.elements.name.value=item?.name||'';itemForm.elements.description.value=item?.description||'';
    itemForm.elements.price.value=item?(item.price_minor/100).toFixed(2):'';itemForm.elements.available.checked=item?.available??true;
    itemForm.scrollIntoView({block:'nearest',behavior:'smooth'});itemForm.elements.name.focus();
  }
  $('#add-menu-item').addEventListener('click',()=>edit());$('#close-item-form').addEventListener('click',()=>itemForm.hidden=true);
  $('#menu-editor-list').addEventListener('click',async event=>{
    const button=event.target.closest('[data-action]');if(!button)return;
    const item=menu.items.find(row=>row.item_id===button.dataset.item),kind=button.dataset.action;
    if(kind==='edit'){edit(item);return;}
    if(kind==='delete'&&!await confirmAction('Remove this menu item?','New orders cannot select it. Saved order snapshots keep their original item and price.','Remove item'))return;
    try{await action('/api/kitchen/menu',{action:kind==='delete'?'delete':'put',version:menu.version,item_id:item.item_id,...(kind==='availability'?{item:{name:item.name,description:item.description,price_minor:item.price_minor,available:!item.available}}:{})},button,'Menu updated.');await refresh();}catch{await refresh();}
  });
  itemForm.addEventListener('submit',async event=>{
    event.preventDefault();const value=itemForm.elements.price.value.trim();
    if(!/^\d+(\.\d{1,2})?$/.test(value)){toast('Use an INR amount with at most two decimal places.',true);return;}
    const [whole,decimal='']=value.split('.'),minor=Number(whole)*100+Number(decimal.padEnd(2,'0'));
    try{await action('/api/kitchen/menu',{action:'put',version:menu.version,item_id:itemForm.elements.item_id.value,item:{name:itemForm.elements.name.value,description:itemForm.elements.description.value,price_minor:minor,available:itemForm.elements.available.checked}},$('button[type=submit]',itemForm),'Menu item saved.');itemForm.hidden=true;await refresh();}catch{await refresh();}
  });
  await refresh();
}
export async function courier(ctx){
  if(!workspaceRequired(ctx))return;
  let latest=null,keepPresence=false,heartbeatRunning=false;
  const fleet=await api('/api/fleet');
  $('#courier-select').innerHTML=fleet.couriers.map(row=>'<option value="'+row.courier_id+'" '+(row.courier_id===ctx.courier_id?'selected':'')+'>'+esc(row.name)+'</option>').join('');
  $('#courier-select').addEventListener('change',async event=>{keepPresence=false;try{await action('/api/context',{courier_id:event.target.value},null,'Courier selected.');location.reload();}catch{}});
  async function refresh(){
    latest=await api('/api/courier');const c=latest.courier,d=latest.delivery,o=latest.order;
    $('#courier-presence').innerHTML='<div class="courier-presence-title"><span class="avatar">'+esc(c.name.split(' ').pop().slice(0,1))+'</span><div><h2>'+esc(c.name)+'</h2>'+badge(c.state)+'</div></div><p class="caption">'+(c.occupied?'This courier has an occupied assignment. Presence loss does not free its capacity.':'Available means online, accepting work, fresh presence, and no occupied assignment.')+'</p><div class="button-row"><button class="button primary" data-presence="online">'+(c.online?'Refresh online presence':'Go online & accept work')+' ↗</button>'+(c.online?'<button class="button" data-presence="pause">Stop accepting new work</button><button class="text-button" data-presence="offline">Go offline</button>':'')+'</div><div class="presence-footer"><label class="checkbox-label"><input id="keep-presence" type="checkbox" '+(keepPresence?'checked':'')+'>Send demo heartbeats while this page is open</label><span class="caption">Last seen '+time(c.last_seen_at)+'</span></div>'+(c.online&&!c.presence_fresh?'<div class="notice">Presence has expired. Refresh it before confirming a physical handoff.</div>':'');
    $('#keep-presence').addEventListener('change',event=>{keepPresence=event.target.checked;});
    if(!d||!o){$('#courier-assignment').innerHTML=empty('A little breathing room.','No assignment is currently held. An eligible online courier can be assigned after the kitchen explicitly requests delivery.',null,null,'bike');return;}
    const address=o.delivery_snapshot;
    const pending=(d.commands||[]).some(cmd=>cmd.state==='PENDING');
    $('#courier-assignment').innerHTML='<div class="section-heading"><div><span class="eyebrow">ORDER '+short(d.order_id)+'</span><h2>Your current delivery.</h2></div>'+badge(d.state)+'</div><div class="assignment-stops"><div class="assignment-stop"><span>1</span><div><strong>Pickup · '+esc(o.restaurant_snapshot.name)+'</strong><p>'+o.items.map(item=>item.quantity+' × '+esc(item.name)).join('<br>')+'</p></div></div><div class="assignment-stop"><span>2</span><div><strong>Drop-off · '+esc(address.label||'Saved address')+'</strong><p>'+esc(address.line1)+'<br>'+esc(address.city)+' '+esc(address.postal_code)+'</p></div></div></div><p class="caption">Assignment '+short(d.assignment.assignment_id)+' · confirmed '+date(d.assigned_at)+'</p>'+
      (pending?'<div class="notice">Your '+esc(human(d.commands.find(cmd=>cmd.state==='PENDING').action))+' request is queued. Waiting for confirmation.</div>':'<button class="button primary wide" id="handoff-action" '+(!c.online||!c.presence_fresh?'disabled':'')+' data-action="'+(d.state==='ASSIGNED'?'pickup':'deliver')+'">'+(d.state==='ASSIGNED'?'Confirm food pickup':'Confirm delivery')+' ✓</button>')+
      '<p class="fineprint">Only the exact assigned courier and assignment may confirm these physical steps.</p>';
  }
  const reload=poll(refresh,4000);
  $('#courier-presence').addEventListener('click',async event=>{
    const button=event.target.closest('[data-presence]');if(!button||!latest)return;
    const c=latest.courier,kind=button.dataset.presence;
    try{await action('/api/couriers/'+c.courier_id+'/presence',{expected_version:c.version,online:kind!=='offline',accepting_work:kind==='online'},button,'Courier presence confirmed.');keepPresence=kind!=='offline';await reload();}catch{await reload();}
  });
  const timer=setInterval(async()=>{
    if(!keepPresence||!latest?.courier.online||heartbeatRunning)return;
    heartbeatRunning=true;
    try{await action('/api/couriers/'+latest.courier.courier_id+'/heartbeat',{},null,null);}catch{}
    finally{heartbeatRunning=false;}
  },9000);
  window.addEventListener('beforeunload',()=>clearInterval(timer));
  $('#courier-assignment').addEventListener('click',async event=>{
    const button=event.target.closest('#handoff-action');if(!button||!latest?.delivery)return;
    const d=latest.delivery;
    try{await action('/api/courier/actions',{action:button.dataset.action,order_id:d.order_id,expected_version:d.order_version,assignment_id:d.assignment.assignment_id},button);await reload();}catch{await reload();}
  });
}
export async function fleet(ctx){
  if(!workspaceRequired(ctx))return;
  let latest,selected=null;
  const colors={AVAILABLE:'#b4c59f',ASSIGNED:'#dfc39c',DELIVERING:'#b1c9d2',OFFLINE:'#d8d2c5',RECONCILIATION_REQUIRED:'#dbb09b'};
  function render(){
    const c=latest.capacity;$('#fleet-as-of').textContent='Observed '+time(c.as_of)+' · '+c.total+' couriers';
    $('#capacity-chart').innerHTML='<div class="capacity-bar" role="group" aria-label="Select a courier state">'+Object.entries(c.counts).filter(([,n])=>n).map(([state,count])=>'<button data-state="'+state+'" aria-label="'+esc(human(state))+': '+count+' couriers"><svg width="100%" height="32" viewBox="0 0 100 32" preserveAspectRatio="none"><rect width="100" height="32" fill="'+colors[state]+'"/></svg></button>').join('')+'</div>';
    const segments=$$('#capacity-chart [data-state]');segments.forEach(button=>button.style.flex=String(c.counts[button.dataset.state]));
    $('#capacity-counts').innerHTML=Object.entries(c.counts).map(([state,count])=>'<button data-state="'+state+'" class="'+(selected===state?'selected':'')+'" aria-pressed="'+(selected===state)+'"><span>'+esc(human(state))+'</span><strong>'+count+'</strong><span>couriers</span></button>').join('');
    const rows=latest.couriers.filter(row=>!selected||row.state===selected);
    $('#fleet-list').innerHTML=rows.length?rows.map(row=>'<article class="courier-card"><span class="avatar">'+esc(row.name.split(' ').pop().slice(0,1))+'</span><h3>'+esc(row.name)+'</h3>'+badge(row.state)+'<p>'+(row.occupied?'Occupied · order '+short(row.active_order_id):'No occupied assignment')+'</p><p>Presence '+(row.presence_fresh?'fresh':'stale or offline')+' · last seen '+time(row.last_seen_at)+'</p><button class="button small-button" data-courier="'+row.courier_id+'">Open this shift ↗</button></article>').join(''):empty('No couriers in this state.','Select another capacity segment to see the rest of the fleet.',null,null,'bike');
    $('#fleet-deliveries').innerHTML=latest.deliveries.length?'<table><thead><tr><th>Order</th><th>State</th><th>Dispatch wait</th><th>Courier</th><th></th></tr></thead><tbody>'+latest.deliveries.map(row=>'<tr><td class="order-code">'+short(row.order_id)+'</td><td>'+badge(row.state)+'</td><td>'+Math.round(row.dispatch_wait_seconds)+' sec</td><td>'+esc(row.assignment?short(row.assignment.courier_id):'Waiting for capacity')+'</td><td><a class="text-link" href="/operations/orders/'+row.order_id+'">Execution ↗</a></td></tr>').join('')+'</tbody></table>':empty('No dispatch requests yet.','Mark food ready and explicitly request a courier from the kitchen board.','/kitchen','Open kitchen','route');
  }
  const reload=poll(async()=>{latest=await api('/api/fleet');render();});
  for(const id of ['#capacity-chart','#capacity-counts'])$(id).addEventListener('click',event=>{const b=event.target.closest('[data-state]');if(!b)return;selected=selected===b.dataset.state?null:b.dataset.state;render();});
  $('#fleet-list').addEventListener('click',async event=>{const b=event.target.closest('[data-courier]');if(!b)return;try{await action('/api/context',{courier_id:b.dataset.courier},b,null);location.href='/courier';}catch{}});
}
export async function services(){
  const labels={customer:['Customers','bag'],restaurant:['Restaurants','chef'],order:['Orders','bag'],payment:['Payments','card'],kitchen:['Kitchen','chef'],delivery:['Delivery','bike'],notification:['Notifications','bell'],task:['Scheduler','layers']};
  poll(async()=>{
    const value=await api('/api/system');
    $('#service-grid').innerHTML=value.services.map(row=>'<article class="service-card">'+icon(labels[row.name][1])+'<h3>'+labels[row.name][0]+'</h3>'+badge(row.status==='healthy'?'AVAILABLE':'FAILED',human(row.status))+'<p>'+(row.zookeeper_connected===true?'ZooKeeper connection confirmed':row.name==='restaurant'?'HTTP → gRPC → Firestore emulator':row.name==='customer'?'Profiles and saved addresses':'Local service readiness')+'</p></article>').join('');
    const s=value.scheduler;
    $('#scheduler-status').innerHTML='<div class="status-summary">'+(s.leader_available?'Leadership confirmed.':'Leadership unavailable.')+'</div>'+badge(s.zookeeper_connected?'AVAILABLE':'FAILED',s.zookeeper_connected?'ZooKeeper connected':'Coordination unavailable')+
      (s.leader?'<dl class="detail-grid"><div><dt>Leader</dt><dd>'+esc(s.leader.name)+'</dd></div><div><dt>Epoch</dt><dd>'+s.leader.epoch+'</dd></div><div><dt>Heartbeat</dt><dd>'+time(s.leader.heartbeat_at)+'</dd></div><div><dt>Instance</dt><dd>'+short(s.leader.instance_id)+'</dd></div></dl>':'<p class="caption">No live elected leader was confirmed.</p>')+
      '<p class="fineprint">Worker leases coordinate backend execution. They do not represent physical courier capacity.</p>';
    $('#worker-list').innerHTML=value.workers.length?value.workers.map(row=>'<div class="worker-row"><div><strong>'+esc(row.name)+'</strong><small>'+esc(row.capabilities.map(human).join(' · '))+'</small></div><span class="order-code">'+short(row.worker_id)+'</span></div>').join(''):'<p class="caption">No connected workers were confirmed.</p>';
  },6000);
}
