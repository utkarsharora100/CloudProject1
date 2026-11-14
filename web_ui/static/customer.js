import {$,$$,api,action,esc,icon,money,short,human,date,time,badge,empty,mealArt,context,poll,toast,workspaceRequired,confirmAction,updateUnread,paged} from './core.js';

export async function discover(ctx){
  let rows=[],next=null,openOnly=false,featured=null;
  const search=$('#restaurant-search');
  function render(){
    const text=search.value.toLowerCase();
    const visible=rows.filter(row=>(!openOnly||row.is_open)&&row.name.toLowerCase().includes(text));
    $('#restaurant-grid').innerHTML=visible.length?visible.map(row=>'<a class="restaurant-card" href="/restaurants/'+encodeURIComponent(row.restaurant_id)+'"><div class="restaurant-art">'+mealArt(row)+'</div><div class="restaurant-body"><h3>'+esc(row.name)+'</h3><p>'+esc(row.description||'A local demo kitchen. Browse its live menu and choose something good.')+'</p><div class="card-bottom">'+badge(row.is_open?'AVAILABLE':'OFFLINE',row.is_open?'Open for orders':'Currently closed')+'<span>'+row.estimated_prep_time_mins+' min prep estimate ↗</span></div></div></a>').join(''):empty('No loaded kitchens match.','Try another name, turn off the open-only filter, or load more kitchens below.',null,null,'chef');
    $('#load-restaurants').hidden=!next;
    if(featured)$('#featured-restaurant').innerHTML='<div class="featured-strip">'+icon('chef')+'<div><span class="eyebrow">YOUR DEMO KITCHEN</span><p>'+esc(featured.name)+' · '+(featured.is_open?'open for your next order':'currently closed')+'</p></div><a class="button" href="/restaurants/'+encodeURIComponent(featured.restaurant_id)+'">Explore the menu ↗</a></div>';
  }
  async function load(after=null){const result=await api('/api/restaurants'+(after?'?after='+encodeURIComponent(after):''));rows.push(...result.restaurants);next=result.next_page_token;render();}
  if(ctx.workspace){try{const value=await api('/api/restaurants/'+ctx.workspace.restaurant_id+'/menu');featured=value;}catch{}}
  search.addEventListener('input',render);
  $('#open-filter').addEventListener('click',event=>{openOnly=!openOnly;event.currentTarget.setAttribute('aria-pressed',openOnly);render();});
  $('#load-restaurants').addEventListener('click',()=>load(next).catch(e=>toast(e.message,true)));
  await load();
}
function cartMarkup(cart,checkout=false){
  if(!cart.items.length)return '<div class="cart-empty">'+icon('bag')+'<h3>Room for something good.</h3><p>Add a favourite from the menu to get started.</p></div>';
  return cart.items.map(item=>'<div class="cart-line"><div>'+esc(item.name)+'<small>'+item.quantity+' × '+money(item.price_minor)+(item.available?'':' · currently unavailable')+'</small></div><strong>'+money(item.line_total_minor)+'</strong></div>').join('')+
    '<div class="total-row"><span>Estimated total</span><strong>'+money(cart.preview_total_minor)+'</strong></div><p class="fineprint">No added fees in this simulation. Final prices are confirmed when your order is saved.</p>'+
    (!checkout?'<a class="button primary wide" href="/checkout">Review your order ↗</a><button class="text-button" id="clear-cart">Clear cart</button>':'');
}
export async function menu(ctx){
  const rid=document.body.dataset.record;
  let data,cart;
  async function refresh(){
    [data,cart]=await Promise.all([api('/api/restaurants/'+rid+'/menu'),api('/api/cart')]);
    $('#menu-heading').innerHTML='<span class="eyebrow">FRESH FROM THE KITCHEN</span><h1>'+esc(data.name)+'.</h1><div class="menu-metadata">'+badge(data.is_open?'AVAILABLE':'OFFLINE',data.is_open?'Open for orders':'Currently closed')+'<span>'+data.estimated_prep_time_mins+' min preparation estimate</span><span>INR · illustrated dishes</span></div>';
    $('#menu-items').innerHTML=data.items.length?data.items.map(item=>{
      const quantity=cart.restaurant?.restaurant_id===rid?cart.items.find(row=>row.item_id===item.item_id)?.quantity||0:0;
      return '<article class="meal-card"><div class="meal-thumb">'+mealArt(item)+'</div><div class="meal-content"><h3>'+esc(item.name)+'</h3><p>'+esc(item.description||'Prepared fresh by this kitchen.')+'</p><div class="meal-actions"><span class="meal-price">'+money(item.price_minor)+'</span>'+
        (!item.available||!data.is_open?badge('OFFLINE','Unavailable'):quantity?'<div class="quantity"><button data-item="'+esc(item.item_id)+'" data-quantity="'+(quantity-1)+'" aria-label="Remove one '+esc(item.name)+'">−</button><span>'+quantity+'</span><button data-item="'+esc(item.item_id)+'" data-quantity="'+(quantity+1)+'" '+(quantity>=20?'disabled':'')+' aria-label="Add one '+esc(item.name)+'">+</button></div>':'<button class="button small-button" data-item="'+esc(item.item_id)+'" data-quantity="1">Add +</button>')+'</div></div></article>';
    }).join(''):empty('The menu is taking shape.','This kitchen has no menu items yet.','/kitchen/menu','Manage the menu','chef');
    $('#cart-summary').innerHTML=(cart.restaurant&&cart.restaurant.restaurant_id!==rid?'<div class="notice">Your cart is from '+esc(cart.restaurant.name)+'. <a class="text-link" href="/restaurants/'+cart.restaurant.restaurant_id+'">Return to that menu</a> or clear the cart to change kitchens.</div>':'')+cartMarkup(cart);
  }
  $('#menu-items').addEventListener('click',async event=>{
    const button=event.target.closest('[data-item]');if(!button)return;
    if(!ctx.workspace){toast('Create or choose a demo workspace before adding a meal.',true);$('[data-open-workspace]').click();return;}
    try{await action('/api/cart',{restaurant_id:rid,item_id:button.dataset.item,quantity:Number(button.dataset.quantity)},button,'Cart updated.');await refresh();}catch{}
  });
  $('#cart-summary').addEventListener('click',async event=>{const button=event.target.closest('#clear-cart');if(!button)return;if(!await confirmAction('Clear this cart?','This removes the selected meals from your cart. Saved orders are kept.','Clear cart'))return;try{await action('/api/cart/clear',{},button,'Cart cleared.');await refresh();}catch{}});
  await refresh();
}
export async function checkout(ctx){
  if(!workspaceRequired(ctx)){$('#place-order').disabled=true;return;}
  let cart,profile;
  [cart,profile]=await Promise.all([api('/api/cart'),api('/api/profile')]);
  $('#checkout-summary').innerHTML=cartMarkup(cart,true);
  $('#back-to-menu').href=cart.restaurant?'/restaurants/'+cart.restaurant.restaurant_id:'/';
  $('#checkout-addresses').innerHTML=profile.addresses.length?profile.addresses.map(row=>'<label class="address-radio"><input type="radio" name="address" value="'+row.id+'" '+(row.is_default?'checked':'')+'><span><strong>'+esc(row.label)+'</strong><p>'+esc(row.line1)+'<br>'+esc(row.city)+' '+esc(row.postal_code)+'</p></span></label>').join(''):empty('A doorstep is missing.','Save a fictional delivery address before placing this order.','/account','Add an address','pin');
  const pending=cart.pending_checkout;
  if(pending?.notification_scenario)$('#notification-scenario').value=pending.notification_scenario;
  if(pending){$('#checkout-pending').hidden=false;$('#checkout-pending').textContent='A previous order submission is unconfirmed. This button safely reuses that exact submission; it will not create another order.';$('#place-order').textContent='Resolve the pending order ↗';}
  if(!cart.items.length&&!pending||!profile.addresses.length)$('#place-order').disabled=true;
  $('#place-order').addEventListener('click',async event=>{
    const address=$('input[name=address]:checked');
    if(!address&&!pending){toast('Choose a delivery address first.',true);return;}
    const body={address_id:pending?.body?.address_id||Number(address.value),notification_scenario:pending?.notification_scenario||$('#notification-scenario').value};
    if(pending)body.operation_id=pending.operation_id;
    try{
      const result=await action('/api/checkout',body,event.currentTarget,'Order saved. You can now choose a simulated payment.');
      location.href='/orders/'+result.order.order_id+(result.notification_subscription==='UNCONFIRMED'?'?subscription=pending':'');
    }catch(error){$('#checkout-error').innerHTML='<div class="error-message">'+esc(error.message)+'</div>';}
  });
}
export async function orders(ctx){
  if(!workspaceRequired(ctx))return;
  let rows=[],next=null,filter='all',pageCount=1;
  const terminal=new Set(['DELIVERED','CANCELLED','REFUNDED']);
  function render(){
    const visible=rows.filter(row=>filter==='all'||(filter==='complete'?terminal.has(row.status):!terminal.has(row.status)));
    $('#order-list').innerHTML=visible.length?visible.map(row=>'<article class="order-card"><div><span class="eyebrow">ORDER '+short(row.order_id)+'</span><h3>'+human(row.status)+'</h3><p>'+date(row.created_at)+'</p></div><div><div class="order-price">'+money(row.total_minor)+'</div>'+badge(row.status)+'</div><div><span class="caption">Latest confirmed progress</span><a class="button" href="/orders/'+row.order_id+'">View your order ↗</a></div></article>').join(''):empty('Your next meal starts here.','No orders match this view yet. Choose a kitchen and begin a real local order.','/','Discover kitchens');
    $('#load-orders').hidden=!next;
  }
  async function refresh(){const value=await paged('/api/orders','orders',pageCount);rows=value.orders;next=value.next_after;render();}
  $('#order-tabs').addEventListener('click',event=>{const button=event.target.closest('[data-filter]');if(!button)return;filter=button.dataset.filter;$$('#order-tabs button').forEach(b=>{b.classList.toggle('active',b===button);b.setAttribute('aria-pressed',b===button);});render();});
  $('#load-orders').addEventListener('click',async()=>{try{const value=await api('/api/orders?after='+next);rows.push(...value.orders);next=value.next_after;pageCount++;render();}catch(e){toast(e.message,true);}});
  poll(refresh);
}
const journey=[['PENDING_PAYMENT','Order saved','Your choices are safely recorded.'],['PAID','Payment confirmed','The kitchen can now accept your order.'],['KITCHEN_ACCEPTED','Kitchen accepted','Your order has a place in the kitchen.'],['PREPARING','Freshly preparing','Cooking is in progress.'],['READY','Ready for pickup','The kitchen has confirmed your food is ready.'],['COURIER_ASSIGNED','Courier assigned','A courier has been reserved for this order.'],['PICKED_UP','On the way','Pickup is confirmed.'],['DELIVERED','At your doorstep','Delivery is confirmed.']];
export async function tracking(ctx){
  if(!workspaceRequired(ctx))return;
  const oid=document.body.dataset.record;let latest=null,paymentSignature=null;
  const titles={PENDING_PAYMENT:'A good meal starts here.',PAYMENT_FAILED:'Let’s give that another try.',PAID:'The kitchen is up next.',KITCHEN_ACCEPTED:'Your kitchen is on it.',PREPARING:'A little care takes time.',READY:'Ready for the next step.',COURIER_ASSIGNED:'Your courier is up next.',PICKED_UP:'Good things are on the move.',DELIVERED:'A good meal, delivered.',CANCELLED:'Your order is cancelled.',REFUND_PENDING:'Your refund is in progress.',REFUNDED:'Your refund is confirmed.'};
  async function refresh(){
    latest=await api('/api/orders/'+oid);const o=latest.order;const history=latest.history;
    $('#tracking-ref').textContent='ORDER '+short(oid);$('#tracking-title').textContent=titles[o.status]||human(o.status);
    $('#tracking-subtitle').textContent='Latest confirmed update · '+date(o.updated_at);
    $('#tracking-state').innerHTML=badge(o.status);
    const seen=new Map();for(const row of history)if(!seen.has(row.to_state))seen.set(row.to_state,row.occurred_at);
    const current=Math.max(0,...journey.map((step,i)=>seen.has(step[0])?i:0));
    const waiting=['Waiting for the order to be saved.','Waiting for a confirmed simulated payment.','Waiting for the kitchen to accept.','Waiting for the kitchen to start preparing.','Waiting for the kitchen to confirm readiness.','Waiting for an eligible courier assignment.','Waiting for the assigned courier to confirm pickup.','Waiting for the assigned courier to confirm delivery.'];
    $('#tracking-timeline').innerHTML=journey.map(([state,title,copy],i)=>'<div class="journey-step '+(seen.has(state)?'done':i===current+1?'current':'')+'"><span class="step-mark">'+(seen.has(state)?'✓':String(i+1))+'</span><div><h3>'+title+'</h3><p>'+(seen.has(state)?copy:waiting[i])+'</p></div><span class="step-time">'+(seen.has(state)?time(seen.get(state)):'')+'</span></div>').join('');
    if(['CANCELLED','REFUND_PENDING','REFUNDED','PAYMENT_FAILED'].includes(o.status))$('#tracking-timeline').insertAdjacentHTML('beforeend','<div class="notice">'+esc(titles[o.status])+' · '+esc(date(o.updated_at))+'</div>');
    $('#tracking-kitchen').textContent=o.restaurant_snapshot.name;
    $('#tracking-items').innerHTML=o.items.map(item=>'<div class="cart-line"><span>'+item.quantity+' × '+esc(item.name)+'</span><strong>'+money(item.line_total_minor??item.unit_price_minor*item.quantity)+'</strong></div>').join('');
    $('#tracking-total').innerHTML='<span>Confirmed total</span><strong>'+money(o.total_minor)+'</strong>';
    const address=o.delivery_snapshot;
    $('#tracking-address').innerHTML='<span class="eyebrow">YOUR DOORSTEP</span><p class="caption">'+esc(address.line1||address.address?.line1||'Saved delivery address')+'<br>'+esc(address.city||address.address?.city||'')+'</p>';
    $('#execution-link').href='/operations/orders/'+oid;
    const p=latest.payment?.payment;
    let body='<div class="payment-heading"><div><span class="eyebrow">SIMULATED PAYMENT</span><h2>'+money(o.total_minor)+'</h2></div>'+badge(p?.state||'PENDING',p?human(p.state):'Not requested')+'</div>';
    if(!p&&o.status==='PENDING_PAYMENT'){
      body+='<p class="caption">No money moves. Choose a controlled outcome, then let the payment workers process it.</p><label>Payment scenario<select id="payment-scenario"><option value="success">Success</option><option value="decline_then_success">Decline, then succeed on explicit retry</option><option value="timeout_once">Lose a response, recover safely</option><option value="decline">Always decline</option></select></label><button class="button primary wide" data-pay="pay">Simulate payment ↗</button>';
    }else if(p?.state==='DECLINED')body+='<p class="caption">No successful charge is recorded for this attempt. Kitchen work stays blocked until a retry succeeds.</p><button class="button primary" data-pay="retry">Retry simulated payment ↗</button>';
    else if(p?.state==='PENDING')body+='<p class="caption">Payment is processing. An interrupted response does not mean it was declined. This page waits for the saved outcome.</p>';
    else if(p?.state==='CAPTURED')body+='<p class="caption">Payment was confirmed by the local simulator. No real payment credentials were used.</p>';
    else if(p?.state==='REFUND_PENDING')body+='<p class="caption">The refund is queued. It will be shown as refunded only after the ledger confirms it.</p>';
    if(['PENDING_PAYMENT','PAYMENT_FAILED','PAID','KITCHEN_ACCEPTED'].includes(o.status))body+='<button class="text-button" data-pay="cancel">Cancel this order</button>';
    if(latest.payment?.unavailable)body+='<div class="notice">Payment details are temporarily unavailable. Current order state is retained.</div>';
    const oldScenario=$('#payment-scenario')?.value;
    const signature=JSON.stringify([o.version,p?.state,latest.payment?.unavailable]);
    if(signature!==paymentSignature){$('#payment-panel').innerHTML=body;if(oldScenario&&$('#payment-scenario'))$('#payment-scenario').value=oldScenario;paymentSignature=signature;}
    const notes=latest.notifications?.notifications||[];
    if(latest.notifications?.recipient_unread_count!=null)updateUnread(latest.notifications.recipient_unread_count);
    $('#tracking-notifications').innerHTML=notes.length?notes.slice(-3).reverse().map(row=>'<div class="cart-line"><div>'+esc(row.title)+'<small>'+esc(human(row.state))+' · '+(row.sent_at?time(row.sent_at):'Awaiting confirmation')+'</small></div>'+badge(row.state)+'</div>').join(''):'<p class="caption">No progress notes are confirmed yet.</p>';
    $('#subscription-recovery').innerHTML=!latest.notifications?.notifications?.length?'<button class="button small-button" id="enable-subscription">Enable or confirm progress notes ↗</button>':'';
  }
  const reload=poll(refresh);
  $('#payment-panel').addEventListener('click',async event=>{
    const button=event.target.closest('[data-pay]');if(!button||!latest)return;
    const kind=button.dataset.pay;
    if(kind==='cancel'&&!await confirmAction('Cancel this order?','Before cooking starts, a paid order requests a full simulated refund. Later cancellation is rejected by the backend.','Request cancellation'))return;
    const payload={action:kind,expected_version:latest.order.version};
    if(kind==='pay'){payload.scenario=$('#payment-scenario').value;payload.refund_scenario='success';}
    try{await action('/api/orders/'+oid+'/payment',payload,button);await reload();}catch{}
  });
  $('#subscription-recovery').addEventListener('click',async event=>{
    if(!event.target.closest('#enable-subscription'))return;
    try{await action('/api/orders/'+oid+'/subscription',{},event.target,'Progress-note subscription confirmed.');await reload();}catch{}
  });
}
export async function account(ctx){
  if(!workspaceRequired(ctx))return;
  let data;let editing=false;
  async function refresh(){
    data=await api('/api/profile');const f=$('#profile-form');
    if(!f.contains(document.activeElement)){f.elements.name.value=data.customer.name;f.elements.email.value=data.customer.email;}
    $('#address-list').innerHTML=data.addresses.length?data.addresses.map(row=>'<article class="address-card"><h3>'+esc(row.label)+(row.is_default?badge('READY','Default'):'')+'</h3><p>'+esc(row.line1)+(row.line2?'<br>'+esc(row.line2):'')+'<br>'+esc(row.city)+' '+esc(row.postal_code)+' · '+esc(row.country_code)+'</p><div class="address-actions"><button data-address="'+row.id+'" data-action="edit">Edit</button>'+(!row.is_default?'<button data-address="'+row.id+'" data-action="default">Make default</button>':'')+'<button data-address="'+row.id+'" data-action="delete">Remove</button></div></article>').join(''):empty('No saved doorsteps.','Add a fictional address for your next local order.',null,null,'pin');
  }
  $('#profile-form').addEventListener('submit',async event=>{event.preventDefault();const f=event.currentTarget;try{await action('/api/profile',{name:f.elements.name.value,email:f.elements.email.value},$('button',f),'Profile saved.');await refresh();}catch{}});
  function addressForm(row=null){
    const f=$('#address-form');f.reset();f.hidden=false;
    $('#address-form-title').textContent=row?'A little update.':'A new doorstep.';
    f.elements.address_id.value=row?.id||'';
    ['label','line1','city','postal_code','country_code'].forEach(key=>f.elements[key].value=row?.[key]||(key==='country_code'?'IN':''));
    f.elements.is_default.checked=!!row?.is_default;f.elements.is_default.disabled=!!row?.is_default;
    f.scrollIntoView({behavior:'smooth',block:'nearest'});f.elements.label.focus();
  }
  $('#new-address').addEventListener('click',()=>addressForm());
  $('#close-address-form').addEventListener('click',()=>$('#address-form').hidden=true);
  $('#address-list').addEventListener('click',async event=>{
    const button=event.target.closest('[data-address]');if(!button)return;
    const id=Number(button.dataset.address),kind=button.dataset.action,row=data.addresses.find(value=>value.id===id);
    if(kind==='edit'){addressForm(row);return;}
    if(kind==='delete'&&!await confirmAction('Remove this saved address?','Existing order snapshots keep their original delivery details. The oldest remaining address becomes default when needed.','Remove address'))return;
    try{await action('/api/addresses',{action:kind,address_id:id},button,'Saved addresses updated.');await refresh();}catch{}
  });
  $('#address-form').addEventListener('submit',async event=>{
    event.preventDefault();const f=event.currentTarget,id=Number(f.elements.address_id.value);
    const address=Object.fromEntries(['label','line1','city','postal_code','country_code'].map(key=>[key,f.elements[key].value]));
    if(!id||!f.elements.is_default.disabled)address.is_default=f.elements.is_default.checked;
    try{await action('/api/addresses',{action:id?'edit':'add',...(id?{address_id:id}:{}),address},$('button[type=submit]',f),'Address saved.');f.hidden=true;await refresh();}catch{}
  });
  await refresh();
}
export async function notifications(ctx){
  if(!workspaceRequired(ctx))return;
  const recipient=document.body.dataset.recipient||'customer';let rows=[],next=null,filter='all',pageCount=1;
  function render(){
    const visible=rows.filter(row=>filter==='all'||(filter==='unread'?row.unread:row.state==='FAILED'));
    $('#notification-list').innerHTML=visible.length?visible.map(row=>'<article class="notification-card '+(row.unread?'unread':'')+'"><div class="notification-icon">'+icon(row.recipient_type==='courier'?'bike':'bell')+'</div><div><h3>'+esc(row.title)+'</h3><p>'+esc(row.body)+'</p><div class="notification-meta">'+badge(row.state)+'<span>Order '+short(row.order_id)+'</span><span>'+esc(date(row.sent_at||row.created_at))+'</span><span>'+row.attempt_count+' transport attempts · round '+row.retry_round+'</span>'+(row.read_at?'<span>Read '+time(row.read_at)+'</span>':'')+'</div></div><div class="notification-actions">'+(row.unread?'<button class="button small-button" data-note="'+row.notification_id+'" data-action="read">Mark read</button>':'')+(row.state==='FAILED'?'<button class="button small-button" data-note="'+row.notification_id+'" data-action="retry">Retry safely ↗</button>':'')+'</div></article>').join(''):empty(filter==='unread'?'All caught up.':'Nothing here, just yet.','Progress notes appear after a subscribed order commits a stage. Queued messages are not counted as unread delivery.',null,null,'bell');
    $('#load-notifications').hidden=!next;
  }
  async function refresh(){const value=await paged('/api/notifications?recipient='+recipient,'notifications',pageCount);rows=value.notifications;next=value.next_after;$('#unread-count').textContent=value.recipient_unread_count+' unread';updateUnread(value.recipient_unread_count);render();}
  const reload=poll(refresh);
  $('#inbox-tabs').addEventListener('click',event=>{const button=event.target.closest('[data-filter]');if(!button)return;filter=button.dataset.filter;$$('#inbox-tabs button').forEach(b=>{b.classList.toggle('active',b===button);b.setAttribute('aria-pressed',b===button);});render();});
  $('#notification-list').addEventListener('click',async event=>{
    const button=event.target.closest('[data-note]');if(!button)return;
    const row=rows.find(value=>value.notification_id===button.dataset.note);
    try{await action('/api/notifications/'+row.notification_id+'/actions',{action:button.dataset.action,expected_version:row.version,recipient,...(button.dataset.action==='retry'?{scenario:'success'}:{})},button,button.dataset.action==='read'?'Marked as read.':'A new bounded retry round was requested.');await reload();}catch{}
  });
  $('#load-notifications').addEventListener('click',async()=>{try{const value=await api('/api/notifications?recipient='+recipient+'&after='+next);rows.push(...value.notifications);next=value.next_after;pageCount++;render();}catch(e){toast(e.message,true);}});
}
