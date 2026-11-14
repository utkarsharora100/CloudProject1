const csrf=document.querySelector('meta[name="csrf-token"]').content;
export const $=(selector,root=document)=>root.querySelector(selector);
export const $$=(selector,root=document)=>[...root.querySelectorAll(selector)];
export const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export const icon=name=>'<svg aria-hidden="true"><use href="/static/icons.svg#'+esc(name)+'"></use></svg>';
export const money=value=>new Intl.NumberFormat('en-IN',{style:'currency',currency:'INR',maximumFractionDigits:2}).format((value||0)/100);
export const short=value=>value?String(value).slice(0,8).toUpperCase():'—';
export const human=value=>String(value??'Unknown').toLowerCase().replace(/_/g,' ').replace(/^./,c=>c.toUpperCase());
export const date=value=>value?new Intl.DateTimeFormat('en',{dateStyle:'medium',timeStyle:'short'}).format(new Date(value)):'Not yet';
export const time=value=>value?new Intl.DateTimeFormat('en',{hour:'2-digit',minute:'2-digit',second:'2-digit'}).format(new Date(value)):'—';
export const duration=value=>value==null?'Not completed':value<60?Math.round(value)+' sec':Math.floor(value/60)+' min '+Math.round(value%60)+' sec';
export const stateClass=value=>['DELIVERED','SUCCEEDED','SENT','PAID','CAPTURED','READY','AVAILABLE','ACCEPTED','KITCHEN_ACCEPTED'].includes(value)?'success':['FAILED','PAYMENT_FAILED','DECLINED','CANCELLED','REFUNDED','REJECTED','RECONCILIATION_REQUIRED'].includes(value)?'bad':['RUNNING','PREPARING','PICKED_UP','DELIVERING','ASSIGNED','COURIER_ASSIGNED','SENDING'].includes(value)?'busy':['PENDING','PENDING_PAYMENT','WAITING','BLOCKED','RETRY_WAIT','REFUND_PENDING'].includes(value)?'wait':'neutral';
export const badge=(value,label)=>'<span class="badge '+stateClass(value)+'">'+esc(label||human(value))+'</span>';
export const empty=(title,copy,href,label,kind='bag')=>'<div class="empty-state">'+icon(kind)+'<h3>'+esc(title)+'</h3><p>'+esc(copy)+'</p>'+(href?'<a class="button primary" href="'+esc(href)+'">'+esc(label)+' ↗</a>':'')+'</div>';
export const food=item=>{const text=(item?.item_id||item?.name||'').toLowerCase();return text.includes('tea')||text.includes('drink')?'drink':text.includes('pasta')?'pasta':text.includes('bread')||text.includes('pizza')?'flatbread':'bowl';};
export const mealArt=item=>'<svg aria-hidden="true"><use href="/static/food.svg#'+food(item)+'"></use></svg>';
export class ApiError extends Error{constructor(message,status,uncertain=false){super(message);this.status=status;this.uncertain=uncertain;}}
let busy=0,shellScope=null;
const pending=new Map();
export async function api(path,{method='GET',body}={}){
  let response;
  try{response=await fetch(path,{method,credentials:'same-origin',headers:{'Accept':'application/json',...(body?{'Content-Type':'application/json','X-CSRF-Token':csrf}:{})},body:body?JSON.stringify(body):undefined,cache:'no-store'});}
  catch{throw new ApiError('The connection was interrupted. The last confirmed information is kept.',0,true);}
  let result;
  try{result=response.status===204?{}:await response.json();}catch{throw new ApiError('The response could not be confirmed. Refresh before trying again.',503,true);}
  if(!response.ok)throw new ApiError(result.error||'This request could not be confirmed.',response.status,!!result.uncertain);
  return result;
}
export function toast(message,error=false){
  const row=document.createElement('div');row.className='toast'+(error?' error':'');row.textContent=message;
  $('#toast-region').append(row);setTimeout(()=>row.remove(),error?9000:4500);
}
export async function action(path,payload={},button=null,message='Request saved. Waiting for confirmed progress.'){
  const semantic=path+':'+JSON.stringify([payload.action||'',payload.item_id??null,payload.address_id??null]);
  const old=pending.get(semantic);
  const body=old||{...payload,operation_id:payload.operation_id||crypto.randomUUID()};
  pending.set(semantic,body);busy++;
  const original=button?.innerHTML;
  if(button){button.disabled=true;button.textContent='Saving…';}
  try{
    const result=await api(path,{method:'POST',body});
    pending.delete(semantic);if(message)toast(message);return result;
  }catch(error){
    if(!error.uncertain)pending.delete(semantic);
    toast(error.message,true);throw error;
  }finally{busy--;if(button?.isConnected){button.disabled=false;button.innerHTML=original;}}
}
export function markUpdated(value){
  $('#updated-at').textContent='Updated '+time(value||new Date().toISOString());
  $('#connection-banner').hidden=true;
}
export function disconnected(error){
  const box=$('#connection-banner');box.hidden=false;
  box.textContent=error.status===409?error.message+' Use the workspace button to get started.':'Live updates paused. '+error.message+' Retrying; the last confirmed view stays visible.';
}
export function poll(callback,interval=5000){
  let stopped=false,working=false,timer;
  async function refresh(force=false){
    if(stopped||working)return;
    if(!force&&(busy||document.hidden)){clearTimeout(timer);timer=setTimeout(()=>refresh(),interval);return;}
    working=true;
    const focused=document.activeElement;
    const focusKey=focused?.matches('a,button,input,select,summary,[role=button]')?JSON.stringify([focused.tagName,focused.id,focused.dataset,focused.getAttribute('aria-label'),focused.getAttribute('href'),focused.textContent?.trim()]):null;
    try{
      if(shellScope){const fresh=await context();const role=document.body.dataset.role;
        if(fresh.workspace?.id!==shellScope.workspace?.id||(role==='kitchen'&&fresh.kitchen_restaurant_id!==shellScope.kitchen_restaurant_id)||(role==='courier'&&fresh.courier_id!==shellScope.courier_id)){location.reload();return;}}
      await callback();markUpdated();
      if(focusKey&&!focused.isConnected&&document.activeElement===document.body){
        const candidate=$$('a,button,input,select,summary,[role=button]').find(el=>JSON.stringify([el.tagName,el.id,el.dataset,el.getAttribute('aria-label'),el.getAttribute('href'),el.textContent?.trim()])===focusKey);
        candidate?.focus({preventScroll:true});
      }
    }catch(error){disconnected(error);}
    finally{working=false;if(!stopped){clearTimeout(timer);timer=setTimeout(()=>refresh(),interval);}}
  }
  document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
  window.addEventListener('beforeunload',()=>{stopped=true;clearTimeout(timer);});
  refresh(true);return ()=>refresh(true);
}
export async function confirmAction(title,copy,label='Confirm'){
  const dialog=$('#confirm-dialog');$('#confirm-title').textContent=title;$('#confirm-copy').textContent=copy;$('#confirm-submit').textContent=label;
  dialog.showModal();return new Promise(resolve=>{
    const finish=value=>{dialog.close();$('#confirm-submit').onclick=null;$('#confirm-cancel').onclick=null;dialog.oncancel=null;resolve(value);};
    $('#confirm-submit').onclick=()=>finish(true);$('#confirm-cancel').onclick=()=>finish(false);dialog.oncancel=()=>finish(false);
    $('[data-close-dialog]',dialog).onclick=()=>finish(false);
  });
}
export async function context(){return api('/api/context');}
export function workspaceRequired(ctx,target='#main'){
  if(ctx.workspace?.status==='READY')return true;
  const root=$(target);
  [...root.children].forEach(child=>{if(!child.classList.contains('page-heading')&&child.id!=='connection-banner')child.hidden=true;});
  root.insertAdjacentHTML('beforeend','<section class="empty-state workspace-onboarding">'+icon('bag')+'<span class="eyebrow">A SMALL START, A REAL JOURNEY</span><h3>Your workspace is waiting.</h3><p>Create a fictional diner, a kitchen, and two offline couriers. Then take a real local order from menu to doorstep.</p><button class="button primary" data-open-workspace>Choose a demo workspace ↗</button></section>');
  return false;
}
export async function initShell(ctx){
  if(new URLSearchParams(location.search).get('recipient')==='courier'&&document.body.dataset.page==='notifications'){
    document.body.dataset.recipient='courier';
    $('.page-heading h1').textContent='Your courier notes.';
  }
  $('#workspace-name').textContent=ctx.workspace?ctx.workspace.name:'Choose a workspace';
  shellScope=ctx;
  const mobile=matchMedia('(max-width:760px)'),sidebar=$('#sidebar'),toggle=$('#menu-toggle');
  function navigation(open=false,focus=false){
    open=mobile.matches&&open;sidebar.classList.toggle('open',open);sidebar.inert=mobile.matches&&!open;
    $('#navigation-scrim').hidden=!open;toggle.setAttribute('aria-expanded',String(open));
    document.body.classList.toggle('navigation-open',open);
    if(focus)(open?$('#navigation-close'):toggle).focus();
  }
  navigation();mobile.addEventListener('change',()=>navigation());
  toggle.addEventListener('click',()=>navigation(!sidebar.classList.contains('open'),true));
  $('#navigation-close').addEventListener('click',()=>navigation(false,true));
  $('#navigation-scrim').addEventListener('click',()=>navigation(false,true));
  document.addEventListener('keydown',event=>{
    if(!sidebar.classList.contains('open'))return;
    if(event.key==='Escape'){navigation(false,true);return;}
    if(event.key==='Tab'){
      const controls=$$('a,button',sidebar).filter(el=>!el.disabled),first=controls[0],last=controls.at(-1);
      if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus();}
      else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus();}
    }
  });
  $$('[data-close-dialog]').forEach(button=>button.addEventListener('click',()=>button.closest('dialog').close()));
  document.addEventListener('click',async event=>{
    if(!event.target.closest('[data-open-workspace]'))return;
    const dialog=$('#workspace-dialog');dialog.showModal();
    try{
      const result=await api('/api/workspaces');
      $('#workspace-list').innerHTML=result.workspaces.length?result.workspaces.map(row=>'<button class="workspace-choice" data-workspace="'+esc(row.id)+'" '+(row.status!=='READY'?'disabled':'')+'><span>'+esc(row.name)+'<small>'+esc(date(row.created_at))+'</small></span>'+badge(row.status==='READY'?'READY':'FAILED',row.status==='READY'?'Use workspace':'Incomplete')+'</button>').join(''):'<p class="caption">No demo workspaces yet. Start with a fresh one below.</p>';
    }catch(error){toast(error.message,true);}
  });
  $('#workspace-list').addEventListener('click',async event=>{
    const button=event.target.closest('[data-workspace]');if(!button)return;
    try{await action('/api/context',{workspace_id:button.dataset.workspace},button,'Workspace selected.');location.reload();}catch{}
  });
  $('#create-workspace').addEventListener('click',async event=>{
    try{await action('/api/workspaces',{},event.currentTarget,'Your demo workspace is ready.');location.reload();}catch{}
  });
  if(ctx.workspace){
    try{
      const result=await api('/api/notifications'+(document.body.dataset.role==='courier'?'?recipient=courier':''));
      updateUnread(result.recipient_unread_count||0);
    }catch{}
  }
}

export function updateUnread(count){
  $('#side-unread')?.replaceChildren(document.createTextNode(count?'('+count+')':''));
  $('#notification-dot').textContent=count?String(count):'';
}

export const utcDate=value=>value?new Intl.DateTimeFormat('en',{dateStyle:'medium',timeStyle:'short',timeZone:'UTC'}).format(new Date(value)):'Not yet';

export async function paged(path,key,count=1,cursorName='next_after'){
  let rows=[],next=null,result;
  for(let page=0;page<count;page++){
    result=await api(path+(next?(path.includes('?')?'&':'?')+'after='+encodeURIComponent(next):''));
    rows.push(...result[key]);next=result[cursorName];if(!next)break;
  }
  return {...result,[key]:rows,[cursorName]:next};
}
