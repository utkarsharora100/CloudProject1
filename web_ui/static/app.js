import {stream} from './stream.js';
import {api,context,initShell,toast,disconnected,markUpdated} from './core.js';
import {discover,menu,checkout,orders,tracking,account,notifications} from './customer.js';
import {kitchen,menuEditor,courier,fleet,services} from './staff.js';
import {operations} from './charts.js';
import {execution} from './graph.js';

const pages={stream,discover,menu,checkout,orders,tracking,account,notifications,kitchen,'menu-editor':menuEditor,courier,fleet,services,operations,execution};
async function main(){
  try{
    const ctx=await context();
    await initShell(ctx);
    const page=pages[document.body.dataset.page];
    if(page){await page(ctx);if(['discover','menu','checkout','account','menu-editor'].includes(document.body.dataset.page)&&!document.querySelector('.workspace-onboarding'))markUpdated();}
  }catch(error){disconnected(error);toast(error.message,true);}
}
main();
