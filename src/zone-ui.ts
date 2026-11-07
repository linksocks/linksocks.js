import { ensureSchema, getSessionContext } from "./db";
import type { Env } from "./types";
import { escapeHtml, layout, renderErrorPage } from "./ui";

interface ZonesPageData {
  user?: { id: string; username: string; isAdmin: boolean };
  csrf?: string;
}

const PRIVATE_INTRO = "Zones are private. Only machines you share a zone ID with can join its network. Each member gets its own virtual IP address.";
const PUBLIC_INTRO = "Anyone with a public zone ID can join and reach machines in the network. Check this machine's firewall before joining one.";

const ZONE_STYLE = `
.wrap{padding:0 22px 56px}
.zone-page{min-width:0}
.zone-intro{margin:0 0 22px}
.zone-intro .sub{margin:5px 0 0}
.zone-toolbar{display:flex;align-items:flex-end;gap:12px;flex-wrap:wrap}
.zone-toolbar .field{margin:0}
.zone-search{flex:1 1 220px}
.zone-scope{flex:0 1 160px}
.zone-toolbar .btn{flex:none}
.zone-count{margin:16px 0 10px;font-size:13px;color:var(--muted)}
.zone-list{padding:0;margin:0;list-style:none}
.zone-entry{padding:22px;border-bottom:1px solid var(--line)}
.zone-entry:last-child{border-bottom:0}
.zone-entry-head{display:flex;align-items:flex-start;justify-content:space-between;gap:16px}
.zone-entry h3{font-size:16px;overflow-wrap:anywhere;margin:0}
.zone-entry-title{display:flex;gap:10px;align-items:center;flex-wrap:wrap;min-width:0}
.zone-entry .badge{font-size:11px;flex:none}
.zone-entry-actions{display:flex;align-items:center;gap:8px;flex:none}
.zone-meta{display:grid;grid-template-columns:minmax(130px,1fr) minmax(100px,.65fr) minmax(160px,1.6fr);gap:12px 22px;margin:16px 0}
.zone-meta dt{font-size:12px;color:var(--muted);margin-bottom:4px}
.zone-meta dd{margin:0;overflow-wrap:anywhere;font-variant-numeric:tabular-nums}
.zone-command-label{display:block;font-size:12px;color:var(--muted);margin:0 0 6px}
.zone-command{display:flex;align-items:center;gap:12px;padding:10px 12px;border:1px solid var(--line);border-radius:var(--radius-sm);background:var(--surface-2);min-width:0}
.zone-command pre{flex:1;min-width:0;margin:0;white-space:pre-wrap;overflow-wrap:anywhere;font-family:var(--mono);font-size:12px;line-height:1.65;color:var(--text)}
.zone-command .btn{flex:none}
.zone-command-text{position:relative;display:flex;flex:1;min-width:0}
.zone-command-text pre{flex:1;min-width:0}
.zone-command-text .zone-reveal{font-size:12px}
.zone-mask-host{position:relative}
.zone-blur{filter:blur(6px);user-select:none;-webkit-user-select:none}
.zone-reveal{position:absolute;inset:0;display:inline-flex;align-items:center;justify-content:center;gap:6px;width:100%;padding:2px 10px;font-family:inherit;font-size:11px;line-height:1.4;color:var(--muted);background:var(--surface-2);border:1px dashed var(--line-strong);border-radius:var(--radius-sm);cursor:pointer;text-align:center;white-space:nowrap;overflow:hidden}
.zone-reveal:hover{border-color:var(--accent);color:var(--accent);background:var(--accent-soft)}
.zone-reveal-zone{font-size:12.5px;color:var(--muted);margin:0 0 16px}
.zone-page .btn,.zone-toolbar input,.zone-toolbar select{min-height:40px}
.zone-page :is(a,button,input,select):focus-visible{outline:2px solid var(--accent);outline-offset:3px;box-shadow:none}
.zone-page input::placeholder{color:var(--muted)}
.zone-page .btn:disabled{cursor:not-allowed;color:var(--muted);background:var(--surface-3);border-color:var(--line)}
.zone-page .btn.primary:disabled{color:var(--surface);background:var(--accent-hover);border-color:var(--accent-hover)}
.zone-status{font-size:13px;min-height:20px;margin:12px 0 0;color:var(--muted)}
.zone-status.error,.zone-field-error{color:var(--danger)}
.zone-field-error{font-size:12px;margin:5px 0 0}
.zone-page [hidden]{display:none!important}
.zone-page input[aria-invalid=true]{border-color:var(--danger)}
.zone-page .field .hint{font-size:12px;color:var(--muted)}
.zone-modal-form{display:flex;flex-direction:column;min-height:0;overflow:hidden;margin:0}
.zone-modal-form .modal-body{min-height:0}
.zone-modal-form .modal-actions{padding:16px 22px;margin:0;flex:none;background:var(--surface)}
.zone-page .modal-close{width:40px;height:40px}
.zone-page dialog.modal{max-height:calc(100dvh - 40px)}
.zone-page dialog.modal .modal-body>p:first-child{margin-top:0}
.zone-page .modal-head{align-items:center}
.zone-modal-note{font-size:13px;color:var(--muted);margin:0 0 20px}
.zone-public-warning{color:var(--danger);margin:10px 0 0}
.zone-delete-name{font-weight:600;color:var(--ink);overflow-wrap:anywhere}
.zone-members{margin:0;padding:0;list-style:none}
.zone-member{display:grid;grid-template-columns:minmax(0,1fr) auto auto;gap:6px 16px;padding:14px 0;border-bottom:1px solid var(--line)}
.zone-member:last-child{border-bottom:0}
.zone-member-name{font-weight:550;overflow-wrap:anywhere}
.zone-member-state{font-size:12px;color:var(--muted)}
.zone-member-state.online{color:var(--ok)}
.zone-member-identity{font-family:var(--mono);font-size:11px;color:var(--muted);overflow-wrap:anywhere;grid-column:1/-1}
.zone-member-ip{font-family:var(--mono);font-size:13px;grid-column:2;grid-row:1/3;align-self:center}
.zone-member-action{grid-column:3;grid-row:1/3;align-self:center;justify-self:end}
.zone-member-confirm{grid-column:1/-1;display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-top:8px;padding:10px 12px;border:1px solid var(--line);border-radius:var(--radius-sm);background:var(--surface-2)}
.zone-member-confirm-text{flex:1 1 200px;font-size:12.5px;color:var(--text)}
.zone-member .btn,.zone-member-confirm .btn{min-height:40px}
.zone-rules .btn{min-height:40px}
.zone-members-summary{font-size:13px;color:var(--muted);margin:0 0 10px}
.zone-members-status{margin:0 0 12px}
dialog.modal.zone-settings-dialog{width:min(860px,calc(100vw - 36px))}
.zone-rules-intro{font-size:12.5px;color:var(--muted);margin:0 0 10px}
.zone-rules-wrap{border:1px solid var(--line);border-radius:var(--radius-sm);overflow:auto;background:var(--surface)}
.zone-rules{width:100%;min-width:0;table-layout:fixed;border-collapse:collapse}
.zone-rules th,.zone-rules td{padding:8px 6px;border-bottom:1px solid var(--line);text-align:left;white-space:normal;vertical-align:middle}
.zone-rules thead th{background:var(--surface-2);font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--faint);font-weight:600;padding:9px 6px}
.zone-rules th:first-child,.zone-rules td:first-child{padding-left:10px}
.zone-rules th:last-child,.zone-rules td:last-child{padding-right:10px}
.zone-rules tbody tr:last-child td{border-bottom:0}
.zone-rules .zone-col-index{width:36px}
.zone-rules .zone-col-port{width:94px}
.zone-rules .zone-col-proto{width:88px}
.zone-rules .zone-col-action{width:96px}
.zone-rules .zone-col-actions{width:112px}
.zone-rules input,.zone-rules select{min-height:36px;padding:6px 8px;font-size:12.5px}
.zone-rule-index{color:var(--faint);font-family:var(--mono);text-align:center}
.zone-rules .zone-rule-actions{display:flex;justify-content:flex-end;gap:4px}
.zone-rules-empty{padding:26px 18px}
.zone-rules-tools{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-top:12px}
.zone-rules-tools .hint{margin:0}
.zone-visually-hidden{position:absolute;width:1px;height:1px;margin:-1px;padding:0;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;border:0}
@media(max-width:760px){
  .wrap{padding:0 16px 40px}
  .zone-entry,.zone-page .modal-body{padding:16px}
  .zone-page .modal-head{padding:16px}
  .zone-modal-form .modal-actions{padding:12px 16px}
  .zone-entry-head{flex-wrap:wrap;gap:12px}
  .zone-entry-actions{gap:8px}
  .zone-meta{grid-template-columns:1fr 1fr;gap:12px 16px}
  .zone-meta>div:last-child{grid-column:1/-1}
  .zone-page .btn,.zone-page .modal-close,.zone-toolbar input,.zone-toolbar select{min-height:44px}
  .zone-page .modal-close{width:44px}
  .zone-page input,.zone-page select{font-size:16px}
  .zone-command{align-items:flex-start;flex-wrap:wrap;gap:8px;padding:12px}
  .zone-command-text{flex-basis:100%}
  .zone-scope{flex:1 1 140px}
  .zone-search{flex-basis:100%}
  .zone-toolbar{gap:10px}
  .zone-page dialog.modal{max-height:calc(100dvh - 24px)}
  dialog.modal.zone-settings-dialog{width:calc(100vw - 24px)}
  .zone-member .btn,.zone-member-confirm .btn,.zone-rules .btn{min-height:44px}
  .zone-rules input,.zone-rules select{font-size:12.5px}
}
@media(prefers-reduced-motion:reduce){.zone-page *{transition:none!important}}
`;

const ZONE_SCRIPT = `<script>
(function(){
  var root=document.getElementById("zone-page");
  var userId=root.dataset.userId;
  var isAdmin=root.dataset.admin==="true";
  var csrf=root.dataset.csrf;
  var privateIntro=root.dataset.privateIntro||"";
  var publicIntro=root.dataset.publicIntro||"";
  var list=document.getElementById("zone-list");
  var count=document.getElementById("zone-count");
  var search=document.getElementById("zone-search");
  var scope=document.getElementById("zone-scope");
  var refresh=document.getElementById("zone-refresh");
  var status=document.getElementById("zone-status");
  var zones=[];
  var revealed={};
  var revealTarget=null;
  var settingsZone=null;
  var loadController;
  var memberController;
  var settingsController;
  var deleteTarget;
  var deleteBusy=false;
  var createButton=document.getElementById("zone-create-open");
  var createForm=document.getElementById("zone-create-form");
  var deleteForm=document.getElementById("zone-delete-form");
  var settingsForm=document.getElementById("zone-settings-form");
  var membersDialog=document.getElementById("zone-members-dialog");
  var membersBody=document.getElementById("zone-members-body");
  var membersStatus=document.getElementById("zone-members-status");
  var revealDialog=document.getElementById("zone-reveal-dialog");
  var rulesBody=document.getElementById("zone-rules-rows");

  function element(tag,cls,text){
    var el=document.createElement(tag);
    if(cls)el.className=cls;
    if(text!==undefined)el.textContent=String(text);
    return el;
  }
  function button(label,handler,cls){
    var el=element("button","btn"+(cls?" "+cls:""),label);
    el.type="button";
    el.addEventListener("click",handler);
    return el;
  }
  function message(el,text,error){
    if(!el)return;
    el.textContent=text;
    el.classList.toggle("error",!!error);
    el.setAttribute("role",error?"alert":"status");
  }
  function notice(el,text,error){
    message(el,text,error);
    if(el)el.hidden=!text;
  }
  function openDialog(dialog,opener){
    dialog.returnFocus=opener||document.activeElement;
    dialog.showModal();
  }
  root.querySelectorAll("dialog").forEach(function(dialog){
    dialog.addEventListener("close",function(){
      var target=dialog.returnFocus;
      if(target&&target.isConnected&&!target.disabled)target.focus();
      else refresh.focus();
    });
    dialog.querySelectorAll("[data-zone-close]").forEach(function(btn){
      btn.addEventListener("click",function(){dialog.close();});
    });
  });
  membersDialog.addEventListener("close",function(){
    if(memberController)memberController.abort();
    notice(membersStatus,"");
  });
  var settingsDialog=document.getElementById("zone-settings-dialog");
  if(settingsDialog)settingsDialog.addEventListener("close",function(){if(settingsController)settingsController.abort();});

  async function api(path,options){
    var response;
    try{
      response=await fetch(path,Object.assign({credentials:"same-origin",headers:{Accept:"application/json"}},options));
    }catch(err){
      if(err.name==="AbortError")throw err;
      throw new Error("Could not connect to the server. Check your connection and try again.");
    }
    var data=await response.json().catch(function(){return null;});
    if(!response.ok){
      if(response.status===401)throw new Error("Your session has expired. Sign in again to manage zones.");
      if(response.status===403)throw new Error("This action is not allowed. Refresh the page and try again.");
      throw new Error(data&&typeof data.error==="string"?data.error:"Could not complete the request. Try again.");
    }
    return data;
  }
  function writeOptions(method,body){
    return {method:method,headers:{Accept:"application/json","Content-Type":"application/json","X-CSRF-Token":csrf},body:body?JSON.stringify(body):undefined};
  }
  function shellArg(value){
    value=String(value);
    return /^[A-Za-z0-9_:/.-]+$/.test(value)?value:"'"+value.replace(/'/g,"'\\\"'\\\"'")+"'";
  }
  function joinCommand(zone){
    return "linksocks zone join "+shellArg(zone.id)+" --url "+shellArg(window.location.origin);
  }
  async function copyText(text){
    if(navigator.clipboard&&navigator.clipboard.writeText){
      try{
        await navigator.clipboard.writeText(text);
        return true;
      }catch(err){}
    }
    var area=document.createElement("textarea");
    area.value=text;
    area.setAttribute("readonly","");
    area.setAttribute("aria-hidden","true");
    area.style.position="fixed";
    area.style.top="-1000px";
    area.style.opacity="0";
    document.body.appendChild(area);
    var copied=false;
    try{
      area.select();
      area.setSelectionRange(0,area.value.length);
      copied=document.execCommand("copy");
    }catch(err){copied=false;}
    document.body.removeChild(area);
    return copied;
  }
  async function copyCommand(btn,text,code){
    var copied=await copyText(text);
    if(copied){
      message(status,"Join command copied.");
      if(btn){
        btn.textContent="Copied";
        setTimeout(function(){if(btn.isConnected)btn.textContent="Copy command";},1600);
      }
      return;
    }
    if(code){
      var selection=window.getSelection();
      var range=document.createRange();
      range.selectNodeContents(code);
      selection.removeAllRanges();
      selection.addRange(range);
    }
    message(status,"Select the command and press Ctrl+C.");
  }
  function updateIntro(){
    var managed=!!(scope&&scope.value==="managed");
    var title=document.getElementById("zone-intro-title");
    var sub=document.getElementById("zone-intro-sub");
    if(managed){
      title.textContent="My zones";
      sub.textContent=privateIntro;
    }else{
      title.textContent="Public zones";
      sub.textContent=publicIntro;
    }
  }
  function publicView(){return !(scope&&scope.value==="managed");}
  function openReveal(zone,copyIntent){
    revealTarget={zone:zone,copy:copyIntent};
    var ack=document.getElementById("zone-reveal-ack");
    ack.checked=false;
    var confirm=document.getElementById("zone-reveal-confirm");
    confirm.disabled=true;
    document.getElementById("zone-reveal-zone").textContent=zone.name;
    message(document.getElementById("zone-reveal-status"),"");
    openDialog(revealDialog,null);
  }
  var revealAck=document.getElementById("zone-reveal-ack");
  var revealConfirm=document.getElementById("zone-reveal-confirm");
  revealAck.addEventListener("change",function(){revealConfirm.disabled=!revealAck.checked;});
  revealConfirm.addEventListener("click",function(){
    if(!revealTarget||!revealAck.checked)return;
    var target=revealTarget;
    revealTarget=null;
    revealed[target.zone.id]=true;
    revealDialog.close();
    renderZones();
    if(target.copy){
      var code=root.querySelector('[data-zone-command="'+target.zone.id+'"]');
      var copyButton=root.querySelector('[data-zone-copy="'+target.zone.id+'"]');
      copyCommand(copyButton,code?code.textContent:"",code);
    }
  });
  revealDialog.addEventListener("close",function(){
    revealTarget=null;
    revealAck.checked=false;
    revealConfirm.disabled=true;
  });

  function meta(label,value,mono){
    var group=element("div");
    group.append(element("dt",null,label),element("dd",mono?"mono":null,value));
    return group;
  }
  function maskedOverlay(zone,what){
    var el=element("button","zone-reveal","Click to reveal");
    el.type="button";
    el.setAttribute("aria-label","Reveal "+what+" for "+zone.name);
    el.addEventListener("click",function(){openReveal(zone,false);});
    return el;
  }
  function zoneIdMeta(zone,masked){
    var group=element("div");
    var cell=element("dd","mono"+(masked?" zone-mask-host":""));
    var value=element("span",masked?"zone-blur":null,zone.id);
    if(masked)value.setAttribute("aria-hidden","true");
    cell.append(value);
    if(masked)cell.append(maskedOverlay(zone,"Zone ID"));
    group.append(element("dt",null,"Zone ID"),cell);
    return group;
  }
  function zoneEntry(zone){
    var masked=zone.visibility==="public"&&!revealed[zone.id];
    var row=element("li","zone-entry");
    var head=element("div","zone-entry-head");
    var title=element("div","zone-entry-title");
    title.append(element("h3",null,zone.name));
    if(zone.visibility==="public")title.append(element("span","badge gray","Public"));
    if(zone.owner_id===userId)title.append(element("span","badge gray","Owned by you"));
    var canManage=!!userId&&(isAdmin||zone.owner_id===userId);
    var actions=element("div","zone-entry-actions");
    var members=button("View members",function(){showMembers(zone,members);});
    members.setAttribute("aria-haspopup","dialog");
    actions.append(members);
    if(canManage){
      var settingsButton=button("Settings",function(){openSettings(zone,settingsButton);});
      settingsButton.setAttribute("aria-haspopup","dialog");
      actions.append(settingsButton);
      var remove=button("Delete zone",function(){
        if(deleteBusy)return;
        deleteTarget=zone;
        document.getElementById("zone-delete-name").textContent=zone.name;
        message(document.getElementById("zone-delete-status"),"");
        openDialog(document.getElementById("zone-delete-dialog"),remove);
      },"danger");
      remove.dataset.zoneDelete="";
      remove.disabled=deleteBusy;
      remove.setAttribute("aria-haspopup","dialog");
      actions.append(remove);
    }
    head.append(title,actions);
    var details=element("dl","zone-meta");
    details.append(meta("IPv4 network",zone.cidr,true),meta("Member limit",zone.max_members),zoneIdMeta(zone,masked));
    var command=joinCommand(zone);
    var code=element("pre");
    code.dataset.zoneCommand=zone.id;
    var codeText=element("span",masked?"zone-blur":null,command);
    if(masked)codeText.setAttribute("aria-hidden","true");
    code.append(codeText);
    var commandText=element("div","zone-command-text");
    commandText.append(code);
    if(masked)commandText.append(maskedOverlay(zone,"join command"));
    var copy=button("Copy command",function(){
      if(masked)openReveal(zone,true);
      else copyCommand(copy,command,code);
    });
    copy.dataset.zoneCopy=zone.id;
    copy.setAttribute("aria-label","Copy join command for "+zone.name);
    var commandBox=element("div","zone-command");
    commandBox.append(commandText,copy);
    row.append(head,details,element("span","zone-command-label","Join from this machine"),commandBox);
    return row;
  }
  function renderZones(){
    var query=search.value.trim().toLowerCase();
    var shown=zones.filter(function(zone){return !query||[zone.name,zone.id,zone.cidr].some(function(value){return String(value).toLowerCase().includes(query);});});
    count.textContent=shown.length===zones.length?zones.length+" zone"+(zones.length===1?"":"s"):shown.length+" of "+zones.length+" zones";
    list.replaceChildren();
    if(!shown.length){
      var empty=element("li","empty");
      if(query){
        empty.append(element("strong",null,"No zones match"),element("div","sub","Try a different name, network or zone ID."));
      }else if(publicView()){
        empty.append(element("strong",null,"No public zones yet"),element("div","sub",userId?"Public zones created by administrators will appear here.":"Public zones created by administrators will appear here. Sign in to create your own private zone."));
      }else{
        empty.append(element("strong",null,"No zones yet"),element("div","sub","Create a zone, then run its join command on each machine."));
      }
      list.append(empty);
    }else shown.forEach(function(zone){list.append(zoneEntry(zone));});
  }
  async function loadZones(){
    if(loadController)loadController.abort();
    var controller=new AbortController();
    loadController=controller;
    list.setAttribute("aria-busy","true");
    list.replaceChildren(element("li","empty","Loading zones…"));
    count.textContent="Loading zones…";
    refresh.disabled=true;
    refresh.textContent="Refreshing…";
    try{
      var data=await api(scope&&scope.value==="managed"?"/api/zones":"/api/zones/public",{signal:controller.signal});
      if(!data||!Array.isArray(data.zones))throw new Error("Could not read the zone list. Try again.");
      zones=data.zones;
      renderZones();
    }catch(err){
      if(err.name==="AbortError")return;
      count.textContent="Zones unavailable";
      var empty=element("li","empty");
      var error=element("div","sub",err.message);
      error.setAttribute("role","alert");
      empty.append(element("strong",null,"Could not load zones"),error,button("Try again",loadZones));
      list.replaceChildren(empty);
    }finally{
      if(loadController===controller){list.setAttribute("aria-busy","false");refresh.disabled=false;refresh.textContent="Refresh";}
    }
  }
  search.addEventListener("input",function(){if(list.getAttribute("aria-busy")!=="true"&&count.textContent!=="Zones unavailable")renderZones();});
  if(scope)scope.addEventListener("change",function(){updateIntro();loadZones();});
  refresh.addEventListener("click",loadZones);

  async function showMembers(zone,opener){
    document.getElementById("zone-members-title").textContent="Members of "+zone.name;
    notice(membersStatus,"");
    if(!membersDialog.open)openDialog(membersDialog,opener);
    else membersDialog.querySelector(".modal-close").focus();
    await loadMembers(zone);
  }
  async function loadMembers(zone){
    if(memberController)memberController.abort();
    var controller=new AbortController();
    memberController=controller;
    membersBody.setAttribute("aria-busy","true");
    membersBody.replaceChildren(element("p","zone-members-summary","Loading members…"));
    try{
      var data=await api("/api/zones/"+encodeURIComponent(zone.id)+"/info",{signal:controller.signal});
      if(!data||!Array.isArray(data.members))throw new Error("Could not read the member list. Try again.");
      var canManage=!!userId&&(isAdmin||zone.owner_id===userId);
      var online=data.members.filter(function(member){return member.online;}).length;
      membersBody.replaceChildren(element("p","zone-members-summary",online+" online · "+data.members.length+" assigned · "+zone.max_members+" member limit"));
      if(!data.members.length)membersBody.append(element("p","muted","No machines have joined this zone yet."));
      var membersList=element("ul","zone-members");
      data.members.forEach(function(member){
        var row=element("li","zone-member");
        row.append(element("span","zone-member-name",member.name),element("span","zone-member-state"+(member.online?" online":""),member.online?"Online":"Offline"),element("span","zone-member-ip",member.ip));
        if(canManage){
          var action=element("div","zone-member-action");
          var trigger=button(member.online?"Kick":"Remove",function(){askKick(zone,member,row,trigger);},member.online?"danger small":"small");
          action.append(trigger);
          row.append(action);
        }
        row.append(element("span","zone-member-identity",member.identity));
        membersList.append(row);
      });
      membersBody.append(membersList,element("p","zone-modal-note","Run the join command on a machine to get a virtual IP. Stop it with Ctrl+C to leave."));
    }catch(err){
      if(err.name!=="AbortError"){
        var error=element("p","zone-status error",err.message);
        error.setAttribute("role","alert");
        var retry=button("Try again",function(){loadMembers(zone);});
        membersBody.replaceChildren(error,retry);
      }
    }finally{
      if(memberController===controller)membersBody.setAttribute("aria-busy","false");
    }
  }
  function askKick(zone,member,row,trigger){
    var existing=row.querySelector(".zone-member-confirm");
    if(existing){existing.remove();return;}
    var confirmEl=element("div","zone-member-confirm");
    confirmEl.setAttribute("role","group");
    confirmEl.setAttribute("aria-label",(member.online?"Kick ":"Remove ")+member.name);
    confirmEl.append(element("span","zone-member-confirm-text",member.online?"Kick "+member.name+" offline and release its address?":"Remove "+member.name+" from the zone and release its address?"));
    var confirm=button(member.online?"Kick":"Remove",async function(){
      confirm.disabled=true;
      cancel.disabled=true;
      notice(membersStatus,"Removing "+member.name+"…");
      try{
        await api("/api/zones/"+encodeURIComponent(zone.id)+"/members/"+encodeURIComponent(member.identity)+"/kick",writeOptions("POST"));
        await loadMembers(zone);
        notice(membersStatus,member.online?"Machine disconnected. Its address is available again.":"Machine removed. Its address is available again.");
      }catch(err){
        if(err.message==="Member not found"){
          await loadMembers(zone);
          notice(membersStatus,"This machine is no longer in the zone.");
        }else{
          notice(membersStatus,err.message,true);
          confirmEl.remove();
        }
      }
    },member.online?"danger small":"small");
    var cancel=button("Cancel",function(){confirmEl.remove();trigger.focus();},"small ghost");
    confirmEl.append(confirm,cancel);
    row.append(confirmEl);
    confirm.focus();
  }

  function fieldError(input,text){
    var error=document.getElementById(input.id+"-error");
    if(!error)return !text;
    error.textContent=text;
    error.hidden=!text;
    input.setAttribute("aria-invalid",text?"true":"false");
    return !text;
  }
  function networkSize(value){
    var match=/^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\/(\\d{1,2})$/.exec(value);
    if(!match)return 0;
    var octets=match.slice(1,5).map(Number),prefix=Number(match[5]);
    if(match.slice(1,5).some(function(value){return !/^(0|[1-9]\\d{0,2})$/.test(value);})||octets.some(function(value){return value>255;})||prefix<16||prefix>30)return 0;
    var size=Math.pow(2,32-prefix);
    var address=octets.reduce(function(total,value){return total*256+value;},0);
    var network=Math.floor(address/size)*size,end=network+size-1;
    return [[0x0a000000,0x0affffff],[0xac100000,0xac1fffff],[0xc0a80000,0xc0a8ffff]].some(function(range){return network>=range[0]&&end<=range[1];})?size-2:0;
  }
  function createVisibility(){
    var checked=createForm.querySelector('input[name="visibility"]:checked');
    return checked?checked.value:"private";
  }
  function updateCreateVisibility(){
    var warning=document.getElementById("zone-public-warning");
    if(warning)warning.hidden=createVisibility()!=="public";
  }
  if(createButton){
    createButton.addEventListener("click",function(){openDialog(document.getElementById("zone-create-dialog"),createButton);});
    createForm.querySelectorAll("input").forEach(function(input){input.addEventListener("input",function(){fieldError(input,"");});});
    createForm.querySelectorAll('input[name="visibility"]').forEach(function(input){input.addEventListener("change",updateCreateVisibility);});
    createForm.addEventListener("submit",async function(event){
      event.preventDefault();
      var name=document.getElementById("zone-name");
      var cidr=document.getElementById("zone-cidr");
      var limit=document.getElementById("zone-limit");
      var maxMembers=Number(limit.value);
      var capacity=networkSize(cidr.value.trim());
      fieldError(name,!name.value.trim()?"Enter a name for this zone.":Array.from(name.value.trim()).length>64||/[\\x00-\\x1f\\x7f]/.test(name.value)?"Use 1–64 characters without control characters.":"");
      fieldError(cidr,capacity?"":"Enter a private IPv4 network from /16 to /30, such as 10.88.0.0/24.");
      fieldError(limit,!Number.isInteger(maxMembers)||maxMembers<2||maxMembers>256?"Enter a member limit from 2 to 256.":capacity&&maxMembers>capacity?"This network has room for "+capacity+" members. Reduce the limit or use a larger network.":"");
      var invalid=createForm.querySelector('[aria-invalid="true"]');
      if(invalid){invalid.focus();return;}
      var visibility=createVisibility();
      var payload={name:name.value.trim(),cidr:cidr.value.trim(),max_members:maxMembers};
      if(createForm.querySelector('input[name="visibility"]'))payload.visibility=visibility;
      var submit=createForm.querySelector('[type="submit"]');
      var feedback=document.getElementById("zone-create-status");
      submit.disabled=true;
      submit.textContent="Creating…";
      createForm.setAttribute("aria-busy","true");
      message(feedback,"Creating zone…");
      try{
        await api("/api/zones",writeOptions("POST",payload));
        document.getElementById("zone-create-dialog").close();
        createForm.reset();
        updateCreateVisibility();
        search.value="";
        message(feedback,"");
        message(status,visibility==="public"?"Public zone created. Anyone with its ID can join and reach its members.":"Zone created. Share its ID and join command only with people you trust.");
        await loadZones();
      }catch(err){message(feedback,err.message,true);}
      finally{submit.disabled=false;submit.textContent="Create zone";createForm.setAttribute("aria-busy","false");}
    });
    deleteForm.addEventListener("submit",async function(event){
      event.preventDefault();
      if(!deleteTarget||deleteBusy)return;
      deleteBusy=true;
      var zone=deleteTarget;
      var submit=deleteForm.querySelector('[type="submit"]');
      var feedback=document.getElementById("zone-delete-status");
      submit.disabled=true;
      submit.textContent="Deleting…";
      deleteForm.setAttribute("aria-busy","true");
      root.querySelectorAll("[data-zone-delete]").forEach(function(btn){btn.disabled=true;});
      message(feedback,"Deleting zone…");
      try{
        await api("/api/zones/"+encodeURIComponent(zone.id),writeOptions("DELETE"));
        document.getElementById("zone-delete-dialog").close();
        message(status,"Zone deleted. Its members have been disconnected.");
        await loadZones();
        refresh.focus();
      }catch(err){message(feedback,err.message,true);}
      finally{deleteBusy=false;submit.disabled=false;submit.textContent="Delete zone";deleteForm.setAttribute("aria-busy","false");root.querySelectorAll("[data-zone-delete]").forEach(function(btn){btn.disabled=false;});}
    });
  }

  function portText(port){
    if(port===undefined||port===null||port==="")return "any";
    if(typeof port==="number")return String(port);
    if(typeof port==="object"&&typeof port.from==="number"&&typeof port.to==="number")return port.from+"-"+port.to;
    return String(port);
  }
  function ruleInput(cls,placeholder,value){
    var input=element("input","zone-rule-input "+cls);
    input.type="text";
    input.spellcheck=false;
    input.autocapitalize="off";
    input.autocomplete="off";
    input.placeholder=placeholder;
    input.value=value===undefined||value===null?"":String(value);
    return input;
  }
  function ruleSelect(cls,options,value){
    var select=element("select","zone-rule-input "+cls);
    options.forEach(function(option){
      var el=element("option",null,option[1]);
      el.value=option[0];
      if(option[0]===value)el.selected=true;
      select.append(el);
    });
    return select;
  }
  function ruleAction(symbol,label,handler,danger){
    var el=element("button","btn small "+(danger?"danger":"ghost"),symbol);
    el.type="button";
    el.setAttribute("aria-label",label);
    el.title=label;
    el.addEventListener("click",handler);
    return el;
  }
  function ruleCell(child){
    var cell=element("td");
    cell.append(child);
    return cell;
  }
  function ruleRows(){return Array.prototype.slice.call(rulesBody.children);}
  function ruleRow(rule){
    rule=rule||{};
    var row=element("tr","zone-rule");
    row.append(element("td","zone-rule-index","1"));
    var src=ruleInput("zone-rule-src","any or 10.88.0.0/24",rule.src||"any");
    var srcPort=ruleInput("zone-rule-src-port","any, 443 or 1000-2000",portText(rule.src_port));
    var dst=ruleInput("zone-rule-dst","any or 10.88.0.0/24",rule.dst||"any");
    var dstPort=ruleInput("zone-rule-dst-port","any, 443 or 1000-2000",portText(rule.dst_port));
    var proto=ruleSelect("zone-rule-proto",[["any","any"],["tcp","tcp"],["udp","udp"],["icmp","icmp"],["arp","arp"]],rule.proto||"any");
    var action=ruleSelect("zone-rule-action",[["allow","Allow"],["deny","Deny"]],rule.action||"allow");
    var actionsGroup=element("div","zone-rule-actions");
    var up=ruleAction("\\u2191","Move rule up",function(){moveRule(row,-1);},false);
    up.dataset.ruleMove="up";
    var down=ruleAction("\\u2193","Move rule down",function(){moveRule(row,1);},false);
    down.dataset.ruleMove="down";
    var remove=ruleAction("\\u2715","Remove rule",function(){
      row.remove();
      updateRulesState();
      var add=document.getElementById("zone-rule-add");
      if(add)add.focus();
    },true);
    actionsGroup.append(up,down,remove);
    var actions=element("td");
    actions.append(actionsGroup);
    row.append(ruleCell(src),ruleCell(srcPort),ruleCell(proto),ruleCell(dst),ruleCell(dstPort),ruleCell(action),actions);
    function syncPorts(){
      var noPorts=proto.value==="icmp"||proto.value==="arp";
      [srcPort,dstPort].forEach(function(input){
        input.disabled=noPorts;
        if(noPorts)input.value="any";
        input.title=noPorts?"Ports do not apply to "+proto.value+" rules.":"any, a single port like 443, or a range like 1000-2000";
      });
    }
    proto.addEventListener("change",syncPorts);
    syncPorts();
    return row;
  }
  function updateRulesState(){
    var rows=ruleRows();
    rows.forEach(function(row,index){
      row.querySelector(".zone-rule-index").textContent=String(index+1);
      var up=row.querySelector('[data-rule-move="up"]');
      var down=row.querySelector('[data-rule-move="down"]');
      up.disabled=index===0;
      down.disabled=index===rows.length-1;
    });
    var empty=document.getElementById("zone-rules-empty");
    if(empty)empty.hidden=rows.length>0;
    var add=document.getElementById("zone-rule-add");
    var limit=document.getElementById("zone-rule-limit");
    if(add)add.disabled=rows.length>=64;
    if(limit)limit.textContent=rows.length>=64?"64 rule limit reached. Remove a rule to add another.":"Up to 64 rules.";
  }
  function moveRule(row,delta){
    var rows=ruleRows();
    var index=rows.indexOf(row);
    var target=index+delta;
    if(target<0||target>=rows.length)return;
    if(delta<0)rulesBody.insertBefore(row,rows[target]);
    else rulesBody.insertBefore(row,rows[target+1]);
    updateRulesState();
    var focus=row.querySelector(delta<0?'[data-rule-move="up"]':'[data-rule-move="down"]');
    if(!focus||focus.disabled)focus=row.querySelector(delta<0?'[data-rule-move="down"]':'[data-rule-move="up"]');
    if(focus&&!focus.disabled)focus.focus();
  }
  function setRuleRows(rules){
    rulesBody.replaceChildren();
    rules.forEach(function(rule){rulesBody.append(ruleRow(rule));});
    updateRulesState();
  }
  async function openSettings(zone,opener){
    if(!settingsForm)return;
    settingsZone=zone.id;
    document.getElementById("zone-settings-zone").textContent=zone.name;
    var feedback=document.getElementById("zone-settings-status");
    message(feedback,"");
    document.getElementById("zone-welcome").value="";
    document.getElementById("zone-default-action").value="allow";
    setRuleRows([]);
    openDialog(document.getElementById("zone-settings-dialog"),opener);
    if(settingsController)settingsController.abort();
    var controller=new AbortController();
    settingsController=controller;
    var submit=settingsForm.querySelector('[type="submit"]');
    submit.disabled=true;
    submit.textContent="Loading…";
    settingsForm.setAttribute("aria-busy","true");
    message(feedback,"Loading settings…");
    try{
      var data=await api("/api/zones/"+encodeURIComponent(zone.id)+"/info",{signal:controller.signal});
      if(!data||!data.settings)throw new Error("Could not read the zone settings. Try again.");
      document.getElementById("zone-welcome").value=typeof data.settings.welcome==="string"?data.settings.welcome:"";
      document.getElementById("zone-default-action").value=data.settings.default_action==="deny"?"deny":"allow";
      setRuleRows(Array.isArray(data.settings.rules)?data.settings.rules:[]);
      message(feedback,"");
    }catch(err){
      if(err.name!=="AbortError")message(feedback,err.message,true);
    }finally{
      if(settingsController===controller){submit.disabled=false;submit.textContent="Save settings";settingsForm.setAttribute("aria-busy","false");}
    }
  }
  if(settingsForm&&rulesBody){
    document.getElementById("zone-rule-add").addEventListener("click",function(){
      if(ruleRows().length>=64)return;
      var row=ruleRow({});
      rulesBody.append(row);
      updateRulesState();
      row.querySelector(".zone-rule-src").focus();
    });
    settingsForm.addEventListener("submit",async function(event){
      event.preventDefault();
      if(!settingsZone)return;
      var submit=settingsForm.querySelector('[type="submit"]');
      var feedback=document.getElementById("zone-settings-status");
      var rules=ruleRows().map(function(row){
        return {
          src:row.querySelector(".zone-rule-src").value.trim(),
          src_port:row.querySelector(".zone-rule-src-port").value.trim(),
          proto:row.querySelector(".zone-rule-proto").value,
          dst:row.querySelector(".zone-rule-dst").value.trim(),
          dst_port:row.querySelector(".zone-rule-dst-port").value.trim(),
          action:row.querySelector(".zone-rule-action").value
        };
      });
      var payload={welcome:document.getElementById("zone-welcome").value,default_action:document.getElementById("zone-default-action").value,rules:rules};
      submit.disabled=true;
      submit.textContent="Saving…";
      settingsForm.setAttribute("aria-busy","true");
      message(feedback,"Saving settings…");
      try{
        var data=await api("/api/zones/"+encodeURIComponent(settingsZone)+"/settings",writeOptions("POST",payload));
        var settings=data&&data.settings?data.settings:payload;
        document.getElementById("zone-welcome").value=typeof settings.welcome==="string"?settings.welcome:"";
        document.getElementById("zone-default-action").value=settings.default_action==="deny"?"deny":"allow";
        setRuleRows(Array.isArray(settings.rules)?settings.rules:[]);
        message(feedback,"Settings saved. Online members received the update.");
      }catch(err){message(feedback,err.message,true);}
      finally{submit.disabled=false;submit.textContent="Save settings";settingsForm.setAttribute("aria-busy","false");}
    });
  }

  updateIntro();
  loadZones();
})();
</script>`;

function managementDialogs(isAdmin: boolean): string {
  return `<dialog class="modal" id="zone-create-dialog" aria-labelledby="zone-create-title" aria-describedby="zone-create-note">
    <div class="modal-head"><h2 id="zone-create-title">Create zone</h2><button class="modal-close" type="button" data-zone-close aria-label="Close create zone dialog">&times;</button></div>
    <form class="zone-modal-form" id="zone-create-form" novalidate>
      <div class="modal-body">
        <p class="zone-modal-note" id="zone-create-note">${isAdmin ? "Private zones are reachable only by machines you share the ID with. Public zones can be joined by anyone who has the ID." : "This zone is private. Only machines you share its ID with can join its network."}</p>
        <div class="field"><label for="zone-name">Zone name</label><input id="zone-name" name="name" maxlength="64" placeholder="Office network" required autofocus aria-describedby="zone-name-error"/><p id="zone-name-error" class="zone-field-error" hidden></p></div>
        <div class="field"><label for="zone-cidr">IPv4 network (CIDR)</label><input id="zone-cidr" name="cidr" value="10.88.0.0/24" required spellcheck="false" autocapitalize="off" aria-describedby="zone-cidr-hint zone-cidr-error"/><div class="hint" id="zone-cidr-hint">Use a private network from /16 to /30 that does not overlap your machines' existing routes.</div><p id="zone-cidr-error" class="zone-field-error" hidden></p></div>
        <div class="field"><label for="zone-limit">Member limit</label><input id="zone-limit" name="max_members" type="number" min="2" max="256" step="1" value="32" required aria-describedby="zone-limit-hint zone-limit-error"/><div class="hint" id="zone-limit-hint">2–256 machines, within the network's available addresses.</div><p id="zone-limit-error" class="zone-field-error" hidden></p></div>
        ${isAdmin
          ? `<div class="field"><label id="zone-visibility-label">Visibility</label>
          <div class="toggle-grid" role="radiogroup" aria-labelledby="zone-visibility-label">
            <label class="check"><input type="radio" id="zone-visibility-private" name="visibility" value="private" checked/><span class="check-body"><span class="check-label">Private <span class="badge gray">recommended</span></span><span class="hint">Only machines you share the zone ID with can join its network.</span></span></label>
            <label class="check"><input type="radio" id="zone-visibility-public" name="visibility" value="public"/><span class="check-body"><span class="check-label">Public</span><span class="hint">Anyone with the zone ID can join and reach exposed services. Only administrators can create public zones.</span></span></label>
          </div>
          <p class="zone-modal-note zone-public-warning" id="zone-public-warning" hidden>Anyone with a public zone ID can join and reach services exposed on member machines. Check your firewall before sharing it.</p>
        </div>`
          : `<div class="field"><label>Visibility</label><div class="note"><div><strong>Private</strong> · Recommended. Only machines you share the zone ID with can join its network.</div></div></div>`}
        <p id="zone-create-status" class="zone-status" role="status" aria-live="polite"></p>
      </div>
      <div class="modal-actions"><button type="button" class="btn" data-zone-close>Cancel</button><button type="submit" class="btn primary">Create zone</button></div>
    </form>
  </dialog>
  <dialog class="modal" id="zone-delete-dialog" aria-labelledby="zone-delete-title" aria-describedby="zone-delete-note">
    <div class="modal-head"><h2 id="zone-delete-title">Delete zone?</h2><button class="modal-close" type="button" data-zone-close aria-label="Close delete zone dialog">&times;</button></div>
    <form class="zone-modal-form" id="zone-delete-form">
      <div class="modal-body"><p class="zone-delete-name" id="zone-delete-name"></p><p class="zone-modal-note" id="zone-delete-note">All members will be disconnected and their virtual IP addresses released. This cannot be undone.</p><p id="zone-delete-status" class="zone-status" role="status" aria-live="polite"></p></div>
      <div class="modal-actions"><button type="button" class="btn" data-zone-close autofocus>Cancel</button><button type="submit" class="btn danger">Delete zone</button></div>
    </form>
  </dialog>
  <dialog class="modal zone-settings-dialog" id="zone-settings-dialog" aria-labelledby="zone-settings-title">
    <div class="modal-head"><div><h2 id="zone-settings-title">Zone settings</h2><div class="sub" id="zone-settings-zone"></div></div><button class="modal-close" type="button" data-zone-close aria-label="Close zone settings dialog">&times;</button></div>
    <form class="zone-modal-form" id="zone-settings-form" novalidate>
      <div class="modal-body">
        <div class="field"><label for="zone-welcome">Welcome message</label><textarea id="zone-welcome" maxlength="512" rows="3" placeholder="Welcome to the zone. Keep it tidy."></textarea><div class="hint">Shown on machines when they join this zone. Up to 512 characters.</div></div>
        <div class="field"><label for="zone-default-action">Default policy</label><select id="zone-default-action"><option value="allow">Allow everything not matched</option><option value="deny">Deny everything not matched</option></select><div class="hint">Rules are checked in order and the first match decides. Traffic that matches no rule follows this policy.</div></div>
        <h3 class="form-title">Firewall rules</h3>
        <p class="zone-rules-intro">Rules apply to traffic inside the zone network. The first matching rule wins, so order matters.</p>
        <div class="zone-rules-wrap">
          <table class="zone-rules">
            <colgroup><col class="zone-col-index"/><col/><col class="zone-col-port"/><col class="zone-col-proto"/><col/><col class="zone-col-port"/><col class="zone-col-action"/><col class="zone-col-actions"/></colgroup>
            <thead><tr><th scope="col" class="zone-rule-index">#</th><th scope="col">From network</th><th scope="col">From port</th><th scope="col">Protocol</th><th scope="col">To network</th><th scope="col">To port</th><th scope="col">Action</th><th scope="col"><span class="zone-visually-hidden">Rule actions</span></th></tr></thead>
            <tbody id="zone-rules-rows"></tbody>
          </table>
          <div class="empty zone-rules-empty" id="zone-rules-empty" hidden><strong>No rules yet</strong><div class="sub">Traffic that matches no rule follows the default policy above. Add a rule to allow or block specific networks and ports.</div></div>
        </div>
        <div class="zone-rules-tools"><button type="button" class="btn" id="zone-rule-add">Add rule</button><span class="hint" id="zone-rule-limit">Up to 64 rules.</span></div>
        <p id="zone-settings-status" class="zone-status" role="status" aria-live="polite"></p>
      </div>
      <div class="modal-actions"><button type="button" class="btn" data-zone-close>Cancel</button><button type="submit" class="btn primary">Save settings</button></div>
    </form>
  </dialog>`;
}

function membersDialog(): string {
  return `<dialog class="modal" id="zone-members-dialog" aria-labelledby="zone-members-title">
    <div class="modal-head"><h2 id="zone-members-title">Zone members</h2><button class="modal-close" type="button" data-zone-close aria-label="Close zone members dialog">&times;</button></div>
    <div class="modal-body">
      <p id="zone-members-status" class="zone-status zone-members-status" role="status" aria-live="polite" hidden></p>
      <div id="zone-members-body" aria-live="polite"></div>
    </div>
  </dialog>`;
}

function revealDialog(): string {
  return `<dialog class="modal" id="zone-reveal-dialog" aria-labelledby="zone-reveal-title">
    <div class="modal-head"><h2 id="zone-reveal-title">Reveal public zone details?</h2><button class="modal-close" type="button" data-zone-close aria-label="Close reveal dialog">&times;</button></div>
    <div class="modal-body">
      <p class="zone-modal-note">Anyone with this zone ID can join. Members of a public zone can reach services running on the machines that join, including this one.</p>
      <p class="zone-reveal-zone">Zone: <span class="mono" id="zone-reveal-zone"></span></p>
      <p class="zone-modal-note">Before revealing the ID and join command, check this machine's firewall and make sure only the services you intend are reachable from the zone network.</p>
      <label class="check"><input type="checkbox" id="zone-reveal-ack"/><span class="check-body"><span class="check-label">I have checked my firewall and understand what this machine exposes</span></span></label>
      <p id="zone-reveal-status" class="zone-status" role="status" aria-live="polite"></p>
      <div class="modal-actions"><button type="button" class="btn" data-zone-close>Cancel</button><button type="button" class="btn primary" id="zone-reveal-confirm" disabled>Reveal command</button></div>
    </div>
  </dialog>`;
}

export function renderZonesPage(data: ZonesPageData = {}): string {
  const signedIn = Boolean(data.user);
  const isAdmin = Boolean(data.user?.isAdmin);
  const nav = signedIn
    ? [
        { href: "/dashboard?tab=overview", label: "Overview", active: false },
        { href: "/dashboard?tab=sites", label: "My sites", active: false },
        { href: "/dashboard?tab=tokens", label: "Tokens", active: false },
        { href: "/zones", label: "My Zones", active: true },
        ...(isAdmin ? [{ href: "/admin", label: "Admin console", active: false }] : []),
      ]
    : [
        { href: "/", label: "Home", active: false },
        { href: "/zones", label: "Zones", active: true },
        { href: "/dashboard", label: "Sign in", active: false },
      ];
  const introTitle = signedIn ? "My zones" : "Public zones";
  const introText = signedIn ? PRIVATE_INTRO : PUBLIC_INTRO;

  return layout({
    title: signedIn ? "My dashboard" : "Zones",
    subtitle: signedIn ? "Your relay access, site providers and traffic" : "Public virtual networks",
    userLabel: data.user?.username,
    logoutAction: signedIn ? "/dashboard?logout" : undefined,
    nav,
    styles: ZONE_STYLE,
    scripts: ZONE_SCRIPT,
    content: `<main class="zone-page" id="zone-page" data-user-id="${escapeHtml(data.user?.id || "")}" data-admin="${Boolean(data.user?.isAdmin)}" data-csrf="${escapeHtml(data.csrf || "")}" data-private-intro="${escapeHtml(PRIVATE_INTRO)}" data-public-intro="${escapeHtml(PUBLIC_INTRO)}">
      <div class="page-head zone-intro"><div><h2 id="zone-intro-title">${introTitle}</h2><p class="sub" id="zone-intro-sub">${escapeHtml(introText)}</p></div>
        ${signedIn ? `<button type="button" class="btn primary" id="zone-create-open" aria-haspopup="dialog" aria-controls="zone-create-dialog">Create zone</button>` : `<a class="btn primary" href="/dashboard">Sign in to create a zone</a>`}
      </div>
      <div class="zone-toolbar">
        <div class="field zone-search"><label for="zone-search">Search zones</label><input id="zone-search" type="search" placeholder="Name, network or zone ID" autocomplete="off"/></div>
        ${signedIn ? `<div class="field zone-scope"><label for="zone-scope">Show</label><select id="zone-scope"><option value="managed" selected>My zones</option><option value="public">Public zones</option></select></div>` : ""}
        <button class="btn" type="button" id="zone-refresh">Refresh</button>
      </div>
      <p class="zone-count" id="zone-count" role="status" aria-live="polite">Loading zones…</p>
      <div class="panel"><ul class="zone-list" id="zone-list" aria-label="Zones" aria-busy="true"><li class="empty">Loading zones…</li></ul></div>
      <p class="zone-status" id="zone-status" role="status" aria-live="polite"></p>
      <noscript><p class="note">Enable JavaScript to browse and manage zones.</p></noscript>
      ${signedIn ? managementDialogs(isAdmin) : ""}
      ${membersDialog()}
      ${revealDialog()}
    </main>`,
  });
}

export async function handleZonesPage(request: Request, env: Env, _url?: URL): Promise<Response> {
  const headers = { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" };
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response(null, { status: 405, headers: { Allow: "GET, HEAD", "Cache-Control": "no-store" } });
  }
  try {
    await ensureSchema(env);
    const context = await getSessionContext(env, request);
    const page = renderZonesPage(context ? {
      user: { id: context.user.id, username: context.user.username, isAdmin: context.user.is_admin === 1 },
      csrf: context.session.csrf,
    } : {});
    return new Response(request.method === "HEAD" ? null : page, { headers });
  } catch (err) {
    console.error("zone page initialization failed:", err instanceof Error ? err.message : String(err));
    return new Response(request.method === "HEAD" ? null : renderErrorPage("Zones unavailable", "Could not load zones. Try again shortly.", "503"), { status: 503, headers });
  }
}
