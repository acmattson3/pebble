import { MediaWidget } from './media-widget.mjs';
import { InputEngine, mappedValue } from './input-engine.mjs';

const $ = id => document.getElementById(id);
const robotSelect=$('robot'), message=$('message'), container=$('modules');
let base='', revision='', manifest=null, online=false, suspended=false;
let contexts=[], keys=new Set(), buttonStates=new Map(), pollsBusy=false, stateBusy=false;
let lastDeviceProfile='default', devices=[];
const queues=new Map();
const mediaSession=crypto.randomUUID?.() || Array.from(crypto.getRandomValues(new Uint8Array(24)),n=>n.toString(16).padStart(2,'0')).join('');

function element(tag, text, cls) {
  const el=document.createElement(tag);
  if (text!==undefined) el.textContent=text;
  if (cls) el.className=cls;
  return el;
}
function say(text,error=false) { message.textContent=text; message.classList.toggle('error',error); }
function usable() { return online && !suspended && document.visibilityState!=='hidden'; }
async function api(url,options={}) {
  const response=await fetch(url,{...options,cache:'no-store'});
  const body=await response.json();
  if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
  return body;
}
function queueCommand(robot,module,command,payload,revision) {
  // Neutral releases must still be queued as the page loses visibility.
  if (!online || suspended) return;
  const key=`${robot}/${module.id}/${command.endpoint}`;
  const state=queues.get(key) || {busy:false,next:null};
  state.next={robot,module:module.id,endpoint:command.endpoint,payload,revision};
  queues.set(key,state);
  if (!state.busy) drain(state);
}
async function drain(state) {
  if (!state.next || state.busy) return;
  const body=state.next; state.next=null; state.busy=true;
  try {
    await api('/modules/api/command',{method:'POST',headers:{'Content-Type':'application/json','X-Pebble-Session':mediaSession},body:JSON.stringify(body),keepalive:true});
  } catch(error) {
    suspended=true;
    for (const q of queues.values()) q.next=null;
    releaseAll(false);
    say(`${error.message}. Interact with a control to retry.`,true);
  } finally {
    state.busy=false;
    if (state.next) drain(state);
  }
}
function releaseAll(publish=true) {
  keys.clear(); buttonStates.clear();
  $('gamepad').checked=false;
  for (const ctx of contexts) { ctx.engine.reset(publish && online && !suspended); ctx.releaseView?.(); for(const media of ctx.mediaWidgets??[])media.stop(); syncValues(ctx); }
}
function beginInteraction() {
  if (!online) return false;
  suspended=false;
  say('Controls ready. Customize defaults in Input mappings.');
  return true;
}
function profileKey(module,device) { return `pebble.modules.bindings.v1:${base}:${module.id}:${module.type}@${module.version}:${device}`; }
function loadBindings(module) {
  function load(device,keyboard) {
    try {
      const saved=JSON.parse(localStorage.getItem(profileKey(module,device)) || 'null');
      if (Array.isArray(saved) && saved.length<=128 && saved.every(b=>
        ['keyboard','gamepad-axis','gamepad-button'].includes(b.input) && (b.input==='keyboard')===keyboard &&
        (b.reset || Object.hasOwn(module.actions,b.action)))) return saved;
    } catch(_) { /* Corrupt or unavailable storage uses robot defaults. */ }
    return structuredClone(module.bindings.filter(b=>(b.input==='keyboard')===keyboard));
  }
  return [...load('keyboard',true),...load(`controller:${lastDeviceProfile}`,false)];
}
function persist(ctx) {
  releaseAll();
  try {
    localStorage.setItem(profileKey(ctx.module,'keyboard'),JSON.stringify(ctx.bindings.filter(b=>b.input==='keyboard')));
    localStorage.setItem(profileKey(ctx.module,`controller:${lastDeviceProfile}`),JSON.stringify(ctx.bindings.filter(b=>b.input!=='keyboard')));
  }
  catch(_) { say('Mappings work for this session; browser storage is unavailable.',true); }
}
function labeled(text,input) { const label=element('label',text); label.append(input); return label; }
function select(options,current,changed) {
  const el=element('select');
  for (const [value,label] of options) { const option=element('option',label);option.value=value;el.append(option); }
  el.value=current;el.addEventListener('change',()=>changed(el.value));return el;
}
function numberInput(value,min,max,changed,step=.05) {
  const el=element('input');el.type='number';el.value=value;el.min=min;el.max=max;el.step=step;
  el.addEventListener('change',()=>{const n=Number(el.value);if (Number.isFinite(n))changed(Math.min(max,Math.max(min,n)));});return el;
}
function renderMappings(ctx,target) {
  target.replaceChildren();
  ctx.bindings.forEach((binding,index)=>{
    const row=element('div',undefined,'mapping');
    const update=(key,value,rebuild=false)=>{binding[key]=value;persist(ctx);if(rebuild)renderMappings(ctx,target);};
    const source=select([['keyboard','Keyboard'],['gamepad-axis','Controller axis'],['gamepad-button','Controller button']],binding.input,value=>{
      binding.input=value;
      if(value==='keyboard')binding.code=binding.code || 'KeyN';else binding.index=binding.index ?? 0;
      persist(ctx);renderMappings(ctx,target);
    });
    let control;
    if(binding.input==='keyboard') {
      control=element('input');control.type='text';control.value=binding.code || '';
      control.setAttribute('aria-label','Keyboard key');
      control.addEventListener('keydown',event=>{event.preventDefault();event.stopPropagation();control.value=event.code;update('code',event.code);});
    } else control=numberInput(binding.index??0,0,63,n=>update('index',Math.round(n)),1);
    const actions=Object.entries(ctx.module.actions).map(([id,a])=>[id,a.label]);
    actions.push(['$reset','Release inputs']);
    const action=select(actions,binding.reset?'$reset':binding.action,value=>{
      binding.reset=value==='$reset';if(!binding.reset)binding.action=value;
      persist(ctx);renderMappings(ctx,target);
    });
    row.append(labeled('Source',source),labeled(binding.input==='keyboard'?'Press a key':'Index',control),labeled('Action',action));
    if(binding.input==='gamepad-axis') {
      row.append(labeled('Scale',numberInput(binding.scale??1,-4,4,n=>update('scale',n))),
                 labeled('Dead zone',numberInput(binding.deadzone??.12,0,.95,n=>update('deadzone',n))),
                 labeled('Offset',numberInput(binding.offset??0,-2,2,n=>update('offset',n))));
    } else if(!binding.reset) row.append(labeled('Value',numberInput(binding.value??1,-4,4,n=>update('value',n))));
    const remove=element('button','Remove');remove.addEventListener('click',()=>{ctx.bindings.splice(index,1);persist(ctx);renderMappings(ctx,target);});row.append(remove);target.append(row);
  });
  const actions=element('div',undefined,'mapping-actions');
  const add=element('button','Add mapping');add.addEventListener('click',()=>{
    ctx.bindings.push({id:`custom-${Date.now()}`,input:'keyboard',code:'KeyN',action:Object.keys(ctx.module.actions)[0],value:1});
    persist(ctx);renderMappings(ctx,target);
  });
  const reset=element('button','Robot defaults');reset.addEventListener('click',()=>{ctx.bindings=structuredClone(ctx.module.bindings);persist(ctx);renderMappings(ctx,target);});
  actions.append(add,reset);target.append(actions);
}
function syncValues(ctx) {
  for(const [action,nodes] of ctx.valueNodes) for(const node of nodes) {
    const value=ctx.engine.values[action];
    if(node.tagName==='INPUT')node.value=value;
    else node.textContent=Number(value).toFixed(2);
  }
  for(const input of ctx.controlNodes)input.disabled=!online;
  ctx.card.classList.toggle('offline',!online);
  for(const media of ctx.mediaWidgets??[])media.sync(ctx.engine.values, online && !suspended);
  for(const widget of ctx.soundboardWidgets??[])syncSoundboardWidget(ctx,widget);
}
function syncSoundboardWidget(ctx, item) {
  const states=ctx.latestState?.states?.[ctx.module.id]??{};
  const filesState=states[item.widget.files_endpoint]?.value;
  const statusState=states[item.widget.status_endpoint]?.value;
  const files=Array.isArray(filesState?.files)?filesState.files.filter(file=>typeof file==='string'):[];
  const previous=item.select.value;
  item.select.replaceChildren();
  if(!files.length) item.select.append(element('option',filesState?'No WAV files available':'Waiting for sound list…'));
  for(const file of files) {const option=element('option',file);option.value=file;item.select.append(option);}
  if(files.includes(previous))item.select.value=previous;
  const canPlay=filesState?.controls!==false&&files.length>0;
  item.select.disabled=!online||!canPlay;
  item.play.disabled=!online||!canPlay;
  item.stop.disabled=!online||filesState?.controls===false;
  if(statusState?.error) item.status.textContent=String(statusState.error);
  else if(statusState?.playing) item.status.textContent=`Playing: ${statusState.file||'sound'}`;
  else if(filesState) item.status.textContent=`Ready · ${files.length} sound${files.length===1?'':'s'}`;
  else item.status.textContent='Waiting for sound list…';
}
function render(manifest) {
  for(const ctx of contexts)for(const media of ctx.mediaWidgets??[])media.destroy();
  container.replaceChildren(); contexts=[];
  const robot=base;
  for(const module of manifest.modules) {
    const card=element('section',undefined,'module');card.dataset.module=module.id;
    const ctx={module,card,bindings:loadBindings(module),valueNodes:new Map(),stateNodes:[],controlNodes:[],mediaWidgets:[],soundboardWidgets:[],latestState:null};
    ctx.engine=new InputEngine(module,(command,payload)=>queueCommand(robot,module,command,payload,manifest.revision));
    contexts.push(ctx);
    const title=element('h2',module.title);title.append(element('span',`${module.type} · v${module.version}`,'type'));
    card.append(title,element('p',module.description,'description'));
    const controls=element('div',undefined,'controls');controls.style.gridTemplateColumns=`repeat(${module.ui.columns},1fr)`;
    for(const widget of module.ui.controls) {
      if(widget.kind==='video' || widget.kind==='audio') {
        const media=new MediaWidget({module,widget,base:robot,revision:manifest.revision,
          set:(action,value,pressed=true,force=false)=>{if(beginInteraction()){if(force){ctx.engine.sources.delete(action);ctx.engine.values[action]=value;ctx.engine.touched.add(action);}else ctx.engine.set(action,`media-${action}`,value,pressed);ctx.engine.flush();syncValues(ctx);}},
          error:text=>say(text,true)});
        ctx.mediaWidgets.push(media);controls.append(media.node);
      } else if(widget.kind==='range') {
        const spec=module.actions[widget.action],input=element('input'),value=element('output');
        input.type='range';input.min=spec.min;input.max=spec.max;input.step=spec.step??.01;input.value=spec.neutral;
        input.setAttribute('aria-label',widget.label);input.dataset.action=widget.action;
        const label=labeled(widget.label,input);label.className='range';label.append(value);controls.append(label);
        ctx.valueNodes.set(widget.action,[...(ctx.valueNodes.get(widget.action)||[]),input,value]);ctx.controlNodes.push(input);
        input.addEventListener('input',()=>{if(beginInteraction()){ctx.engine.set(widget.action,`range-${widget.action}`,Number(input.value));ctx.engine.flush();syncValues(ctx);}});
        const release=()=>{if(spec.reset_on_release){ctx.engine.set(widget.action,`range-${widget.action}`,0,false);ctx.engine.flush();syncValues(ctx);}};
        input.addEventListener('change',release);input.addEventListener('pointercancel',release);input.addEventListener('blur',release);
      } else if(widget.kind==='button' || widget.kind==='reset') {
        const button=element('button',widget.label);ctx.controlNodes.push(button);controls.append(button);
        if(widget.kind==='reset')button.addEventListener('click',()=>releaseAll());
        else {
          button.addEventListener('pointerdown',event=>{if(!beginInteraction())return;button.setPointerCapture(event.pointerId);ctx.engine.set(widget.action,`button-${widget.label}`,widget.value);ctx.engine.flush();syncValues(ctx);});
          const release=()=>{ctx.engine.set(widget.action,`button-${widget.label}`,0,false);ctx.engine.flush();syncValues(ctx);};
          button.addEventListener('pointerup',release);button.addEventListener('pointercancel',release);
          button.addEventListener('click',event=>{if(event.detail===0 && beginInteraction()){ctx.engine.set(widget.action,`button-${widget.label}`,widget.value);ctx.engine.flush();release();}});
        }
      } else if(widget.kind==='joystick') {
        const stick=element('div',undefined,'stick'),thumb=element('span',undefined,'thumb');stick.append(thumb);
        stick.setAttribute('role','group');stick.setAttribute('aria-label',widget.label);stick.tabIndex=0;
        let pointer=null;
        const update=event=>{
          const rect=stick.getBoundingClientRect(),radius=rect.width*.36;
          let x=(event.clientX-(rect.left+rect.width/2))/radius,y=(event.clientY-(rect.top+rect.height/2))/radius;
          const norm=Math.max(1,Math.hypot(x,y));x/=norm;y/=norm;
          thumb.style.transform=`translate(${x*radius}px,${y*radius}px)`;
          const sx=module.actions[widget.x],sy=module.actions[widget.y];
          ctx.engine.set(widget.x,'stick-x',x*Math.max(Math.abs(sx.min),Math.abs(sx.max)));
          ctx.engine.set(widget.y,'stick-y',y*(widget.invert_y?-1:1)*Math.max(Math.abs(sy.min),Math.abs(sy.max)));
          ctx.engine.flush();syncValues(ctx);
        };
        const release=()=>{pointer=null;thumb.style.transform='';ctx.engine.set(widget.x,'stick-x',0,false);ctx.engine.set(widget.y,'stick-y',0,false);ctx.engine.flush();syncValues(ctx);};
        ctx.releaseView=()=>{pointer=null;thumb.style.transform='';};
        stick.addEventListener('pointerdown',event=>{if(!beginInteraction())return;pointer=event.pointerId;stick.setPointerCapture(pointer);update(event);});
        stick.addEventListener('pointermove',event=>{if(pointer===event.pointerId&&usable())update(event);});
        stick.addEventListener('pointerup',release);stick.addEventListener('pointercancel',release);controls.append(stick);
      } else if(widget.kind==='state') {
        const node=element('div',widget.label,'state'),value=element('strong','—');node.append(value);controls.append(node);
        ctx.stateNodes.push({widget,value});
      } else if(widget.kind==='soundboard') {
        const node=element('div',undefined,'soundboard-widget');
        const title=element('strong',widget.label,'soundboard-title');
        const fileSelect=element('select');fileSelect.setAttribute('aria-label',`${widget.label} sound file`);
        const status=element('p','Waiting for sound list…','soundboard-status');
        const actions=element('div',undefined,'soundboard-actions');
        const play=element('button','Play');play.setAttribute('aria-label',`Play selected ${widget.label} sound`);
        const stop=element('button','Stop');stop.className='stop';stop.setAttribute('aria-label',`Stop ${widget.label}`);
        actions.append(play,stop);node.append(title,fileSelect,actions,status);controls.append(node);
        const item={widget,select:fileSelect,play,stop,status};ctx.soundboardWidgets.push(item);ctx.controlNodes.push(fileSelect,play,stop);
        play.addEventListener('click',()=>{
          if(!beginInteraction()||!fileSelect.value)return;
          queueCommand(robot,module,{endpoint:widget.command_endpoint},{action:'play',file:fileSelect.value},manifest.revision);
        });
        stop.addEventListener('click',()=>{
          if(!beginInteraction())return;
          queueCommand(robot,module,{endpoint:widget.command_endpoint},{action:'stop',file:''},manifest.revision);
        });
      }
    }
    card.append(controls);
    const details=element('details'),mappings=element('div');details.append(element('summary','Input mappings'),mappings);card.append(details);
    renderMappings(ctx,mappings);syncValues(ctx);container.append(card);
  }
}
function applyState(state) {
  online=state.online;
  $('connection').textContent=online?`Online · ${manifest?.modules.length??0} modules`:'Robot offline';
  for(const ctx of contexts) {
    ctx.latestState=state;
    for(const {widget,value} of ctx.stateNodes) {
      const body=state.states[ctx.module.id]?.[widget.endpoint]?.value;
      const datum=widget.field?body?.[widget.field]:body;
      value.textContent=datum===undefined?'—':(typeof datum==='object'?JSON.stringify(datum):String(datum));
    }
    syncValues(ctx);
  }
}
async function refreshRobots() {
  if(pollsBusy)return;pollsBusy=true;
  try {
    const data=await api('/modules/api/robots'),selected=base;
    robotSelect.replaceChildren();
    for(const r of data.robots){const option=element('option',`${r.identity.id} · ${r.identity.system}${r.online?'':' (offline)'}`);option.value=r.base;robotSelect.append(option);}
    if(!data.robots.length){robotSelect.append(element('option','No module manifests yet'));if(base)changeRobot('');$('connection').textContent=data.connected?'Broker connected':'Broker disconnected';return;}
    robotSelect.value=data.robots.some(r=>r.base===selected)?selected:data.robots[0].base;
    if(robotSelect.value!==base)changeRobot(robotSelect.value);
    if(!data.connected){online=false;releaseAll(false);say('Broker disconnected. Connect from Classic controls.',true);}
  } catch(error){say(error.message,true);}finally{pollsBusy=false;}
}
async function refreshState() {
  if(!base||stateBusy)return;stateBusy=true;
  const target=base;
  try {
    const state=await api(`/modules/api/state?robot=${encodeURIComponent(target)}&revision=${encodeURIComponent(revision)}`);
    if(target!==base)return;
    if(state.manifest){releaseAll();manifest=state.manifest;revision=state.revision;render(manifest);say('Use controls directly, or customize keyboard and controller mappings.');}
    if(online&&!state.online)releaseAll(false);
    applyState(state);
  } catch(error){online=false;releaseAll(false);say(error.message,true);}finally{stateBusy=false;}
}
function changeRobot(value) {
  releaseAll();for(const ctx of contexts)for(const media of ctx.mediaWidgets??[])media.destroy();base=value;revision='';manifest=null;online=false;container.replaceChildren();contexts=[];
  refreshState();
}
function editing(target,code){
  if(target?.isContentEditable || ['SELECT','TEXTAREA'].includes(target?.tagName))return true;
  const native=/^(Space|Enter|ArrowUp|ArrowDown|ArrowLeft|ArrowRight|Home|End)$/.test(code);
  if(target?.tagName==='BUTTON')return native;
  if(target?.tagName==='INPUT')return ['checkbox','radio','range','button'].includes(target.type)?native:true;
  return false;
}
for(const down of [true,false]) window.addEventListener(down?'keydown':'keyup',event=>{
  if(down&&(editing(event.target,event.code)||event.repeat||!$('keyboard').checked||!usable()))return;
  if(!down&&!keys.has(event.code))return;
  if(down)keys.add(event.code);else keys.delete(event.code);
  let matched=false;
  for(const ctx of contexts) {
    for(const b of ctx.bindings)if(b.input==='keyboard'&&b.code===event.code){
      matched=true;
      if(b.reset){if(down)releaseAll();continue;}
      if(!down) {
        const media=ctx.mediaWidgets?.find(item=>item.widget.talk_action===b.action);
        const sources=ctx.engine.sources.get(b.action);
        if(media&&(!sources||sources.size<=1)) {
          media.releaseTalk(()=>{ctx.engine.set(b.action,`key-${b.id}`,b.value??1,false);ctx.engine.flush();syncValues(ctx);});
          continue;
        }
      }
      ctx.engine.set(b.action,`key-${b.id}`,b.value??1,down);
    }
    ctx.engine.flush();syncValues(ctx);
  }
  if(matched)event.preventDefault();
});
function refreshDevices() {
  const pads=[...(navigator.getGamepads?.()||[])].filter(Boolean);
  const signature=pads.map(p=>`${p.index}:${p.id}`).join('|');
  if(signature===devices.join('|'))return;
  devices=pads.map(p=>`${p.index}:${p.id}`);const selected=$('device').value;
  $('device').replaceChildren();
  for(const pad of pads){const option=element('option',pad.id);option.value=String(pad.index);$('device').append(option);}
  if(!pads.length){const option=element('option','No controller connected');option.value='';$('device').append(option);releaseAll();}
  else if(pads.some(p=>String(p.index)===selected))$('device').value=selected;
  changeDevice();
}
function changeDevice() {
  releaseAll();
  const pad=[...(navigator.getGamepads?.()||[])].find(p=>p&&String(p.index)===$('device').value);
  lastDeviceProfile=pad?.id||'default';
  if(manifest)render(manifest);
}
setInterval(()=>{
  refreshDevices();
  const pad=[...(navigator.getGamepads?.()||[])].find(p=>p&&String(p.index)===$('device').value);
  for(const ctx of contexts) {
    if(pad&&$('gamepad').checked&&usable())for(const b of ctx.bindings) {
      if(b.input==='gamepad-axis'&&!b.reset)ctx.engine.set(b.action,`pad-${b.id}`,mappedValue(b,pad.axes[b.index]??0));
      if(b.input==='gamepad-button'){
        const key=`${ctx.module.id}:${b.id}`,pressed=!!pad.buttons[b.index]?.pressed;
        if(pressed!==!!buttonStates.get(key)){
          buttonStates.set(key,pressed);
          if(b.reset){if(pressed)releaseAll();}
          else ctx.engine.set(b.action,`pad-${b.id}`,b.value??1,pressed);
        }
      }
    }
    if(usable())ctx.engine.flush();syncValues(ctx);
  }
},50);
$('keyboard').addEventListener('change',()=>releaseAll());
$('gamepad').addEventListener('change',()=>{const enabled=$('gamepad').checked;releaseAll();$('gamepad').checked=enabled&&online;});
$('device').addEventListener('change',changeDevice);
$('stop').addEventListener('click',()=>releaseAll());
robotSelect.addEventListener('change',()=>changeRobot(robotSelect.value));
window.addEventListener('blur',()=>releaseAll());
document.addEventListener('visibilitychange',()=>{if(document.hidden)releaseAll();});
window.addEventListener('pagehide',()=>releaseAll());
window.addEventListener('gamepadconnected',refreshDevices);window.addEventListener('gamepaddisconnected',refreshDevices);
setInterval(refreshRobots,3000);setInterval(refreshState,500);refreshRobots();
