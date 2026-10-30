import { AudioUplink } from './audio-uplink.mjs';

// Shared camera/audio widget. Topics, formats and action names come from the robot.
export class MediaWidget {
  constructor({module,widget,base,revision,set,error}) {
    Object.assign(this,{module,widget,base,revision,set,error});
    this.active=false;this.talking=false;this.cursor=0;this.generation=0;this.destroyed=false;
    this.node=document.createElement('div');this.node.className='media-widget';
    this.status=document.createElement('p');this.status.textContent='Off';
    this.toggle=document.createElement('button');this.toggle.textContent=widget.kind==='video'?'View camera':'Listen';
    this.toggle.addEventListener('click',()=>set(widget.action,this.active?0:1));
    this.node.append(this.toggle,this.status);
    if(widget.kind==='video') {
      this.fullscreen=document.createElement('button');this.fullscreen.textContent='Fullscreen';this.fullscreen.className='media-fullscreen-toggle';
      this.fullscreen.setAttribute('aria-label',`${widget.label} fullscreen`);
      this.fullscreen.addEventListener('click',()=>{
        if(document.fullscreenElement===this.node)document.exitFullscreen?.();
        else this.node.requestFullscreen?.().catch(error=>this.error(error.message||String(error)));
      });
      this.fullscreenChange=()=>{const active=document.fullscreenElement===this.node;this.fullscreen.textContent=active?'Exit fullscreen':'Fullscreen';this.fullscreen.setAttribute('aria-label',`${active?'Exit':'Enter'} ${widget.label} fullscreen`);};
      document.addEventListener('fullscreenchange',this.fullscreenChange);this.node.append(this.fullscreen);
      this.image=document.createElement('img');this.image.alt=widget.label;this.image.hidden=true;this.node.append(this.image);
    } else if(widget.uplink) {
      this.talk=document.createElement('button');this.talk.textContent='Hold to talk';
      this.talk.addEventListener('pointerdown',e=>{if(this.releasing)return;this.talk.setPointerCapture(e.pointerId);set(widget.talk_action,1);});
      const release=()=>this.releaseTalk(()=>set(widget.talk_action,0,false));
      this.talk.addEventListener('pointerup',release);this.talk.addEventListener('pointercancel',release);
      this.talk.addEventListener('keydown',e=>{if(['Space','Enter'].includes(e.code)&&!e.repeat){e.preventDefault();set(widget.talk_action,1);}});
      this.talk.addEventListener('keyup',e=>{if(['Space','Enter'].includes(e.code)){e.preventDefault();release();}});
      this.talk.addEventListener('blur',release);this.node.append(this.talk);
    }
    this.timer=setInterval(()=>this.poll(),widget.kind==='video'?67:40);
  }
  url(endpoint) { return `/modules/api/media/${encodeURIComponent(this.module.id)}/${encodeURIComponent(endpoint)}?robot=${encodeURIComponent(this.base)}&revision=${encodeURIComponent(this.revision)}`; }
  sync(values,online) {
    this.toggle.disabled=!online;if(this.talk)this.talk.disabled=!online;if(this.fullscreen)this.fullscreen.disabled=!online;
    const active=online&&values[this.widget.action]>.5;
    if(active!==this.active) {
      this.active=active;this.cursor=0;
      this.toggle.textContent=active?'Stop':this.widget.kind==='video'?'View camera':'Listen';
      if(!active){this.abort?.abort();this.image?.removeAttribute('src');if(this.image)this.image.hidden=true;this.clearPlayback();}
      else if(this.widget.kind==='audio')this.audio().catch(e=>this.fail(e,this.widget.action));
    }
    const talking=online&&this.widget.talk_action&&values[this.widget.talk_action]>.5;
    if(!!talking!==this.talking){this.talking=!!talking;if(talking)this.startMic();else this.stopMic();}
    if(this.talk)this.talk.classList.toggle('active',this.talking);
    if(!this.active&&!this.talking)this.status.textContent='Off';
  }
  async audio() {
    if(!this.context||this.context.state==='closed')this.context=new AudioContext({latencyHint:'interactive'});
    await this.context.resume();return this.context;
  }
  fail(error,action) {
    if(this.destroyed)return;
    this.set(action,0,false,true);this.status.textContent=error.message||String(error);this.error(error.message||String(error));
  }
  async poll() {
    if(!this.active||this.busy||this.destroyed)return;
    this.busy=true;this.abort=new AbortController();
    try {
      const res=await fetch(this.url(this.widget.endpoint)+`&after=${this.cursor}`,{cache:'no-store',signal:this.abort.signal});
      if(res.status===204){this.status.textContent='Waiting for stream…';return;}
      if(!res.ok)throw new Error((await res.json()).error||'Media unavailable');
      if(!this.active)return;
      if(this.image) {
        const blob=await res.blob();if(!this.active)return;
        if(this.imageURL)URL.revokeObjectURL(this.imageURL);
        this.imageURL=URL.createObjectURL(blob);this.image.src=this.imageURL;this.image.hidden=false;
        const codec=res.headers.get('X-Video-Codec')==='h264'?'H.264':'delta video';
        const kbps=Number(res.headers.get('X-Video-Kbps')),fps=Number(res.headers.get('X-Video-Fps'));
        this.status.textContent=`Live · ${codec}${fps?` · ${fps} fps`:''}${kbps?` · ${kbps} kbit/s MQTT`:''}`;
      } else {
        const data=await res.arrayBuffer();if(!this.active)return;
        this.cursor=Number(res.headers.get('X-Audio-Cursor'));
        const rate=Number(res.headers.get('X-Audio-Rate')),channels=Number(res.headers.get('X-Audio-Channels'));
        const ctx=await this.audio();if(!this.active)return;
        const pcm=new DataView(data),count=data.byteLength/2/channels;
        const buffer=ctx.createBuffer(channels,count,rate);
        for(let c=0;c<channels;c++){const target=buffer.getChannelData(c);for(let i=0;i<count;i++)target[i]=pcm.getInt16((i*channels+c)*2,true)/32768;}
        if(!this.nextTime||this.nextTime<ctx.currentTime||this.nextTime>ctx.currentTime+.4)this.nextTime=ctx.currentTime+.04;
        const source=ctx.createBufferSource();source.buffer=buffer;source.connect(ctx.destination);source.start(this.nextTime);this.nextTime+=buffer.duration;
        (this.sources??=new Set()).add(source);source.onended=()=>this.sources.delete(source);
        this.status.textContent=this.talking?'Listening · talking':'Listening · Opus over MQTT';
      }
    } catch(e){if(e.name!=='AbortError')this.fail(e,this.widget.action);}
    finally{this.busy=false;}
  }
  clearPlayback(){for(const source of this.sources??[])try{source.stop();}catch(_){}this.sources?.clear();this.nextTime=0;}
  async startMic() {
    const generation=++this.generation;
    try {
      if(!navigator.mediaDevices?.getUserMedia)throw new Error('Microphone access requires HTTPS or localhost.');
      const stream=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:true,noiseSuppression:false,autoGainControl:false},video:false});
      if(!this.talking||generation!==this.generation){stream.getTracks().forEach(t=>t.stop());return;}
      this.microphone=stream;
      const ctx=await this.audio();
      await ctx.audioWorklet.addModule('/modules/assets/microphone-worklet.mjs');
      if(!this.talking||generation!==this.generation)return;
      const ep=this.module.endpoints[this.widget.uplink];
      this.micSource=ctx.createMediaStreamSource(stream);
      this.worklet=new AudioWorkletNode(ctx,'pebble-microphone',{processorOptions:{rate:ep.rate,channels:ep.channels}});
      this.uplink=new AudioUplink({maxBytes:ep.rate*ep.channels*2*.24,
        send:async(pcm,signal)=>{
          const res=await fetch(this.url(this.widget.uplink),{method:'POST',headers:{'Content-Type':'application/octet-stream'},body:pcm,signal});
          if(!res.ok)throw new Error((await res.json()).error||'Audio uplink unavailable');
        },
        onError:e=>{if(generation===this.generation)this.fail(e,this.widget.talk_action);},
        onCongestion:()=>{this.status.textContent='Talk link congested · dropping stale audio';}});
      this.worklet.port.onmessage=e=>{
        if(e.data?.type==='pcm')this.uplink?.push(e.data.buffer);
        else if(e.data?.type==='flushed')this.flushResolve?.();
      };
      this.micSource.connect(this.worklet);this.worklet.connect(ctx.destination);
      this.status.textContent='Talking · Opus over MQTT';
    } catch(e){if(generation===this.generation)this.fail(e,this.widget.talk_action);}
  }
  async releaseTalk(releaseCommand) {
    if(this.releasePromise){await this.releasePromise;releaseCommand();return;}
    this.releasing=true;
    const worklet=this.worklet,uplink=this.uplink;
    this.releasePromise=(async()=>{
      if(worklet)await new Promise(resolve=>{
        let done=false;const finish=()=>{if(done)return;done=true;clearTimeout(timer);this.flushResolve=null;resolve();};
        const timer=setTimeout(finish,250);this.flushResolve=finish;worklet.port.postMessage({type:'flush'});
      });
      await uplink?.finish();
      this.cleanupMic(uplink);
    })();
    try{await this.releasePromise;}catch(error){uplink?.stop();this.cleanupMic(uplink);this.error(error.message||String(error));}
    finally{this.releasePromise=null;this.releasing=false;releaseCommand();}
  }
  cleanupMic(uplink=this.uplink){
    if(uplink===this.uplink)this.uplink=null;
    this.microphone?.getTracks().forEach(t=>t.stop());this.worklet?.disconnect();this.micSource?.disconnect();
    this.microphone=null;this.worklet=null;this.micSource=null;
  }
  stopMic(){this.generation++;this.uplink?.stop();this.cleanupMic();}
  stop(){this.active=false;this.talking=false;this.abort?.abort();this.stopMic();this.clearPlayback();if(this.image){this.image.hidden=true;this.image.removeAttribute('src');}if(this.imageURL){URL.revokeObjectURL(this.imageURL);this.imageURL=null;}this.toggle.textContent=this.widget.kind==='video'?'View camera':'Listen';this.status.textContent='Off';}
  destroy(){this.destroyed=true;clearInterval(this.timer);if(this.fullscreenChange)document.removeEventListener('fullscreenchange',this.fullscreenChange);this.stop();this.context?.close();if(this.imageURL)URL.revokeObjectURL(this.imageURL);}
}
