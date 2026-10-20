// One ordered upload at a time. Coalesce waiting PCM frames so a request's
// round trip does not discard speech. Bound stale speech on a congested link.
export class AudioUplink {
  constructor({send,maxBytes,onError,onCongestion=()=>{}}) {
    Object.assign(this,{send,maxBytes,onError,onCongestion});
    this.pending=[];this.bytes=0;this.busy=false;this.active=true;this.accepting=true;this.finishers=[];
  }
  push(buffer) {
    if(!this.active||!this.accepting)return;
    const chunk=new Uint8Array(buffer);
    if(chunk.byteLength>this.maxBytes){this.onCongestion();return;}
    let dropped=false;
    while(this.pending.length&&this.bytes+chunk.byteLength>this.maxBytes){this.bytes-=this.pending.shift().byteLength;dropped=true;}
    this.pending.push(chunk);this.bytes+=chunk.byteLength;
    if(dropped)this.onCongestion();
    this.drain();
  }
  async drain() {
    if(this.busy||!this.active||!this.bytes)return;
    this.busy=true;
    const pcm=new Uint8Array(this.bytes);let offset=0;
    for(const chunk of this.pending){pcm.set(chunk,offset);offset+=chunk.byteLength;}
    this.pending=[];this.bytes=0;this.abort=new AbortController();
    try {await this.send(pcm,this.abort.signal);}
    catch(error){if(this.active&&error.name!=='AbortError'){this.stop();this.onError(error);}}
    finally{
      this.busy=false;
      if(this.active&&this.bytes)this.drain();
      else if(!this.bytes){for(const resolve of this.finishers.splice(0))resolve();}
    }
  }
  finish() {
    this.accepting=false;
    if(!this.busy&&!this.bytes)return Promise.resolve();
    this.drain();
    return new Promise(resolve=>this.finishers.push(resolve));
  }
  stop(){this.active=false;this.accepting=false;this.pending=[];this.bytes=0;this.abort?.abort();for(const resolve of this.finishers.splice(0))resolve();}
}
