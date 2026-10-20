// Resample browser input with an anti-alias low-pass filter. Linear
// interpolation alone aliases everything above the robot sample rate.
class Microphone extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const {rate,channels}=options.processorOptions;
    this.rate=rate;this.channels=channels;this.phase=0;
    this.taps=95;this.history=new Float32Array(this.taps);this.historyAt=0;
    this.coefficients=new Float32Array(this.taps);
    const cutoff=.46*rate/sampleRate,mid=(this.taps-1)/2;
    let total=0;
    for(let i=0;i<this.taps;i++) {
      const x=i-mid;
      const sinc=x===0?2*cutoff:Math.sin(2*Math.PI*cutoff*x)/(Math.PI*x);
      const window=.54-.46*Math.cos(2*Math.PI*i/(this.taps-1));
      this.coefficients[i]=sinc*window;total+=this.coefficients[i];
    }
    for(let i=0;i<this.taps;i++)this.coefficients[i]/=total;
    this.frameBytes=rate/50*channels*2;
    this.batchSize=rate/25*channels*2;
    this.buffer=new ArrayBuffer(this.batchSize);this.view=new DataView(this.buffer);this.offset=0;
    this.port.onmessage=event=>{if(event.data?.type==='flush')this.flush();};
  }
  emit() {
    if(!this.offset)return;
    // The gateway accepts whole 20 ms Opus frames. Pad the last capture frame
    // with silence so a short tail is encoded instead of discarded.
    const size=Math.ceil(this.offset/this.frameBytes)*this.frameBytes;
    const output=this.offset===size?this.buffer:this.buffer.slice(0,size);
    this.port.postMessage({type:'pcm',buffer:output},[output]);
    this.buffer=new ArrayBuffer(this.batchSize);this.view=new DataView(this.buffer);this.offset=0;
  }
  flush() {
    this.emit();
    this.port.postMessage({type:'flushed'});
  }
  process(inputs) {
    const source=inputs[0]?.[0];if(!source)return true;
    for(let i=0;i<source.length;i++) {
      this.history[this.historyAt]=source[i];
      this.phase+=this.rate;
      if(this.phase>=sampleRate) {
        this.phase-=sampleRate;
        let value=0,index=this.historyAt;
        for(let k=0;k<this.taps;k++) {
          value+=this.history[index]*this.coefficients[k];
          if(--index<0)index=this.taps-1;
        }
        const pcm=Math.max(-32768,Math.min(32767,Math.round(value*32768)));
        for(let c=0;c<this.channels;c++){this.view.setInt16(this.offset,pcm,true);this.offset+=2;}
        if(this.offset===this.buffer.byteLength)this.emit();
      }
      if(++this.historyAt===this.taps)this.historyAt=0;
    }
    return true;
  }
}
registerProcessor('pebble-microphone',Microphone);
