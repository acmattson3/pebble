export class LightsWidget {
  constructor({widget,send}) {
    this.widget=widget;this.send=send;this.online=false;this.lastMode='solid';this.isOn=false;this.busy=false;
    this.node=document.createElement('div');this.node.className='lights-widget';
    this.color=document.createElement('input');this.color.type='color';this.color.value='#ff0000';
    this.color.setAttribute('aria-label','Light color');
    const colorLabel=document.createElement('label');colorLabel.textContent='Color';colorLabel.append(this.color);
    this.period=document.createElement('input');this.period.type='number';this.period.min='0.05';this.period.max='30';this.period.step='0.05';this.period.value='2';
    this.period.setAttribute('aria-label','Flash period in seconds');
    const periodLabel=document.createElement('label');periodLabel.textContent='Flash period (s)';periodLabel.append(this.period);
    const controls=document.createElement('div');controls.className='lights-actions';
    this.solid=this.button('Solid',()=>this.setMode('solid'));
    this.flash=this.button('Flash',()=>this.setMode('flash'));
    this.off=this.button('Off',()=>this.turnOff());
    this.off.classList.add('stop');controls.append(this.solid,this.flash,this.off);
    this.status=document.createElement('p');this.status.className='lights-status';this.status.textContent='Ready';
    this.node.append(colorLabel,periodLabel,controls,this.status);
  }
  button(label,action) {
    const button=document.createElement('button');button.type='button';button.textContent=label;
    button.addEventListener('click',action);return button;
  }
  colorPayload() {
    const hex=this.color.value.slice(1);
    return {r:parseInt(hex.slice(0,2),16)/255,g:parseInt(hex.slice(2,4),16)/255,b:parseInt(hex.slice(4,6),16)/255};
  }
  periodValue() {
    const value=Number(this.period.value);
    return Number.isFinite(value)?Math.min(30,Math.max(.05,value)):2;
  }
  async setMode(mode) {
    const endpoint=mode==='flash'?this.widget.flash_endpoint:this.widget.solid_endpoint;
    const payload=mode==='flash'?{...this.colorPayload(),period:this.periodValue()}:this.colorPayload();
    if(await this.command(endpoint,payload,mode==='flash'?'Flashing color sent.':'Solid color sent.')) {
      this.lastMode=mode;this.isOn=true;
    }
  }
  async turnOff() {
    if(await this.command(this.widget.solid_endpoint,{b:0,g:0,r:0},'Lights off.'))this.isOn=false;
  }
  async toggle() {
    if(this.isOn)await this.turnOff();else await this.setMode(this.lastMode);
  }
  async command(endpoint,payload,status) {
    if(!this.online||this.busy)return false;
    this.busy=true;this.updateDisabled();
    this.status.textContent='Sending light command…';
    try { await this.send(endpoint,payload);this.status.textContent=status;return true; }
    catch(error) { this.status.textContent=`Command failed: ${error.message||error}`;return false; }
    finally {this.busy=false;this.updateDisabled();}
  }
  updateDisabled() {
    for(const control of [this.color,this.period,this.solid,this.flash,this.off])control.disabled=!this.online||this.busy;
  }
  setOnline(online) {
    this.online=Boolean(online);
    this.updateDisabled();
  }
}
