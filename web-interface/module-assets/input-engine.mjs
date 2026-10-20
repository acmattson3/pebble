// Input semantics are independent of widgets, robot names and transport.
export function mappedValue(binding, raw) {
  const zone = binding.deadzone ?? 0;
  const value = Math.abs(raw) <= zone ? 0 : Math.sign(raw) * (Math.abs(raw) - zone) / (1 - zone);
  return value * (binding.scale ?? 1) + (binding.offset ?? 0);
}

export class InputEngine {
  constructor(module, send) {
    this.module = module;
    this.send = send;
    this.values = Object.fromEntries(Object.entries(module.actions).map(([k,a]) => [k,a.neutral]));
    this.sources = new Map();
    this.touched = new Set();
    this.last = new Map();
  }
  set(action, source, value, pressed = true) {
    const spec = this.module.actions[action];
    if (!spec || !Number.isFinite(value)) return;
    if (!spec.reset_on_release) {
      if (pressed) this.values[action] = Math.min(spec.max, Math.max(spec.min, value));
      else return;
    } else {
      const sources = this.sources.get(action) ?? new Map();
      if (pressed) sources.set(source, value); else sources.delete(source);
      this.sources.set(action, sources);
      const sum = [...sources.values()].reduce((a,b) => a+b, spec.neutral);
      this.values[action] = Math.min(spec.max, Math.max(spec.min, sum));
    }
    this.touched.add(action);
  }
  flush(now = performance.now()) {
    for (const command of this.module.commands) {
      const actions = Object.values(command.fields);
      const last = this.last.get(command.endpoint);
      if (!last && !actions.some(a => this.touched.has(a))) continue;
      const payload = Object.fromEntries(Object.entries(command.fields).map(([field,a]) => [field,this.values[a]]));
      const signature = JSON.stringify(payload);
      const active = actions.some(a => (this.module.actions[a].reset_on_release || this.module.endpoints?.[command.endpoint]?.resource) && this.values[a] !== this.module.actions[a].neutral);
      if (!last || signature !== last.signature || (active && command.repeat_ms > 0 && now-last.time >= command.repeat_ms)) {
        this.send(command, payload);
        this.last.set(command.endpoint, {signature, time:now});
      }
    }
    this.touched.clear();
  }
  reset(publish = true) {
    this.sources.clear();
    for (const [a,spec] of Object.entries(this.module.actions)) {
      if (spec.reset_on_release || this.module.commands.some(c=>this.module.endpoints?.[c.endpoint]?.resource && Object.values(c.fields).includes(a))) this.values[a] = spec.neutral;
    }
    for (const command of this.module.commands) {
      if (!this.module.endpoints?.[command.endpoint]?.resource && !Object.values(command.fields).some(a => this.module.actions[a].reset_on_release)) continue;
      if (publish && this.last.has(command.endpoint)) {
        this.send(command, Object.fromEntries(Object.entries(command.fields).map(([f,a]) => [f,this.values[a]])));
      }
      this.last.delete(command.endpoint);
    }
    this.touched.clear();
  }
}
