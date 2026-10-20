import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {InputEngine,mappedValue} from '../web-interface/module-assets/input-engine.mjs';

function fixture(path) {
  const module=JSON.parse(readFileSync(new URL(path,import.meta.url)));
  const calls=[];
  return {engine:new InputEngine(module,(c,p)=>calls.push({endpoint:c.endpoint,payload:p})),calls};
}
test('opening controls publishes nothing; opposing keys cancel and releases neutralize',()=>{
  const {engine:e,calls}=fixture('../modules/drive/module.json');
  e.flush(0);assert.equal(calls.length,0);
  e.set('forward','W',1);e.flush(10);assert.deepEqual(calls.at(-1).payload,{x:0,z:1});
  e.set('forward','S',-1);e.flush(20);assert.deepEqual(calls.at(-1).payload,{x:0,z:0});
  e.set('forward','S',0,false);e.flush(30);assert.equal(calls.at(-1).payload.z,1);
  e.set('forward','W',0,false);e.flush(40);assert.equal(calls.at(-1).payload.z,0);
});
test('hold commands repeat at their declared cadence; reset sends one neutral release',()=>{
  const {engine:e,calls}=fixture('../modules/drive/module.json');
  e.set('turn','axis',.5);e.flush(0);e.flush(249);assert.equal(calls.length,1);
  e.flush(250);assert.equal(calls.length,2);
  e.reset();assert.deepEqual(calls.at(-1).payload,{x:0,z:0});
  e.reset();e.flush(1000);assert.equal(calls.length,3);
});
test('latched custom actions publish zero, retain position on release, and never repeat',()=>{
  const {engine:e,calls}=fixture('../robots/goob/modules/gripper-demo/module.json');
  e.set('position','close',0);e.flush(0);assert.deepEqual(calls.at(-1).payload,{position:0});
  e.set('position','open',1);e.flush(1);e.set('position','open',0,false);e.flush(2);e.flush(1000);
  assert.deepEqual(calls.at(-1).payload,{position:1});assert.equal(calls.length,2);
  e.reset();assert.equal(e.values.position,1);assert.equal(calls.length,2);
});
test('axis transforms apply dead zone, inversion and offset; invalid values are ignored',()=>{
  assert.equal(mappedValue({deadzone:.1,scale:-1},.05),0);
  assert.equal(mappedValue({scale:.5,offset:.5},-1),0);
  assert.equal(mappedValue({scale:.5,offset:.5},1),1);
  const {engine:e,calls}=fixture('../modules/drive/module.json');
  e.set('forward','bad',NaN);e.set('missing','bad',1);e.flush(0);assert.equal(calls.length,0);
});
