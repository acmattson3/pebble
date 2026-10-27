import * as THREE from 'three';
import { GLTFLoader } from './vendor/three-r186/GLTFLoader.js';

function el(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function disposeObject(root) {
  root.traverse(object => {
    object.geometry?.dispose();
    const materials = Array.isArray(object.material) ? object.material : [object.material];
    for (const material of materials) {
      if (!material) continue;
      for (const value of Object.values(material)) {
        if (value?.isTexture) value.dispose();
      }
      material.dispose();
    }
  });
}

function resolveModelUrl(raw) {
  const url = new URL(raw, window.location.href);
  if (url.username || url.password) throw new Error('Model URL cannot contain credentials');
  if (raw.startsWith('//')) throw new Error('Use an HTTPS model URL or a same-origin path');
  if (url.origin !== window.location.origin && url.protocol !== 'https:') {
    throw new Error('Remote model URLs must use HTTPS');
  }
  return url.href;
}

async function fetchSelfContainedGlb(url, signal) {
  const response = await fetch(url, { signal, credentials: 'omit' });
  if (!response.ok) throw new Error(`Model request failed (HTTP ${response.status})`);
  const declaredLength = Number(response.headers.get('content-length'));
  const maxBytes = 16 * 1024 * 1024;
  if (Number.isFinite(declaredLength) && declaredLength > maxBytes) throw new Error('Model exceeds the 16 MiB limit');
  const data = await response.arrayBuffer();
  if (data.byteLength > maxBytes) throw new Error('Model exceeds the 16 MiB limit');

  const view = new DataView(data);
  if (data.byteLength < 20 || view.getUint32(0, true) !== 0x46546c67 || view.getUint32(4, true) !== 2) {
    throw new Error('Expected a binary glTF 2.0 (.glb) file');
  }
  const jsonLength = view.getUint32(12, true);
  if (view.getUint32(16, true) !== 0x4e4f534a || 20 + jsonLength > data.byteLength) {
    throw new Error('Model has an invalid GLB JSON chunk');
  }
  const json = new TextDecoder().decode(new Uint8Array(data, 20, jsonLength)).replace(/\0+\s*$/, '');
  const document = JSON.parse(json);
  if ((document.buffers ?? []).some(buffer => buffer.uri)
      || (document.images ?? []).some(image => image.uri)) {
    throw new Error('GLB must include its geometry and materials in one file');
  }
  return data;
}

export class OrientationWidget {
  constructor(widget, onCalibrate = null) {
    this.widget = widget;
    this.onCalibrate = onCalibrate;
    this.destroyed = false;
    this.model = null;
    this.latestAngles = null;
    this.hasYaw = false;
    this.orientationReference = new THREE.Quaternion();
    this.currentOrientation = new THREE.Quaternion();
    this.hasOrientationReference = false;
    this.calibrationGeneration = null;
    this.calibrationPending = false;
    this.gravityMps2 = null;
    this.accelerationDynamic = [0, 0, 0];
    this.accelerationTarget = [0, 0, 0];
    this.accelerationBall = [0, 0, 0];
    this.accelerationVelocity = [0, 0, 0];
    this.lastAccelerationAt = null;
    this.lastFrameAt = null;
    this.abortController = new AbortController();

    this.node = el('section', undefined, 'orientation-widget');
    this.title = el('strong', widget.label, 'orientation-title');
    this.stage = el('div', undefined, 'orientation-stage');
    this.visuals = el('div', undefined, 'orientation-visuals');
    this.modelView = el('div', undefined, 'orientation-model-view');
    this.canvas = el('canvas', undefined, 'orientation-canvas');
    this.canvas.setAttribute('role', 'img');
    this.canvas.setAttribute('aria-label', `${widget.label} 3D model orientation`);
    this.canvas.dataset.modelLoaded = 'false';
    this.modelView.append(this.canvas);
    this.motionPanel = el('section', undefined, 'orientation-motion-panel');
    this.motionTitle = el('strong', 'Motion cue · dynamic acceleration', 'orientation-motion-title');
    this.motionCanvas = el('canvas', undefined, 'orientation-motion-canvas');
    this.motionCanvas.setAttribute('role', 'img');
    this.motionCanvas.setAttribute('aria-label', 'Body orientation axes and spring-damped acceleration cue');
    this.motionValues = el('span', 'Waiting for acceleration data…', 'orientation-motion-values');
    this.motionPanel.append(this.motionTitle, this.motionCanvas, this.motionValues);
    this.visuals.append(this.modelView, this.motionPanel);
    this.status = el('p', 'Preparing 3D view…', 'orientation-status');
    this.actions = el('div', undefined, 'orientation-actions');
    if (widget.command_endpoint) {
      this.calibrateGyro = el('button', 'Calibrate gyro');
      this.calibrateGyro.type = 'button';
      this.calibrateGyro.setAttribute('aria-label', `Recalibrate ${widget.label} gyroscope bias`);
      this.calibrateGyro.addEventListener('click', () => {
        if (this.calibrationPending || !this.onCalibrate?.()) return;
        this.calibrationBaseline = this.calibrationGeneration;
        this.calibrationPending = true;
        this.calibrationDeadline = performance.now() + 10000;
        this.calibrateGyro.disabled = true;
        this.setStatus('Calibrating gyro · keep the robot completely still');
      });
      this.actions.append(this.calibrateGyro);
    }
    this.reset = el('button', 'Reset view');
    this.reset.type = 'button';
    this.reset.setAttribute('aria-label', `Reset ${widget.label} camera view`);
    this.resetOrientation = el('button', 'Reset orientation');
    this.resetOrientation.type = 'button';
    this.resetOrientation.disabled = true;
    this.resetOrientation.setAttribute('aria-label', `Set the current ${widget.label} pose as the viewer's zero orientation`);
    this.actions.append(this.resetOrientation, this.reset);
    this.footer = el('div', undefined, 'orientation-footer');
    this.footer.append(this.status, this.actions);
    this.stage.append(this.visuals, this.footer);
    this.node.append(this.title, this.stage);

    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x111e27);
    this.camera = new THREE.PerspectiveCamera(34, 1, 0.01, 100);
    this.renderer = null;
    this.orientationRoot = new THREE.Group();
    this.orientationAxes = new THREE.AxesHelper(0.72);
    this.orientationRoot.add(this.orientationAxes);
    this.modelFrame = new THREE.Group();
    const modelRotation = widget.model_rotation_deg ?? {};
    const toRadians = THREE.MathUtils.degToRad;
    this.modelFrame.rotation.set(
      toRadians(modelRotation.x ?? 0),
      toRadians(modelRotation.y ?? 0),
      toRadians(modelRotation.z ?? 0),
      'ZYX',
    );
    this.modelAlignment = this.modelFrame.quaternion.clone();
    this.inverseModelAlignment = this.modelAlignment.clone().invert();
    this.relativeOrientation = new THREE.Quaternion();
    this.displayOrientation = new THREE.Quaternion();
    this.orientationAxes.quaternion.copy(this.modelAlignment);
    this.orientationRoot.add(this.modelFrame);
    this.scene.add(this.orientationRoot);
    this.scene.add(new THREE.HemisphereLight(0xe6f3ff, 0x24303a, 2.2));
    const keyLight = new THREE.DirectionalLight(0xffffff, 2.5);
    keyLight.position.set(3, 5, 6);
    this.scene.add(keyLight);

    this.cameraState = { theta: 0.7, phi: 1.02, radius: 3.5 };
    this.setCamera = () => {
      const { theta, phi, radius } = this.cameraState;
      this.camera.position.set(
        radius * Math.sin(phi) * Math.sin(theta),
        radius * Math.cos(phi),
        radius * Math.sin(phi) * Math.cos(theta),
      );
      this.camera.lookAt(0, 0, 0);
      this.draw();
    };
    this.reset.addEventListener('click', () => {
      this.cameraState = { theta: 0.7, phi: 1.02, radius: 3.5 };
      this.setCamera();
    });
    this.resetOrientation.addEventListener('click', () => {
      if (!this.latestAngles) return;
      this.orientationReference.copy(this.currentOrientation);
      this.hasOrientationReference = true;
      this.applyOrientation();
      this.setStatus(this.liveStatus(this.latestAngles, this.hasYaw));
      this.draw();
    });
    this.attachCameraGestures();

    try {
      this.renderer = new THREE.WebGLRenderer({ canvas: this.canvas, antialias: true, alpha: false });
      this.renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
      this.renderer.outputColorSpace = THREE.SRGBColorSpace;
      this.setCamera();
      this.resizeObserver = new ResizeObserver(() => this.resize());
      this.resizeObserver.observe(this.visuals);
      this.resizeObserver.observe(this.modelView);
      this.resizeObserver.observe(this.motionPanel);
      this.loadModel();
    } catch (error) {
      this.setStatus(`3D view unavailable: ${error.message}`, true);
    }
    this.animateMotion = now => {
      if (this.destroyed) return;
      const dt = this.lastFrameAt == null ? 1 / 60 : THREE.MathUtils.clamp((now - this.lastFrameAt) / 1000, 0, 0.05);
      this.lastFrameAt = now;
      for (let i = 0; i < 3; i += 1) {
        this.accelerationVelocity[i] += ((this.accelerationTarget[i] - this.accelerationBall[i]) * 42
          - this.accelerationVelocity[i] * 12) * dt;
        this.accelerationBall[i] += this.accelerationVelocity[i] * dt;
      }
      this.drawMotion();
      this.motionFrame = requestAnimationFrame(this.animateMotion);
    };
    this.motionFrame = requestAnimationFrame(this.animateMotion);
  }

  attachCameraGestures() {
    let pointer = null;
    let previous = null;
    this.canvas.addEventListener('pointerdown', event => {
      if (event.button !== 0) return;
      pointer = event.pointerId;
      previous = { x: event.clientX, y: event.clientY };
      this.canvas.setPointerCapture(pointer);
    });
    this.canvas.addEventListener('pointermove', event => {
      if (event.pointerId !== pointer || !previous) return;
      this.cameraState.theta -= (event.clientX - previous.x) * 0.008;
      this.cameraState.phi = THREE.MathUtils.clamp(
        this.cameraState.phi + (event.clientY - previous.y) * 0.008,
        0.12,
        Math.PI - 0.12,
      );
      previous = { x: event.clientX, y: event.clientY };
      this.setCamera();
    });
    const release = () => { pointer = null; previous = null; };
    this.canvas.addEventListener('pointerup', release);
    this.canvas.addEventListener('pointercancel', release);
    this.canvas.addEventListener('wheel', event => {
      event.preventDefault();
      this.cameraState.radius = THREE.MathUtils.clamp(
        this.cameraState.radius * Math.exp(event.deltaY * 0.001),
        1.5,
        10,
      );
      this.setCamera();
    }, { passive: false });
  }

  setStatus(text, error = false) {
    this.status.textContent = text;
    this.status.classList.toggle('error', error);
  }

  async loadModel() {
    try {
      const url = resolveModelUrl(this.widget.model_url);
      if (!/\.glb(?:$|[?#])/i.test(url)) {
        throw new Error('Use a single-file .glb model');
      }
      this.setStatus('Loading model…');
      const data = await fetchSelfContainedGlb(url, this.abortController.signal);
      const gltf = await new GLTFLoader().parseAsync(data, new URL('.', url).href);
      if (this.destroyed) {
        disposeObject(gltf.scene);
        return;
      }
      const bounds = new THREE.Box3().setFromObject(gltf.scene);
      const size = bounds.getSize(new THREE.Vector3());
      const largest = Math.max(size.x, size.y, size.z);
      if (!Number.isFinite(largest) || largest <= 0) throw new Error('Model has no visible geometry');
      const center = bounds.getCenter(new THREE.Vector3());
      gltf.scene.position.sub(center);
      gltf.scene.scale.setScalar(1.6 / largest);
      this.modelFrame.add(gltf.scene);
      this.model = gltf.scene;
      this.canvas.dataset.modelLoaded = 'true';
      this.setStatus(this.latestAngles ? this.liveStatus(this.latestAngles, this.hasYaw) : 'Model ready · waiting for orientation');
      this.draw();
    } catch (error) {
      if (!this.destroyed) {
        this.canvas.dataset.modelLoaded = 'error';
        this.setStatus(`Could not load model: ${error.message}`, true);
      }
    }
  }

  resize() {
    if (!this.renderer || this.destroyed) return;
    const width = Math.max(1, this.modelView.clientWidth);
    const height = Math.max(1, this.modelView.clientHeight);
    this.renderer.setSize(width, height, false);
    this.camera.aspect = width / height;
    this.camera.updateProjectionMatrix();
    this.draw();
  }

  update(payload) {
    this.updateAcceleration(payload?.accel_mps2);
    const generation = Number(payload?.health?.calibration_generation);
    if (Number.isFinite(generation)) {
      this.calibrationGeneration = generation;
      if (this.calibrationPending && generation !== this.calibrationBaseline) {
        this.calibrationPending = false;
        this.calibrateGyro.disabled = false;
        const biasZ = Number(payload?.health?.gyro_bias_dps?.z);
        this.calibrationResult = Number.isFinite(biasZ)
          ? `Gyro calibrated · Z bias ${biasZ.toFixed(3)}°/s`
          : 'Gyro calibration complete';
        this.calibrationNoticeUntil = performance.now() + 5000;
      }
    }
    if (this.calibrationPending && performance.now() > this.calibrationDeadline) {
      this.calibrationPending = false;
      this.calibrateGyro.disabled = false;
      this.calibrationResult = 'No calibration response · check the MCU connection';
      this.calibrationResultError = true;
      this.calibrationNoticeUntil = performance.now() + 5000;
    }
    const angles = payload?.orientation_deg;
    const roll = Number(angles?.roll);
    const pitch = Number(angles?.pitch);
    const yaw = angles?.yaw == null ? 0 : Number(angles.yaw);
    if (![roll, pitch, yaw].every(Number.isFinite)) {
      this.latestAngles = null;
      this.hasYaw = false;
      this.resetOrientation.disabled = true;
      this.setStatus(this.model ? 'Waiting for valid orientation' : this.status.textContent);
      return;
    }
    this.latestAngles = { roll, pitch, yaw };
    this.hasYaw = angles.yaw != null;
    this.resetOrientation.disabled = false;
    const toRadians = THREE.MathUtils.degToRad;
    // Robot convention: roll about X, pitch about Y, yaw about Z.
    this.currentOrientation.setFromEuler(new THREE.Euler(
      toRadians(roll), toRadians(pitch), toRadians(yaw), 'ZYX',
    ));
    this.applyOrientation();
    if (this.calibrationPending) {
      this.setStatus('Calibrating gyro · keep the robot completely still');
    } else if (this.calibrationNoticeUntil > performance.now()) {
      this.setStatus(this.calibrationResult, this.calibrationResultError);
    } else {
      this.calibrationResultError = false;
      this.setStatus(this.liveStatus(this.latestAngles, this.hasYaw));
    }
    this.draw();
  }

  applyOrientation() {
    this.relativeOrientation.copy(this.currentOrientation);
    if (this.hasOrientationReference) {
      this.relativeOrientation.premultiply(this.orientationReference.clone().invert());
    }
    // Convert the sensor orientation into the same axis frame used to align
    // the GLB. Without this basis change, a Z-up sensor rendered in a Y-up
    // model frame makes yaw appear as pitch (and vice versa).
    this.orientationRoot.quaternion.copy(this.modelAlignment)
      .multiply(this.relativeOrientation)
      .multiply(this.inverseModelAlignment);
    this.displayOrientation.copy(this.orientationRoot.quaternion).multiply(this.modelAlignment);
  }

  updateAcceleration(acceleration) {
    const sample = [Number(acceleration?.x), Number(acceleration?.y), Number(acceleration?.z)];
    if (!sample.every(Number.isFinite)) return;
    const now = performance.now();
    const dt = this.lastAccelerationAt == null ? 0.125 : THREE.MathUtils.clamp((now - this.lastAccelerationAt) / 1000, 0.001, 0.5);
    this.lastAccelerationAt = now;
    if (!this.gravityMps2) this.gravityMps2 = [...sample];
    const alpha = 1 - Math.exp(-dt / 1.25);
    for (let i = 0; i < 3; i += 1) {
      this.gravityMps2[i] += alpha * (sample[i] - this.gravityMps2[i]);
      this.accelerationDynamic[i] = sample[i] - this.gravityMps2[i];
      this.accelerationTarget[i] = THREE.MathUtils.clamp(this.accelerationDynamic[i] / (2 * 9.80665), -1, 1);
    }
    this.motionValues.textContent = `Δa ${this.accelerationDynamic.map(value => `${value >= 0 ? '+' : ''}${value.toFixed(2)}`).join(', ')} m/s²`;
  }

  drawMotion() {
    const canvas = this.motionCanvas;
    const ctx = canvas.getContext('2d');
    if (!ctx || this.destroyed) return;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(1, rect.width);
    const height = Math.max(1, rect.height);
    const pixelRatio = Math.min(window.devicePixelRatio || 1, 2);
    const pixelWidth = Math.round(width * pixelRatio);
    const pixelHeight = Math.round(height * pixelRatio);
    if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {
      canvas.width = pixelWidth;
      canvas.height = pixelHeight;
    }
    ctx.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
    ctx.clearRect(0, 0, width, height);
    const axisCenter = [width / 2, height * 0.28];
    const ballCenter = [width / 2, height * 0.75];
    const axisScale = Math.min(width * 0.2, height * 0.25);
    const accelScale = Math.min(width * 0.21, height * 0.19);
    const colors = ['#ff6878', '#6ce0a0', '#69adff'];
    const axes = [[1, 0, 0], [0, 1, 0], [0, 0, 1]];
    const project = (center, vector, scale) => [
      center[0] + scale * (vector[0] - vector[1]) * 0.7071,
      center[1] + scale * ((vector[0] + vector[1]) * 0.4082 - vector[2]),
    ];
    const arrow = (start, end, color, lineWidth = 2, dashed = false) => {
      const angle = Math.atan2(end[1] - start[1], end[0] - start[0]);
      ctx.beginPath();
      ctx.setLineDash(dashed ? [3, 4] : []);
      ctx.moveTo(...start);
      ctx.lineTo(...end);
      ctx.strokeStyle = color;
      ctx.lineWidth = lineWidth;
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.beginPath();
      ctx.moveTo(...end);
      ctx.lineTo(end[0] - 8 * Math.cos(angle - 0.45), end[1] - 8 * Math.sin(angle - 0.45));
      ctx.lineTo(end[0] - 8 * Math.cos(angle + 0.45), end[1] - 8 * Math.sin(angle + 0.45));
      ctx.closePath();
      ctx.fillStyle = color;
      ctx.fill();
    };

    axes.forEach((vector, index) => {
      const reference = project(axisCenter, vector, axisScale * 0.7);
      arrow(axisCenter, reference, '#40536a', 1, true);
      const rotated = new THREE.Vector3(...vector).applyQuaternion(this.displayOrientation);
      const endpoint = project(axisCenter, [rotated.x, rotated.y, rotated.z], axisScale);
      arrow(axisCenter, endpoint, colors[index], 3);
      ctx.fillStyle = colors[index];
      ctx.font = 'bold 11px sans-serif';
      ctx.fillText(['X', 'Y', 'Z'][index], endpoint[0] + 7, endpoint[1] - 6);
    });

    axes.forEach((vector, index) => {
      const end = project(ballCenter, vector, accelScale);
      const opposite = project(ballCenter, vector.map(value => -value), accelScale);
      ctx.beginPath();
      ctx.moveTo(...opposite);
      ctx.lineTo(...end);
      ctx.strokeStyle = '#40536a';
      ctx.lineWidth = 1;
      ctx.stroke();
      ctx.fillStyle = colors[index];
      ctx.font = 'bold 10px sans-serif';
      ctx.fillText(['X', 'Y', 'Z'][index], end[0] + 5, end[1] - 5);
    });
    const ball = project(ballCenter, this.accelerationBall, accelScale);
    ctx.beginPath();
    ctx.moveTo(...ballCenter);
    ctx.lineTo(...ball);
    ctx.strokeStyle = '#ffd166';
    ctx.lineWidth = 3;
    ctx.stroke();
    ctx.beginPath();
    ctx.arc(ball[0], ball[1], 7, 0, Math.PI * 2);
    ctx.fillStyle = '#ffd166';
    ctx.fill();
    ctx.strokeStyle = '#fff1bd';
    ctx.lineWidth = 2;
    ctx.stroke();
  }

  liveStatus({ roll, pitch, yaw }, hasYaw = false) {
    const reference = this.hasOrientationReference ? ' · viewer zero set' : '';
    return `Live · roll ${roll.toFixed(1)}° · pitch ${pitch.toFixed(1)}°${hasYaw ? ` · yaw ${yaw.toFixed(1)}°` : ''}${reference}`;
  }

  draw() {
    if (this.renderer && !this.destroyed) this.renderer.render(this.scene, this.camera);
  }

  destroy() {
    this.destroyed = true;
    this.abortController.abort();
    this.resizeObserver?.disconnect();
    if (this.motionFrame != null) cancelAnimationFrame(this.motionFrame);
    if (this.model) disposeObject(this.model);
    if (this.orientationAxes) disposeObject(this.orientationAxes);
    this.renderer?.dispose();
  }
}
