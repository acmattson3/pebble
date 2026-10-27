#define PIN_LED_B   D0
#define PIN_LED_G   D1
#define PIN_LED_R   D2

#include <Arduino.h>
#include <Wire.h>
#include <math.h>
#if defined(ARDUINO_ARCH_ESP32)
#include <esp_system.h>
#include <esp_task_wdt.h>
#endif

// XIAO ESP32-C6 only exposes ADC on D0-D2, which are Goob's RGB output
// channels. The battery divider is wired to the SAMD21's D6, which is not an
// ADC pin on the C6. Do not report a bogus voltage until the divider is
// rewired to a dedicated ADC input.
#if defined(ARDUINO_XIAO_ESP32C6)
#define PIN_BATT    (-1)
#else
#define PIN_BATT    D6
#endif

#define PIN_L_FWD   D7
#define PIN_L_BWD   D8
#define PIN_R_FWD   D9
#define PIN_R_BWD   D10

enum LedMode { LED_OFF, LED_STATIC, LED_FADE };

const float BATTERY_DIVIDER_RATIO = 2.0f;
const float BATTERY_ADC_REF = 3.3f;
#if defined(ARDUINO_ARCH_ESP32)
const float BATTERY_ADC_MAX = 4095.0f;
#else
// XIAO SAMD21/M0 defaults to 10-bit ADC reads unless the sketch changes
// resolution explicitly.
const float BATTERY_ADC_MAX = 1023.0f;
#endif

struct {
  LedMode mode = LED_OFF;
  uint8_t targetB = 0, targetG = 0, targetR = 0;
  uint8_t staticB = 0, staticG = 0, staticR = 0;
  unsigned long startMs = 0;
  unsigned long periodMs = 1000;
} g_led;

static inline uint8_t clamp8(int v) {
  if (v < 0) return 0;
  if (v > 255) return 255;
  return (uint8_t)v;
}

static inline float clampf(float v, float lo, float hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

void setLedRawBGR(uint8_t b, uint8_t g, uint8_t r) {
  analogWrite(PIN_LED_B, b);
  analogWrite(PIN_LED_G, g);
  analogWrite(PIN_LED_R, r);
}

void setLedFloatBGR(float b, float g, float r) {
  uint8_t B = clamp8((int)(clampf(b, 0.0f, 1.0f) * 255.0f + 0.5f));
  uint8_t G = clamp8((int)(clampf(g, 0.0f, 1.0f) * 255.0f + 0.5f));
  uint8_t R = clamp8((int)(clampf(r, 0.0f, 1.0f) * 255.0f + 0.5f));
  setLedRawBGR(B, G, R);
}

void driveOneMotor(float val, uint8_t pinFwd, uint8_t pinBwd) {
  val = clampf(val, -1.0f, 1.0f);
  if (val >= 0.0f) {
    uint8_t pwm = clamp8((int)(val * 255.0f + 0.5f));
    analogWrite(pinFwd, pwm);
    analogWrite(pinBwd, 0);
  } else {
    uint8_t pwm = clamp8((int)((-val) * 255.0f + 0.5f));
    analogWrite(pinFwd, 0);
    analogWrite(pinBwd, pwm);
  }
}

// Wiring compensation flags.
const bool INVERT_LEFT_MOTOR = false;
const bool INVERT_RIGHT_MOTOR = true;

void driveMotors(float left, float right) {
  if (INVERT_LEFT_MOTOR) left = -left;
  if (INVERT_RIGHT_MOTOR) right = -right;
  driveOneMotor(left,  PIN_L_FWD, PIN_L_BWD);
  driveOneMotor(right, PIN_R_FWD, PIN_R_BWD);
}

static float triangle01(unsigned long elapsed, unsigned long period) {
  if (period == 0) return 0.0f;
  unsigned long t = elapsed % period;
  float x = (float)t / (float)period;
  float y = 1.0f - fabsf(2.0f * x - 1.0f);
  return clampf(y, 0.0f, 1.0f);
}

void updateLedFade() {
  if (g_led.mode != LED_FADE) return;
  unsigned long now = millis();
  float k = triangle01(now - g_led.startMs, g_led.periodMs);
  uint8_t B = (uint8_t)(g_led.targetB * k + 0.5f);
  uint8_t G = (uint8_t)(g_led.targetG * k + 0.5f);
  uint8_t R = (uint8_t)(g_led.targetR * k + 0.5f);
  setLedRawBGR(B, G, R);
}

// --------- idle handling state ----------
unsigned long lastMotorCmdMs = 0;
unsigned long lastLedCmdMs   = 0;
bool motorsTimeoutApplied    = false;
bool ledIsDefault            = false;

// --------- telemetry state -------------
unsigned long lastBatteryTelemetryMs = 0;  // last time we printed slower battery telemetry
const unsigned long BATTERY_TELEMETRY_PERIOD_MS = 5000;
static inline float readBatteryVoltage();
static inline void emitBatteryTelemetry(float volts);

// Goob's former encoder inputs D4/D5 now host the onboard MPU-6050 I2C bus.
// D3 is no longer sampled. Keep this legacy ASCII drive protocol so the
// existing serial_mcu_bridge continues to own MQTT and motor commands.
const uint8_t MPU_ADDR = 0x68;
const uint8_t MPU_WHO_AM_I = 0x75;
const uint8_t MPU_PWR_MGMT_1 = 0x6B;
const uint8_t MPU_CONFIG = 0x1A;
const uint8_t MPU_SMPLRT_DIV = 0x19;
const uint8_t MPU_GYRO_CONFIG = 0x1B;
const uint8_t MPU_ACCEL_CONFIG = 0x1C;
const uint8_t MPU_ACCEL_XOUT_H = 0x3B;
const unsigned long IMU_PERIOD_US = 10000UL;
const unsigned long IMU_SUMMARY_PERIOD_MS = 125UL; // 8 Hz summary telemetry
const float ACCEL_LSB_PER_G = 16384.0f;
const float GYRO_LSB_PER_DPS = 131.0f;
const float GRAVITY_MPS2 = 9.80665f;
float gyroBias[3] = {0, 0, 0};
float filteredAccel[3] = {0, 0, 0};
float filteredGyro[3] = {0, 0, 0};
float rollDeg = 0, pitchDeg = 0, yawDeg = 0;
float accelNormG = 1;
bool imuReady = false;
bool imuWireStarted = false;
bool imuFilterReady = false;
bool orientationReady = false;
uint32_t imuSequence = 0, imuSamplesOk = 0, imuSamplesError = 0;
uint16_t imuCalibrationSamples = 0;
uint32_t imuCalibrationGeneration = 0;
const uint16_t IMU_MANUAL_CALIBRATION_SAMPLES = 400;
bool imuCalibrating = false;
uint16_t imuCalibrationCollected = 0;
float imuCalibrationTotals[3] = {0, 0, 0};
unsigned long nextImuReadUs = 0, lastImuSummaryMs = 0, lastImuInitAttemptMs = 0;
uint32_t lastImuSampleUs = 0;
const char *imuFailureStage = "none";
uint8_t imuWireError = 0;
uint8_t imuWhoAmI = 0;
uint8_t imuBytesReceived = 0;
const unsigned long MOTOR_COMMAND_TIMEOUT_MS = 1000UL;

void configureWatchdog() {
#if defined(ARDUINO_ARCH_SAMD)
  // The SAMD21 Wire implementation can busy-wait forever if an I2C read
  // stalls. A watchdog reset is the independent escape path for that case.
  while (WDT->STATUS.bit.SYNCBUSY) {}
  WDT->CTRL.reg = 0;
  while (WDT->STATUS.bit.SYNCBUSY) {}
  WDT->CONFIG.reg = WDT_CONFIG_PER_1K; // about one second at 1.024 kHz
  while (WDT->STATUS.bit.SYNCBUSY) {}
  WDT->CTRL.reg = WDT_CTRL_ENABLE;
  while (WDT->STATUS.bit.SYNCBUSY) {}
  WDT->CLEAR.reg = WDT_CLEAR_CLEAR_KEY;
  while (WDT->STATUS.bit.SYNCBUSY) {}
#elif defined(ARDUINO_ARCH_ESP32)
  // Keep an independent task watchdog on the ESP32 loop task. I2C is given
  // its own transaction timeout below, while this catches any other hang.
  esp_task_wdt_config_t config = {};
  config.timeout_ms = 5000;
  config.idle_core_mask = 0;
  config.trigger_panic = true;
  esp_err_t err = esp_task_wdt_reconfigure(&config);
  if (err == ESP_ERR_INVALID_STATE) err = esp_task_wdt_init(&config);
  if (err == ESP_OK) {
    esp_task_wdt_add(NULL); // Already subscribed is harmless; feed below.
    esp_task_wdt_reset();
  }
#endif
}

void feedWatchdog() {
#if defined(ARDUINO_ARCH_SAMD)
  WDT->CLEAR.reg = WDT_CLEAR_CLEAR_KEY;
  while (WDT->STATUS.bit.SYNCBUSY) {}
#elif defined(ARDUINO_ARCH_ESP32)
  esp_task_wdt_reset();
#endif
}

bool mpuWrite(uint8_t reg, uint8_t value) {
  // The SAMD21 Wire core has unbounded SERCOM waits. Avoid entering it when
  // either line is already held low (including a disconnected/floating bus).
  if (digitalRead(SDA) == LOW || digitalRead(SCL) == LOW) {
    imuFailureStage = "bus-line-low";
    return false;
  }

  Wire.beginTransmission(MPU_ADDR);
  Wire.write(reg);
  Wire.write(value);
  imuWireError = Wire.endTransmission();
  if (imuWireError != 0) {
    imuFailureStage = "write";
    return false;
  }
  return true;
}

bool beginMpuBus() {
  // Match the AERO-Beam example's bus lifecycle: initialize Wire once and
  // leave it configured while the sensor is probed/retried. Enable internal
  // pull-ups first so an absent sensor leaves SDA/SCL high instead of
  // floating into a SERCOM busy-wait. They supplement Goob's external 4.7k
  // pull-ups when the IMU is connected.
  if (!imuWireStarted) {
    pinMode(SDA, INPUT_PULLUP);
    pinMode(SCL, INPUT_PULLUP);
    delayMicroseconds(10);
    if (digitalRead(SDA) == LOW || digitalRead(SCL) == LOW) {
      imuFailureStage = "bus-line-low";
      return false;
    }

    Wire.begin();
    Wire.setClock(100000UL);
#if defined(ARDUINO_ARCH_ESP32)
    Wire.setTimeOut(100);
#endif
    imuWireStarted = true;
  }

  if (digitalRead(SDA) == LOW || digitalRead(SCL) == LOW) {
    imuFailureStage = "bus-line-low";
    return false;
  }
  return true;
}

bool mpuRead(uint8_t reg, uint8_t *out, size_t count) {
  if (digitalRead(SDA) == LOW || digitalRead(SCL) == LOW) {
    imuFailureStage = "bus-line-low";
    return false;
  }

  Wire.beginTransmission(MPU_ADDR);
  Wire.write(reg);
  imuWireError = Wire.endTransmission(false);
  if (imuWireError != 0) {
    imuFailureStage = "register-address";
    return false;
  }
  imuBytesReceived = Wire.requestFrom((int)MPU_ADDR, (int)count, (int)true);
  if (imuBytesReceived != count) {
    imuFailureStage = "short-read";
    return false;
  }
  for (size_t i = 0; i < count; ++i) {
    if (!Wire.available()) {
      imuFailureStage = "fifo-empty";
      return false;
    }
    out[i] = (uint8_t)Wire.read();
  }
  return true;
}

int16_t mpuI16(const uint8_t *p) {
  return (int16_t)(((uint16_t)p[0] << 8) | p[1]);
}

bool readMpuRaw(int16_t *accel, int16_t *gyro, int16_t &temp) {
  uint8_t b[14];
  if (!mpuRead(MPU_ACCEL_XOUT_H, b, sizeof(b))) return false;
  accel[0] = mpuI16(b); accel[1] = mpuI16(b + 2); accel[2] = mpuI16(b + 4);
  temp = mpuI16(b + 6);
  gyro[0] = mpuI16(b + 8); gyro[1] = mpuI16(b + 10); gyro[2] = mpuI16(b + 12);
  return true;
}

bool initMpu6050() {
  uint8_t who = 0;
  if (!mpuRead(MPU_WHO_AM_I, &who, 1)) {
    imuFailureStage = "WHO_AM_I-read";
    return false;
  }
  imuWhoAmI = who;
  if (who != 0x68 && who != 0x69) {
    imuFailureStage = "WHO_AM_I-value";
    return false;
  }
  if (!mpuWrite(MPU_PWR_MGMT_1, 0x00)) { imuFailureStage = "PWR_MGMT_1"; return false; }
  delay(100);
  if (!mpuWrite(MPU_CONFIG, 3)) { imuFailureStage = "CONFIG"; return false; }
  if (!mpuWrite(MPU_SMPLRT_DIV, 9)) { imuFailureStage = "SMPLRT_DIV"; return false; }
  if (!mpuWrite(MPU_GYRO_CONFIG, 0)) { imuFailureStage = "GYRO_CONFIG"; return false; }
  if (!mpuWrite(MPU_ACCEL_CONFIG, 0)) { imuFailureStage = "ACCEL_CONFIG"; return false; }

  float totals[3] = {0, 0, 0};
  uint16_t count = 0;
  delay(100);
  while (count < 100) {
    int16_t a[3], g[3], t;
    if (!readMpuRaw(a, g, t)) { imuFailureStage = "calibration-sample"; return false; }
    for (uint8_t axis = 0; axis < 3; ++axis) totals[axis] += g[axis];
    ++count;
    delay(5);
    feedWatchdog();
  }
  for (uint8_t axis = 0; axis < 3; ++axis) gyroBias[axis] = totals[axis] / count;
  imuCalibrationSamples = count;
  ++imuCalibrationGeneration;
  imuFilterReady = orientationReady = false;
  yawDeg = 0.0f;
  imuReady = true;
  imuSamplesError = 0;
  nextImuReadUs = micros();
  lastImuSummaryMs = millis();
  Serial.println("INFO IMU MPU-6050 ready");
  return true;
}

void emitImuSample() {
  int16_t rawAccel[3], rawGyro[3], rawTemp;
  if (!readMpuRaw(rawAccel, rawGyro, rawTemp)) {
    ++imuSamplesError;
    imuReady = false;
    if (imuCalibrating) {
      imuCalibrating = false;
      Serial.println("ERR IMU recalibration aborted: sample read failed");
    }
    Serial.print("WARN IMU sample read failed stage="); Serial.print(imuFailureStage);
    Serial.print(" wire="); Serial.print(imuWireError);
    Serial.print(" bytes="); Serial.println(imuBytesReceived);
    return;
  }
  ++imuSamplesOk;
  ++imuSequence;
  const uint32_t nowUs = micros();
  const float dt = lastImuSampleUs ? (uint32_t)(nowUs - lastImuSampleUs) / 1000000.0f : 0.01f;
  lastImuSampleUs = nowUs;
  if (imuCalibrating) {
    for (uint8_t i = 0; i < 3; ++i) imuCalibrationTotals[i] += rawGyro[i];
    ++imuCalibrationCollected;
    if (imuCalibrationCollected >= IMU_MANUAL_CALIBRATION_SAMPLES) {
      for (uint8_t i = 0; i < 3; ++i) gyroBias[i] = imuCalibrationTotals[i] / imuCalibrationCollected;
      imuCalibrationSamples = imuCalibrationCollected;
      ++imuCalibrationGeneration;
      imuCalibrating = false;
      imuFilterReady = orientationReady = false;
      Serial.print("INFO IMU recalibration complete samples="); Serial.print(imuCalibrationSamples);
      Serial.print(" gbx="); Serial.print(gyroBias[0] / GYRO_LSB_PER_DPS, 3);
      Serial.print(" gby="); Serial.print(gyroBias[1] / GYRO_LSB_PER_DPS, 3);
      Serial.print(" gbz="); Serial.println(gyroBias[2] / GYRO_LSB_PER_DPS, 3);
    }
    return;
  }
  float a[3], g[3];
  for (uint8_t i = 0; i < 3; ++i) {
    a[i] = rawAccel[i] / ACCEL_LSB_PER_G;
    g[i] = (rawGyro[i] - gyroBias[i]) / GYRO_LSB_PER_DPS;
    if (!imuFilterReady) { filteredAccel[i] = a[i]; filteredGyro[i] = g[i]; }
    else {
      filteredAccel[i] += 0.25f * (a[i] - filteredAccel[i]);
      filteredGyro[i] += 0.25f * (g[i] - filteredGyro[i]);
    }
  }
  imuFilterReady = true;
  accelNormG = sqrtf(filteredAccel[0]*filteredAccel[0] + filteredAccel[1]*filteredAccel[1] + filteredAccel[2]*filteredAccel[2]);
  const float z = fabsf(filteredAccel[2]) > 1e-6f ? filteredAccel[2] : 1e-6f;
  const float rollAcc = atan2f(filteredAccel[1], z) * 57.2957795f;
  const float pitchAcc = atan2f(-filteredAccel[0], sqrtf(filteredAccel[1]*filteredAccel[1] + filteredAccel[2]*filteredAccel[2])) * 57.2957795f;
  if (!orientationReady) { rollDeg = rollAcc; pitchDeg = pitchAcc; orientationReady = true; }
  else {
    const float blend = accelNormG < 0.25f ? 1.0f : 0.98f;
    rollDeg = blend * (rollDeg + filteredGyro[0] * dt) + (1.0f - blend) * rollAcc;
    pitchDeg = blend * (pitchDeg + filteredGyro[1] * dt) + (1.0f - blend) * pitchAcc;
  }
  // The MPU-6050 has no magnetometer, so yaw is gyro-integrated and relative
  // to this initialization. It will drift over time and is not compass heading.
  yawDeg += filteredGyro[2] * dt;
  if (yawDeg > 180.0f) yawDeg -= 360.0f;
  else if (yawDeg < -180.0f) yawDeg += 360.0f;
  const float tempC = rawTemp / 340.0f + 36.53f;
  Serial.print("I n="); Serial.print(imuSequence); Serial.print(" ms="); Serial.print(millis());
  Serial.print(" ax="); Serial.print(filteredAccel[0] * GRAVITY_MPS2, 3);
  Serial.print(" ay="); Serial.print(filteredAccel[1] * GRAVITY_MPS2, 3);
  Serial.print(" az="); Serial.print(filteredAccel[2] * GRAVITY_MPS2, 3);
  Serial.print(" gx="); Serial.print(filteredGyro[0], 3);
  Serial.print(" gy="); Serial.print(filteredGyro[1], 3);
  Serial.print(" gz="); Serial.print(filteredGyro[2], 3);
  Serial.print(" t="); Serial.print(tempC, 2);
  Serial.print(" r="); Serial.print(rollDeg, 2);
  Serial.print(" p="); Serial.print(pitchDeg, 2);
  Serial.print(" yaw="); Serial.println(yawDeg, 2);

  const unsigned long nowMs = millis();
  if (nowMs - lastImuSummaryMs >= IMU_SUMMARY_PERIOD_MS) {
    lastImuSummaryMs = nowMs;
    Serial.print("S n="); Serial.print(imuSequence); Serial.print(" ms="); Serial.print(nowMs);
    Serial.print(" r="); Serial.print(rollDeg, 2); Serial.print(" p="); Serial.print(pitchDeg, 2);
    Serial.print(" yaw="); Serial.print(yawDeg, 2);
    Serial.print(" t="); Serial.print(tempC, 2); Serial.print(" an="); Serial.print(accelNormG, 3);
    Serial.print(" gbx="); Serial.print(gyroBias[0] / GYRO_LSB_PER_DPS, 3);
    Serial.print(" gby="); Serial.print(gyroBias[1] / GYRO_LSB_PER_DPS, 3);
    Serial.print(" gbz="); Serial.print(gyroBias[2] / GYRO_LSB_PER_DPS, 3);
    Serial.print(" ok="); Serial.print(imuSamplesOk); Serial.print(" err="); Serial.print(imuSamplesError);
    Serial.print(" cal="); Serial.print(imuCalibrationSamples);
    Serial.print(" gen="); Serial.println(imuCalibrationGeneration);
  }
}

void setDefaultLed() {
  g_led.mode = LED_FADE;
  g_led.targetB = 0;
  g_led.targetG = 0;
  g_led.targetR = 255;         // flashing red
  g_led.periodMs = 2000;       // 2s full in+out period
  g_led.startMs = millis();
  ledIsDefault = true;
}

void setup() {
#if defined(ARDUINO_ARCH_SAMD)
  const uint8_t resetCause = PM->RCAUSE.reg;
  const bool watchdogReset = (resetCause & PM_RCAUSE_WDT) != 0;
#elif defined(ARDUINO_ARCH_ESP32)
  const esp_reset_reason_t resetCause = esp_reset_reason();
  const bool watchdogReset = resetCause == ESP_RST_TASK_WDT ||
                             resetCause == ESP_RST_INT_WDT ||
                             resetCause == ESP_RST_WDT;
#else
  const bool watchdogReset = false;
#endif

  // Latch motor outputs low immediately on every boot/reset before doing
  // serial, sensor, or battery setup.
  digitalWrite(PIN_L_FWD, 0);
  pinMode(PIN_L_FWD, OUTPUT);
  digitalWrite(PIN_L_BWD, 0);
  pinMode(PIN_L_BWD, OUTPUT);
  digitalWrite(PIN_R_FWD, 0);
  pinMode(PIN_R_FWD, OUTPUT);
  digitalWrite(PIN_R_BWD, 0);
  pinMode(PIN_R_BWD, OUTPUT);

  configureWatchdog();
  Serial.begin(460800);
  Serial.println("Starting setup...");
  if (watchdogReset) {
    Serial.println("WARN MCU restarted by watchdog; motor outputs forced off");
  }

  pinMode(PIN_LED_B, OUTPUT);
  digitalWrite(PIN_LED_B, 0);
  pinMode(PIN_LED_G, OUTPUT);
  digitalWrite(PIN_LED_G, 0);
  pinMode(PIN_LED_R, OUTPUT);
  digitalWrite(PIN_LED_R, 0);

  if (PIN_BATT >= 0) pinMode(PIN_BATT, INPUT);
#if defined(ARDUINO_ARCH_ESP32)
  analogReadResolution(12);
  if (PIN_BATT >= 0) analogSetPinAttenuation(PIN_BATT, ADC_11db);
#endif

  setDefaultLed();
  const unsigned long now = millis();
  lastMotorCmdMs = now;
  motorsTimeoutApplied = true;
  lastLedCmdMs = now;
  lastBatteryTelemetryMs = now;
  emitBatteryTelemetry(readBatteryVoltage());

  // D4/D5 are the XIAO SAMD21 hardware I2C pins. Leave their electrical
  // configuration to Wire; the MPU-6050 bus uses external 3.3 V pull-ups.
  lastImuInitAttemptMs = millis();
  if (!beginMpuBus() || !initMpu6050()) {
    Serial.print("WARN IMU unavailable stage=");
    Serial.println(imuFailureStage);
  }

  feedWatchdog();
  Serial.println("Done!");
}

String buffer;

static inline float readBatteryVoltage() {
#if defined(ARDUINO_XIAO_ESP32C6)
  return NAN;
#else
  const int samples = 8;
  uint32_t total = 0;
  for (int i = 0; i < samples; i++) {
    total += analogRead(PIN_BATT);
    delay(2);
  }
  float raw = (float)total / (float)samples;
#if defined(ARDUINO_ARCH_ESP32)
  uint32_t mv = analogReadMilliVolts(PIN_BATT);
  (void)raw;
  return (mv / 1000.0f) * BATTERY_DIVIDER_RATIO;
#else
  return (raw / BATTERY_ADC_MAX) * BATTERY_ADC_REF * BATTERY_DIVIDER_RATIO;
#endif
#endif
}

static inline void emitBatteryTelemetry(float volts) {
  // `chg=-1` indicates the dedicated charge-detect line is no longer wired,
  // while `v=` carries the divided battery reading from D6.
  Serial.print("C ");
  Serial.print("chg=-1");
#if !defined(ARDUINO_XIAO_ESP32C6)
  Serial.print(" v=");
  Serial.print(volts, 3);
#endif
  Serial.println();
}

void loop() {
  while (Serial.available()>0) {
    int c=Serial.read();
    if (c==-1) break;
    if (c=='\n') {
      handleCommand(buffer);
      buffer="";
    } else if (c!='\r') {
      buffer+=(char)c;
    }
  }

  updateLedFade();

  // Secondary software failsafe. The watchdog above still resets the MCU if
  // a blocking peripheral call prevents this loop from reaching the timeout.
  unsigned long now = millis();
  if (!motorsTimeoutApplied && (now - lastMotorCmdMs > MOTOR_COMMAND_TIMEOUT_MS)) {
    driveMotors(0.0f, 0.0f);
    motorsTimeoutApplied = true;
  }

  if (!imuReady && now - lastImuInitAttemptMs >= 1000UL) {
    lastImuInitAttemptMs = now;
    if (!beginMpuBus() || !initMpu6050()) {
      Serial.print("WARN IMU init failed stage="); Serial.print(imuFailureStage);
      Serial.print(" who=0x"); Serial.print(imuWhoAmI, HEX);
      Serial.print(" wire="); Serial.print(imuWireError);
      Serial.print(" bytes="); Serial.println(imuBytesReceived);
    }
  }

  if (imuReady && (long)(micros() - nextImuReadUs) >= 0) {
    nextImuReadUs += IMU_PERIOD_US;
    if ((long)(micros() - nextImuReadUs) > (long)(IMU_PERIOD_US * 4UL)) nextImuReadUs = micros() + IMU_PERIOD_US;
    emitImuSample();
  }

  if (now - lastBatteryTelemetryMs >= BATTERY_TELEMETRY_PERIOD_MS) {
    lastBatteryTelemetryMs = now;
    emitBatteryTelemetry(readBatteryVoltage());
  }

  // Feed only after serial, drive timeout, sensor, and telemetry work returns.
  feedWatchdog();
}

void handleCommand(String &cmd) {
  cmd.trim();
  if (cmd.length() == 0) return;

  String tokens[6];
  int ntok = 0;
  int start = 0;
  for (int i = 0; i <= cmd.length(); ++i) {
    if (i == cmd.length() || cmd[i] == ' ') {
      if (i > start) {
        if (ntok < 6) tokens[ntok++] = cmd.substring(start, i);
      }
      start = i + 1;
    }
  }
  if (ntok == 0) return;

  char type = tokens[0][0];
  if (type >= 'a' && type <= 'z') type = type - 'a' + 'A';

  switch (type) {
    case 'M': {
      if (ntok < 3) { Serial.println("ERR M needs 2 floats"); break; }
      float l = tokens[1].toFloat();
      float r = tokens[2].toFloat();
      if (imuCalibrating && (fabsf(l) > 0.001f || fabsf(r) > 0.001f)) {
        driveMotors(0.0f, 0.0f);
        Serial.println("ERR M ignored during IMU calibration");
        break;
      }
      driveMotors(l, r);
      lastMotorCmdMs = millis();
      motorsTimeoutApplied = false;
      Serial.println("OK M");
    } break;

    case 'C': {
      if (ntok != 1) { Serial.println("ERR C takes no arguments"); break; }
      if (!imuReady || imuCalibrating) {
        Serial.println(imuCalibrating ? "ERR IMU calibration already running" : "ERR IMU not ready");
        break;
      }
      driveMotors(0.0f, 0.0f);
      lastMotorCmdMs = millis();
      motorsTimeoutApplied = true;
      for (uint8_t i = 0; i < 3; ++i) imuCalibrationTotals[i] = 0.0f;
      imuCalibrationCollected = 0;
      imuCalibrating = true;
      Serial.print("INFO IMU recalibration started samples=");
      Serial.println(IMU_MANUAL_CALIBRATION_SAMPLES);
    } break;

    case 'L': {
      if (ntok < 4) { Serial.println("ERR L needs 3 floats (B G R)"); break; }
      float b = clampf(tokens[1].toFloat(), 0.0f, 1.0f);
      float g = clampf(tokens[2].toFloat(), 0.0f, 1.0f);
      float r = clampf(tokens[3].toFloat(), 0.0f, 1.0f);
      g_led.mode = LED_STATIC;
      g_led.staticB = clamp8((int)(b * 255.0f + 0.5f));
      g_led.staticG = clamp8((int)(g * 255.0f + 0.5f));
      g_led.staticR = clamp8((int)(r * 255.0f + 0.5f));
      setLedRawBGR(g_led.staticB, g_led.staticG, g_led.staticR);
      lastLedCmdMs = millis();
      ledIsDefault = false;
      Serial.println("OK L");
    } break;

    case 'F': {
      if (ntok < 5) { Serial.println("ERR F needs 4 floats (B G R Tsec)"); break; }
      float b = clampf(tokens[1].toFloat(), 0.0f, 1.0f);
      float g = clampf(tokens[2].toFloat(), 0.0f, 1.0f);
      float r = clampf(tokens[3].toFloat(), 0.0f, 1.0f);
      float t = tokens[4].toFloat();
      unsigned long period = (unsigned long)(t * 1000.0f);
      if (period == 0) period = 1;

      g_led.mode = LED_FADE;
      g_led.targetB = clamp8((int)(b * 255.0f + 0.5f));
      g_led.targetG = clamp8((int)(g * 255.0f + 0.5f));
      g_led.targetR = clamp8((int)(r * 255.0f + 0.5f));
      g_led.periodMs = period;
      g_led.startMs = millis();
      lastLedCmdMs = millis();
      ledIsDefault = false;
      Serial.println("OK F");
    } break;

    default:
      Serial.println("ERR unknown cmd");
      break;
  }
}
