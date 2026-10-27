#define PIN_LED_B   D0
#define PIN_LED_G   D1
#define PIN_LED_R   D2

#include <Arduino.h>
#include "BoundedI2C.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <Adafruit_SleepyDog.h>

#define PIN_BATT    D6

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
BoundedI2C imuBus;
bool imuFilterReady = false;
bool orientationReady = false;
uint32_t imuSequence = 0, imuSamplesOk = 0, imuSamplesError = 0;
uint16_t imuCalibrationSamples = 0;
uint32_t imuCalibrationGeneration = 0;
const uint16_t IMU_MANUAL_CALIBRATION_SAMPLES = 400;
bool imuCalibrating = false;
uint16_t imuCalibrationCollected = 0;
float imuCalibrationTotals[3] = {0, 0, 0};
unsigned long nextImuReadUs = 0, lastImuSummaryMs = 0;
enum ImuInitStage { IMU_UNAVAILABLE, IMU_BUS, IMU_IDENTIFY, IMU_WAKE,
  IMU_WAIT_WAKE, IMU_CONFIGURE, IMU_WAIT_SETTLE, IMU_CALIBRATE, IMU_RUNNING,
  IMU_WATCHDOG_LOCKOUT };
ImuInitStage imuStage = IMU_UNAVAILABLE;
uint8_t imuConfigIndex = 0;
unsigned long imuStepAtMs = 0, imuRetryAtMs = 0;
unsigned long imuRetryDelayMs = 1000;
const unsigned long IMU_RETRY_MAX_MS = 30000;
const unsigned long IMU_CALIBRATION_DEADLINE_MS = 10000;
unsigned long imuCalibrationStartedMs = 0;
uint16_t imuCalibrationTarget = 0;
uint32_t lastImuSampleUs = 0;
const char *imuFailureStage = "none";
uint8_t imuWireError = 0;
uint8_t imuWhoAmI = 0;
uint8_t imuBytesReceived = 0;
const unsigned long MOTOR_COMMAND_TIMEOUT_MS = 1000UL;
const char *FIRMWARE_ID = "goob-samd21-bounded-recovery-v2";
uint8_t bootResetCause = 0;
int watchdogPeriodMs = 0;

void configureWatchdog() {
#if defined(ARDUINO_ARCH_SAMD)
  // SleepyDog explicitly configures the SAMD21's independent OSCULP32K
  // clock divided to 1024 Hz. Setting PER alone does not establish a clock.
  watchdogPeriodMs = Watchdog.enable(1000);
#endif
}

void feedWatchdog() {
#if defined(ARDUINO_ARCH_SAMD)
  Watchdog.reset();
#endif
}

// All I2C waits are bounded in the project-owned SERCOM2 adapter.
bool mpuWrite(uint8_t reg, uint8_t value) {
  const bool ok = imuBus.writeRegister(MPU_ADDR, reg, value);
  imuWireError = imuBus.error;
  if (!ok) imuFailureStage = "write";
  return ok;
}

bool beginMpuBus() {
  const bool ok = imuBus.begin();
  imuWireError = imuBus.error;
  if (!ok) imuFailureStage = "bus-start";
  return ok;
}

bool mpuRead(uint8_t reg, uint8_t *out, size_t count) {
  const bool ok = imuBus.readRegister(MPU_ADDR, reg, out, count);
  imuWireError = imuBus.error;
  imuBytesReceived = imuBus.received;
  if (!ok) imuFailureStage = "read";
  return ok;
}

void stopMotors() {
  driveMotors(0.0f, 0.0f);
  motorsTimeoutApplied = true;
}

void serviceMotorTimeout() {
  if (!motorsTimeoutApplied && millis() - lastMotorCmdMs >= MOTOR_COMMAND_TIMEOUT_MS)
    stopMotors();
}

void resetEstimator() {
  lastImuSampleUs = 0;
  imuFilterReady = orientationReady = false;
  yawDeg = 0.0f;
}

void imuFault() {
  stopMotors();
  if (imuCalibrating) Serial.println("ERR IMU recalibration aborted: sensor fault");
  imuReady = false;
  imuCalibrating = false;
  imuStage = IMU_UNAVAILABLE;
  resetEstimator();
  imuRetryAtMs = millis() + imuRetryDelayMs;
  imuRetryDelayMs = min(imuRetryDelayMs * 2UL, IMU_RETRY_MAX_MS);
  Serial.print("WARN IMU unavailable stage="); Serial.print(imuFailureStage);
  Serial.print(" who=0x"); Serial.print(imuWhoAmI, HEX);
  Serial.print(" wire="); Serial.print(imuWireError);
  Serial.print(" bytes="); Serial.println(imuBytesReceived);
}

void startCalibration(uint16_t samples) {
  stopMotors();
  imuCalibrating = true;
  imuCalibrationCollected = 0;
  imuCalibrationTarget = samples;
  for (uint8_t i = 0; i < 3; ++i) imuCalibrationTotals[i] = 0;
  imuCalibrationStartedMs = millis();
  resetEstimator();
}

void finishCalibration() {
  for (uint8_t i = 0; i < 3; ++i)
    gyroBias[i] = imuCalibrationTotals[i] / imuCalibrationCollected;
  imuCalibrationSamples = imuCalibrationCollected;
  ++imuCalibrationGeneration;
  imuCalibrating = false;
  resetEstimator();
}

void startImuInitialization() {
  stopMotors();
  imuReady = false;
  // Lock nonzero drive commands throughout initialization AND calibration.
  startCalibration(100);
  imuStage = IMU_BUS;
  imuStepAtMs = millis();
  imuConfigIndex = 0;
  imuWireError = 0;
  imuBytesReceived = 0;
  imuWhoAmI = 0;
  Serial.println("INFO IMU initialization/calibration started");
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

// At most one bounded bus operation per initialization/calibration step.
// No delays, calibration loops, or watchdog feeds here.
void serviceImuInitialization() {
  const unsigned long now = millis();
  if (imuStage == IMU_RUNNING || imuStage == IMU_WATCHDOG_LOCKOUT) return;
  if (imuStage == IMU_UNAVAILABLE) {
    if ((int32_t)(now - imuRetryAtMs) >= 0) startImuInitialization();
    return;
  }
  if (now - imuCalibrationStartedMs >= IMU_CALIBRATION_DEADLINE_MS) {
    imuFailureStage = "calibration-deadline";
    imuFault();
    return;
  }
  if ((int32_t)(now - imuStepAtMs) < 0) return;
  switch (imuStage) {
    case IMU_BUS:
      if (!beginMpuBus()) { imuFault(); return; }
      imuStage = IMU_IDENTIFY;
      break;
    case IMU_IDENTIFY: {
      uint8_t who = 0;
      if (!mpuRead(MPU_WHO_AM_I, &who, 1)) { imuFailureStage = "WHO_AM_I-read"; imuFault(); return; }
      imuWhoAmI = who;
      if (who != 0x68 && who != 0x69) { imuFailureStage = "WHO_AM_I-value"; imuFault(); return; }
      imuStage = IMU_WAKE;
    } break;
    case IMU_WAKE:
      if (!mpuWrite(MPU_PWR_MGMT_1, 0)) { imuFault(); return; }
      imuStepAtMs = now + 100;
      imuStage = IMU_WAIT_WAKE;
      break;
    case IMU_WAIT_WAKE:
      imuStage = IMU_CONFIGURE;
      break;
    case IMU_CONFIGURE: {
      const uint8_t regs[] = { MPU_CONFIG, MPU_SMPLRT_DIV, MPU_GYRO_CONFIG, MPU_ACCEL_CONFIG };
      const uint8_t values[] = { 3, 9, 0, 0 };
      if (!mpuWrite(regs[imuConfigIndex], values[imuConfigIndex])) { imuFault(); return; }
      if (++imuConfigIndex == 4) {
        imuStepAtMs = now + 100;
        imuStage = IMU_WAIT_SETTLE;
      }
    } break;
    case IMU_WAIT_SETTLE:
      imuStage = IMU_CALIBRATE;
      break;
    case IMU_CALIBRATE: {
      int16_t a[3], g[3], t;
      if (!readMpuRaw(a, g, t)) { imuFailureStage = "calibration-sample"; imuFault(); return; }
      for (uint8_t i = 0; i < 3; ++i) imuCalibrationTotals[i] += g[i];
      ++imuCalibrationCollected;
      imuStepAtMs = millis() + 5;
      if (imuCalibrationCollected >= imuCalibrationTarget) {
        finishCalibration();
        imuReady = true;
        imuStage = IMU_RUNNING;
        imuFailureStage = "none";
        imuRetryDelayMs = 1000;
        nextImuReadUs = micros();
        lastImuSummaryMs = millis();
        Serial.println("INFO IMU MPU-6050 ready");
      }
    } break;
    default: break;
  }
}

void emitImuSample() {
  int16_t rawAccel[3], rawGyro[3], rawTemp;
  if (!readMpuRaw(rawAccel, rawGyro, rawTemp)) {
    ++imuSamplesError;
    imuFault();
    return;
  }
  ++imuSamplesOk;
  ++imuSequence;
  const uint32_t nowUs = micros();
  const uint32_t elapsedUs = nowUs - lastImuSampleUs;
  // A gap >100 ms restarts the estimator instead of integrating the outage.
  if (lastImuSampleUs && elapsedUs > 100000UL) resetEstimator();
  const float dt = lastImuSampleUs ? elapsedUs / 1000000.0f : 0.0f;
  lastImuSampleUs = nowUs;
  if (imuCalibrating) {
    if (millis() - imuCalibrationStartedMs >= IMU_CALIBRATION_DEADLINE_MS) {
      imuFailureStage = "calibration-deadline";
      imuFault();
      return;
    }
    for (uint8_t i = 0; i < 3; ++i) imuCalibrationTotals[i] += rawGyro[i];
    ++imuCalibrationCollected;
    if (imuCalibrationCollected >= imuCalibrationTarget) {
      finishCalibration();
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
  const uint8_t resetCause = PM->RCAUSE.reg;
  bootResetCause = resetCause;

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
  Serial.print("INFO firmware="); Serial.println(FIRMWARE_ID);
  Serial.println("Starting setup...");
  if (resetCause & PM_RCAUSE_WDT) {
    Serial.println("WARN MCU restarted by watchdog; motor outputs forced off");
  }

  pinMode(PIN_LED_B, OUTPUT);
  digitalWrite(PIN_LED_B, 0);
  pinMode(PIN_LED_G, OUTPUT);
  digitalWrite(PIN_LED_G, 0);
  pinMode(PIN_LED_R, OUTPUT);
  digitalWrite(PIN_LED_R, 0);

  pinMode(PIN_BATT, INPUT);
#if defined(ARDUINO_ARCH_ESP32)
  analogReadResolution(12);
  analogSetPinAttenuation(PIN_BATT, ADC_11db);
#endif

  setDefaultLed();
  const unsigned long now = millis();
  lastMotorCmdMs = now;
  motorsTimeoutApplied = true;
  lastLedCmdMs = now;
  lastBatteryTelemetryMs = now;
  emitBatteryTelemetry(readBatteryVoltage());

  // Do not re-enter a possibly defective sensor after a watchdog reset.
  // USB and normal drive/light commands remain available for this boot.
  if (resetCause & PM_RCAUSE_WDT) {
    imuStage = IMU_WATCHDOG_LOCKOUT;
    imuFailureStage = "watchdog-lockout";
    Serial.println("WARN IMU probing disabled after watchdog reset; reset/power-cycle to retry");
  } else {
    startImuInitialization();
  }
  feedWatchdog();
  Serial.println("Done!");
}

// Maximum is 128 payload bytes, excluding CR/LF; one extra byte for NUL.
const size_t MAX_COMMAND_BYTES = 128;
const size_t SERIAL_BYTES_PER_PASS = 64;
char commandBuffer[MAX_COMMAND_BYTES + 1];
size_t commandLength = 0;
bool discardCommand = false;

void serviceSerialInput() {
  for (size_t n = 0; n < SERIAL_BYTES_PER_PASS && Serial.available() > 0; ++n) {
    const int c = Serial.read();
    if (c < 0) break;
    if (c == '\n') {
      if (discardCommand) Serial.println("ERR command too long or invalid byte");
      else {
        commandBuffer[commandLength] = '\0';
        handleCommand(commandBuffer);
      }
      commandLength = 0;
      discardCommand = false;
    } else if (c != '\r' && !discardCommand) {
      if (c == 0 || commandLength >= MAX_COMMAND_BYTES) discardCommand = true;
      else commandBuffer[commandLength++] = (char)c;
    }
  }
}

bool parseFiniteFloat(const char *text, float &out) {
  if (!text || !*text) return false;
  char *end = nullptr;
  errno = 0;
  const float value = strtof(text, &end);
  if (end == text || *end != '\0' || errno == ERANGE || !isfinite(value)) return false;
  out = value;
  return true;
}

static inline float readBatteryVoltage() {
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
}

static inline void emitBatteryTelemetry(float volts) {
  // `chg=-1` indicates the dedicated charge-detect line is no longer wired,
  // while `v=` carries the divided battery reading from D6.
  Serial.print("C ");
  Serial.print("chg=-1 v=");
  Serial.println(volts, 3);
}

void loop() {
  serviceMotorTimeout();
  serviceSerialInput();
  serviceMotorTimeout();
  updateLedFade();
  serviceImuInitialization();
  serviceMotorTimeout();

  if (imuReady && (int32_t)(micros() - nextImuReadUs) >= 0) {
    nextImuReadUs += IMU_PERIOD_US;
    if ((int32_t)(micros() - nextImuReadUs) > (int32_t)(IMU_PERIOD_US * 4UL))
      nextImuReadUs = micros() + IMU_PERIOD_US;
    emitImuSample();
  }
  serviceMotorTimeout();
  const unsigned long now = millis();
  if (now - lastBatteryTelemetryMs >= BATTERY_TELEMETRY_PERIOD_MS) {
    lastBatteryTelemetryMs = now;
    emitBatteryTelemetry(readBatteryVoltage());
  }
  serviceMotorTimeout();
  // Final fallback only; all scheduled work and motor safety returned.
  feedWatchdog();
}

void handleCommand(char *cmd) {
  char *tokens[6];
  uint8_t ntok = 0;
  char *context = nullptr;
  for (char *t = strtok_r(cmd, " \t", &context); t; t = strtok_r(nullptr, " \t", &context)) {
    if (ntok == 6) { Serial.println("ERR too many arguments"); return; }
    tokens[ntok++] = t;
  }
  if (!ntok) return;
  if (strlen(tokens[0]) != 1) { Serial.println("ERR unknown cmd"); return; }
  char type = tokens[0][0];
  if (type >= 'a' && type <= 'z') type = type - 'a' + 'A';

  switch (type) {
    case 'V': {
      Serial.print("INFO firmware="); Serial.print(FIRMWARE_ID);
      Serial.print(" uptime_ms="); Serial.print(millis());
      Serial.print(" reset_cause=0x"); Serial.print(bootResetCause, HEX);
      Serial.print(" watchdog_ms="); Serial.print(watchdogPeriodMs);
      Serial.print(" imu_ready="); Serial.print(imuReady ? 1 : 0);
      Serial.print(" imu_calibrating="); Serial.print(imuCalibrating ? 1 : 0);
      Serial.print(" imu_state="); Serial.print((int)imuStage);
      Serial.print(" imu_error="); Serial.print(imuWireError);
      Serial.print(" imu_stage="); Serial.print(imuFailureStage);
      Serial.print(" motor_cmd_ms="); Serial.print(lastMotorCmdMs);
      Serial.print(" motor_timeout="); Serial.println(motorsTimeoutApplied ? 1 : 0);
    } break;

    case 'M': {
      if (ntok != 3) { Serial.println("ERR M needs 2 floats"); break; }
      float l, r;
      if (!parseFiniteFloat(tokens[1], l) || !parseFiniteFloat(tokens[2], r)) {
        stopMotors();
        Serial.println("ERR M requires finite numeric values");
        break;
      }
      if (imuCalibrating && (l != 0.0f || r != 0.0f)) {
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
      startCalibration(IMU_MANUAL_CALIBRATION_SAMPLES);
      Serial.print("INFO IMU recalibration started samples=");
      Serial.println(IMU_MANUAL_CALIBRATION_SAMPLES);
    } break;

    case 'L': {
      if (ntok < 4) { Serial.println("ERR L needs 3 floats (B G R)"); break; }
      float b, g, r;
      if (!parseFiniteFloat(tokens[1], b) || !parseFiniteFloat(tokens[2], g) ||
          !parseFiniteFloat(tokens[3], r)) { Serial.println("ERR light requires finite numeric values"); break; }
      b = clampf(b, 0.0f, 1.0f); g = clampf(g, 0.0f, 1.0f); r = clampf(r, 0.0f, 1.0f);
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
      float b, g, r;
      if (!parseFiniteFloat(tokens[1], b) || !parseFiniteFloat(tokens[2], g) ||
          !parseFiniteFloat(tokens[3], r)) { Serial.println("ERR light requires finite numeric values"); break; }
      b = clampf(b, 0.0f, 1.0f); g = clampf(g, 0.0f, 1.0f); r = clampf(r, 0.0f, 1.0f);
      float t;
      if (!parseFiniteFloat(tokens[4], t) || t < 0.0f || t > 2147483.0f) {
        Serial.println("ERR F period out of range"); break;
      }
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
