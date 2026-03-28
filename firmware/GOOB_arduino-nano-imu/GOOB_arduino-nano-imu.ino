#include <Arduino.h>
#include <Wire.h>
#include <math.h>

/*
  Goob Arduino Nano IMU bridge

  Wiring for Goob's existing GY-521 / MPU-6050 breakout:

  Nano 5V   -> GY-521 VCC   (Goob's working Nano setup)
  Nano GND  -> GY-521 GND
  Nano A4   -> GY-521 SDA
  Nano A5   -> GY-521 SCL

  Keep AD0 tied low for address 0x68.
  Leave INT disconnected unless you later decide to use interrupt-driven reads.

  This assumes the IMU bus is owned only by the Nano. Do not share these SDA/SCL
  lines directly with a 3.3V host while the breakout is powered from 5V.

  Serial output:
    I ...   high-rate filtered motion sample
    S ...   low-rate summary / health sample
*/

namespace {

const uint8_t MPU6050_ADDR = 0x68;

const uint8_t REG_SMPLRT_DIV = 0x19;
const uint8_t REG_CONFIG = 0x1A;
const uint8_t REG_GYRO_CONFIG = 0x1B;
const uint8_t REG_ACCEL_CONFIG = 0x1C;
const uint8_t REG_ACCEL_XOUT_H = 0x3B;
const uint8_t REG_PWR_MGMT_1 = 0x6B;
const uint8_t REG_WHO_AM_I = 0x75;

const unsigned long SERIAL_BAUD = 115200UL;
const unsigned long I2C_CLOCK_HZ = 100000UL;

const uint8_t DLPF_CONFIG = 3;
const uint8_t SAMPLE_RATE_DIVIDER = 9;  // 1 kHz / (1 + 9) = 100 Hz
const uint8_t GYRO_FULL_SCALE = 0;      // +/- 250 dps
const uint8_t ACCEL_FULL_SCALE = 0;     // +/- 2 g

const float ACCEL_LSB_PER_G = 16384.0f;
const float GYRO_LSB_PER_DPS = 131.0f;
const float GRAVITY_MPS2 = 9.80665f;
const float ACCEL_ALPHA = 0.25f;
const float GYRO_ALPHA = 0.25f;
const float ORIENTATION_ALPHA = 0.98f;

const unsigned long READ_PERIOD_US = 10000UL;   // 100 Hz
const unsigned long LOW_RATE_PERIOD_MS = 200UL; // 5 Hz
const unsigned long REINIT_DELAY_MS = 1000UL;

const uint16_t CALIBRATION_SAMPLES = 200;
const unsigned long CALIBRATION_SAMPLE_DELAY_MS = 5UL;
const uint8_t MAX_CONSECUTIVE_ERRORS = 5;

struct RawSample {
  uint32_t us;
  uint32_t ms;
  int16_t accel[3];
  int16_t gyro[3];
  int16_t tempRaw;
};

struct ProcessedSample {
  uint32_t seq;
  uint32_t ms;
  float dtSeconds;
  float accelMps2[3];
  float gyroDps[3];
  float tempC;
  float rollDeg;
  float pitchDeg;
  float accelNormG;
};

float gyroBiasRaw[3] = {0.0f, 0.0f, 0.0f};
float filteredAccelG[3] = {0.0f, 0.0f, 0.0f};
float filteredGyroDps[3] = {0.0f, 0.0f, 0.0f};

bool imuReady = false;
bool haveFilterState = false;
bool haveOrientation = false;

uint32_t seq = 0;
uint32_t lastSampleUs = 0;
unsigned long nextReadAtUs = 0;
unsigned long lastLowTelemetryMs = 0;
unsigned long lastInitAttemptMs = 0;

uint32_t samplesOk = 0;
uint32_t samplesError = 0;
uint32_t calibrationSamplesUsed = 0;
uint8_t consecutiveErrors = 0;

float rollDeg = 0.0f;
float pitchDeg = 0.0f;

int16_t toInt16(uint8_t msb, uint8_t lsb) {
  return (int16_t)(((uint16_t)msb << 8) | (uint16_t)lsb);
}

float clampf(float value, float lo, float hi) {
  if (value < lo) return lo;
  if (value > hi) return hi;
  return value;
}

float lpf(float previous, float current, float alpha) {
  return previous + alpha * (current - previous);
}

bool writeRegister(uint8_t reg, uint8_t value) {
  Wire.beginTransmission(MPU6050_ADDR);
  Wire.write(reg);
  Wire.write(value);
  return Wire.endTransmission(true) == 0;
}

bool readRegister(uint8_t reg, uint8_t &value) {
  Wire.beginTransmission(MPU6050_ADDR);
  Wire.write(reg);
  if (Wire.endTransmission(false) != 0) {
    return false;
  }
  uint8_t readCount = Wire.requestFrom((int)MPU6050_ADDR, 1, 1);
  if (readCount != 1 || Wire.available() < 1) {
    return false;
  }
  value = Wire.read();
  return true;
}

bool readBlock(uint8_t reg, uint8_t *buffer, size_t length) {
  Wire.beginTransmission(MPU6050_ADDR);
  Wire.write(reg);
  if (Wire.endTransmission(false) != 0) {
    return false;
  }
  uint8_t readCount = Wire.requestFrom((int)MPU6050_ADDR, (int)length, 1);
  if (readCount != length) {
    return false;
  }
  for (size_t i = 0; i < length; ++i) {
    if (Wire.available() < 1) {
      return false;
    }
    buffer[i] = (uint8_t)Wire.read();
  }
  return true;
}

bool readRawSample(RawSample &sample) {
  uint8_t block[14];
  if (!readBlock(REG_ACCEL_XOUT_H, block, sizeof(block))) {
    return false;
  }
  sample.us = micros();
  sample.ms = millis();
  sample.accel[0] = toInt16(block[0], block[1]);
  sample.accel[1] = toInt16(block[2], block[3]);
  sample.accel[2] = toInt16(block[4], block[5]);
  sample.tempRaw = toInt16(block[6], block[7]);
  sample.gyro[0] = toInt16(block[8], block[9]);
  sample.gyro[1] = toInt16(block[10], block[11]);
  sample.gyro[2] = toInt16(block[12], block[13]);
  return true;
}

void resetFilterState() {
  haveFilterState = false;
  haveOrientation = false;
  lastSampleUs = 0;
  rollDeg = 0.0f;
  pitchDeg = 0.0f;
}

bool calibrateGyroBias() {
  float totals[3] = {0.0f, 0.0f, 0.0f};
  uint16_t collected = 0;

  delay(100);

  while (collected < CALIBRATION_SAMPLES) {
    RawSample sample;
    if (!readRawSample(sample)) {
      return false;
    }
    totals[0] += (float)sample.gyro[0];
    totals[1] += (float)sample.gyro[1];
    totals[2] += (float)sample.gyro[2];
    collected++;
    delay(CALIBRATION_SAMPLE_DELAY_MS);
  }

  gyroBiasRaw[0] = totals[0] / (float)collected;
  gyroBiasRaw[1] = totals[1] / (float)collected;
  gyroBiasRaw[2] = totals[2] / (float)collected;
  calibrationSamplesUsed = collected;
  return true;
}

bool initImu() {
  uint8_t whoAmI = 0;
  if (!readRegister(REG_WHO_AM_I, whoAmI)) {
    return false;
  }
  if (whoAmI != 0x68 && whoAmI != 0x69) {
    return false;
  }

  if (!writeRegister(REG_PWR_MGMT_1, 0x00)) {
    return false;
  }
  delay(100);
  if (!writeRegister(REG_CONFIG, DLPF_CONFIG)) {
    return false;
  }
  if (!writeRegister(REG_SMPLRT_DIV, SAMPLE_RATE_DIVIDER)) {
    return false;
  }
  if (!writeRegister(REG_GYRO_CONFIG, (uint8_t)(GYRO_FULL_SCALE << 3))) {
    return false;
  }
  if (!writeRegister(REG_ACCEL_CONFIG, (uint8_t)(ACCEL_FULL_SCALE << 3))) {
    return false;
  }

  if (!calibrateGyroBias()) {
    return false;
  }

  resetFilterState();
  consecutiveErrors = 0;
  imuReady = true;
  nextReadAtUs = micros();
  lastLowTelemetryMs = millis();

  Serial.print(F("IMU ready who=0x"));
  Serial.print(whoAmI, HEX);
  Serial.print(F(" gbx="));
  Serial.print(gyroBiasRaw[0] / GYRO_LSB_PER_DPS, 4);
  Serial.print(F(" gby="));
  Serial.print(gyroBiasRaw[1] / GYRO_LSB_PER_DPS, 4);
  Serial.print(F(" gbz="));
  Serial.println(gyroBiasRaw[2] / GYRO_LSB_PER_DPS, 4);

  return true;
}

void emitHighRateTelemetry(const ProcessedSample &sample) {
  Serial.print(F("I "));
  Serial.print(F("n="));
  Serial.print(sample.seq);
  Serial.print(F(" ms="));
  Serial.print(sample.ms);
  Serial.print(F(" ax="));
  Serial.print(sample.accelMps2[0], 4);
  Serial.print(F(" ay="));
  Serial.print(sample.accelMps2[1], 4);
  Serial.print(F(" az="));
  Serial.print(sample.accelMps2[2], 4);
  Serial.print(F(" gx="));
  Serial.print(sample.gyroDps[0], 4);
  Serial.print(F(" gy="));
  Serial.print(sample.gyroDps[1], 4);
  Serial.print(F(" gz="));
  Serial.print(sample.gyroDps[2], 4);
  Serial.print(F(" t="));
  Serial.print(sample.tempC, 3);
  Serial.print(F(" r="));
  Serial.print(sample.rollDeg, 3);
  Serial.print(F(" p="));
  Serial.println(sample.pitchDeg, 3);
}

void emitLowRateTelemetry(const ProcessedSample &sample) {
  Serial.print(F("S "));
  Serial.print(F("n="));
  Serial.print(sample.seq);
  Serial.print(F(" ms="));
  Serial.print(sample.ms);
  Serial.print(F(" r="));
  Serial.print(sample.rollDeg, 3);
  Serial.print(F(" p="));
  Serial.print(sample.pitchDeg, 3);
  Serial.print(F(" t="));
  Serial.print(sample.tempC, 3);
  Serial.print(F(" an="));
  Serial.print(sample.accelNormG, 4);
  Serial.print(F(" gbx="));
  Serial.print(gyroBiasRaw[0] / GYRO_LSB_PER_DPS, 4);
  Serial.print(F(" gby="));
  Serial.print(gyroBiasRaw[1] / GYRO_LSB_PER_DPS, 4);
  Serial.print(F(" gbz="));
  Serial.print(gyroBiasRaw[2] / GYRO_LSB_PER_DPS, 4);
  Serial.print(F(" ok="));
  Serial.print(samplesOk);
  Serial.print(F(" err="));
  Serial.print(samplesError);
  Serial.print(F(" cal="));
  Serial.println(calibrationSamplesUsed);
}

void processSample(const RawSample &raw, ProcessedSample &out) {
  seq++;

  float dtSeconds = 0.0f;
  if (lastSampleUs != 0) {
    dtSeconds = (float)(raw.us - lastSampleUs) / 1000000.0f;
    if (!isfinite(dtSeconds) || dtSeconds < 0.0f) {
      dtSeconds = 0.0f;
    }
  }
  lastSampleUs = raw.us;

  float accelG[3];
  float gyroDps[3];
  for (uint8_t i = 0; i < 3; ++i) {
    accelG[i] = (float)raw.accel[i] / ACCEL_LSB_PER_G;
    gyroDps[i] = ((float)raw.gyro[i] - gyroBiasRaw[i]) / GYRO_LSB_PER_DPS;
  }

  if (!haveFilterState) {
    for (uint8_t i = 0; i < 3; ++i) {
      filteredAccelG[i] = accelG[i];
      filteredGyroDps[i] = gyroDps[i];
    }
    haveFilterState = true;
  } else {
    for (uint8_t i = 0; i < 3; ++i) {
      filteredAccelG[i] = lpf(filteredAccelG[i], accelG[i], ACCEL_ALPHA);
      filteredGyroDps[i] = lpf(filteredGyroDps[i], gyroDps[i], GYRO_ALPHA);
    }
  }

  const float accelNormG = sqrtf(
    filteredAccelG[0] * filteredAccelG[0] +
    filteredAccelG[1] * filteredAccelG[1] +
    filteredAccelG[2] * filteredAccelG[2]
  );

  const float az = (fabsf(filteredAccelG[2]) > 1e-6f) ? filteredAccelG[2] : 1e-6f;
  const float rollAccDeg = atan2f(filteredAccelG[1], az) * 57.2957795f;
  const float pitchAccDeg = atan2f(
    -filteredAccelG[0],
    sqrtf(filteredAccelG[1] * filteredAccelG[1] + filteredAccelG[2] * filteredAccelG[2])
  ) * 57.2957795f;

  if (!haveOrientation) {
    rollDeg = rollAccDeg;
    pitchDeg = pitchAccDeg;
    haveOrientation = true;
  } else {
    const float rollGyroDeg = rollDeg + filteredGyroDps[0] * dtSeconds;
    const float pitchGyroDeg = pitchDeg + filteredGyroDps[1] * dtSeconds;
    const float blend = (accelNormG < 0.25f) ? 1.0f : ORIENTATION_ALPHA;
    rollDeg = blend * rollGyroDeg + (1.0f - blend) * rollAccDeg;
    pitchDeg = blend * pitchGyroDeg + (1.0f - blend) * pitchAccDeg;
  }

  out.seq = seq;
  out.ms = raw.ms;
  out.dtSeconds = dtSeconds;
  out.tempC = (float)raw.tempRaw / 340.0f + 36.53f;
  out.rollDeg = rollDeg;
  out.pitchDeg = pitchDeg;
  out.accelNormG = accelNormG;

  for (uint8_t i = 0; i < 3; ++i) {
    out.accelMps2[i] = filteredAccelG[i] * GRAVITY_MPS2;
    out.gyroDps[i] = filteredGyroDps[i];
  }
}

void tryInitImu() {
  const unsigned long nowMs = millis();
  if ((nowMs - lastInitAttemptMs) < REINIT_DELAY_MS) {
    return;
  }
  lastInitAttemptMs = nowMs;
  if (initImu()) {
    Serial.println(F("Done!"));
  } else {
    imuReady = false;
    Serial.println(F("WARN imu_init_failed"));
  }
}

}  // namespace

void setup() {
  Serial.begin(SERIAL_BAUD);
  delay(50);
  Serial.println(F("Starting setup..."));

  Wire.begin();
  // Rely on the breakout's bus pull-ups rather than the Nano's weak internal
  // pull-ups so the IMU side can stay at its own logic rail.
  digitalWrite(A4, LOW);
  digitalWrite(A5, LOW);
  Wire.setClock(I2C_CLOCK_HZ);

  tryInitImu();
}

void loop() {
  if (!imuReady) {
    tryInitImu();
    delay(20);
    return;
  }

  const unsigned long nowUs = micros();
  if ((long)(nowUs - nextReadAtUs) < 0) {
    return;
  }
  nextReadAtUs += READ_PERIOD_US;
  if ((long)(nowUs - nextReadAtUs) > (long)(READ_PERIOD_US * 4UL)) {
    nextReadAtUs = nowUs + READ_PERIOD_US;
  }

  RawSample raw;
  if (!readRawSample(raw)) {
    samplesError++;
    consecutiveErrors++;
    Serial.println(F("WARN imu_read_failed"));
    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      imuReady = false;
      resetFilterState();
      Serial.println(F("WARN imu_reinit"));
    }
    delay(5);
    return;
  }

  samplesOk++;
  consecutiveErrors = 0;

  ProcessedSample sample;
  processSample(raw, sample);
  emitHighRateTelemetry(sample);

  const unsigned long nowMs = millis();
  if ((nowMs - lastLowTelemetryMs) >= LOW_RATE_PERIOD_MS) {
    lastLowTelemetryMs = nowMs;
    emitLowRateTelemetry(sample);
  }
}
