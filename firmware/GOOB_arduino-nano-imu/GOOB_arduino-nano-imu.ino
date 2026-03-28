#include <Arduino.h>
#include <Wire.h>
#include <math.h>
#include <string.h>

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
    pebble_serial_v1 framed binary packets over USB serial
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

const uint8_t PROTOCOL_VERSION = 1;
const uint8_t MSG_HELLO = 0x01;
const uint8_t MSG_DESCRIBE_REQ = 0x02;
const uint8_t MSG_DESCRIBE = 0x03;
const uint8_t MSG_SAMPLE = 0x10;
const uint8_t MSG_STATE = 0x11;
const uint8_t MSG_LOG = 0x30;
const uint8_t MSG_PING = 0x31;
const uint8_t MSG_PONG = 0x32;

const uint8_t DIR_IN = 1;
const uint8_t DIR_OUT = 2;

const uint8_t KIND_SAMPLE = 1;
const uint8_t KIND_STATE = 2;

const uint8_t ENCODING_STRUCT_V1 = 1;

const uint8_t TYPE_BOOL = 1;
const uint8_t TYPE_U8 = 2;
const uint8_t TYPE_I8 = 3;
const uint8_t TYPE_U16 = 4;
const uint8_t TYPE_I16 = 5;
const uint8_t TYPE_U32 = 6;
const uint8_t TYPE_I32 = 7;
const uint8_t TYPE_F32 = 8;

const uint8_t FLAG_RETAIN = 0x01;
const uint8_t FLAG_LOCAL_ONLY = 0x02;

const uint16_t INTERFACE_IMU_FAST = 1;
const uint16_t INTERFACE_IMU = 2;

const char DEVICE_UID[] = "goob-imu-nano";
const char FIRMWARE_VERSION[] = "2026.03.28";
const char MODEL_NAME[] = "arduino-nano";
const char SCHEMA_HASH[] = "goob-imu-v2";
const uint8_t SCHEMA_VERSION = 1;

const size_t HEADER_SIZE = 13;
const size_t CRC_SIZE = 2;
const size_t MAX_PACKET_BODY = 384;
const size_t MAX_FRAME_SIZE = 512;
const size_t MAX_RX_FRAME_SIZE = 160;
const size_t MAX_DECODED_FRAME_SIZE = 384;

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

struct FieldSpec {
  const char *name;
  uint8_t typeCode;
  float scale;
  float offset;
  const char *unit;
};

struct InterfaceSpec {
  uint16_t id;
  uint8_t dirCode;
  uint8_t kindCode;
  uint8_t flags;
  uint16_t rateHz;
  const char *name;
  const char *channel;
  const char *profile;
  uint8_t encodingKind;
  uint8_t fieldCount;
  const FieldSpec *fields;
};

const FieldSpec IMU_FAST_FIELDS[] = {
  {"ax", TYPE_I16, 0.001f, 0.0f, "mps2"},
  {"ay", TYPE_I16, 0.001f, 0.0f, "mps2"},
  {"az", TYPE_I16, 0.001f, 0.0f, "mps2"},
  {"gx", TYPE_I16, 0.01f, 0.0f, "dps"},
  {"gy", TYPE_I16, 0.01f, 0.0f, "dps"},
  {"gz", TYPE_I16, 0.01f, 0.0f, "dps"},
  {"temp_c", TYPE_I16, 0.01f, 0.0f, "c"},
  {"roll_deg", TYPE_I16, 0.01f, 0.0f, "deg"},
  {"pitch_deg", TYPE_I16, 0.01f, 0.0f, "deg"},
};

const FieldSpec IMU_SUMMARY_FIELDS[] = {
  {"accel_norm_g", TYPE_U16, 0.001f, 0.0f, "g"},
  {"gyro_bias_x_dps", TYPE_I16, 0.01f, 0.0f, "dps"},
  {"gyro_bias_y_dps", TYPE_I16, 0.01f, 0.0f, "dps"},
  {"gyro_bias_z_dps", TYPE_I16, 0.01f, 0.0f, "dps"},
  {"samples_ok", TYPE_U32, 1.0f, 0.0f, ""},
  {"samples_error", TYPE_U32, 1.0f, 0.0f, ""},
  {"calibration_samples", TYPE_U16, 1.0f, 0.0f, ""},
};

const InterfaceSpec INTERFACES[] = {
  {
    INTERFACE_IMU_FAST,
    DIR_OUT,
    KIND_SAMPLE,
    FLAG_LOCAL_ONLY,
    100,
    "imu_fast",
    "sensors/imu-fast",
    "imu.motion.v1",
    ENCODING_STRUCT_V1,
    (uint8_t)(sizeof(IMU_FAST_FIELDS) / sizeof(IMU_FAST_FIELDS[0])),
    IMU_FAST_FIELDS,
  },
  {
    INTERFACE_IMU,
    DIR_OUT,
    KIND_STATE,
    0,
    5,
    "imu",
    "sensors/imu",
    "imu.summary.v1",
    ENCODING_STRUCT_V1,
    (uint8_t)(sizeof(IMU_SUMMARY_FIELDS) / sizeof(IMU_SUMMARY_FIELDS[0])),
    IMU_SUMMARY_FIELDS,
  },
};

const uint8_t INTERFACE_COUNT = (uint8_t)(sizeof(INTERFACES) / sizeof(INTERFACES[0]));

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

uint8_t txPayloadBuffer[MAX_PACKET_BODY];
uint8_t txBodyBuffer[MAX_PACKET_BODY];
uint8_t txFrameBuffer[MAX_FRAME_SIZE];
uint8_t rxFrameBuffer[MAX_RX_FRAME_SIZE];
uint8_t rxDecodedBuffer[MAX_DECODED_FRAME_SIZE];
size_t rxFrameLength = 0;

int16_t toInt16(uint8_t msb, uint8_t lsb) {
  return (int16_t)(((uint16_t)msb << 8) | (uint16_t)lsb);
}

float lpf(float previous, float current, float alpha) {
  return previous + alpha * (current - previous);
}

bool appendBytes(uint8_t *buffer, size_t maxLength, size_t &index, const void *src, size_t length) {
  if ((index + length) > maxLength) {
    return false;
  }
  memcpy(buffer + index, src, length);
  index += length;
  return true;
}

bool appendU8(uint8_t *buffer, size_t maxLength, size_t &index, uint8_t value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendU16(uint8_t *buffer, size_t maxLength, size_t &index, uint16_t value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendI16(uint8_t *buffer, size_t maxLength, size_t &index, int16_t value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendU32(uint8_t *buffer, size_t maxLength, size_t &index, uint32_t value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendFloat32(uint8_t *buffer, size_t maxLength, size_t &index, float value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendShortString(uint8_t *buffer, size_t maxLength, size_t &index, const char *value) {
  const size_t length = strlen(value);
  if (length > 255) {
    return false;
  }
  if (!appendU8(buffer, maxLength, index, (uint8_t)length)) {
    return false;
  }
  return appendBytes(buffer, maxLength, index, value, length);
}

uint16_t crc16Ccitt(const uint8_t *data, size_t length) {
  uint16_t crc = 0xFFFF;
  for (size_t i = 0; i < length; ++i) {
    crc ^= (uint16_t)data[i] << 8;
    for (uint8_t bit = 0; bit < 8; ++bit) {
      if (crc & 0x8000) {
        crc = (uint16_t)((crc << 1) ^ 0x1021);
      } else {
        crc = (uint16_t)(crc << 1);
      }
    }
  }
  return crc;
}

bool cobsEncode(const uint8_t *input, size_t length, uint8_t *output, size_t maxOutput, size_t &encodedLength) {
  encodedLength = 0;
  if (maxOutput == 0) {
    return false;
  }
  if (length == 0) {
    output[0] = 0x01;
    encodedLength = 1;
    return true;
  }
  size_t codeIndex = 0;
  if (!appendU8(output, maxOutput, encodedLength, 0x00)) {
    return false;
  }
  uint8_t code = 1;
  for (size_t i = 0; i < length; ++i) {
    const uint8_t value = input[i];
    if (value == 0) {
      output[codeIndex] = code;
      codeIndex = encodedLength;
      if (!appendU8(output, maxOutput, encodedLength, 0x00)) {
        return false;
      }
      code = 1;
      continue;
    }
    if (!appendU8(output, maxOutput, encodedLength, value)) {
      return false;
    }
    code++;
    if (code == 0xFF) {
      output[codeIndex] = code;
      codeIndex = encodedLength;
      if (!appendU8(output, maxOutput, encodedLength, 0x00)) {
        return false;
      }
      code = 1;
    }
  }
  output[codeIndex] = code;
  return true;
}

bool cobsDecode(const uint8_t *input, size_t length, uint8_t *output, size_t maxOutput, size_t &decodedLength) {
  decodedLength = 0;
  if (length == 0) {
    return false;
  }
  size_t index = 0;
  while (index < length) {
    const uint8_t code = input[index];
    if (code == 0) {
      return false;
    }
    index++;
    const size_t nextIndex = index + (size_t)code - 1U;
    if (nextIndex > length) {
      return false;
    }
    const size_t chunkLength = nextIndex - index;
    if ((decodedLength + chunkLength) > maxOutput) {
      return false;
    }
    memcpy(output + decodedLength, input + index, chunkLength);
    decodedLength += chunkLength;
    index = nextIndex;
    if ((code != 0xFF) && (index < length)) {
      if (decodedLength >= maxOutput) {
        return false;
      }
      output[decodedLength++] = 0x00;
    }
  }
  return true;
}

bool sendPacket(
  uint8_t msgType,
  uint16_t interfaceId,
  uint32_t packetSeq,
  uint32_t timestampMs,
  const uint8_t *payload,
  size_t payloadLength,
  uint8_t flags = 0
) {
  size_t bodyLength = 0;
  if (!appendU8(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, PROTOCOL_VERSION)) return false;
  if (!appendU8(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, msgType)) return false;
  if (!appendU8(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, flags)) return false;
  if (!appendU16(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, interfaceId)) return false;
  if (!appendU32(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, packetSeq)) return false;
  if (!appendU32(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, timestampMs)) return false;
  if ((payloadLength > 0) && ((payload == nullptr) || !appendBytes(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, payload, payloadLength))) return false;
  const uint16_t crc = crc16Ccitt(txBodyBuffer, bodyLength);
  if (!appendU16(txBodyBuffer, sizeof(txBodyBuffer), bodyLength, crc)) return false;

  size_t frameLength = 0;
  if (!cobsEncode(txBodyBuffer, bodyLength, txFrameBuffer, sizeof(txFrameBuffer), frameLength)) {
    return false;
  }
  if ((frameLength + 1) > sizeof(txFrameBuffer)) {
    return false;
  }
  txFrameBuffer[frameLength++] = 0x00;
  Serial.write(txFrameBuffer, frameLength);
  return true;
}

void sendLog(const char *message) {
  if (message == nullptr) {
    return;
  }
  sendPacket(MSG_LOG, 0, 0, millis(), (const uint8_t *)message, strlen(message));
}

bool buildHelloPayload(uint8_t *buffer, size_t maxLength, size_t &payloadLength) {
  payloadLength = 0;
  if (!appendU8(buffer, maxLength, payloadLength, SCHEMA_VERSION)) return false;
  if (!appendU8(buffer, maxLength, payloadLength, INTERFACE_COUNT)) return false;
  if (!appendShortString(buffer, maxLength, payloadLength, SCHEMA_HASH)) return false;
  if (!appendShortString(buffer, maxLength, payloadLength, DEVICE_UID)) return false;
  if (!appendShortString(buffer, maxLength, payloadLength, FIRMWARE_VERSION)) return false;
  if (!appendShortString(buffer, maxLength, payloadLength, MODEL_NAME)) return false;
  return true;
}

bool buildDescribePayload(uint8_t *buffer, size_t maxLength, size_t &payloadLength) {
  if (!buildHelloPayload(buffer, maxLength, payloadLength)) {
    return false;
  }
  for (uint8_t i = 0; i < INTERFACE_COUNT; ++i) {
    const InterfaceSpec &iface = INTERFACES[i];
    if (!appendU16(buffer, maxLength, payloadLength, iface.id)) return false;
    if (!appendU8(buffer, maxLength, payloadLength, iface.dirCode)) return false;
    if (!appendU8(buffer, maxLength, payloadLength, iface.kindCode)) return false;
    if (!appendU8(buffer, maxLength, payloadLength, iface.flags)) return false;
    if (!appendU16(buffer, maxLength, payloadLength, iface.rateHz)) return false;
    if (!appendShortString(buffer, maxLength, payloadLength, iface.name)) return false;
    if (!appendShortString(buffer, maxLength, payloadLength, iface.channel)) return false;
    if (!appendShortString(buffer, maxLength, payloadLength, iface.profile)) return false;
    if (!appendU8(buffer, maxLength, payloadLength, iface.encodingKind)) return false;
    if (!appendU8(buffer, maxLength, payloadLength, iface.fieldCount)) return false;
    for (uint8_t fieldIndex = 0; fieldIndex < iface.fieldCount; ++fieldIndex) {
      const FieldSpec &field = iface.fields[fieldIndex];
      if (!appendShortString(buffer, maxLength, payloadLength, field.name)) return false;
      if (!appendU8(buffer, maxLength, payloadLength, field.typeCode)) return false;
      if (!appendFloat32(buffer, maxLength, payloadLength, field.scale)) return false;
      if (!appendFloat32(buffer, maxLength, payloadLength, field.offset)) return false;
      if (!appendShortString(buffer, maxLength, payloadLength, field.unit)) return false;
    }
  }
  return true;
}

void sendHello() {
  size_t payloadLength = 0;
  if (!buildHelloPayload(txPayloadBuffer, sizeof(txPayloadBuffer), payloadLength)) {
    return;
  }
  sendPacket(MSG_HELLO, 0, 0, millis(), txPayloadBuffer, payloadLength);
}

void sendDescribe() {
  size_t payloadLength = 0;
  if (!buildDescribePayload(txPayloadBuffer, sizeof(txPayloadBuffer), payloadLength)) {
    return;
  }
  sendPacket(MSG_DESCRIBE, 0, 0, millis(), txPayloadBuffer, payloadLength);
}

int16_t scaledI16(float value, float scale) {
  if (scale == 0.0f) {
    return 0;
  }
  float raw = value / scale;
  if (!isfinite(raw)) {
    raw = 0.0f;
  }
  if (raw > 32767.0f) raw = 32767.0f;
  if (raw < -32768.0f) raw = -32768.0f;
  return (int16_t)lroundf(raw);
}

uint16_t scaledU16(float value, float scale) {
  if (scale == 0.0f) {
    return 0;
  }
  float raw = value / scale;
  if (!isfinite(raw) || raw < 0.0f) {
    raw = 0.0f;
  }
  if (raw > 65535.0f) raw = 65535.0f;
  return (uint16_t)lroundf(raw);
}

bool buildImuFastPayload(const ProcessedSample &sample, uint8_t *buffer, size_t maxLength, size_t &payloadLength) {
  payloadLength = 0;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.accelMps2[0], 0.001f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.accelMps2[1], 0.001f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.accelMps2[2], 0.001f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.gyroDps[0], 0.01f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.gyroDps[1], 0.01f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.gyroDps[2], 0.01f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.tempC, 0.01f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.rollDeg, 0.01f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(sample.pitchDeg, 0.01f))) return false;
  return true;
}

bool buildImuSummaryPayload(const ProcessedSample &sample, uint8_t *buffer, size_t maxLength, size_t &payloadLength) {
  payloadLength = 0;
  const float gyroBiasXDps = gyroBiasRaw[0] / GYRO_LSB_PER_DPS;
  const float gyroBiasYDps = gyroBiasRaw[1] / GYRO_LSB_PER_DPS;
  const float gyroBiasZDps = gyroBiasRaw[2] / GYRO_LSB_PER_DPS;
  if (!appendU16(buffer, maxLength, payloadLength, scaledU16(sample.accelNormG, 0.001f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(gyroBiasXDps, 0.01f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(gyroBiasYDps, 0.01f))) return false;
  if (!appendI16(buffer, maxLength, payloadLength, scaledI16(gyroBiasZDps, 0.01f))) return false;
  if (!appendU32(buffer, maxLength, payloadLength, samplesOk)) return false;
  if (!appendU32(buffer, maxLength, payloadLength, samplesError)) return false;
  if (!appendU16(buffer, maxLength, payloadLength, (uint16_t)calibrationSamplesUsed)) return false;
  return true;
}

void emitHighRateTelemetry(const ProcessedSample &sample) {
  size_t payloadLength = 0;
  if (!buildImuFastPayload(sample, txPayloadBuffer, sizeof(txPayloadBuffer), payloadLength)) {
    return;
  }
  sendPacket(MSG_SAMPLE, INTERFACE_IMU_FAST, sample.seq, sample.ms, txPayloadBuffer, payloadLength);
}

void emitLowRateTelemetry(const ProcessedSample &sample) {
  size_t payloadLength = 0;
  if (!buildImuSummaryPayload(sample, txPayloadBuffer, sizeof(txPayloadBuffer), payloadLength)) {
    return;
  }
  sendPacket(MSG_STATE, INTERFACE_IMU, sample.seq, sample.ms, txPayloadBuffer, payloadLength);
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
  const uint8_t readCount = Wire.requestFrom((int)MPU6050_ADDR, 1, 1);
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
  const uint8_t readCount = Wire.requestFrom((int)MPU6050_ADDR, (int)length, 1);
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

  return true;
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
    sendLog("imu_ready");
  } else {
    imuReady = false;
    sendLog("imu_init_failed");
  }
}

uint16_t readU16(const uint8_t *buffer, size_t index) {
  uint16_t value = 0;
  memcpy(&value, buffer + index, sizeof(value));
  return value;
}

uint32_t readU32(const uint8_t *buffer, size_t index) {
  uint32_t value = 0;
  memcpy(&value, buffer + index, sizeof(value));
  return value;
}

void handleDecodedPacket(const uint8_t *buffer, size_t length) {
  if (length < (HEADER_SIZE + CRC_SIZE)) {
    return;
  }
  const uint16_t expectedCrc = readU16(buffer, length - CRC_SIZE);
  const uint16_t actualCrc = crc16Ccitt(buffer, length - CRC_SIZE);
  if (expectedCrc != actualCrc) {
    return;
  }
  const uint8_t version = buffer[0];
  if (version != PROTOCOL_VERSION) {
    return;
  }
  const uint8_t msgType = buffer[1];
  const uint16_t interfaceId = readU16(buffer, 3);
  const uint32_t packetSeq = readU32(buffer, 5);
  const uint32_t timestampMs = readU32(buffer, 9);
  (void)interfaceId;
  (void)packetSeq;
  if (msgType == MSG_DESCRIBE_REQ) {
    sendHello();
    sendDescribe();
    return;
  }
  if (msgType == MSG_PING) {
    sendPacket(MSG_PONG, 0, packetSeq, millis(), nullptr, 0);
    return;
  }
  (void)timestampMs;
}

void processSerialInput() {
  while (Serial.available() > 0) {
    const int incoming = Serial.read();
    if (incoming < 0) {
      return;
    }
    const uint8_t byteValue = (uint8_t)incoming;
    if (byteValue == 0x00) {
      if (rxFrameLength > 0) {
        size_t decodedLength = 0;
        if (cobsDecode(rxFrameBuffer, rxFrameLength, rxDecodedBuffer, sizeof(rxDecodedBuffer), decodedLength)) {
          handleDecodedPacket(rxDecodedBuffer, decodedLength);
        }
      }
      rxFrameLength = 0;
      continue;
    }
    if (rxFrameLength >= sizeof(rxFrameBuffer)) {
      rxFrameLength = 0;
      continue;
    }
    rxFrameBuffer[rxFrameLength++] = byteValue;
  }
}

}  // namespace

void setup() {
  Serial.begin(SERIAL_BAUD);
  delay(50);

  Wire.begin();
  // Rely on the breakout's bus pull-ups rather than the Nano's weak internal
  // pull-ups so the IMU side can stay at its own logic rail.
  digitalWrite(A4, LOW);
  digitalWrite(A5, LOW);
  Wire.setClock(I2C_CLOCK_HZ);

  tryInitImu();
  sendHello();
  sendDescribe();
}

void loop() {
  processSerialInput();

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
    if (consecutiveErrors == 1) {
      sendLog("imu_read_failed");
    }
    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      imuReady = false;
      resetFilterState();
      sendLog("imu_reinit");
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
