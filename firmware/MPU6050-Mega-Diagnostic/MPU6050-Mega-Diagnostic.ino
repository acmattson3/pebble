#include <Wire.h>
#include <math.h>

// Arduino Mega 2560: SDA=20, SCL=21. Intended for MPU-6050 / GY-521.
const uint8_t ADDR0 = 0x68;
const uint8_t ADDR1 = 0x69;
const uint8_t REG_WHO_AM_I = 0x75;
const uint8_t REG_PWR_MGMT_1 = 0x6B;
const uint8_t REG_CONFIG = 0x1A;
const uint8_t REG_SMPLRT_DIV = 0x19;
const uint8_t REG_GYRO_CONFIG = 0x1B;
const uint8_t REG_ACCEL_CONFIG = 0x1C;
const uint8_t REG_ACCEL_XOUT_H = 0x3B;
const float G0 = 9.80665f;
const float ACCEL_LSB_G = 16384.0f;  // +/- 2 g
const float GYRO_LSB_DPS = 131.0f;   // +/- 250 deg/s
const float DEG2RAD_F = 0.01745329252f;
const float RAD2DEG_F = 57.29577951f;
const float MAHONY_KP = 1.5f;
const uint16_t CAL_N = 200;

uint8_t imuAddr = 0;
float gyroBias[3] = {0, 0, 0};
float q0 = 1, q1 = 0, q2 = 0, q3 = 0;
bool filterReady = false;
uint32_t seq = 0;
uint32_t nextSampleMs = 0;
uint32_t lastUpdateUs = 0;

float clampUnit(float x) {
  if (x < -1.0f) return -1.0f;
  if (x > 1.0f) return 1.0f;
  return x;
}

bool writeReg(uint8_t reg, uint8_t value) {
  Wire.beginTransmission(imuAddr);
  Wire.write(reg);
  Wire.write(value);
  return Wire.endTransmission() == 0;
}

bool readRegs(uint8_t reg, uint8_t *dst, uint8_t count) {
  Wire.beginTransmission(imuAddr);
  Wire.write(reg);
  uint8_t e = Wire.endTransmission(false);
  if (e != 0) return false;
  uint8_t got = Wire.requestFrom(imuAddr, count, (uint8_t)true);
  if (got != count) return false;
  for (uint8_t i = 0; i < count; ++i) {
    if (!Wire.available()) return false;
    dst[i] = (uint8_t)Wire.read();
  }
  return true;
}

bool probe(uint8_t addr, uint8_t &who) {
  imuAddr = addr;
  Wire.beginTransmission(addr);
  if (Wire.endTransmission() != 0) return false;
  return readRegs(REG_WHO_AM_I, &who, 1);
}

int16_t be16(const uint8_t *p) { return (int16_t)(((uint16_t)p[0] << 8) | p[1]); }

void initializeOrientation(float ax, float ay, float az) {
  float roll = atan2f(ay, az);
  float pitch = atan2f(-ax, sqrtf(ay * ay + az * az));
  float cr = cosf(roll * 0.5f), sr = sinf(roll * 0.5f);
  float cp = cosf(pitch * 0.5f), sp = sinf(pitch * 0.5f);
  q0 = cr * cp;
  q1 = sr * cp;
  q2 = cr * sp;
  q3 = -sr * sp;
  filterReady = true;
}

void updateOrientation(float ax, float ay, float az, float gx, float gy, float gz, float dt) {
  float norm = sqrtf(ax * ax + ay * ay + az * az);
  if (norm < 0.01f || dt <= 0.0f || dt > 0.2f) return;
  ax /= norm; ay /= norm; az /= norm;
  if (!filterReady) initializeOrientation(ax, ay, az);

  // Mahony's gravity correction constrains tilt in quaternion space and avoids
  // integrating Euler roll/pitch directly near their singularities.
  float vx = q1 * q3 - q0 * q2;
  float vy = q0 * q1 + q2 * q3;
  float vz = q0 * q0 - 0.5f + q3 * q3;
  float ex = ay * vz - az * vy;
  float ey = az * vx - ax * vz;
  float ez = ax * vy - ay * vx;
  gx += 2.0f * MAHONY_KP * ex;
  gy += 2.0f * MAHONY_KP * ey;
  gz += 2.0f * MAHONY_KP * ez;

  float halfDt = 0.5f * dt;
  float qa = q0, qb = q1, qc = q2, qd = q3;
  q0 += (-qb * gx - qc * gy - qd * gz) * halfDt;
  q1 += ( qa * gx + qc * gz - qd * gy) * halfDt;
  q2 += ( qa * gy - qb * gz + qd * gx) * halfDt;
  q3 += ( qa * gz + qb * gy - qc * gx) * halfDt;
  norm = sqrtf(q0 * q0 + q1 * q1 + q2 * q2 + q3 * q3);
  if (norm > 0.0f) { q0 /= norm; q1 /= norm; q2 /= norm; q3 /= norm; }
}

void getEulerDegrees(float &roll, float &pitch, float &yaw) {
  roll = atan2f(2.0f * (q0 * q1 + q2 * q3), 1.0f - 2.0f * (q1 * q1 + q2 * q2)) * RAD2DEG_F;
  pitch = asinf(clampUnit(2.0f * (q0 * q2 - q3 * q1))) * RAD2DEG_F;
  yaw = atan2f(2.0f * (q0 * q3 + q1 * q2), 1.0f - 2.0f * (q2 * q2 + q3 * q3)) * RAD2DEG_F;
}

bool readSample(int16_t a[3], int16_t &temp, int16_t g[3]) {
  uint8_t b[14];
  if (!readRegs(REG_ACCEL_XOUT_H, b, sizeof(b))) return false;
  a[0] = be16(b + 0); a[1] = be16(b + 2); a[2] = be16(b + 4);
  temp = be16(b + 6);
  g[0] = be16(b + 8); g[1] = be16(b + 10); g[2] = be16(b + 12);
  return true;
}

void setup() {
  Serial.begin(115200);
  delay(300);
  Serial.println(F("BOOT,mega2560,mpu6050_diag_v1"));
  Serial.println(F("I2C pins: SDA=D20, SCL=D21; testing without external pull resistors"));

  Wire.begin();
  Wire.setClock(100000UL);
  Wire.setWireTimeout(25000, true);

  uint8_t who = 0;
  if (probe(ADDR0, who)) {
    imuAddr = ADDR0;
    Serial.print(F("FOUND,address=0x68,who=0x")); Serial.println(who, HEX);
  } else if (probe(ADDR1, who)) {
    imuAddr = ADDR1;
    Serial.print(F("FOUND,address=0x69,who=0x")); Serial.println(who, HEX);
  } else {
    Serial.println(F("ERROR,no MPU at 0x68 or 0x69; check power, ground, SDA20, SCL21"));
    return;
  }

  if (!writeReg(REG_PWR_MGMT_1, 0x01)) { Serial.println(F("ERROR,wake_failed")); return; }
  delay(100);
  if (!writeReg(REG_CONFIG, 0x03) ||       // DLPF ~44 Hz accel / 42 Hz gyro
      !writeReg(REG_SMPLRT_DIV, 19) ||     // 1 kHz / (19+1) = 50 Hz
      !writeReg(REG_GYRO_CONFIG, 0x00) ||  // +/-250 deg/s
      !writeReg(REG_ACCEL_CONFIG, 0x00)) { // +/-2 g
    Serial.println(F("ERROR,configuration_write_failed")); return;
  }
  delay(100);

  Serial.println(F("CALIBRATING,keep_imu_still,200_samples"));
  int32_t sums[3] = {0, 0, 0};
  uint16_t good = 0;
  for (uint16_t i = 0; i < CAL_N; ++i) {
    int16_t a[3], t, g[3];
    if (readSample(a, t, g)) {
      sums[0] += g[0]; sums[1] += g[1]; sums[2] += g[2];
      ++good;
    }
    delay(5);
  }
  if (good < CAL_N * 9 / 10) {
    Serial.print(F("ERROR,calibration_reads=")); Serial.println(good);
    return;
  }
  gyroBias[0] = (float)sums[0] / good;
  gyroBias[1] = (float)sums[1] / good;
  gyroBias[2] = (float)sums[2] / good;
  Serial.print(F("CAL_OK,samples=")); Serial.print(good);
  Serial.print(F(",gyro_bias_raw="));
  Serial.print(gyroBias[0], 2); Serial.print(',');
  Serial.print(gyroBias[1], 2); Serial.print(',');
  Serial.println(gyroBias[2], 2);
  Serial.println(F("t_ms,seq,ax_mps2,ay_mps2,az_mps2,gx_dps,gy_dps,gz_dps,temp_c,qw,qx,qy,qz,roll_deg,pitch_deg,yaw_deg,accel_norm_g"));
  nextSampleMs = millis();
  lastUpdateUs = micros();
}

void loop() {
  if (!imuAddr) { delay(500); return; }
  uint32_t now = millis();
  if ((int32_t)(now - nextSampleMs) < 0) return;
  nextSampleMs += 20;
  if ((int32_t)(now - nextSampleMs) > 100) nextSampleMs = now + 20;

  int16_t ar[3], tr, gr[3];
  if (!readSample(ar, tr, gr)) {
    Serial.print(F("ERROR,read_failed,t_ms=")); Serial.println(now);
    return;
  }
  float ax = ar[0] / ACCEL_LSB_G * G0;
  float ay = ar[1] / ACCEL_LSB_G * G0;
  float az = ar[2] / ACCEL_LSB_G * G0;
  float gx = (gr[0] - gyroBias[0]) / GYRO_LSB_DPS;
  float gy = (gr[1] - gyroBias[1]) / GYRO_LSB_DPS;
  float gz = (gr[2] - gyroBias[2]) / GYRO_LSB_DPS;
  float normG = sqrtf(ax * ax + ay * ay + az * az) / G0;
  uint32_t nowUs = micros();
  float dt = (uint32_t)(nowUs - lastUpdateUs) * 0.000001f;
  lastUpdateUs = nowUs;
  updateOrientation(ax, ay, az, gx * DEG2RAD_F, gy * DEG2RAD_F, gz * DEG2RAD_F, dt);
  float rollDeg, pitchDeg, yawDeg;
  getEulerDegrees(rollDeg, pitchDeg, yawDeg);
  float tempC = tr / 340.0f + 36.53f;

  Serial.print(now); Serial.print(','); Serial.print(++seq); Serial.print(',');
  Serial.print(ax, 3); Serial.print(','); Serial.print(ay, 3); Serial.print(','); Serial.print(az, 3); Serial.print(',');
  Serial.print(gx, 3); Serial.print(','); Serial.print(gy, 3); Serial.print(','); Serial.print(gz, 3); Serial.print(',');
  Serial.print(tempC, 2); Serial.print(',');
  Serial.print(q0, 5); Serial.print(','); Serial.print(q1, 5); Serial.print(',');
  Serial.print(q2, 5); Serial.print(','); Serial.print(q3, 5); Serial.print(',');
  Serial.print(rollDeg, 2); Serial.print(','); Serial.print(pitchDeg, 2); Serial.print(','); Serial.print(yawDeg, 2); Serial.print(',');
  Serial.println(normG, 3);
}
