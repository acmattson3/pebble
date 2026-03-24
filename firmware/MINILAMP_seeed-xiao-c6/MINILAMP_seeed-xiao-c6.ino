#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <string.h>
#include <time.h>
#include <math.h>

// Local credentials live in private_config.h.
#include "private_config.h"

// --------- battery sensing ---------
// D0 is tied to the Wemos R330 battery shield divider via a 100k/100k pair.
const uint8_t PIN_BATT = D0;
// External dividers use (R1 + R2) / R2.
// Two 100k resistors -> ratio 2.0.
const float BATTERY_DIVIDER_RATIO = 2.0f;
const float BATTERY_ADC_REF = 3.3f;
const float BATTERY_ADC_MAX = 4095.0f;      // 12-bit ADC (fallback if mV API unavailable)

// --------- DRV8833 pin mapping ---------
// Back DRV8833
// IN1 -> D5, IN2 -> D6, IN3 -> D9, IN4 -> D10
// Front DRV8833
// IN1 -> D1, IN2 -> D2, IN3 -> D3, IN4 -> D4
const uint8_t PIN_BACK_IN1 = D6;
const uint8_t PIN_BACK_IN2 = D5;
const uint8_t PIN_BACK_IN3 = D7;
const uint8_t PIN_BACK_IN4 = D8;

const uint8_t PIN_FRONT_IN1 = D1;
const uint8_t PIN_FRONT_IN2 = D2;
const uint8_t PIN_FRONT_IN3 = D4;
const uint8_t PIN_FRONT_IN4 = D3;

struct MotorPins {
  uint8_t in1;
  uint8_t in2;
};

static inline float clampf(float v, float lo, float hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

static inline float quantizePwm(float v) {
  float mag = fabsf(v);
  if (mag < 0.25f) return 0.0f;
  return v > 0.0f ? 1.0f : -1.0f;
}

static inline void dbgPrint(const char *msg) {
  if (!DEBUG_LOGS) return;
  Serial.print(msg);
}

static inline void dbgPrintln(const char *msg) {
  if (!DEBUG_LOGS) return;
  Serial.println(msg);
}

void setupMotorPin(uint8_t pin) {
  pinMode(pin, OUTPUT);
  digitalWrite(pin, LOW);
}

// Motor mapping (front/back, left/right)
// Front left  -> front IN1/IN2
// Front right -> front IN3/IN4
// Back left   -> back IN1/IN2 (direction inverted via pin swap below)
// Back right  -> back IN3/IN4 (direction inverted via pin swap below)
const MotorPins MOTOR_FRONT_LEFT  = { PIN_FRONT_IN1, PIN_FRONT_IN2 };
const MotorPins MOTOR_FRONT_RIGHT = { PIN_FRONT_IN3, PIN_FRONT_IN4 };
const MotorPins MOTOR_BACK_LEFT   = { PIN_BACK_IN2,  PIN_BACK_IN1 };
const MotorPins MOTOR_BACK_RIGHT  = { PIN_BACK_IN4,  PIN_BACK_IN3 };

// --------- MQTT ---------
#if MQTT_USE_TLS
WiFiClientSecure wifiClient;
#else
WiFiClient wifiClient;
#endif
PubSubClient mqtt(wifiClient);

String topicDrive;
String topicHeartbeat;
String topicChargingLevel;

unsigned long lastDriveLogMs = 0;
unsigned long lastDriveCmdMs = 0;
const unsigned long MOTOR_CMD_TIMEOUT_MS = 1000;
float targetLeft = 0.0f;
float targetRight = 0.0f;
float targetGrouser = 0.0f;
unsigned long lastChargingMs = 0;
const unsigned long CHARGING_PERIOD_MS = 5000;
unsigned long lastHeartbeatMs = 0;
const unsigned long HEARTBEAT_PERIOD_MS = 250;
unsigned long lastTimeSyncAttemptMs = 0;
const unsigned long TIME_SYNC_RETRY_MS = 10000;

void stopMotors() {
  digitalWrite(MOTOR_FRONT_LEFT.in1, LOW);
  digitalWrite(MOTOR_FRONT_LEFT.in2, LOW);
  digitalWrite(MOTOR_FRONT_RIGHT.in1, LOW);
  digitalWrite(MOTOR_FRONT_RIGHT.in2, LOW);
  digitalWrite(MOTOR_BACK_LEFT.in1, LOW);
  digitalWrite(MOTOR_BACK_LEFT.in2, LOW);
  digitalWrite(MOTOR_BACK_RIGHT.in1, LOW);
  digitalWrite(MOTOR_BACK_RIGHT.in2, LOW);
}

void driveOneMotor(const MotorPins &m, float val) {
  val = clampf(val, -1.0f, 1.0f);
  if (DEBUG_LOGS) {
    Serial.print("t=");
    Serial.print(millis());
    Serial.print(" Motor set dir=");
    Serial.print(val > 0.0f ? "FWD" : (val < 0.0f ? "REV" : "STOP"));
    Serial.println();
  }
  if (val > 0.0f) {
    digitalWrite(m.in1, HIGH);
    digitalWrite(m.in2, LOW);
  } else if (val < 0.0f) {
    digitalWrite(m.in1, LOW);
    digitalWrite(m.in2, HIGH);
  } else {
    digitalWrite(m.in1, LOW);
    digitalWrite(m.in2, LOW);
  }
}

void driveMotors(float left, float right) {
  if (DEBUG_LOGS) {
    Serial.print("t=");
    Serial.print(millis());
    Serial.print(" driveMotors L=");
    Serial.print(left, 3);
    Serial.print(" R=");
    Serial.println(right, 3);
  }
  driveOneMotor(MOTOR_FRONT_LEFT, left);
  driveOneMotor(MOTOR_BACK_LEFT, left);
  driveOneMotor(MOTOR_FRONT_RIGHT, right);
  driveOneMotor(MOTOR_BACK_RIGHT, right);
}

void driveGrousers(float val) {
  driveOneMotor(MOTOR_FRONT_LEFT, val);
  driveOneMotor(MOTOR_FRONT_RIGHT, val);
  driveOneMotor(MOTOR_BACK_LEFT, -val);
  driveOneMotor(MOTOR_BACK_RIGHT, -val);
}

void driveMotors(float left, float right, float grouser) {
  if (DEBUG_LOGS) {
    Serial.print("t=");
    Serial.print(millis());
    Serial.print(" driveMotors L=");
    Serial.print(left, 3);
    Serial.print(" R=");
    Serial.print(right, 3);
    Serial.print(" G=");
    Serial.println(grouser, 3);
  }
  // Clamp each motor after mixing to avoid overdriving
  driveOneMotor(MOTOR_FRONT_LEFT, left + grouser);
  driveOneMotor(MOTOR_FRONT_RIGHT, right - grouser);
  driveOneMotor(MOTOR_BACK_LEFT, left - grouser);
  driveOneMotor(MOTOR_BACK_RIGHT, right + grouser);
}

bool timeSynced = false;

static bool ensureTimeSync() {
  if (timeSynced) return true;
  configTime(0, 0, "pool.ntp.org", "time.nist.gov", "time.google.com");
  for (int i = 0; i < 20; i++) {
    time_t now = time(nullptr);
    if (now > 1600000000) {
      timeSynced = true;
      return true;
    }
    delay(250);
  }
  return false;
}

static uint64_t currentUnixMillis() {
  time_t now = time(nullptr);
  if (now <= 1600000000) {
    return 0;
  }
  return (uint64_t)now * 1000ULL;
}

bool publishHeartbeat() {
  uint64_t unixMs = currentUnixMillis();
  if (unixMs == 0) {
    timeSynced = false;
    if (!ensureTimeSync()) {
      dbgPrintln("Heartbeat skipped: time sync unavailable");
      return false;
    }
    unixMs = currentUnixMillis();
    if (unixMs == 0) {
      dbgPrintln("Heartbeat skipped: invalid unix time");
      return false;
    }
  } else {
    timeSynced = true;
  }

  StaticJsonDocument<96> doc;
  doc["t"] = unixMs;
  char payload[96];
  size_t len = serializeJson(doc, payload, sizeof(payload));
  mqtt.publish(topicHeartbeat.c_str(), reinterpret_cast<const uint8_t*>(payload), len, true);
  if (DEBUG_LOGS) {
    Serial.println("MQTT heartbeat publish");
  }
  return true;
}

float readBatteryVoltage() {
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

void publishBatteryVoltage() {
  StaticJsonDocument<96> doc;
  doc["value"] = readBatteryVoltage();
  doc["unit"] = "v";
  char payload[96];
  size_t len = serializeJson(doc, payload, sizeof(payload));
  mqtt.publish(topicChargingLevel.c_str(), reinterpret_cast<const uint8_t*>(payload), len, false);
}

void handleDriveCommand(const JsonVariant &payload) {
  float x = payload["x"] | 0.0f;
  float z = payload["z"] | 0.0f;
  float y = payload["y"] | 0.0f;

  float left = z + x;
  float right = z - x;

  if (DEBUG_LOGS) {
    Serial.print("t=");
    Serial.print(millis());
    Serial.print(" handleDrive x=");
    Serial.print(x, 3);
    Serial.print(" z=");
    Serial.print(z, 3);
    Serial.print(" y=");
    Serial.print(y, 3);
    Serial.print(" -> L=");
    Serial.print(left, 3);
    Serial.print(" R=");
    Serial.println(right, 3);
  }

  targetLeft = quantizePwm(left);
  targetRight = quantizePwm(right);
  targetGrouser = quantizePwm(y);
  lastDriveCmdMs = millis();
  if (DEBUG_LOGS) {
    unsigned long now = millis();
    if (now - lastDriveLogMs > 250UL) {
      lastDriveLogMs = now;
      Serial.print("Drive x=");
      Serial.print(x, 3);
      Serial.print(" z=");
      Serial.print(z, 3);
      Serial.print(" y=");
      Serial.print(y, 3);
      Serial.print(" -> L=");
      Serial.print(left, 3);
      Serial.print(" R=");
      Serial.println(right, 3);
    }
  }
}

void onMqttMessage(char *topic, byte *payload, unsigned int length) {
  if (DEBUG_LOGS) {
    Serial.print("t=");
    Serial.print(millis());
    Serial.print(" MQTT message topic=");
    Serial.print(topic);
    Serial.print(" len=");
    Serial.println(length);
  }
  if (topicDrive != topic) {
    if (DEBUG_LOGS) {
      Serial.print("t=");
      Serial.print(millis());
      Serial.println(" MQTT topic mismatch; ignoring");
    }
    return;
  }

  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, payload, length);
  if (err) {
    if (DEBUG_LOGS) {
      Serial.print("MQTT JSON error: ");
      Serial.println(err.c_str());
    }
    return;
  }

  if (DEBUG_LOGS) {
    Serial.print("t=");
    Serial.print(millis());
    Serial.println(" MQTT JSON parsed OK");
  }

  if (doc.containsKey("value") && doc["value"].is<JsonObject>()) {
    handleDriveCommand(doc["value"]);
  } else {
    handleDriveCommand(doc.as<JsonVariant>());
  }
}

void connectWifi() {
  if (DEBUG_LOGS) {
    Serial.print("WiFi connecting to ");
    Serial.println(WIFI_SSID);
  }
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) {
    delay(250);
  }
  if (DEBUG_LOGS) {
    Serial.print("WiFi connected. IP=");
    Serial.println(WiFi.localIP());
  }
}

void connectMqtt() {
  while (!mqtt.connected()) {
    uint64_t chipId = ESP.getEfuseMac();
    String clientId = String("minilamp-") + String((uint32_t)(chipId & 0xFFFFFFFF), HEX);
    if (DEBUG_LOGS) {
      Serial.print("MQTT connecting as ");
      Serial.println(clientId);
    }
    if (mqtt.connect(clientId.c_str(), MQTT_USER, MQTT_PASS)) {
      mqtt.subscribe(topicDrive.c_str(), 1);
      if (DEBUG_LOGS) {
        Serial.print("MQTT subscribed: ");
        Serial.println(topicDrive);
      }
      publishHeartbeat();
      lastHeartbeatMs = millis();
    } else {
      if (DEBUG_LOGS) {
        Serial.print("MQTT connect failed, rc=");
        Serial.println(mqtt.state());
      }
      delay(1000);
    }
  }
}

void configureMqttTls() {
#if MQTT_USE_TLS
  if (MQTT_TLS_INSECURE) {
    wifiClient.setInsecure();
    dbgPrintln("MQTT TLS: insecure mode enabled");
  } else {
    if (strlen(MQTT_CA_CERT) < 32) {
      dbgPrintln("MQTT TLS enabled but CA cert is missing/short");
    }
    wifiClient.setCACert(MQTT_CA_CERT);
    dbgPrintln("MQTT TLS: CA cert configured");
  }
#endif
}

void setup() {
  pinMode(WIFI_ENABLE, OUTPUT);
  digitalWrite(WIFI_ENABLE, LOW); // Activate RF switch control

  delay(100);

  pinMode(WIFI_ANT_CONFIG, OUTPUT);
  digitalWrite(WIFI_ANT_CONFIG, HIGH); // Use external antenna

  Serial.begin(115200);
  dbgPrintln("Booting MiniLAMP (Seeed ESP32C6)...");

  setupMotorPin(MOTOR_FRONT_LEFT.in1);
  setupMotorPin(MOTOR_FRONT_LEFT.in2);
  setupMotorPin(MOTOR_FRONT_RIGHT.in1);
  setupMotorPin(MOTOR_FRONT_RIGHT.in2);
  setupMotorPin(MOTOR_BACK_LEFT.in1);
  setupMotorPin(MOTOR_BACK_LEFT.in2);
  setupMotorPin(MOTOR_BACK_RIGHT.in1);
  setupMotorPin(MOTOR_BACK_RIGHT.in2);
  stopMotors();
  lastDriveCmdMs = millis();
  pinMode(PIN_BATT, INPUT);
#if defined(ARDUINO_ARCH_ESP32)
  analogReadResolution(12);
  analogSetPinAttenuation(PIN_BATT, ADC_11db);
#endif

  topicDrive = String(SYSTEM_NAME) + "/robots/" + ROBOT_ID + "/incoming/drive-values";
  topicHeartbeat = String(SYSTEM_NAME) + "/robots/" + ROBOT_ID + "/outgoing/online";
  topicChargingLevel = String(SYSTEM_NAME) + "/robots/" + ROBOT_ID + "/outgoing/charging-level";

  connectWifi();
  ensureTimeSync();
  lastTimeSyncAttemptMs = millis();
  configureMqttTls();
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMqttMessage);
  connectMqtt();
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    connectWifi();
  }
  if (!mqtt.connected()) {
    connectMqtt();
  }
  mqtt.loop();

  unsigned long now = millis();
  if (now - lastDriveCmdMs > MOTOR_CMD_TIMEOUT_MS) {
    targetLeft = 0.0f;
    targetRight = 0.0f;
    targetGrouser = 0.0f;
  }
  driveMotors(targetLeft, targetRight, targetGrouser);
  if (mqtt.connected() && (now - lastHeartbeatMs >= HEARTBEAT_PERIOD_MS)) {
    lastHeartbeatMs = now;
    publishHeartbeat();
  }
  if (!timeSynced && (now - lastTimeSyncAttemptMs >= TIME_SYNC_RETRY_MS)) {
    lastTimeSyncAttemptMs = now;
    ensureTimeSync();
  }
  if (mqtt.connected() && (now - lastChargingMs >= CHARGING_PERIOD_MS)) {
    lastChargingMs = now;
    publishBatteryVoltage();
  }
}
