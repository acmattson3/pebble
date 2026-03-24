#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <WEMOS_Motor.h>
#include <ArduinoJson.h>
#include <string.h>
#include <time.h>

// Local credentials live in private_config.h.
#include "private_config.h"

// --------- I2C motor controller setup ---------
// Default D1 mini I2C pins.
const uint8_t I2C_SDA_PIN = D2;
const uint8_t I2C_SCL_PIN = D1;

// Single motor controller board with two channels.
const uint8_t MOTOR_CTRL_ADDR = 0x30;
Motor MOTOR_LEFT(MOTOR_CTRL_ADDR, _MOTOR_A, 1000);
Motor MOTOR_RIGHT(MOTOR_CTRL_ADDR, _MOTOR_B, 1000);

// Invert these flags when wiring or motor orientation is reversed.
const bool INVERT_LEFT_MOTOR = false;
const bool INVERT_RIGHT_MOTOR = true;

// Keep legacy transform from the previous Wemos firmware for joystick parity.
const bool DRIVE_NEGATE_Z = true;
const bool DRIVE_SWAP_LEFT_RIGHT = true;

// --------- MQTT ---------
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);

String topicDrive;
String topicOnline;
String topicChargingStatus;

float targetLeft = 0.0f;
float targetRight = 0.0f;
unsigned long lastDriveCmdMs = 0;
unsigned long lastHeartbeatMs = 0;
unsigned long lastChargingMs = 0;
unsigned long lastTimeSyncAttemptMs = 0;

const unsigned long MOTOR_CMD_TIMEOUT_MS = 1000;
const unsigned long HEARTBEAT_PERIOD_MS = 250;
const unsigned long CHARGING_PERIOD_MS = 5000;
const unsigned long TIME_SYNC_RETRY_MS = 10000;

bool timeSynced = false;

static inline void dbgPrintln(const char *msg) {
  if (!DEBUG_LOGS) return;
  Serial.println(msg);
}

static inline float clampf(float v, float lo, float hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

static inline bool isTopicEqual(const char *a, const String &b) {
  return strcmp(a, b.c_str()) == 0;
}

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

void setOneMotor(Motor &motor, float value, bool invertDirection) {
  value = clampf(value, -1.0f, 1.0f);
  if (invertDirection) {
    value = -value;
  }

  if (value > 0.0f) {
    uint8_t pwm = (uint8_t)(value * 100.0f + 0.5f);
    motor.setmotor(_CW, pwm);
  } else if (value < 0.0f) {
    uint8_t pwm = (uint8_t)((-value) * 100.0f + 0.5f);
    motor.setmotor(_CCW, pwm);
  } else {
    motor.setmotor(_STOP, 0);
  }
}

void driveMotors(float left, float right) {
  setOneMotor(MOTOR_LEFT, left, INVERT_LEFT_MOTOR);
  setOneMotor(MOTOR_RIGHT, right, INVERT_RIGHT_MOTOR);
}

void stopMotors() {
  MOTOR_LEFT.setmotor(_STOP, 0);
  MOTOR_RIGHT.setmotor(_STOP, 0);
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
  return mqtt.publish(topicOnline.c_str(), reinterpret_cast<const uint8_t *>(payload), len, true);
}

void publishChargingStatusFalse() {
  StaticJsonDocument<96> doc;
  doc["value"] = false;
  char payload[96];
  size_t len = serializeJson(doc, payload, sizeof(payload));
  mqtt.publish(topicChargingStatus.c_str(), reinterpret_cast<const uint8_t *>(payload), len, false);
}

void handleDriveCommand(const JsonVariant &payload) {
  float x = clampf(payload["x"] | 0.0f, -1.0f, 1.0f);
  float z = clampf(payload["z"] | 0.0f, -1.0f, 1.0f);
  // Compatibility: accept optional auxiliary `y` but ignore it on 2-channel hardware.
  (void)(payload["y"] | 0.0f);
  if (DRIVE_NEGATE_Z) {
    z = -z;
  }

  float left = clampf(z + x, -1.0f, 1.0f);
  float right = clampf(z - x, -1.0f, 1.0f);

  if (DRIVE_SWAP_LEFT_RIGHT) {
    float tmp = left;
    left = right;
    right = tmp;
  }

  targetLeft = left;
  targetRight = right;
  lastDriveCmdMs = millis();
}

void onMqttMessage(char *topic, byte *payload, unsigned int length) {
  if (!isTopicEqual(topic, topicDrive)) {
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

bool mqttConnectWithAuth(const char *clientId) {
  if (strlen(MQTT_USER) == 0) {
    return mqtt.connect(clientId);
  }
  return mqtt.connect(clientId, MQTT_USER, MQTT_PASS);
}

void connectMqtt() {
  while (!mqtt.connected()) {
    String clientId = String("bumperbot-wemos-") + String(ESP.getChipId(), HEX);
    if (DEBUG_LOGS) {
      Serial.print("MQTT connecting as ");
      Serial.println(clientId);
    }
    if (mqttConnectWithAuth(clientId.c_str())) {
      mqtt.subscribe(topicDrive.c_str(), 1);
      publishHeartbeat();
      publishChargingStatusFalse();
      lastHeartbeatMs = millis();
      lastChargingMs = millis();
      if (DEBUG_LOGS) {
        Serial.print("MQTT subscribed: ");
        Serial.println(topicDrive);
      }
    } else {
      if (DEBUG_LOGS) {
        Serial.print("MQTT connect failed, rc=");
        Serial.println(mqtt.state());
      }
      delay(1000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  dbgPrintln("Booting BumperBot Wemos (ESP8266)...");

  Wire.begin(I2C_SDA_PIN, I2C_SCL_PIN);
  stopMotors();
  lastDriveCmdMs = millis();

  topicDrive = String(SYSTEM_NAME) + "/robots/" + ROBOT_ID + "/incoming/drive-values";
  topicOnline = String(SYSTEM_NAME) + "/robots/" + ROBOT_ID + "/outgoing/online";
  topicChargingStatus = String(SYSTEM_NAME) + "/robots/" + ROBOT_ID + "/outgoing/charging-status";

  connectWifi();
  ensureTimeSync();
  lastTimeSyncAttemptMs = millis();
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
  }
  driveMotors(targetLeft, targetRight);

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
    publishChargingStatusFalse();
  }
}
