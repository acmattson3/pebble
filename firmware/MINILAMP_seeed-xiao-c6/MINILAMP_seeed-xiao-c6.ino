#include <Arduino.h>
#include <WiFi.h>
#include <atomic>
#include <time.h>
#include <cJSON.h>
#include <mqtt_client.h>
#include <esp_crt_bundle.h>
#include <esp_task_wdt.h>
#include <esp_system.h>
#include <esp_random.h>
#include "private_config.h"
#include "RobotConfig.h"
#include "GeneratedManifest.h"
#include "DriveSafety.h"

// ESP-MQTT owns networking in its task. Only the higher-priority motor task
// touches motor outputs; neither TLS nor MQTT publishing can delay its timeout.
enum CommandKind : uint8_t { DRIVE, GROUSER, STOP };
struct PendingCommand {
  CommandKind kind;
  uint32_t receivedAt, epoch;
  float x, z, y;
  bool hasY;
};
struct MotorSnapshot {
  float applied[4];
  uint32_t driveAt, grouserAt, accepted;
  bool driveActive, grouserActive;
};
QueueHandle_t driveQueue;
portMUX_TYPE snapshotMux = portMUX_INITIALIZER_UNLOCKED;
MotorSnapshot motorSnapshot = {};
std::atomic<bool> wifiUp{false}, mqttUp{false}, announce{false}, motorsReady{false};
std::atomic<uint32_t> connectionEpoch{0}, rejectedCommands{0}, retainedIgnored{0}, batteryMv{0};
esp_mqtt_client_handle_t mqtt = nullptr;
char driveTopic[128], grouserTopic[128], onlineTopic[128], manifestTopic[128];
char moduleStatusTopic[128], batteryTopic[128], statusTopic[128];
char bootId[20], clientId[48], willPayload[96];

void setupMotorPins() {
  for (const auto &m : MOTORS) {
    digitalWrite(m.forward, LOW); pinMode(m.forward, OUTPUT);
    digitalWrite(m.reverse, LOW); pinMode(m.reverse, OUTPUT);
  }
}

void writeMotor(uint8_t channel, float value) {
  const MotorConfig &m = MOTORS[MOTOR_OUTPUT_ORDER[channel]];
  if (MOTOR_INVERT[channel]) value = -value;
  // On/off outputs, as on the original MiniLAMP. Always lower the inactive
  // input before raising the active one, including direction reversals.
  if (value > 0) {
    digitalWrite(m.reverse, LOW); digitalWrite(m.forward, HIGH);
  } else if (value < 0) {
    digitalWrite(m.forward, LOW); digitalWrite(m.reverse, HIGH);
  } else {
    digitalWrite(m.forward, LOW); digitalWrite(m.reverse, LOW);
  }
}

bool configureMotorWatchdog() {
  esp_task_wdt_config_t config = {};
  config.timeout_ms = 2000;
  config.idle_core_mask = 0;
  config.trigger_panic = true;
  esp_err_t err = esp_task_wdt_reconfigure(&config);
  if (err == ESP_ERR_INVALID_STATE) err = esp_task_wdt_init(&config);
  if (err != ESP_OK) return false;
  if (esp_task_wdt_status(nullptr) != ESP_OK && esp_task_wdt_add(nullptr) != ESP_OK) return false;
  return true;
}

void motorTask(void *) {
  MiniLamp::DriveSafety state;
  uint32_t epoch = connectionEpoch.load();
  uint32_t accepted = 0;
  uint8_t seen = 0;
  bool ready = true;
  for (uint8_t index : MOTOR_OUTPUT_ORDER) {
    if (index >= 4 || (seen & (1 << index))) ready = false;
    else seen |= 1 << index;
  }
  const bool watchdogReady = configureMotorWatchdog();
  ready = ready && watchdogReady;
  motorsReady = ready;
  for (;;) {
    const uint32_t now = millis();
    const uint32_t currentEpoch = connectionEpoch.load();
    if (currentEpoch != epoch || !wifiUp || !mqttUp || !ready) {
      state.stop();
      if (currentEpoch != epoch) { epoch = currentEpoch; xQueueReset(driveQueue); }
    }
    PendingCommand command;
    for (unsigned n = 0; n < 8 && xQueueReceive(driveQueue, &command, 0) == pdTRUE; ++n) {
      if (command.kind == STOP) { state.stop(); continue; }
      if (!ready || !wifiUp || !mqttUp || command.epoch != epoch) continue;
      bool ok;
      if (command.kind == DRIVE) {
        ok = state.drive(command.x, command.z, command.receivedAt, now);
        if (ok && command.hasY) ok = state.rotate(command.y, command.receivedAt, now);
      } else ok = state.rotate(command.y, command.receivedAt, now);
      if (ok) ++accepted;
    }
    state.expire(now);
    float applied[4]; state.mixed(applied);
    if (ready) {
      for (uint8_t i = 0; i < 4; ++i) writeMotor(i, applied[i]);
    }
    if (!ready) {
      for (uint8_t i = 0; i < 4; ++i) {
        // A valid pin map is required before indexing configured outputs.
        if (MOTOR_OUTPUT_ORDER[i] < 4) writeMotor(i, 0);
        applied[i] = 0;
      }
    }
    portENTER_CRITICAL(&snapshotMux);
    memcpy(motorSnapshot.applied, applied, sizeof(applied));
    motorSnapshot.driveAt = state.driveAt; motorSnapshot.grouserAt = state.grouserAt;
    motorSnapshot.driveActive = state.driveActive; motorSnapshot.grouserActive = state.grouserActive;
    motorSnapshot.accepted = accepted;
    portEXIT_CRITICAL(&snapshotMux);
    if (watchdogReady) esp_task_wdt_reset();
    vTaskDelay(1);
  }
}

bool numeric(cJSON *object, const char *key, float &out) {
  cJSON *field = cJSON_GetObjectItemCaseSensitive(object, key);
  if (!cJSON_IsNumber(field) || !isfinite(field->valuedouble) || fabs(field->valuedouble) > 1) return false;
  out = float(field->valuedouble);
  return true;
}

void queueStop() {
  PendingCommand command = {}; command.kind = STOP;
  // A full queue is treated as loss of control, clearing rather than replaying.
  if (xQueueSend(driveQueue, &command, 0) != pdTRUE) {
    xQueueReset(driveQueue); xQueueSend(driveQueue, &command, 0);
  }
}

void mqttEvent(void *, esp_event_base_t, int32_t eventId, void *data) {
  auto *event = static_cast<esp_mqtt_event_t *>(data);
  if (eventId == MQTT_EVENT_CONNECTED) {
    ++connectionEpoch; mqttUp = true; announce = true;
    esp_mqtt_client_subscribe(event->client, driveTopic, 0);
    esp_mqtt_client_subscribe(event->client, grouserTopic, 0);
    return;
  }
  if (eventId == MQTT_EVENT_DISCONNECTED) {
    mqttUp = false; ++connectionEpoch; return;
  }
  if (eventId != MQTT_EVENT_DATA) return;
  // Only whole bounded commands are admitted. Oversize/fragmented commands
  // cannot grow a buffer or renew the motor lease.
  bool drive = event->topic && event->topic_len == int(strlen(driveTopic)) &&
               memcmp(event->topic, driveTopic, event->topic_len) == 0;
  bool grouser = event->topic && event->topic_len == int(strlen(grouserTopic)) &&
                 memcmp(event->topic, grouserTopic, event->topic_len) == 0;
  if (!drive && !grouser) return;
  if (event->retain) { ++retainedIgnored; return; }
  if (event->current_data_offset || event->total_data_len != event->data_len ||
      event->data_len <= 0 || event->data_len > 256 || memchr(event->data, 0, event->data_len)) {
    ++rejectedCommands; queueStop(); return;
  }
  char bytes[257]; memcpy(bytes, event->data, event->data_len); bytes[event->data_len] = 0;
  cJSON *doc = cJSON_ParseWithLengthOpts(bytes, event->data_len + 1, nullptr, true);
  cJSON *body = doc;
  if (cJSON_IsObject(doc) && cJSON_IsObject(cJSON_GetObjectItemCaseSensitive(doc, "value")))
    body = cJSON_GetObjectItemCaseSensitive(doc, "value");
  PendingCommand command = {};
  command.kind = drive ? DRIVE : GROUSER;
  command.receivedAt = millis(); command.epoch = connectionEpoch.load();
  bool valid = cJSON_IsObject(body);
  if (drive) {
    valid = valid && numeric(body, "x", command.x) && numeric(body, "z", command.z);
    command.hasY = cJSON_HasObjectItem(body, "y");
    if (command.hasY) valid = valid && numeric(body, "y", command.y);
  } else valid = valid && numeric(body, "value", command.y);
  cJSON_Delete(doc);
  if (!valid || xQueueSend(driveQueue, &command, 0) != pdTRUE) {
    ++rejectedCommands; queueStop();
  }
}

void topic(char *destination, size_t size, const char *suffix) {
  snprintf(destination, size, "%s/%s", TOPIC_BASE, suffix);
}

void enqueue(const char *destination, const char *body, bool retained, int qos = 1) {
  if (mqtt && mqttUp) esp_mqtt_client_enqueue(mqtt, destination, body, 0, qos, retained, true);
}

void publishPresence() {
  char body[128];
  snprintf(body, sizeof(body), "{\"online\":true,\"boot_id\":\"%s\",\"t\":%llu}",
           bootId, (unsigned long long)time(nullptr) * 1000);
  enqueue(onlineTopic, body, true); enqueue(moduleStatusTopic, body, true);
}

void statusJson(char *buffer, size_t size) {
  MotorSnapshot snapshot;
  portENTER_CRITICAL(&snapshotMux); snapshot = motorSnapshot; portEXIT_CRITICAL(&snapshotMux);
  snprintf(buffer, size,
    "{\"firmware\":\"%s\",\"boot_id\":\"%s\",\"uptime_ms\":%lu,\"mqtt_connected\":%s,"
    "\"wifi_connected\":%s,\"time_ready\":%s,\"motors_ready\":%s,\"drive_active\":%s,\"grouser_active\":%s,"
    "\"drive_at_ms\":%lu,\"grouser_at_ms\":%lu,\"accepted_commands\":%lu,"
    "\"rejected_commands\":%lu,\"retained_ignored\":%lu,\"motor_values\":[%.3f,%.3f,%.3f,%.3f],"
    "\"free_heap\":%lu,\"battery_v\":%.3f}",
    FIRMWARE_ID, bootId, (unsigned long)millis(), mqttUp ? "true" : "false", wifiUp ? "true" : "false",
    time(nullptr) > 1735689600 ? "true" : "false", motorsReady ? "true" : "false", snapshot.driveActive ? "true" : "false", snapshot.grouserActive ? "true" : "false",
    (unsigned long)snapshot.driveAt, (unsigned long)snapshot.grouserAt, (unsigned long)snapshot.accepted,
    (unsigned long)rejectedCommands.load(), (unsigned long)retainedIgnored.load(),
    snapshot.applied[0], snapshot.applied[1], snapshot.applied[2], snapshot.applied[3],
    (unsigned long)ESP.getFreeHeap(), batteryMv.load() * BATTERY_DIVIDER_RATIO / 1000.0f);
}

void startMqtt() {
  esp_mqtt_client_config_t config = {};
  config.broker.address.uri = MQTT_URI;
  config.broker.verification.crt_bundle_attach = esp_crt_bundle_attach;
  config.credentials.client_id = clientId;
  config.credentials.username = MQTT_USER;
  config.credentials.authentication.password = MQTT_PASS;
  config.session.protocol_ver = MQTT_PROTOCOL_V_3_1_1;
  config.session.keepalive = 10;
  config.session.last_will.topic = onlineTopic;
  config.session.last_will.msg = willPayload;
  config.session.last_will.qos = 1; config.session.last_will.retain = true;
  config.network.reconnect_timeout_ms = 3000;
  config.network.timeout_ms = 5000;
  config.buffer.size = 1024; config.buffer.out_size = 1024;
  config.outbox.limit = 32768;
  config.task.priority = 2;
  mqtt = esp_mqtt_client_init(&config);
  if (!mqtt) { Serial.println("ERR MQTT initialization failed"); return; }
  esp_mqtt_client_register_event(mqtt, static_cast<esp_mqtt_event_id_t>(ESP_EVENT_ANY_ID), mqttEvent, nullptr);
  if (esp_mqtt_client_start(mqtt) != ESP_OK) {
    esp_mqtt_client_destroy(mqtt); mqtt = nullptr;
    Serial.println("ERR MQTT start failed");
  }
}

void setup() {
  // First user-code action: latch every driver input low.
  setupMotorPins();
  Serial.begin(115200);
  driveQueue = xQueueCreate(8, sizeof(PendingCommand));
  if (!driveQueue || xTaskCreate(motorTask, "motor-safety", 4096, nullptr, 3, nullptr) != pdPASS) {
    Serial.println("ERR motor safety task unavailable; all outputs remain off");
    while (true) delay(1000);
  }
  snprintf(bootId, sizeof(bootId), "%08lx%08lx", (unsigned long)esp_random(), (unsigned long)esp_random());
  snprintf(clientId, sizeof(clientId), "minilamp-%012llx", (unsigned long long)ESP.getEfuseMac());
  topic(driveTopic, sizeof(driveTopic), "i/drive");
  topic(grouserTopic, sizeof(grouserTopic), "i/grouser");
  topic(onlineTopic, sizeof(onlineTopic), "o/online");
  topic(manifestTopic, sizeof(manifestTopic), "o/manifest");
  topic(moduleStatusTopic, sizeof(moduleStatusTopic), "o/modules/status");
  topic(batteryTopic, sizeof(batteryTopic), "o/telemetry/battery");
  topic(statusTopic, sizeof(statusTopic), "o/status");
  snprintf(willPayload, sizeof(willPayload), "{\"online\":false,\"boot_id\":\"%s\"}", bootId);
  pinMode(BATTERY_PIN, INPUT);
  analogReadResolution(12); analogSetPinAttenuation(BATTERY_PIN, ADC_11db);
  pinMode(WIFI_ENABLE, OUTPUT); digitalWrite(WIFI_ENABLE, LOW);
  pinMode(WIFI_ANT_CONFIG, OUTPUT); digitalWrite(WIFI_ANT_CONFIG, USE_EXTERNAL_ANTENNA ? HIGH : LOW);
  WiFi.persistent(false); WiFi.mode(WIFI_STA); WiFi.setSleep(false);
  WiFi.onEvent([](WiFiEvent_t event) {
    if (event == ARDUINO_EVENT_WIFI_STA_GOT_IP) wifiUp = true;
    if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) { wifiUp = false; ++connectionEpoch; }
  });
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  Serial.printf("INFO firmware=%s base=%s reset_reason=%d\n", FIRMWARE_ID, TOPIC_BASE, int(esp_reset_reason()));
}

void loop() {
  static uint32_t lastWifiRetry = 0, lastBatterySample = 0, lastPublish = 0, lastManifest = 0;
  static uint32_t batteryTotal = 0, batterySamples = 0, lastMqttStart = 0;
  const uint32_t now = millis();
  if (!wifiUp && now - lastWifiRetry >= 10000) { lastWifiRetry = now; WiFi.reconnect(); }
  // Accurate time is needed for certificate validity checks. Never block motor
  // servicing while waiting for Wi-Fi, SNTP, TLS, or broker availability.
  if (!mqtt && wifiUp && time(nullptr) > 1735689600 && now - lastMqttStart >= 3000) {
    lastMqttStart = now; startMqtt();
  }
  if (now - lastBatterySample >= 10) {
    lastBatterySample = now;
    batteryTotal += analogReadMilliVolts(BATTERY_PIN);
    if (++batterySamples == 8) { batteryMv = batteryTotal / 8; batteryTotal = batterySamples = 0; }
  }
  if (mqttUp && (announce.exchange(false) || now - lastManifest >= 60000)) {
    enqueue(manifestTopic, MODULE_MANIFEST, true);
    lastManifest = now; publishPresence();
  }
  if (mqttUp && now - lastPublish >= 5000) {
    lastPublish = now;
    publishPresence();
    char body[1024]; statusJson(body, sizeof(body)); enqueue(statusTopic, body, true);
    snprintf(body, sizeof(body), "{\"value\":%.3f,\"unit\":\"v\"}", batteryMv.load() * BATTERY_DIVIDER_RATIO / 1000.0f);
    enqueue(batteryTopic, body, true);
  }
  // Read-only diagnostic command. Bounded reception; V + newline or CRLF.
  static char diagnostic[8]; static size_t length = 0; static bool discard = false;
  for (unsigned n = 0; n < 32 && Serial.available(); ++n) {
    int c = Serial.read();
    if (c == '\n') {
      if (!discard && length == 1 && (diagnostic[0] == 'V' || diagnostic[0] == 'v')) {
        char body[1024]; statusJson(body, sizeof(body)); Serial.println(body);
      }
      length = 0; discard = false;
    } else if (c != '\r' && !discard) {
      if (c <= 0 || length == sizeof(diagnostic)) discard = true;
      else diagnostic[length++] = char(c);
    }
  }
  delay(1);
}
