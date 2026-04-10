#include <Arduino.h>
#include <ArduinoJson.h>
#include <PubSubClient.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <esp_camera.h>
#include <mbedtls/base64.h>

#include <ctype.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "private_config.h"

/*
  Pebble firmware for Seeeduino XIAO ESP32-S3 Sense controlling a WowWee MiP.

  MQTT follows mqtt_standard.md:
    {system}/robots/{robot_id}/{incoming|outgoing}/{metric}

  MiP UART uses the raw WowWee command bytes documented by MiP-Capi and the
  WowWee MiP BLE protocol. D6 is TX to MiP RX. D7 is RX from MiP TX.
*/

#ifndef MIP_UART_TX_PIN
#define MIP_UART_TX_PIN D6
#endif

#ifndef MIP_UART_RX_PIN
#define MIP_UART_RX_PIN D7
#endif

#ifndef MIP_UART_BAUD
#define MIP_UART_BAUD 115200UL
#endif

#ifndef MIP_UART_PROBE_ALTERNATE_BAUD
#define MIP_UART_PROBE_ALTERNATE_BAUD 1
#endif

#ifndef MIP_UART_ALTERNATE_BAUD
#define MIP_UART_ALTERNATE_BAUD 9600UL
#endif

#ifndef MIP_UART_TX_ONLY_FALLBACK_ALTERNATE
#define MIP_UART_TX_ONLY_FALLBACK_ALTERNATE 0
#endif

#ifndef MIP_UART_SPARKFUN_WAKE
#define MIP_UART_SPARKFUN_WAKE 1
#endif

#ifndef MIP_UART_APPEND_ZERO
#define MIP_UART_APPEND_ZERO 1
#endif

#ifndef MIP_TURN_INVERT
#define MIP_TURN_INVERT 0
#endif

#ifndef MQTT_PACKET_BUFFER_SIZE
#define MQTT_PACKET_BUFFER_SIZE 24576
#endif

#ifndef MQTT_KEEPALIVE_SECONDS
#define MQTT_KEEPALIVE_SECONDS 30
#endif

#ifndef DRIVE_REFRESH_MS
#define DRIVE_REFRESH_MS 50UL
#endif

#ifndef DRIVE_TIMEOUT_MS
#define DRIVE_TIMEOUT_MS 750UL
#endif

#ifndef MQTT_VIDEO_ENABLED_BY_DEFAULT
#define MQTT_VIDEO_ENABLED_BY_DEFAULT 0
#endif

#ifndef MIP_CAMERA_ENABLED
#define MIP_CAMERA_ENABLED 0
#endif

#ifndef MQTT_VIDEO_INTERVAL_MS
#define MQTT_VIDEO_INTERVAL_MS 1000UL
#endif

#ifndef CAMERA_FRAME_SIZE
#define CAMERA_FRAME_SIZE FRAMESIZE_QQVGA
#endif

#ifndef CAMERA_JPEG_QUALITY
#define CAMERA_JPEG_QUALITY 38
#endif

#ifndef MIP_ODOM_MAX_TURN_RADPS
#define MIP_ODOM_MAX_TURN_RADPS 2.4f
#endif

#ifndef ALLOW_REMOTE_REBOOT
#define ALLOW_REMOTE_REBOOT 1
#endif

#ifndef DEBUG_LOGS
#define DEBUG_LOGS 0
#endif

// Seeed XIAO ESP32-S3 Sense camera pin map.
#define PWDN_GPIO_NUM -1
#define RESET_GPIO_NUM -1
#define XCLK_GPIO_NUM 10
#define SIOD_GPIO_NUM 40
#define SIOC_GPIO_NUM 39
#define Y9_GPIO_NUM 48
#define Y8_GPIO_NUM 11
#define Y7_GPIO_NUM 12
#define Y6_GPIO_NUM 14
#define Y5_GPIO_NUM 16
#define Y4_GPIO_NUM 18
#define Y3_GPIO_NUM 17
#define Y2_GPIO_NUM 15
#define VSYNC_GPIO_NUM 38
#define HREF_GPIO_NUM 47
#define PCLK_GPIO_NUM 13

namespace {

const char FIRMWARE_VERSION[] = "2026.04.10.4";
const char CAPABILITIES_SCHEMA[] = "pebble-capabilities/v1";
const char MQTT_OFFLINE_PAYLOAD[] = "{\"online\":false,\"status\":\"offline\"}";

const uint8_t MIP_CMD_PLAY_SOUND = 0x06;
const uint8_t MIP_CMD_GET_GESTURE_RESPONSE = 0x0A;
const uint8_t MIP_CMD_SET_GESTURE_RADAR_MODE = 0x0C;
const uint8_t MIP_CMD_GET_RADAR_RESPONSE = 0x0C;
const uint8_t MIP_CMD_GET_GESTURE_RADAR_MODE = 0x0D;
const uint8_t MIP_CMD_GET_SOFTWARE_VERSION = 0x14;
const uint8_t MIP_CMD_SET_VOLUME = 0x15;
const uint8_t MIP_CMD_GET_VOLUME = 0x16;
const uint8_t MIP_CMD_GET_HARDWARE_INFO = 0x19;
const uint8_t MIP_CMD_SHAKE_RESPONSE = 0x1A;
const uint8_t MIP_CMD_CLAP_RESPONSE = 0x1D;
const uint8_t MIP_CMD_GET_UP = 0x23;
const uint8_t MIP_CMD_DISTANCE_DRIVE = 0x70;
const uint8_t MIP_CMD_DRIVE_FORWARD = 0x71;
const uint8_t MIP_CMD_DRIVE_BACKWARD = 0x72;
const uint8_t MIP_CMD_TURN_LEFT = 0x73;
const uint8_t MIP_CMD_TURN_RIGHT = 0x74;
const uint8_t MIP_CMD_STOP = 0x77;
const uint8_t MIP_CMD_CONTINUOUS_DRIVE = 0x78;
const uint8_t MIP_CMD_GET_STATUS = 0x79;
const uint8_t MIP_CMD_GET_WEIGHT = 0x81;
const uint8_t MIP_CMD_GET_CHEST_LED = 0x83;
const uint8_t MIP_CMD_SET_CHEST_LED = 0x84;
const uint8_t MIP_CMD_READ_ODOMETER = 0x85;
const uint8_t MIP_CMD_RESET_ODOMETER = 0x86;
const uint8_t MIP_CMD_FLASH_CHEST_LED = 0x89;
const uint8_t MIP_CMD_SET_HEAD_LEDS = 0x8A;
const uint8_t MIP_CMD_GET_HEAD_LEDS = 0x8B;

const uint8_t MIP_SOUND_SHORT_MUTE_FOR_STOP = 105;
const uint8_t MIP_SOUND_MAX_BUILTIN = 106;
const uint8_t MIP_GET_UP_FROM_EITHER = 0x02;

#if MQTT_USE_TLS
WiFiClientSecure wifiClient;
#else
WiFiClient wifiClient;
#endif
PubSubClient mqtt(wifiClient);

String baseTopic;
String topicDrive;
String topicLightsSolid;
String topicLightsFlash;
String topicVideoControl;
String topicVideoFlag;
String topicRebootFlag;
String topicSoundboardCommand;
String topicAutonomyCommand;
String topicHeartbeat;
String topicCapabilities;
String topicLogs;
String topicStatus;
String topicChargingLevel;
String topicChargingStatus;
String topicWheelOdometry;
String topicSoundboardFiles;
String topicSoundboardStatus;
String topicVideo;
String topicVideoOverlays;

bool timeSynced = false;
bool cameraReady = false;
bool mqttLargeBufferReady = false;
bool videoEnabled = MQTT_VIDEO_ENABLED_BY_DEFAULT;

unsigned long lastHeartbeatMs = 0;
unsigned long lastCapabilitiesMs = 0;
unsigned long lastStatusMs = 0;
unsigned long lastVideoMs = 0;
unsigned long lastDriveCommandMs = 0;
unsigned long lastDriveRefreshMs = 0;
unsigned long lastMipResponseMs = 0;
unsigned long lastOdomPollMs = 0;
unsigned long lastGetUpMs = 0;

int8_t targetVelocity = 0;
int8_t targetTurnRate = 0;
bool driveActive = false;
bool driveStoppedForTimeout = true;
uint32_t mqttMessageCount = 0;
uint32_t driveCommandCount = 0;
uint32_t lightsCommandCount = 0;
uint32_t getUpCommandCount = 0;

uint32_t mipBaud = MIP_UART_BAUD;
bool mipRxOk = false;
uint8_t mipPosition = 0xFF;
float mipBatteryVolts = NAN;
float mipOdometerCm = NAN;
float odomReferenceCm = NAN;
float odomLastCm = NAN;
float odomXmm = 0.0f;
float odomYmm = 0.0f;
float odomHeadingRad = 0.0f;
uint32_t odomPublishSeq = 0;
unsigned long odomLastIntegrateMs = 0;
uint8_t mipVolume = 0xFF;
char mipSoftwareVersion[24] = "";
char mipHardwareInfo[24] = "";

uint8_t lastRadar = 0;
uint8_t lastGesture = 0;
uint8_t lastClapCount = 0;
int8_t lastWeight = 0;
bool lastShake = false;
unsigned long lastRadarMs = 0;
unsigned long lastGestureMs = 0;
unsigned long lastClapMs = 0;
unsigned long lastWeightMs = 0;
unsigned long lastShakeMs = 0;

uint32_t videoFrameId = 0;
bool soundPlaying = false;
uint8_t currentSoundIndex = 0;
char currentSoundFile[40] = "";

struct SoundSpec {
  uint8_t index;
  const char *name;
};

const SoundSpec MIP_SOUNDS[] = {
  {1, "001-onekhz-500ms-8k16bit.wav"},
  {2, "002-action-burping.wav"},
  {3, "003-action-drinking.wav"},
  {4, "004-action-eating.wav"},
  {5, "005-action-farting-short.wav"},
  {6, "006-action-out-of-breath.wav"},
  {7, "007-boxing-punchconnect-1.wav"},
  {8, "008-boxing-punchconnect-2.wav"},
  {9, "009-boxing-punchconnect-3.wav"},
  {10, "010-freestyle-tracking-1.wav"},
  {11, "011-mip-1.wav"},
  {12, "012-mip-2.wav"},
  {13, "013-mip-3.wav"},
  {14, "014-mip-app.wav"},
  {15, "015-mip-awww.wav"},
  {16, "016-mip-big-shot.wav"},
  {17, "017-mip-bleh.wav"},
  {18, "018-mip-boom.wav"},
  {19, "019-mip-bye.wav"},
  {20, "020-mip-converse-1.wav"},
  {21, "021-mip-converse-2.wav"},
  {22, "022-mip-drop.wav"},
  {23, "023-mip-dunno.wav"},
  {24, "024-mip-fall-over-1.wav"},
  {25, "025-mip-fall-over-2.wav"},
  {26, "026-mip-fight.wav"},
  {27, "027-mip-game.wav"},
  {28, "028-mip-gloat.wav"},
  {29, "029-mip-go.wav"},
  {30, "030-mip-gogogo.wav"},
  {31, "031-mip-grunt-1.wav"},
  {32, "032-mip-grunt-2.wav"},
  {33, "033-mip-grunt-3.wav"},
  {34, "034-mip-haha-got-it.wav"},
  {35, "035-mip-hi-confident.wav"},
  {36, "036-mip-hi-not-sure.wav"},
  {37, "037-mip-hi-scared.wav"},
  {38, "038-mip-huh.wav"},
  {39, "039-mip-humming-1.wav"},
  {40, "040-mip-humming-2.wav"},
  {41, "041-mip-hurt.wav"},
  {42, "042-mip-huuurgh.wav"},
  {43, "043-mip-in-love.wav"},
  {44, "044-mip-it.wav"},
  {45, "045-mip-joke.wav"},
  {46, "046-mip-k.wav"},
  {47, "047-mip-loop-1.wav"},
  {48, "048-mip-loop-2.wav"},
  {49, "049-mip-low-battery.wav"},
  {50, "050-mip-mippee.wav"},
  {51, "051-mip-more.wav"},
  {52, "052-mip-muah-ha.wav"},
  {53, "053-mip-music.wav"},
  {54, "054-mip-obstacle.wav"},
  {55, "055-mip-ohoh.wav"},
  {56, "056-mip-oh-yeah.wav"},
  {57, "057-mip-oopsie.wav"},
  {58, "058-mip-ouch-1.wav"},
  {59, "059-mip-ouch-2.wav"},
  {60, "060-mip-play.wav"},
  {61, "061-mip-push.wav"},
  {62, "062-mip-run.wav"},
  {63, "063-mip-shake.wav"},
  {64, "064-mip-sigh.wav"},
  {65, "065-mip-singing.wav"},
  {66, "066-mip-sneeze.wav"},
  {67, "067-mip-snore.wav"},
  {68, "068-mip-stack.wav"},
  {69, "069-mip-swipe-1.wav"},
  {70, "070-mip-swipe-2.wav"},
  {71, "071-mip-tricks.wav"},
  {72, "072-mip-triiick.wav"},
  {73, "073-mip-trumpet.wav"},
  {74, "074-mip-waaaaa.wav"},
  {75, "075-mip-wakey.wav"},
  {76, "076-mip-wheee.wav"},
  {77, "077-mip-whistling.wav"},
  {78, "078-mip-whoah.wav"},
  {79, "079-mip-woo.wav"},
  {80, "080-mip-yeah.wav"},
  {81, "081-mip-yeeesss.wav"},
  {82, "082-mip-yo.wav"},
  {83, "083-mip-yummy.wav"},
  {84, "084-mood-activated.wav"},
  {85, "085-mood-angry.wav"},
  {86, "086-mood-anxious.wav"},
  {87, "087-mood-boring.wav"},
  {88, "088-mood-cranky.wav"},
  {89, "089-mood-energetic.wav"},
  {90, "090-mood-excited.wav"},
  {91, "091-mood-giddy.wav"},
  {92, "092-mood-grumpy.wav"},
  {93, "093-mood-happy.wav"},
  {94, "094-mood-idea.wav"},
  {95, "095-mood-impatient.wav"},
  {96, "096-mood-nice.wav"},
  {97, "097-mood-sad.wav"},
  {98, "098-mood-short.wav"},
  {99, "099-mood-sleepy.wav"},
  {100, "100-mood-tired.wav"},
  {101, "101-sound-boost.wav"},
  {102, "102-sound-cage.wav"},
  {103, "103-sound-guns.wav"},
  {104, "104-sound-zings.wav"},
  {105, "105-short-mute-for-stop.wav"},
  {106, "106-freestyle-tracking-2.wav"},
};

static inline float clampf(float value, float lo, float hi) {
  if (value < lo) return lo;
  if (value > hi) return hi;
  return value;
}

static inline int clampi(int value, int lo, int hi) {
  if (value < lo) return lo;
  if (value > hi) return hi;
  return value;
}

static inline bool debugEnabled() {
  return DEBUG_LOGS != 0;
}

void debugf(const char *fmt, ...) {
  if (!debugEnabled()) return;
  char buffer[192];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);
  Serial.println(buffer);
}

uint64_t unixMillis() {
  struct timeval tv;
  if (gettimeofday(&tv, nullptr) == 0 && tv.tv_sec > 1600000000) {
    return (uint64_t)tv.tv_sec * 1000ULL + (uint64_t)(tv.tv_usec / 1000);
  }
  return 0;
}

double unixSecondsDouble() {
  uint64_t ms = unixMillis();
  if (ms == 0) return 0.0;
  return (double)ms / 1000.0;
}

bool ensureTimeSync() {
  if (unixMillis() != 0) {
    timeSynced = true;
    return true;
  }
  configTime(0, 0, "pool.ntp.org", "time.nist.gov", "time.google.com");
  for (int i = 0; i < 24; ++i) {
    delay(250);
    if (unixMillis() != 0) {
      timeSynced = true;
      return true;
    }
  }
  timeSynced = false;
  return false;
}

String topic(const char *direction, const char *metric) {
  return baseTopic + "/" + direction + "/" + metric;
}

bool publishString(const String &topicName, const String &payload, bool retain = false) {
  if (!mqtt.connected()) return false;
  return mqtt.publish(topicName.c_str(), reinterpret_cast<const uint8_t *>(payload.c_str()), payload.length(), retain);
}

template <typename TJsonDocument>
bool publishJsonDocument(const String &topicName, TJsonDocument &doc, bool retain = false) {
  String payload;
  size_t estimated = measureJson(doc) + 1;
  payload.reserve(estimated);
  serializeJson(doc, payload);
  return publishString(topicName, payload, retain);
}

void publishLog(const char *level, const char *message) {
  StaticJsonDocument<384> doc;
  uint64_t t = unixMillis();
  if (t != 0) doc["t"] = t;
  doc["level"] = level;
  doc["service"] = "mip_firmware";
  doc["message"] = message;
  doc["logger"] = "firmware";
  publishJsonDocument(topicLogs, doc, false);
}

void publishLogf(const char *level, const char *fmt, ...) {
  char buffer[192];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);
  publishLog(level, buffer);
}

uint8_t mipExpectedFrameLength(uint8_t cmd) {
  switch (cmd) {
    case MIP_CMD_GET_GESTURE_RESPONSE:
    case MIP_CMD_GET_RADAR_RESPONSE:
    case MIP_CMD_GET_GESTURE_RADAR_MODE:
    case MIP_CMD_GET_VOLUME:
    case MIP_CMD_GET_WEIGHT:
    case MIP_CMD_CLAP_RESPONSE:
      return 2;
    case MIP_CMD_GET_STATUS:
    case MIP_CMD_GET_HARDWARE_INFO:
      return 3;
    case MIP_CMD_GET_SOFTWARE_VERSION:
    case MIP_CMD_READ_ODOMETER:
    case MIP_CMD_GET_HEAD_LEDS:
      return 5;
    case MIP_CMD_GET_CHEST_LED:
      return 6;
    case MIP_CMD_SHAKE_RESPONSE:
      return 1;
    default:
      return 0;
  }
}

const char *positionName(uint8_t position) {
  switch (position) {
    case 0x00: return "on_back";
    case 0x01: return "face_down";
    case 0x02: return "upright";
    case 0x03: return "picked_up";
    case 0x04: return "hand_stand";
    case 0x05: return "face_down_on_tray";
    case 0x06: return "on_back_with_kickstand";
    default: return "unknown";
  }
}

float batteryRawToVolts(uint8_t raw) {
  return (((float)raw - 0x4D) / (float)(0x7C - 0x4D)) * (6.4f - 4.0f) + 4.0f;
}

void cacheMipFrame(const uint8_t *frame, size_t length) {
  if (length == 0) return;
  lastMipResponseMs = millis();
  mipRxOk = true;
  switch (frame[0]) {
    case MIP_CMD_GET_STATUS:
      if (length == 3) {
        mipBatteryVolts = batteryRawToVolts(frame[1]);
        mipPosition = frame[2];
      }
      break;
    case MIP_CMD_READ_ODOMETER:
      if (length == 5) {
        uint32_t ticks = ((uint32_t)frame[1] << 24) | ((uint32_t)frame[2] << 16) | ((uint32_t)frame[3] << 8) | frame[4];
        mipOdometerCm = (float)((double)ticks / 48.5);
      }
      break;
    case MIP_CMD_GET_VOLUME:
      if (length == 2) mipVolume = frame[1];
      break;
    case MIP_CMD_GET_SOFTWARE_VERSION:
      if (length == 5) {
        snprintf(mipSoftwareVersion, sizeof(mipSoftwareVersion), "%u.%u.%u.%u",
                 (unsigned int)(2000 + frame[1]), (unsigned int)frame[2],
                 (unsigned int)frame[3], (unsigned int)frame[4]);
      }
      break;
    case MIP_CMD_GET_HARDWARE_INFO:
      if (length == 3) {
        snprintf(mipHardwareInfo, sizeof(mipHardwareInfo), "voice=%u hardware=%u",
                 (unsigned int)frame[1], (unsigned int)frame[2]);
      }
      break;
    case MIP_CMD_GET_RADAR_RESPONSE:
      if (length == 2) {
        lastRadar = frame[1];
        lastRadarMs = millis();
      }
      break;
    case MIP_CMD_GET_GESTURE_RESPONSE:
      if (length == 2) {
        lastGesture = frame[1];
        lastGestureMs = millis();
      }
      break;
    case MIP_CMD_CLAP_RESPONSE:
      if (length == 2) {
        lastClapCount = frame[1];
        lastClapMs = millis();
      }
      break;
    case MIP_CMD_GET_WEIGHT:
      if (length == 2) {
        lastWeight = (int8_t)frame[1];
        lastWeightMs = millis();
      }
      break;
    case MIP_CMD_SHAKE_RESPONSE:
      lastShake = true;
      lastShakeMs = millis();
      break;
    default:
      break;
  }
}

bool readMipFrame(uint8_t *frame, size_t frameSize, size_t &length, unsigned long timeoutMs) {
  unsigned long deadline = millis() + timeoutMs;
  while ((long)(deadline - millis()) >= 0) {
    if (Serial1.available() <= 0) {
      delay(1);
      continue;
    }
    uint8_t cmd = (uint8_t)Serial1.read();
    uint8_t expected = mipExpectedFrameLength(cmd);
    if (expected == 0 || expected > frameSize) {
      continue;
    }
    frame[0] = cmd;
    size_t offset = 1;
    unsigned long byteDeadline = millis() + 20;
    while (offset < expected && (long)(byteDeadline - millis()) >= 0) {
      if (Serial1.available() > 0) {
        frame[offset++] = (uint8_t)Serial1.read();
        byteDeadline = millis() + 20;
      } else {
        delay(1);
      }
    }
    if (offset == expected) {
      length = expected;
      cacheMipFrame(frame, length);
      return true;
    }
  }
  return false;
}

void processMipIncoming(unsigned long budgetMs) {
  unsigned long deadline = millis() + budgetMs;
  do {
    if (Serial1.available() <= 0) return;
    uint8_t frame[8];
    size_t length = 0;
    if (!readMipFrame(frame, sizeof(frame), length, 1)) return;
  } while ((long)(deadline - millis()) >= 0);
}

bool mipSend(const uint8_t *request, size_t length) {
  if (length == 0) return false;
  size_t written = Serial1.write(request, length);
#if MIP_UART_APPEND_ZERO
  written += Serial1.write((uint8_t)0x00);
  length += 1;
#endif
  Serial1.flush();
  return written == length;
}

void mipSendSparkFunWake() {
#if MIP_UART_SPARKFUN_WAKE
  const uint8_t wake[] = {'T', 'T', 'M', ':', 'O', 'K', '\r', '\n'};
  mipSend(wake, sizeof(wake));
  delay(100);
#endif
}

bool mipRequestResponse(const uint8_t *request, size_t requestLength, uint8_t expectedCmd,
                        uint8_t *response, size_t responseSize, size_t &responseLength,
                        unsigned long timeoutMs) {
  processMipIncoming(2);
  if (!mipSend(request, requestLength)) return false;
  unsigned long deadline = millis() + timeoutMs;
  while ((long)(deadline - millis()) >= 0) {
    uint8_t frame[8];
    size_t length = 0;
    if (!readMipFrame(frame, sizeof(frame), length, 4)) {
      continue;
    }
    if (length > 0 && frame[0] == expectedCmd) {
      if (length <= responseSize) {
        memcpy(response, frame, length);
        responseLength = length;
      }
      return true;
    }
  }
  return false;
}

bool mipQuery(uint8_t cmd, uint8_t *response, size_t responseSize, size_t &responseLength, unsigned long timeoutMs = 80) {
  uint8_t request[1] = {cmd};
  return mipRequestResponse(request, sizeof(request), cmd, response, responseSize, responseLength, timeoutMs);
}

void mipStop() {
  uint8_t command[1] = {MIP_CMD_STOP};
  mipSend(command, sizeof(command));
}

void mipGetUpEither() {
  uint8_t command[2] = {MIP_CMD_GET_UP, MIP_GET_UP_FROM_EITHER};
  if (mipSend(command, sizeof(command))) {
    lastGetUpMs = millis();
    ++getUpCommandCount;
  }
}

uint8_t encodeVelocity(int8_t velocity) {
  velocity = (int8_t)clampi(velocity, -32, 32);
  if (velocity == 0) return 0x00;
  if (velocity < 0) return (uint8_t)(0x20 + (-velocity));
  return (uint8_t)velocity;
}

uint8_t encodeTurnRate(int8_t turnRate) {
  turnRate = (int8_t)clampi(turnRate, -32, 32);
  if (turnRate == 0) return 0x00;
  if (turnRate < 0) return (uint8_t)(0x60 + (-turnRate));
  return (uint8_t)(0x40 + turnRate);
}

void mipContinuousDrive(int8_t velocity, int8_t turnRate) {
  uint8_t command[3] = {
    MIP_CMD_CONTINUOUS_DRIVE,
    encodeVelocity(velocity),
    encodeTurnRate(turnRate),
  };
  mipSend(command, sizeof(command));
}

void mipSetChestLed(uint8_t red, uint8_t green, uint8_t blue) {
  uint8_t command[4] = {MIP_CMD_SET_CHEST_LED, red, green, blue};
  mipSend(command, sizeof(command));
}

void mipFlashChestLed(uint8_t red, uint8_t green, uint8_t blue, uint16_t onMs, uint16_t offMs) {
  uint8_t command[6] = {
    MIP_CMD_FLASH_CHEST_LED,
    red,
    green,
    blue,
    (uint8_t)clampi((int)(onMs / 20), 1, 255),
    (uint8_t)clampi((int)(offMs / 20), 1, 255),
  };
  mipSend(command, sizeof(command));
}

void mipPlaySound(uint8_t soundIndex, uint8_t repeatCount = 0) {
  (void)repeatCount;
  uint8_t command[4] = {
    MIP_CMD_PLAY_SOUND,
    soundIndex,
    0x00,
    0x00,
  };
  mipSend(command, sizeof(command));
}

void mipSetVolume(uint8_t volume) {
  uint8_t command[2] = {MIP_CMD_SET_VOLUME, (uint8_t)clampi(volume, 0, 7)};
  mipSend(command, sizeof(command));
}

bool queryMipStatus(unsigned long timeoutMs = 80) {
  uint8_t response[8];
  size_t length = 0;
  return mipQuery(MIP_CMD_GET_STATUS, response, sizeof(response), length, timeoutMs) && length == 3;
}

bool queryMipOdometer(unsigned long timeoutMs = 80) {
  uint8_t response[8];
  size_t length = 0;
  return mipQuery(MIP_CMD_READ_ODOMETER, response, sizeof(response), length, timeoutMs) && length == 5;
}

void queryMipStartupInfo() {
  uint8_t response[8];
  size_t length = 0;
  mipQuery(MIP_CMD_GET_SOFTWARE_VERSION, response, sizeof(response), length, 120);
  mipQuery(MIP_CMD_GET_HARDWARE_INFO, response, sizeof(response), length, 120);
  mipQuery(MIP_CMD_GET_VOLUME, response, sizeof(response), length, 120);
}

bool beginMipUartAt(uint32_t baud, bool probe) {
  Serial1.end();
  Serial1.begin(baud, SERIAL_8N1, MIP_UART_RX_PIN, MIP_UART_TX_PIN);
  delay(80);
  while (Serial1.available() > 0) Serial1.read();
  mipBaud = baud;
  mipSendSparkFunWake();
  mipStop();
  if (!probe) return true;
  return queryMipStatus(120);
}

void beginMipUart() {
  mipRxOk = false;
  bool gotResponse = beginMipUartAt(MIP_UART_BAUD, true);
#if MIP_UART_PROBE_ALTERNATE_BAUD
  if (!gotResponse && MIP_UART_ALTERNATE_BAUD != MIP_UART_BAUD) {
    gotResponse = beginMipUartAt(MIP_UART_ALTERNATE_BAUD, true);
    if (!gotResponse) {
#if MIP_UART_TX_ONLY_FALLBACK_ALTERNATE
      beginMipUartAt(MIP_UART_ALTERNATE_BAUD, false);
#else
      beginMipUartAt(MIP_UART_BAUD, false);
#endif
    }
  }
#endif
  if (gotResponse) {
    queryMipStartupInfo();
    publishLogf("INFO", "MiP UART RX OK at %lu baud", (unsigned long)mipBaud);
    debugf("MiP UART RX OK at %lu", (unsigned long)mipBaud);
  } else {
    publishLogf("WARNING", "MiP UART no response; TX-only mode at %lu baud", (unsigned long)mipBaud);
    debugf("MiP UART TX-only/no response at %lu", (unsigned long)mipBaud);
  }
}

bool jsonBoolValue(JsonVariant value, bool &out) {
  if (value.is<bool>()) {
    out = value.as<bool>();
    return true;
  }
  if (value.is<int>()) {
    out = value.as<int>() != 0;
    return true;
  }
  if (value.is<JsonObject>()) {
    JsonObject obj = value.as<JsonObject>();
    if (obj["value"].is<bool>() || obj["value"].is<int>()) {
      out = obj["value"].as<bool>();
      return true;
    }
    if (obj["enabled"].is<bool>() || obj["enabled"].is<int>()) {
      out = obj["enabled"].as<bool>();
      return true;
    }
  }
  return false;
}

JsonVariant unwrapValue(JsonVariant value) {
  if (value.is<JsonObject>()) {
    JsonObject obj = value.as<JsonObject>();
    if (obj["value"].is<JsonObject>()) {
      return obj["value"].as<JsonVariant>();
    }
  }
  return value;
}

uint8_t colorByte(JsonVariant value) {
  float raw = value | 0.0f;
  return (uint8_t)clampi((int)lroundf(clampf(raw, 0.0f, 1.0f) * 255.0f), 0, 255);
}

void handleDrivePayload(JsonVariant payload) {
  JsonVariant v = unwrapValue(payload);
  float x = clampf(v["x"] | 0.0f, -1.0f, 1.0f);
  float z = clampf(v["z"] | 0.0f, -1.0f, 1.0f);
  int turnSign = MIP_TURN_INVERT ? -1 : 1;
  targetVelocity = (int8_t)clampi((int)lroundf(z * 32.0f), -32, 32);
  targetTurnRate = (int8_t)clampi((int)lroundf(x * 32.0f * turnSign), -32, 32);
  ++driveCommandCount;
  lastDriveCommandMs = millis();
  driveActive = true;
  driveStoppedForTimeout = false;
  if ((targetVelocity != 0 || targetTurnRate != 0) &&
      mipPosition != 0x02 &&
      (lastGetUpMs == 0 || (millis() - lastGetUpMs) > 5000UL)) {
    mipGetUpEither();
  }
  mipContinuousDrive(targetVelocity, targetTurnRate);
  lastDriveRefreshMs = millis();
}

void handleLightsSolidPayload(JsonVariant payload) {
  JsonVariant v = unwrapValue(payload);
  uint8_t blue = colorByte(v["b"]);
  uint8_t green = colorByte(v["g"]);
  uint8_t red = colorByte(v["r"]);
  ++lightsCommandCount;
  mipSetChestLed(red, green, blue);
  publishLogf("INFO", "lights solid r=%u g=%u b=%u", (unsigned int)red, (unsigned int)green, (unsigned int)blue);
}

void handleLightsFlashPayload(JsonVariant payload) {
  JsonVariant v = unwrapValue(payload);
  uint8_t blue = colorByte(v["b"]);
  uint8_t green = colorByte(v["g"]);
  uint8_t red = colorByte(v["r"]);
  float periodSeconds = clampf(v["period"] | 2.0f, 0.05f, 30.0f);
  uint16_t halfPeriodMs = (uint16_t)clampi((int)lroundf(periodSeconds * 500.0f), 20, 5100);
  ++lightsCommandCount;
  mipFlashChestLed(red, green, blue, halfPeriodMs, halfPeriodMs);
  publishLogf("INFO", "lights flash r=%u g=%u b=%u period_ms=%u",
              (unsigned int)red, (unsigned int)green, (unsigned int)blue,
              (unsigned int)(halfPeriodMs * 2));
}

bool equalIgnoreCase(const char *lhs, const char *rhs) {
  if (lhs == nullptr || rhs == nullptr) return false;
  while (*lhs && *rhs) {
    if (tolower((unsigned char)*lhs) != tolower((unsigned char)*rhs)) return false;
    ++lhs;
    ++rhs;
  }
  return *lhs == '\0' && *rhs == '\0';
}

int parseSoundIndex(const char *fileName) {
  if (fileName == nullptr) return -1;
  for (size_t i = 0; i < sizeof(MIP_SOUNDS) / sizeof(MIP_SOUNDS[0]); ++i) {
    if (equalIgnoreCase(fileName, MIP_SOUNDS[i].name)) {
      return MIP_SOUNDS[i].index;
    }
  }
  int value = -1;
  for (const char *p = fileName; *p != '\0'; ++p) {
    if (!isdigit((unsigned char)*p)) continue;
    value = 0;
    while (isdigit((unsigned char)*p)) {
      value = value * 10 + (*p - '0');
      ++p;
    }
    break;
  }
  if (value >= 1 && value <= MIP_SOUND_MAX_BUILTIN) return value;
  return -1;
}

const char *soundNameForIndex(uint8_t index) {
  for (size_t i = 0; i < sizeof(MIP_SOUNDS) / sizeof(MIP_SOUNDS[0]); ++i) {
    if (MIP_SOUNDS[i].index == index) return MIP_SOUNDS[i].name;
  }
  return "";
}

void publishSoundboardStatus();
bool initCamera();

void handleSoundboardPayload(JsonVariant payload) {
  JsonVariant v = unwrapValue(payload);
  bool stopRequested = false;
  const char *action = nullptr;
  const char *file = nullptr;
  if (v.is<bool>()) {
    stopRequested = !v.as<bool>();
  } else if (v.is<JsonObject>()) {
    action = v["action"] | "";
    file = v["file"] | "";
    if (action && (equalIgnoreCase(action, "stop") || equalIgnoreCase(action, "stopped"))) {
      stopRequested = true;
    }
    if ((file == nullptr || file[0] == '\0') && v["enabled"].is<bool>() && !v["enabled"].as<bool>()) {
      stopRequested = true;
    }
  }

  if (stopRequested) {
    mipPlaySound(MIP_SOUND_SHORT_MUTE_FOR_STOP, 0);
    soundPlaying = false;
    currentSoundIndex = 0;
    currentSoundFile[0] = '\0';
    publishLog("INFO", "soundboard stop");
    publishSoundboardStatus();
    return;
  }

  int soundIndex = parseSoundIndex(file);
  if (soundIndex < 1) {
    publishLog("WARNING", "soundboard command missing known MiP sound file");
    return;
  }
  mipPlaySound((uint8_t)soundIndex, 0);
  soundPlaying = true;
  currentSoundIndex = (uint8_t)soundIndex;
  snprintf(currentSoundFile, sizeof(currentSoundFile), "%s", soundNameForIndex(currentSoundIndex));
  publishLogf("INFO", "soundboard play index=%u file=%s",
              (unsigned int)currentSoundIndex, currentSoundFile);
  publishSoundboardStatus();
}

void handleVideoControlPayload(JsonVariant payload) {
  bool enabled = false;
  if (!jsonBoolValue(payload, enabled)) return;
  if (!MIP_CAMERA_ENABLED) {
    videoEnabled = false;
    cameraReady = false;
    publishLog("WARNING", "video command ignored because camera support is disabled");
    return;
  }
  if (!enabled) {
    videoEnabled = false;
#if MIP_CAMERA_ENABLED
    if (cameraReady) {
      esp_camera_deinit();
      cameraReady = false;
    }
#endif
    publishLog("INFO", "video disabled");
    return;
  }
  if (!mqttLargeBufferReady) {
    videoEnabled = false;
    publishLog("WARNING", "video requested but MQTT buffer is not ready");
    return;
  }
  if (!cameraReady) {
    cameraReady = initCamera();
  }
  videoEnabled = cameraReady;
  if (!cameraReady) {
    publishLog("WARNING", "video requested but camera init failed");
  } else {
    publishLog("INFO", "video enabled");
  }
}

void handleRebootPayload(JsonVariant payload) {
  bool requested = false;
  if (!jsonBoolValue(payload, requested) || !requested) return;
#if ALLOW_REMOTE_REBOOT
  publishLog("WARNING", "remote reboot requested");
  mipStop();
  delay(250);
  ESP.restart();
#else
  publishLog("WARNING", "remote reboot ignored by firmware config");
#endif
}

void onMqttMessage(char *rawTopic, byte *payload, unsigned int length) {
  ++mqttMessageCount;
  StaticJsonDocument<1024> doc;
  DeserializationError err = deserializeJson(doc, payload, length);
  if (err) {
    publishLog("WARNING", "incoming MQTT JSON parse failed");
    return;
  }
  JsonVariant root = doc.as<JsonVariant>();
  String incomingTopic(rawTopic);
  if (incomingTopic == topicDrive) {
    handleDrivePayload(root);
  } else if (incomingTopic == topicLightsSolid) {
    handleLightsSolidPayload(root);
  } else if (incomingTopic == topicLightsFlash) {
    handleLightsFlashPayload(root);
  } else if (incomingTopic == topicVideoControl || incomingTopic == topicVideoFlag) {
    handleVideoControlPayload(root);
  } else if (incomingTopic == topicRebootFlag) {
    handleRebootPayload(root);
  } else if (incomingTopic == topicSoundboardCommand) {
    handleSoundboardPayload(root);
  }
}

void publishHeartbeat() {
  StaticJsonDocument<128> doc;
  uint64_t t = unixMillis();
  if (t == 0) {
    ensureTimeSync();
    t = unixMillis();
  }
  doc["t"] = t;
  doc["online"] = true;
  doc["status"] = "online";
  publishJsonDocument(topicHeartbeat, doc, true);
}

void publishCapabilities() {
  StaticJsonDocument<6144> doc;
  uint64_t t = unixMillis();
  doc["schema"] = CAPABILITIES_SCHEMA;
  if (t != 0) doc["t"] = t;
  JsonObject value = doc.createNestedObject("value");
  JsonObject identity = value.createNestedObject("identity");
  identity["system"] = SYSTEM_NAME;
  identity["type"] = "robots";
  identity["id"] = ROBOT_ID;
  identity["name"] = ROBOT_NAME;
  identity["model"] = "wowwee_mip_xiao_esp32s3_sense";

  JsonObject video = value.createNestedObject("video");
  video["available"] = MIP_CAMERA_ENABLED && mqttLargeBufferReady;
  video["controls"] = MIP_CAMERA_ENABLED;
  video["topic"] = topicVideo;
  video["command_topic"] = topicVideoControl;
  video["flag_topic"] = topicVideoFlag;
  video["overlays_topic"] = topicVideoOverlays;
  video["encoding"] = "jpeg";
  video["transport"] = "binary_mqtt";
  video["width"] = 160;
  video["height"] = 120;
  video["fps"] = 1;

  JsonObject audio = value.createNestedObject("audio");
  audio["available"] = false;
  audio["controls"] = false;

  JsonObject soundboard = value.createNestedObject("soundboard");
  soundboard["available"] = true;
  soundboard["controls"] = true;
  soundboard["command_topic"] = topicSoundboardCommand;
  soundboard["files_topic"] = topicSoundboardFiles;
  soundboard["status_topic"] = topicSoundboardStatus;

  JsonObject autonomy = value.createNestedObject("autonomy");
  autonomy["available"] = false;
  autonomy["controls"] = false;
  autonomy["command_topic"] = topicAutonomyCommand;

  JsonObject system = value.createNestedObject("system");
  JsonObject reboot = system.createNestedObject("reboot");
  reboot["available"] = true;
  reboot["controls"] = ALLOW_REMOTE_REBOOT ? true : false;
  reboot["flag_topic"] = topicRebootFlag;
  JsonObject serviceRestart = system.createNestedObject("service_restart");
  serviceRestart["available"] = false;
  serviceRestart["controls"] = false;
  JsonObject gitPull = system.createNestedObject("git_pull");
  gitPull["available"] = false;
  gitPull["controls"] = false;

  JsonObject drive = value.createNestedObject("drive");
  drive["available"] = true;
  drive["controls"] = true;
  drive["topic"] = topicDrive;
  drive["command_topic"] = topicDrive;

  JsonObject lights = value.createNestedObject("lights");
  lights["available"] = true;
  lights["controls"] = true;
  lights["solid"] = true;
  lights["flash"] = true;
  lights["solid_topic"] = topicLightsSolid;
  lights["flash_topic"] = topicLightsFlash;

  JsonObject telemetry = value.createNestedObject("telemetry");
  telemetry["touch_sensors"] = false;
  telemetry["charging_status"] = true;
  telemetry["charging_topic"] = topicChargingStatus;
  telemetry["charging_level_topic"] = topicChargingLevel;
  telemetry["wheel_odometry"] = true;
  telemetry["wheel_odometry_topic"] = topicWheelOdometry;

  publishJsonDocument(topicCapabilities, doc, true);
}

void publishChargingStatus() {
  StaticJsonDocument<128> doc;
  doc["value"] = false;
  doc["source"] = "mip_firmware";
  publishJsonDocument(topicChargingStatus, doc, true);
}

void publishChargingLevel() {
  if (!isfinite(mipBatteryVolts)) return;
  StaticJsonDocument<160> doc;
  doc["value"] = mipBatteryVolts;
  doc["unit"] = "v";
  double ts = unixSecondsDouble();
  if (ts > 0.0) doc["timestamp"] = ts;
  publishJsonDocument(topicChargingLevel, doc, false);
}

void publishStatus() {
  StaticJsonDocument<1024> doc;
  uint64_t t = unixMillis();
  if (t != 0) doc["t"] = t;
  JsonObject value = doc.createNestedObject("value");
  value["firmware_version"] = FIRMWARE_VERSION;
  value["video_enabled"] = videoEnabled;
  value["camera_ready"] = cameraReady;
  value["mqtt_large_buffer_ready"] = mqttLargeBufferReady;
  value["mip_baud"] = mipBaud;
  value["mip_rx_ok"] = mipRxOk;
  value["mqtt_message_count"] = mqttMessageCount;
  value["drive_command_count"] = driveCommandCount;
  value["lights_command_count"] = lightsCommandCount;
  value["get_up_command_count"] = getUpCommandCount;
  value["target_velocity"] = targetVelocity;
  value["target_turn_rate"] = targetTurnRate;
  if (isfinite(mipBatteryVolts)) value["battery_v"] = mipBatteryVolts;
  if (mipPosition != 0xFF) {
    value["position_code"] = mipPosition;
    value["position"] = positionName(mipPosition);
  }
  if (mipSoftwareVersion[0]) value["mip_software_version"] = mipSoftwareVersion;
  if (mipHardwareInfo[0]) value["mip_hardware_info"] = mipHardwareInfo;
  if (mipVolume != 0xFF) value["volume"] = mipVolume;
  if (lastRadarMs > 0) value["last_radar"] = lastRadar;
  if (lastGestureMs > 0) value["last_gesture"] = lastGesture;
  if (lastClapMs > 0) value["last_clap_count"] = lastClapCount;
  if (lastWeightMs > 0) value["last_weight"] = lastWeight;
  if (lastShakeMs > 0) value["last_shake"] = lastShake;
  publishJsonDocument(topicStatus, doc, true);
}

void updateOdometryFromMip() {
  if (!isfinite(mipOdometerCm)) return;
  if (!isfinite(odomReferenceCm)) {
    odomReferenceCm = mipOdometerCm;
    odomLastCm = mipOdometerCm;
    odomLastIntegrateMs = millis();
    return;
  }
  float deltaCm = mipOdometerCm - odomLastCm;
  if (deltaCm < -1.0f) {
    odomReferenceCm = mipOdometerCm;
    odomLastCm = mipOdometerCm;
    return;
  }
  deltaCm = fmaxf(0.0f, deltaCm);
  unsigned long now = millis();
  float dt = odomLastIntegrateMs == 0 ? 0.0f : (float)(now - odomLastIntegrateMs) / 1000.0f;
  odomLastIntegrateMs = now;
  float signedDeltaMm = deltaCm * 10.0f;
  if (targetVelocity < 0) signedDeltaMm = -signedDeltaMm;
  odomHeadingRad += ((float)targetTurnRate / 32.0f) * MIP_ODOM_MAX_TURN_RADPS * dt;
  odomXmm += signedDeltaMm * cosf(odomHeadingRad);
  odomYmm += signedDeltaMm * sinf(odomHeadingRad);
  odomLastCm = mipOdometerCm;
}

void publishWheelOdometry() {
  StaticJsonDocument<512> doc;
  JsonObject value = doc.createNestedObject("value");
  float totalMm = isfinite(mipOdometerCm) && isfinite(odomReferenceCm) ? (mipOdometerCm - odomReferenceCm) * 10.0f : 0.0f;
  value["x_mm"] = odomXmm;
  value["y_mm"] = odomYmm;
  value["heading_rad"] = odomHeadingRad;
  value["heading_deg"] = odomHeadingRad * 180.0f / PI;
  value["distance_mm"] = totalMm;
  value["sample_seq"] = odomPublishSeq;
  value["publish_seq"] = odomPublishSeq++;
  doc["unit"] = "mm";
  doc["source"] = "mip-odometer-command-integrator";
  double ts = unixSecondsDouble();
  if (ts > 0.0) doc["timestamp"] = ts;
  publishJsonDocument(topicWheelOdometry, doc, false);
}

void publishSoundboardFiles() {
  String payload;
  payload.reserve(4096);
  payload += "{\"files\":[";
  for (size_t i = 0; i < sizeof(MIP_SOUNDS) / sizeof(MIP_SOUNDS[0]); ++i) {
    if (i > 0) payload += ",";
    payload += "\"";
    payload += MIP_SOUNDS[i].name;
    payload += "\"";
  }
  payload += "],\"controls\":true";
  double ts = unixSecondsDouble();
  if (ts > 0.0) {
    payload += ",\"timestamp\":";
    payload += String(ts, 3);
  }
  payload += "}";
  publishString(topicSoundboardFiles, payload, true);
}

void publishSoundboardStatus() {
  StaticJsonDocument<256> doc;
  doc["playing"] = soundPlaying;
  if (currentSoundFile[0]) doc["file"] = currentSoundFile;
  doc["sound_index"] = currentSoundIndex;
  doc["controls"] = true;
  double ts = unixSecondsDouble();
  if (ts > 0.0) doc["timestamp"] = ts;
  publishJsonDocument(topicSoundboardStatus, doc, true);
}

uint32_t adler32(const uint8_t *data, size_t length) {
  const uint32_t MOD_ADLER = 65521;
  uint32_t a = 1;
  uint32_t b = 0;
  for (size_t i = 0; i < length; ++i) {
    a = (a + data[i]) % MOD_ADLER;
    b = (b + a) % MOD_ADLER;
  }
  return (b << 16) | a;
}

bool zlibStoredWrap(const uint8_t *input, size_t inputLength, uint8_t *&output, size_t &outputLength) {
  size_t blockCount = (inputLength + 65534UL) / 65535UL;
  if (blockCount == 0) blockCount = 1;
  outputLength = 2 + inputLength + blockCount * 5 + 4;
  output = (uint8_t *)malloc(outputLength);
  if (!output) return false;
  size_t out = 0;
  output[out++] = 0x78;
  output[out++] = 0x01;
  size_t offset = 0;
  while (offset < inputLength || (inputLength == 0 && offset == 0)) {
    size_t remaining = inputLength - offset;
    uint16_t blockLen = (uint16_t)min((size_t)65535, remaining);
    bool finalBlock = (offset + blockLen) >= inputLength;
    output[out++] = finalBlock ? 0x01 : 0x00;
    output[out++] = (uint8_t)(blockLen & 0xFF);
    output[out++] = (uint8_t)(blockLen >> 8);
    uint16_t nlen = ~blockLen;
    output[out++] = (uint8_t)(nlen & 0xFF);
    output[out++] = (uint8_t)(nlen >> 8);
    if (blockLen > 0) {
      memcpy(output + out, input + offset, blockLen);
      out += blockLen;
      offset += blockLen;
    } else {
      break;
    }
  }
  uint32_t checksum = adler32(input, inputLength);
  output[out++] = (uint8_t)(checksum >> 24);
  output[out++] = (uint8_t)(checksum >> 16);
  output[out++] = (uint8_t)(checksum >> 8);
  output[out++] = (uint8_t)checksum;
  outputLength = out;
  return true;
}

bool base64Encode(const uint8_t *input, size_t inputLength, char *&output, size_t &outputLength) {
  size_t outSize = ((inputLength + 2) / 3) * 4 + 1;
  output = (char *)malloc(outSize);
  if (!output) return false;
  size_t encodedLength = 0;
  int rc = mbedtls_base64_encode((unsigned char *)output, outSize, &encodedLength, input, inputLength);
  if (rc != 0) {
    free(output);
    output = nullptr;
    outputLength = 0;
    return false;
  }
  output[encodedLength] = '\0';
  outputLength = encodedLength;
  return true;
}

bool initCamera() {
  if (!MIP_CAMERA_ENABLED) return false;
  camera_config_t config;
  memset(&config, 0, sizeof(config));
  config.ledc_channel = LEDC_CHANNEL_0;
  config.ledc_timer = LEDC_TIMER_0;
  config.pin_d0 = Y2_GPIO_NUM;
  config.pin_d1 = Y3_GPIO_NUM;
  config.pin_d2 = Y4_GPIO_NUM;
  config.pin_d3 = Y5_GPIO_NUM;
  config.pin_d4 = Y6_GPIO_NUM;
  config.pin_d5 = Y7_GPIO_NUM;
  config.pin_d6 = Y8_GPIO_NUM;
  config.pin_d7 = Y9_GPIO_NUM;
  config.pin_xclk = XCLK_GPIO_NUM;
  config.pin_pclk = PCLK_GPIO_NUM;
  config.pin_vsync = VSYNC_GPIO_NUM;
  config.pin_href = HREF_GPIO_NUM;
#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
  config.pin_sccb_sda = SIOD_GPIO_NUM;
  config.pin_sccb_scl = SIOC_GPIO_NUM;
#else
  config.pin_sscb_sda = SIOD_GPIO_NUM;
  config.pin_sscb_scl = SIOC_GPIO_NUM;
#endif
  config.pin_pwdn = PWDN_GPIO_NUM;
  config.pin_reset = RESET_GPIO_NUM;
  config.xclk_freq_hz = 20000000;
  config.pixel_format = PIXFORMAT_JPEG;
  config.frame_size = CAMERA_FRAME_SIZE;
  config.jpeg_quality = CAMERA_JPEG_QUALITY;
  config.fb_count = psramFound() ? 2 : 1;
  config.fb_location = psramFound() ? CAMERA_FB_IN_PSRAM : CAMERA_FB_IN_DRAM;
  config.grab_mode = CAMERA_GRAB_LATEST;

  esp_err_t err = esp_camera_init(&config);
  if (err != ESP_OK) {
    debugf("camera init failed: 0x%08x", (unsigned int)err);
    return false;
  }
  return true;
}

void publishVideoFrame() {
  if (!MIP_CAMERA_ENABLED) return;
  if (!videoEnabled || !cameraReady || !mqttLargeBufferReady || !mqtt.connected()) return;
  camera_fb_t *fb = esp_camera_fb_get();
  if (!fb) {
    publishLog("WARNING", "camera frame capture failed");
    return;
  }

  size_t payloadLength = fb->len;
  if (payloadLength + topicVideo.length() + 16 >= MQTT_PACKET_BUFFER_SIZE) {
    publishLogf("WARNING", "video frame exceeds MQTT packet buffer jpeg_bytes=%u",
                (unsigned int)payloadLength);
    esp_camera_fb_return(fb);
    return;
  }
  bool ok = mqtt.publish(topicVideo.c_str(), fb->buf, fb->len, false);
  esp_camera_fb_return(fb);
  if (!ok) {
    publishLog("WARNING", "video frame exceeds MQTT packet buffer");
  }
}

void connectWifi() {
  if (WiFi.status() == WL_CONNECTED) return;
  mipStop();
  publishLog("WARNING", "WiFi disconnected; reconnecting");
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) {
    delay(250);
  }
  publishLogf("INFO", "WiFi connected ip=%s", WiFi.localIP().toString().c_str());
}

void configureMqttTls() {
#if MQTT_USE_TLS
  if (MQTT_TLS_INSECURE) {
    wifiClient.setInsecure();
  } else {
    wifiClient.setCACert(MQTT_CA_CERT);
  }
#endif
}

void publishStartupState() {
  publishCapabilities();
  publishHeartbeat();
  publishChargingStatus();
  publishStatus();
  publishSoundboardFiles();
  publishSoundboardStatus();
  publishLog("INFO", "MiP firmware connected to MQTT");
}

void subscribeTopics() {
  mqtt.subscribe(topicDrive.c_str(), 0);
  mqtt.subscribe(topicLightsSolid.c_str(), 1);
  mqtt.subscribe(topicLightsFlash.c_str(), 1);
  mqtt.subscribe(topicVideoControl.c_str(), 1);
  mqtt.subscribe(topicVideoFlag.c_str(), 1);
  mqtt.subscribe(topicRebootFlag.c_str(), 1);
  mqtt.subscribe(topicSoundboardCommand.c_str(), 1);
  mqtt.subscribe(topicAutonomyCommand.c_str(), 1);
}

void connectMqtt() {
  while (!mqtt.connected()) {
    mipStop();
    uint64_t chipId = ESP.getEfuseMac();
    String clientId = String("mip-xiao-s3-") + String((uint32_t)(chipId & 0xFFFFFFFF), HEX);
    bool connected = false;
    if (strlen(MQTT_USER) > 0) {
      connected = mqtt.connect(
          clientId.c_str(), MQTT_USER, MQTT_PASS, topicHeartbeat.c_str(), 1, true, MQTT_OFFLINE_PAYLOAD);
    } else {
      connected = mqtt.connect(clientId.c_str(), topicHeartbeat.c_str(), 1, true, MQTT_OFFLINE_PAYLOAD);
    }
    if (connected) {
      subscribeTopics();
      publishStartupState();
    } else {
      publishLogf("WARNING", "MQTT connect failed state=%d", mqtt.state());
      delay(1000);
    }
  }
}

void setupTopics() {
  baseTopic = String(SYSTEM_NAME) + "/robots/" + ROBOT_ID;
  topicDrive = topic("incoming", "drive-values");
  topicLightsSolid = topic("incoming", "lights-solid");
  topicLightsFlash = topic("incoming", "lights-flash");
  topicVideoControl = topic("incoming", "front-camera");
  topicVideoFlag = topic("incoming", "flags/mqtt-video");
  topicRebootFlag = topic("incoming", "flags/reboot");
  topicSoundboardCommand = topic("incoming", "soundboard-command");
  topicAutonomyCommand = topic("incoming", "autonomy-command");
  topicHeartbeat = topic("outgoing", "online");
  topicCapabilities = topic("outgoing", "capabilities");
  topicLogs = topic("outgoing", "logs");
  topicStatus = topic("outgoing", "status");
  topicChargingLevel = topic("outgoing", "charging-level");
  topicChargingStatus = topic("outgoing", "charging-status");
  topicWheelOdometry = topic("outgoing", "wheel-odometry");
  topicSoundboardFiles = topic("outgoing", "soundboard-files");
  topicSoundboardStatus = topic("outgoing", "soundboard-status");
  topicVideo = topic("outgoing", "front-camera");
  topicVideoOverlays = topic("outgoing", "video-overlays");
}

void driveSafetyTick() {
  unsigned long now = millis();
  if (driveActive && (now - lastDriveCommandMs) > DRIVE_TIMEOUT_MS) {
    if (!driveStoppedForTimeout) {
      mipStop();
      targetVelocity = 0;
      targetTurnRate = 0;
      driveStoppedForTimeout = true;
    }
    return;
  }
  if (driveActive && !driveStoppedForTimeout && (now - lastDriveRefreshMs) >= DRIVE_REFRESH_MS) {
    lastDriveRefreshMs = now;
    mipContinuousDrive(targetVelocity, targetTurnRate);
  }
}

void telemetryTick() {
  unsigned long now = millis();
  if ((now - lastStatusMs) < 1000UL) return;
  lastStatusMs = now;
  bool statusOk = queryMipStatus(50);
  bool odomOk = false;
  if ((now - lastOdomPollMs) >= 1000UL) {
    lastOdomPollMs = now;
    odomOk = queryMipOdometer(50);
  }
  if (!statusOk && !odomOk && lastMipResponseMs > 0 && (now - lastMipResponseMs) > 5000UL) {
    if (mipRxOk) {
      mipRxOk = false;
      publishLog("WARNING", "MiP UART RX lost");
    }
  }
  updateOdometryFromMip();
  publishStatus();
  publishChargingLevel();
  publishWheelOdometry();
}

void periodicPublishTick() {
  unsigned long now = millis();
  if ((now - lastHeartbeatMs) >= 1000UL) {
    lastHeartbeatMs = now;
    publishHeartbeat();
  }
  if ((now - lastCapabilitiesMs) >= 3600000UL) {
    lastCapabilitiesMs = now;
    publishCapabilities();
    publishSoundboardFiles();
    publishChargingStatus();
  }
  if (MIP_CAMERA_ENABLED && videoEnabled && (now - lastVideoMs) >= MQTT_VIDEO_INTERVAL_MS) {
    lastVideoMs = now;
    publishVideoFrame();
  }
}

}  // namespace

void setup() {
  Serial.begin(115200);
  delay(200);
  setupTopics();

  beginMipUart();
  mipGetUpEither();
  connectWifi();
  ensureTimeSync();
  configureMqttTls();
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMqttMessage);
  mqtt.setKeepAlive(MQTT_KEEPALIVE_SECONDS);
  mqttLargeBufferReady = mqtt.setBufferSize(MQTT_PACKET_BUFFER_SIZE);
  if (!mqttLargeBufferReady) {
    videoEnabled = false;
  }
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
  processMipIncoming(1);
  driveSafetyTick();
  telemetryTick();
  periodicPublishTick();
}
