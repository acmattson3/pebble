#include <Arduino.h>
#include <WiFi.h>
#include <ArduinoJson.h>

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "private_config.h"

/*
  HS105 outlet bridge for Seeeduino XIAO ESP32-C3.

  Behavior:
  - joins an existing Wi-Fi LAN as a normal station client
  - talks to four TP-Link HS105 outlets at configured static IPs
  - talks to the host over USB serial using pebble_serial_v1 framed packets

  Slot ownership:
  - `o1`..`o4` are mapped by configured outlet IP, not by discovery order
  - the TP-Link alias is informational only in this architecture
*/

namespace {

const char DEVICE_UID[] = "hs105-sta";
const char MODEL_NAME[] = "seeed_xiao_esp32c3_hs105_station_bridge";
const char FIRMWARE_VERSION[] = "2026.04.03";
const char SCHEMA_HASH[] = "hs105-sta-v1";
const uint8_t SCHEMA_VERSION = 1;

const uint8_t PROTOCOL_VERSION = 1;
const uint8_t MSG_HELLO = 0x01;
const uint8_t MSG_DESCRIBE_REQ = 0x02;
const uint8_t MSG_DESCRIBE = 0x03;
const uint8_t MSG_STATE = 0x11;
const uint8_t MSG_CMD = 0x20;
const uint8_t MSG_ACK = 0x21;
const uint8_t MSG_NACK = 0x22;
const uint8_t MSG_LOG = 0x30;
const uint8_t MSG_PING = 0x31;
const uint8_t MSG_PONG = 0x32;

const uint8_t DIR_IN = 1;
const uint8_t DIR_OUT = 2;

const uint8_t KIND_STATE = 2;
const uint8_t KIND_COMMAND = 3;

const uint8_t ENCODING_STRUCT_V1 = 1;

const uint8_t TYPE_BOOL = 1;
const uint8_t TYPE_F32 = 8;

const uint8_t FLAG_RETAIN = 0x01;

const uint16_t INTERFACE_O1_SET = 1;
const uint16_t INTERFACE_O1_STATE = 2;
const uint16_t INTERFACE_O2_SET = 3;
const uint16_t INTERFACE_O2_STATE = 4;
const uint16_t INTERFACE_O3_SET = 5;
const uint16_t INTERFACE_O3_STATE = 6;
const uint16_t INTERFACE_O4_SET = 7;
const uint16_t INTERFACE_O4_STATE = 8;

const uint8_t OUTLET_SLOT_COUNT = 4;

const uint16_t TP_LINK_PORT = 9999;
const char TP_LINK_GET_SYSINFO[] = "{\"system\":{\"get_sysinfo\":{}}}";
const char TP_LINK_RELAY_ON[] = "{\"system\":{\"set_relay_state\":{\"state\":1}}}";
const char TP_LINK_RELAY_OFF[] = "{\"system\":{\"set_relay_state\":{\"state\":0}}}";
const uint8_t TP_LINK_XOR_KEY = 171;
const uint8_t TP_LINK_COMMAND_ATTEMPTS = 3;
const unsigned long TP_LINK_COMMAND_RETRY_DELAY_MS = 250UL;
const uint8_t TP_LINK_POST_COMMAND_REFRESH_ATTEMPTS = 4;
const unsigned long TP_LINK_POST_COMMAND_REFRESH_DELAY_MS = 200UL;

const size_t HEADER_SIZE = 13;
const size_t CRC_SIZE = 2;
const size_t MAX_PACKET_BODY = 1024;
const size_t MAX_FRAME_SIZE = 1280;
const size_t MAX_RX_FRAME_SIZE = 384;
const size_t MAX_DECODED_FRAME_SIZE = 1024;
const size_t MAX_ALIAS_LENGTH = 95;
const size_t MAX_MAC_LENGTH = 31;
const size_t MAX_TCP_PAYLOAD = 4096;
const size_t LOG_BUFFER_SIZE = 192;

const IPAddress STA_IP(WIFI_STA_IP_1, WIFI_STA_IP_2, WIFI_STA_IP_3, WIFI_STA_IP_4);
const IPAddress STA_GATEWAY(WIFI_STA_GATEWAY_1, WIFI_STA_GATEWAY_2, WIFI_STA_GATEWAY_3, WIFI_STA_GATEWAY_4);
const IPAddress STA_SUBNET(WIFI_STA_SUBNET_1, WIFI_STA_SUBNET_2, WIFI_STA_SUBNET_3, WIFI_STA_SUBNET_4);

const IPAddress OUTLET_IPS[OUTLET_SLOT_COUNT] = {
  IPAddress(OUTLET_O1_IP_1, OUTLET_O1_IP_2, OUTLET_O1_IP_3, OUTLET_O1_IP_4),
  IPAddress(OUTLET_O2_IP_1, OUTLET_O2_IP_2, OUTLET_O2_IP_3, OUTLET_O2_IP_4),
  IPAddress(OUTLET_O3_IP_1, OUTLET_O3_IP_2, OUTLET_O3_IP_3, OUTLET_O3_IP_4),
  IPAddress(OUTLET_O4_IP_1, OUTLET_O4_IP_2, OUTLET_O4_IP_3, OUTLET_O4_IP_4),
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

struct SlotProbeResult {
  char alias[MAX_ALIAS_LENGTH + 1];
  char mac[MAX_MAC_LENGTH + 1];
  bool relayOn;
};

struct OutletSlot {
  uint8_t slotNumber;
  uint16_t commandInterfaceId;
  uint16_t stateInterfaceId;
  IPAddress configuredIp;
  char alias[MAX_ALIAS_LENGTH + 1];
  char mac[MAX_MAC_LENGTH + 1];
  bool bound;
  bool reachable;
  bool on;
  uint32_t stateSeq;
};

const FieldSpec OUTLET_COMMAND_FIELDS[] = {
  {"on", TYPE_BOOL, 1.0f, 0.0f, ""},
};

const FieldSpec OUTLET_STATE_FIELDS[] = {
  {"bound", TYPE_BOOL, 1.0f, 0.0f, ""},
  {"reachable", TYPE_BOOL, 1.0f, 0.0f, ""},
  {"on", TYPE_BOOL, 1.0f, 0.0f, ""},
};

const InterfaceSpec INTERFACES[] = {
  {INTERFACE_O1_SET, DIR_IN, KIND_COMMAND, 0, 0, "o1_set", "outlets/o1/power", "outlet.switch.v1", ENCODING_STRUCT_V1, 1, OUTLET_COMMAND_FIELDS},
  {INTERFACE_O1_STATE, DIR_OUT, KIND_STATE, FLAG_RETAIN, 1, "o1_state", "outlets/o1/status", "outlet.switch.v1", ENCODING_STRUCT_V1, 3, OUTLET_STATE_FIELDS},
  {INTERFACE_O2_SET, DIR_IN, KIND_COMMAND, 0, 0, "o2_set", "outlets/o2/power", "outlet.switch.v1", ENCODING_STRUCT_V1, 1, OUTLET_COMMAND_FIELDS},
  {INTERFACE_O2_STATE, DIR_OUT, KIND_STATE, FLAG_RETAIN, 1, "o2_state", "outlets/o2/status", "outlet.switch.v1", ENCODING_STRUCT_V1, 3, OUTLET_STATE_FIELDS},
  {INTERFACE_O3_SET, DIR_IN, KIND_COMMAND, 0, 0, "o3_set", "outlets/o3/power", "outlet.switch.v1", ENCODING_STRUCT_V1, 1, OUTLET_COMMAND_FIELDS},
  {INTERFACE_O3_STATE, DIR_OUT, KIND_STATE, FLAG_RETAIN, 1, "o3_state", "outlets/o3/status", "outlet.switch.v1", ENCODING_STRUCT_V1, 3, OUTLET_STATE_FIELDS},
  {INTERFACE_O4_SET, DIR_IN, KIND_COMMAND, 0, 0, "o4_set", "outlets/o4/power", "outlet.switch.v1", ENCODING_STRUCT_V1, 1, OUTLET_COMMAND_FIELDS},
  {INTERFACE_O4_STATE, DIR_OUT, KIND_STATE, FLAG_RETAIN, 1, "o4_state", "outlets/o4/status", "outlet.switch.v1", ENCODING_STRUCT_V1, 3, OUTLET_STATE_FIELDS},
};

const uint8_t INTERFACE_COUNT = (uint8_t)(sizeof(INTERFACES) / sizeof(INTERFACES[0]));

OutletSlot gSlots[OUTLET_SLOT_COUNT];

uint8_t gTxPayloadBuffer[MAX_PACKET_BODY];
uint8_t gTxBodyBuffer[MAX_PACKET_BODY];
uint8_t gTxFrameBuffer[MAX_FRAME_SIZE];
uint8_t gRxFrameBuffer[MAX_RX_FRAME_SIZE];
uint8_t gRxDecodedBuffer[MAX_DECODED_FRAME_SIZE];
uint8_t gNetworkBuffer[MAX_TCP_PAYLOAD];
char gResponseBuffer[MAX_TCP_PAYLOAD];
size_t gRxFrameLength = 0;

unsigned long gNextPollAtMs = 0;
unsigned long gNextWifiRetryAtMs = 0;
uint8_t gNextPollSlot = 0;
bool gWifiWasConnected = false;

void processSerialInput();

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

bool appendBool(uint8_t *buffer, size_t maxLength, size_t &index, bool value) {
  const uint8_t raw = value ? 1 : 0;
  return appendBytes(buffer, maxLength, index, &raw, sizeof(raw));
}

bool appendU16(uint8_t *buffer, size_t maxLength, size_t &index, uint16_t value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendU32(uint8_t *buffer, size_t maxLength, size_t &index, uint32_t value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendFloat32(uint8_t *buffer, size_t maxLength, size_t &index, float value) {
  return appendBytes(buffer, maxLength, index, &value, sizeof(value));
}

bool appendShortString(uint8_t *buffer, size_t maxLength, size_t &index, const char *value) {
  const char *safeValue = value == nullptr ? "" : value;
  const size_t length = strlen(safeValue);
  if (length > 255) {
    return false;
  }
  if (!appendU8(buffer, maxLength, index, (uint8_t)length)) {
    return false;
  }
  return appendBytes(buffer, maxLength, index, safeValue, length);
}

void safeStringCopy(char *dst, size_t dstSize, const char *src) {
  if (dstSize == 0) {
    return;
  }
  if (src == nullptr) {
    dst[0] = '\0';
    return;
  }
  strncpy(dst, src, dstSize - 1);
  dst[dstSize - 1] = '\0';
}

void normalizeMac(const char *src, char *dst, size_t dstSize) {
  if (dstSize == 0) {
    return;
  }
  dst[0] = '\0';
  if (src == nullptr) {
    return;
  }
  size_t outIndex = 0;
  for (size_t i = 0; src[i] != '\0' && outIndex + 1 < dstSize; ++i) {
    const char c = src[i];
    if (isxdigit((unsigned char)c) == 0) {
      continue;
    }
    dst[outIndex++] = (char)toupper((unsigned char)c);
  }
  dst[outIndex] = '\0';
}

bool ipIsZero(const IPAddress &ip) {
  return ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0;
}

void formatIp(IPAddress ip, char *buffer, size_t bufferSize) {
  if (bufferSize == 0) {
    return;
  }
  snprintf(
    buffer,
    bufferSize,
    "%u.%u.%u.%u",
    (unsigned int)ip[0],
    (unsigned int)ip[1],
    (unsigned int)ip[2],
    (unsigned int)ip[3]
  );
}

void expectedAliasForSlot(uint8_t slotNumber, char *buffer, size_t bufferSize) {
  if (bufferSize == 0) {
    return;
  }
  snprintf(buffer, bufferSize, "O%u", (unsigned int)slotNumber);
}

bool normalizedAliasesEqual(const char *lhs, const char *rhs) {
  char left[8];
  char right[8];
  size_t leftIndex = 0;
  size_t rightIndex = 0;

  if (lhs != nullptr) {
    for (size_t i = 0; lhs[i] != '\0' && leftIndex + 1 < sizeof(left); ++i) {
      if (isalnum((unsigned char)lhs[i]) == 0) {
        continue;
      }
      left[leftIndex++] = (char)toupper((unsigned char)lhs[i]);
    }
  }
  if (rhs != nullptr) {
    for (size_t i = 0; rhs[i] != '\0' && rightIndex + 1 < sizeof(right); ++i) {
      if (isalnum((unsigned char)rhs[i]) == 0) {
        continue;
      }
      right[rightIndex++] = (char)toupper((unsigned char)rhs[i]);
    }
  }
  left[leftIndex] = '\0';
  right[rightIndex] = '\0';
  return strcmp(left, right) == 0;
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
    if (value == 0x00) {
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
    if (code == 0x00) {
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
  if (!appendU8(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, PROTOCOL_VERSION)) return false;
  if (!appendU8(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, msgType)) return false;
  if (!appendU8(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, flags)) return false;
  if (!appendU16(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, interfaceId)) return false;
  if (!appendU32(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, packetSeq)) return false;
  if (!appendU32(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, timestampMs)) return false;
  if ((payloadLength > 0) && ((payload == nullptr) || !appendBytes(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, payload, payloadLength))) return false;

  const uint16_t crc = crc16Ccitt(gTxBodyBuffer, bodyLength);
  if (!appendU16(gTxBodyBuffer, sizeof(gTxBodyBuffer), bodyLength, crc)) return false;

  size_t frameLength = 0;
  if (!cobsEncode(gTxBodyBuffer, bodyLength, gTxFrameBuffer, sizeof(gTxFrameBuffer), frameLength)) {
    return false;
  }
  if ((frameLength + 1) > sizeof(gTxFrameBuffer)) {
    return false;
  }
  gTxFrameBuffer[frameLength++] = 0x00;
  Serial.write(gTxFrameBuffer, frameLength);
  return true;
}

void sendLog(const char *message) {
  if (message == nullptr || message[0] == '\0') {
    return;
  }
  sendPacket(MSG_LOG, 0, 0, millis(), reinterpret_cast<const uint8_t *>(message), strlen(message));
}

void sendLogf(const char *fmt, ...) {
  char buffer[LOG_BUFFER_SIZE];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);
  sendLog(buffer);
}

void sendAck(uint16_t interfaceId, uint32_t packetSeq) {
  sendPacket(MSG_ACK, interfaceId, packetSeq, millis(), nullptr, 0);
}

void sendNack(uint16_t interfaceId, uint32_t packetSeq, const char *reason) {
  const char *payload = reason == nullptr ? "" : reason;
  sendPacket(MSG_NACK, interfaceId, packetSeq, millis(), reinterpret_cast<const uint8_t *>(payload), strlen(payload));
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
  if (buildHelloPayload(gTxPayloadBuffer, sizeof(gTxPayloadBuffer), payloadLength)) {
    sendPacket(MSG_HELLO, 0, 0, millis(), gTxPayloadBuffer, payloadLength);
  }
}

void sendDescribe() {
  size_t payloadLength = 0;
  if (buildDescribePayload(gTxPayloadBuffer, sizeof(gTxPayloadBuffer), payloadLength)) {
    sendPacket(MSG_DESCRIBE, 0, 0, millis(), gTxPayloadBuffer, payloadLength);
  } else {
    sendLog("describe_build_failed");
  }
}

bool buildOutletStatePayload(const OutletSlot &slot, uint8_t *buffer, size_t maxLength, size_t &payloadLength) {
  payloadLength = 0;
  if (!appendBool(buffer, maxLength, payloadLength, slot.bound)) return false;
  if (!appendBool(buffer, maxLength, payloadLength, slot.reachable)) return false;
  if (!appendBool(buffer, maxLength, payloadLength, slot.on)) return false;
  return true;
}

void publishSlotState(OutletSlot &slot) {
  size_t payloadLength = 0;
  if (!buildOutletStatePayload(slot, gTxPayloadBuffer, sizeof(gTxPayloadBuffer), payloadLength)) {
    return;
  }
  slot.stateSeq++;
  sendPacket(MSG_STATE, slot.stateInterfaceId, slot.stateSeq, millis(), gTxPayloadBuffer, payloadLength);
}

void publishAllStates() {
  for (uint8_t i = 0; i < OUTLET_SLOT_COUNT; ++i) {
    publishSlotState(gSlots[i]);
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

const char *jsonStringOrEmpty(JsonVariantConst value) {
  if (!value.is<const char *>()) {
    return "";
  }
  const char *text = value.as<const char *>();
  return text == nullptr ? "" : text;
}

bool parseSysinfoJson(const char *json, SlotProbeResult &result) {
  DynamicJsonDocument doc(3072);
  const DeserializationError err = deserializeJson(doc, json);
  if (err) {
    return false;
  }

  JsonVariantConst sysinfo = doc["system"]["get_sysinfo"];
  if (sysinfo.isNull()) {
    return false;
  }

  safeStringCopy(result.alias, sizeof(result.alias), jsonStringOrEmpty(sysinfo["alias"]));
  normalizeMac(jsonStringOrEmpty(sysinfo["mac"]), result.mac, sizeof(result.mac));
  if (result.mac[0] == '\0') {
    normalizeMac(jsonStringOrEmpty(sysinfo["mic_mac"]), result.mac, sizeof(result.mac));
  }
  result.relayOn = (sysinfo["relay_state"].is<int>() ? sysinfo["relay_state"].as<int>() : 0) != 0;
  return true;
}

bool parseRelayCommandSuccess(const char *json) {
  DynamicJsonDocument doc(1024);
  const DeserializationError err = deserializeJson(doc, json);
  if (err) {
    return false;
  }
  JsonVariantConst result = doc["system"]["set_relay_state"];
  if (result.isNull()) {
    return false;
  }
  const int errCode = result["err_code"].is<int>() ? result["err_code"].as<int>() : -1;
  return errCode == 0;
}

void xorEncrypt(const char *plaintext, uint8_t *output, size_t length) {
  uint8_t key = TP_LINK_XOR_KEY;
  for (size_t i = 0; i < length; ++i) {
    const uint8_t cipher = (uint8_t)plaintext[i] ^ key;
    output[i] = cipher;
    key = cipher;
  }
}

void xorDecryptInPlace(uint8_t *buffer, size_t length) {
  uint8_t key = TP_LINK_XOR_KEY;
  for (size_t i = 0; i < length; ++i) {
    const uint8_t cipher = buffer[i];
    buffer[i] = cipher ^ key;
    key = cipher;
  }
}

bool sendTcpRequest(IPAddress ip, const char *requestJson, char *response, size_t responseSize, size_t &responseLength) {
  responseLength = 0;
  if (requestJson == nullptr || response == nullptr || responseSize < 2) {
    return false;
  }

  const size_t requestLength = strlen(requestJson);
  if (requestLength == 0 || requestLength > sizeof(gNetworkBuffer)) {
    return false;
  }

  WiFiClient client;
  client.setTimeout(TP_LINK_TCP_TIMEOUT_MS);
  if (!client.connect(ip, TP_LINK_PORT)) {
    return false;
  }

  xorEncrypt(requestJson, gNetworkBuffer, requestLength);

  uint8_t lengthPrefix[4];
  lengthPrefix[0] = (uint8_t)((requestLength >> 24) & 0xFF);
  lengthPrefix[1] = (uint8_t)((requestLength >> 16) & 0xFF);
  lengthPrefix[2] = (uint8_t)((requestLength >> 8) & 0xFF);
  lengthPrefix[3] = (uint8_t)(requestLength & 0xFF);

  if (client.write(lengthPrefix, sizeof(lengthPrefix)) != sizeof(lengthPrefix)) {
    client.stop();
    return false;
  }
  if (client.write(gNetworkBuffer, requestLength) != requestLength) {
    client.stop();
    return false;
  }

  uint8_t responsePrefix[4];
  if (client.readBytes(reinterpret_cast<char *>(responsePrefix), sizeof(responsePrefix)) != sizeof(responsePrefix)) {
    client.stop();
    return false;
  }

  const uint32_t payloadLength = ((uint32_t)responsePrefix[0] << 24) |
                                 ((uint32_t)responsePrefix[1] << 16) |
                                 ((uint32_t)responsePrefix[2] << 8) |
                                 (uint32_t)responsePrefix[3];
  if (payloadLength == 0 || payloadLength >= responseSize || payloadLength > sizeof(gNetworkBuffer)) {
    client.stop();
    return false;
  }

  if (client.readBytes(reinterpret_cast<char *>(gNetworkBuffer), payloadLength) != payloadLength) {
    client.stop();
    return false;
  }
  client.stop();

  xorDecryptInPlace(gNetworkBuffer, payloadLength);
  memcpy(response, gNetworkBuffer, payloadLength);
  response[payloadLength] = '\0';
  responseLength = payloadLength;
  return true;
}

bool fetchSysinfo(IPAddress ip, SlotProbeResult &result) {
  size_t responseLength = 0;
  if (!sendTcpRequest(ip, TP_LINK_GET_SYSINFO, gResponseBuffer, sizeof(gResponseBuffer), responseLength)) {
    return false;
  }
  return parseSysinfoJson(gResponseBuffer, result);
}

bool setRelay(IPAddress ip, bool on) {
  size_t responseLength = 0;
  if (!sendTcpRequest(ip, on ? TP_LINK_RELAY_ON : TP_LINK_RELAY_OFF, gResponseBuffer, sizeof(gResponseBuffer), responseLength)) {
    return false;
  }
  return parseRelayCommandSuccess(gResponseBuffer);
}

bool waitForWifiConnection(unsigned long timeoutMs) {
  const unsigned long deadline = millis() + timeoutMs;
  while (WiFi.status() != WL_CONNECTED) {
    processSerialInput();
    if ((long)(millis() - deadline) >= 0) {
      break;
    }
    delay(25);
  }
  return WiFi.status() == WL_CONNECTED;
}

void markAllSlotsUnreachable(bool publishStates) {
  for (uint8_t i = 0; i < OUTLET_SLOT_COUNT; ++i) {
    gSlots[i].reachable = false;
  }
  if (publishStates) {
    publishAllStates();
  }
}

bool connectWifiNow(bool logFailures) {
  char ipBuffer[20];
  formatIp(STA_IP, ipBuffer, sizeof(ipBuffer));
  sendLogf("wifi_connecting ssid=%s ip=%s", WIFI_STA_SSID, ipBuffer);

  WiFi.disconnect(false, false);
  WiFi.begin(WIFI_STA_SSID, WIFI_STA_PSK);
  const bool connected = waitForWifiConnection(WIFI_STA_CONNECT_TIMEOUT_MS);

  if (!connected) {
    if (logFailures) {
      sendLog("wifi_connect_failed");
    }
    gWifiWasConnected = false;
    gNextWifiRetryAtMs = millis() + WIFI_STA_RETRY_INTERVAL_MS;
    markAllSlotsUnreachable(true);
    return false;
  }

  formatIp(WiFi.localIP(), ipBuffer, sizeof(ipBuffer));
  sendLogf("wifi_connected ip=%s", ipBuffer);
  gWifiWasConnected = true;
  gNextWifiRetryAtMs = millis() + WIFI_STA_RETRY_INTERVAL_MS;
  return true;
}

void configureWifiStation() {
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  if (!WiFi.config(STA_IP, STA_GATEWAY, STA_SUBNET)) {
    sendLog("wifi_static_ip_failed");
  }
}

void initSlots() {
  memset(gSlots, 0, sizeof(gSlots));
  for (uint8_t i = 0; i < OUTLET_SLOT_COUNT; ++i) {
    gSlots[i].slotNumber = (uint8_t)(i + 1);
    gSlots[i].commandInterfaceId = INTERFACES[i * 2].id;
    gSlots[i].stateInterfaceId = INTERFACES[i * 2 + 1].id;
    gSlots[i].configuredIp = OUTLET_IPS[i];
    gSlots[i].bound = !ipIsZero(gSlots[i].configuredIp);
  }
}

bool refreshSlotFromIp(OutletSlot &slot, bool publishState) {
  if (!slot.bound || ipIsZero(slot.configuredIp)) {
    slot.bound = false;
    slot.reachable = false;
    if (publishState) {
      publishSlotState(slot);
    }
    return false;
  }
  if (WiFi.status() != WL_CONNECTED) {
    slot.reachable = false;
    if (publishState) {
      publishSlotState(slot);
    }
    return false;
  }

  SlotProbeResult probe;
  memset(&probe, 0, sizeof(probe));
  if (!fetchSysinfo(slot.configuredIp, probe)) {
    slot.reachable = false;
    if (publishState) {
      publishSlotState(slot);
    }
    return false;
  }

  slot.reachable = true;
  slot.on = probe.relayOn;
  safeStringCopy(slot.alias, sizeof(slot.alias), probe.alias);
  safeStringCopy(slot.mac, sizeof(slot.mac), probe.mac);

  char expectedAlias[8];
  expectedAliasForSlot(slot.slotNumber, expectedAlias, sizeof(expectedAlias));
  if (slot.alias[0] != '\0' && !normalizedAliasesEqual(slot.alias, expectedAlias)) {
    sendLogf("alias_mismatch slot=o%u expected=%s actual=%s", (unsigned int)slot.slotNumber, expectedAlias, slot.alias);
  }

  if (publishState) {
    publishSlotState(slot);
  }
  return true;
}

void refreshAllSlots() {
  for (uint8_t i = 0; i < OUTLET_SLOT_COUNT; ++i) {
    refreshSlotFromIp(gSlots[i], true);
  }
}

bool executeOutletCommand(OutletSlot &slot, bool onRequested) {
  if (!slot.bound || ipIsZero(slot.configuredIp)) {
    return false;
  }
  if (WiFi.status() != WL_CONNECTED) {
    if (!connectWifiNow(false)) {
      return false;
    }
  }
  for (uint8_t commandAttempt = 0; commandAttempt < TP_LINK_COMMAND_ATTEMPTS; ++commandAttempt) {
    if (!setRelay(slot.configuredIp, onRequested)) {
      slot.reachable = false;
      publishSlotState(slot);
      if (commandAttempt + 1U < TP_LINK_COMMAND_ATTEMPTS) {
        delay(TP_LINK_COMMAND_RETRY_DELAY_MS);
        processSerialInput();
        continue;
      }
      return false;
    }

    for (uint8_t refreshAttempt = 0; refreshAttempt < TP_LINK_POST_COMMAND_REFRESH_ATTEMPTS; ++refreshAttempt) {
      if (refreshSlotFromIp(slot, true)) {
        return true;
      }
      delay(TP_LINK_POST_COMMAND_REFRESH_DELAY_MS);
      processSerialInput();
    }

    if (commandAttempt + 1U < TP_LINK_COMMAND_ATTEMPTS) {
      delay(TP_LINK_COMMAND_RETRY_DELAY_MS);
      processSerialInput();
    }
  }
  return false;
}

void pollNextSlot() {
  for (uint8_t attempt = 0; attempt < OUTLET_SLOT_COUNT; ++attempt) {
    OutletSlot &slot = gSlots[gNextPollSlot];
    gNextPollSlot = (uint8_t)((gNextPollSlot + 1) % OUTLET_SLOT_COUNT);
    if (!slot.bound) {
      continue;
    }
    refreshSlotFromIp(slot, true);
    return;
  }
}

int findSlotIndexByCommandInterface(uint16_t interfaceId) {
  for (uint8_t i = 0; i < OUTLET_SLOT_COUNT; ++i) {
    if (gSlots[i].commandInterfaceId == interfaceId) {
      return (int)i;
    }
  }
  return -1;
}

void handleCommandPacket(uint16_t interfaceId, uint32_t packetSeq, const uint8_t *payload, size_t payloadLength) {
  const int slotIndex = findSlotIndexByCommandInterface(interfaceId);
  if (slotIndex < 0) {
    sendNack(interfaceId, packetSeq, "unknown_interface");
    return;
  }
  if (payload == nullptr || payloadLength != 1) {
    sendNack(interfaceId, packetSeq, "bad_payload");
    return;
  }

  OutletSlot &slot = gSlots[slotIndex];
  if (!slot.bound) {
    sendNack(interfaceId, packetSeq, "slot_unconfigured");
    publishSlotState(slot);
    return;
  }

  const bool requestedOn = payload[0] != 0;
  if (!executeOutletCommand(slot, requestedOn)) {
    sendNack(interfaceId, packetSeq, "command_failed");
    publishSlotState(slot);
    return;
  }
  sendAck(interfaceId, packetSeq);
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
  if (buffer[0] != PROTOCOL_VERSION) {
    return;
  }

  const uint8_t msgType = buffer[1];
  const uint16_t interfaceId = readU16(buffer, 3);
  const uint32_t packetSeq = readU32(buffer, 5);
  const uint8_t *payload = buffer + HEADER_SIZE;
  const size_t payloadLength = length - HEADER_SIZE - CRC_SIZE;

  if (msgType == MSG_DESCRIBE_REQ) {
    sendHello();
    sendDescribe();
    publishAllStates();
    return;
  }
  if (msgType == MSG_PING) {
    sendPacket(MSG_PONG, 0, packetSeq, millis(), nullptr, 0);
    return;
  }
  if (msgType == MSG_CMD) {
    handleCommandPacket(interfaceId, packetSeq, payload, payloadLength);
    return;
  }
}

void processSerialInput() {
  while (Serial.available() > 0) {
    const int incoming = Serial.read();
    if (incoming < 0) {
      return;
    }
    const uint8_t byteValue = (uint8_t)incoming;
    if (byteValue == 0x00) {
      if (gRxFrameLength > 0) {
        size_t decodedLength = 0;
        if (cobsDecode(gRxFrameBuffer, gRxFrameLength, gRxDecodedBuffer, sizeof(gRxDecodedBuffer), decodedLength)) {
          handleDecodedPacket(gRxDecodedBuffer, decodedLength);
        }
      }
      gRxFrameLength = 0;
      continue;
    }
    if (gRxFrameLength >= sizeof(gRxFrameBuffer)) {
      gRxFrameLength = 0;
      continue;
    }
    gRxFrameBuffer[gRxFrameLength++] = byteValue;
  }
}

void maintainWifiConnection() {
  const bool connected = WiFi.status() == WL_CONNECTED;
  if (connected) {
    if (!gWifiWasConnected) {
      char ipBuffer[20];
      formatIp(WiFi.localIP(), ipBuffer, sizeof(ipBuffer));
      sendLogf("wifi_connected ip=%s", ipBuffer);
      gWifiWasConnected = true;
      refreshAllSlots();
    }
    return;
  }

  if (gWifiWasConnected) {
    gWifiWasConnected = false;
    sendLog("wifi_disconnected");
    markAllSlotsUnreachable(true);
  }

  if ((long)(millis() - gNextWifiRetryAtMs) >= 0) {
    connectWifiNow(true);
  }
}

void setupImpl() {
  Serial.begin(SERIAL_BAUD);
  delay(100);

  initSlots();
  configureWifiStation();

  sendHello();
  sendDescribe();
  publishAllStates();

  connectWifiNow(true);
  if (WiFi.status() == WL_CONNECTED) {
    refreshAllSlots();
  }

  gNextPollAtMs = millis() + TP_LINK_POLL_INTERVAL_MS;
}

void loopImpl() {
  processSerialInput();
  maintainWifiConnection();

  const unsigned long nowMs = millis();
  if ((long)(nowMs - gNextPollAtMs) >= 0) {
    pollNextSlot();
    gNextPollAtMs = nowMs + TP_LINK_POLL_INTERVAL_MS;
  }

  delay(5);
}

}  // namespace

void setup() {
  setupImpl();
}

void loop() {
  loopImpl();
}
