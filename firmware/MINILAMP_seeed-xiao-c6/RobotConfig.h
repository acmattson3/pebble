#pragma once
#include <Arduino.h>
constexpr char FIRMWARE_ID[] = "minilamp-c6-onoff-wss-v2";
constexpr bool USE_EXTERNAL_ANTENNA = true;
constexpr uint8_t BATTERY_PIN = D0;
constexpr float BATTERY_DIVIDER_RATIO = 2.0f;
// Preserve the previous actual definitions, correcting its stale comments.
struct MotorConfig { uint8_t forward, reverse; };
constexpr MotorConfig MOTORS[4] = {
  {D1, D2},  // front left
  {D4, D3},  // front right
  {D5, D6},  // rear left
  {D8, D7},  // rear right
};
// Each entry indexes MOTORS; changing these swaps physical outputs.
constexpr uint8_t MOTOR_OUTPUT_ORDER[4] = {0, 1, 2, 3};
constexpr bool MOTOR_INVERT[4] = {false, false, false, false};
