#pragma once
#include <stdint.h>
#include <math.h>

namespace MiniLamp {
constexpr uint32_t COMMAND_TIMEOUT_MS = 1000;
constexpr float COMMAND_DEADBAND = 0.25f;
inline float limit(float value) { return fmaxf(-1.0f, fminf(1.0f, value)); }
inline float direction(float value) {
  return fabsf(value) < COMMAND_DEADBAND ? 0.0f : (value > 0 ? 1.0f : -1.0f);
}

// No networking or hardware dependencies: called only by the motor task.
struct DriveSafety {
  float left = 0, right = 0, grouser = 0;
  uint32_t driveAt = 0, grouserAt = 0;
  bool driveActive = false, grouserActive = false;
  void stop() {
    left = right = grouser = 0;
    driveActive = grouserActive = false;
  }
  bool drive(float x, float z, uint32_t receivedAt, uint32_t now) {
    if (!isfinite(x) || !isfinite(z) || fabsf(x) > 1 || fabsf(z) > 1 ||
        uint32_t(now - receivedAt) >= COMMAND_TIMEOUT_MS) { stop(); return false; }
    // Match the original on/off drivetrain: quantize each control before
    // grouser mixing, rather than synthesizing PWM or a speed setting.
    left = direction(limit(z + x)); right = direction(limit(z - x));
    driveAt = receivedAt; driveActive = true;
    return true;
  }
  bool rotate(float value, uint32_t receivedAt, uint32_t now) {
    if (!isfinite(value) || fabsf(value) > 1 ||
        uint32_t(now - receivedAt) >= COMMAND_TIMEOUT_MS) { stop(); return false; }
    grouser = direction(value); grouserAt = receivedAt; grouserActive = true;
    return true;
  }
  void expire(uint32_t now) {
    if (driveActive && uint32_t(now - driveAt) >= COMMAND_TIMEOUT_MS) {
      left = right = 0; driveActive = false;
    }
    if (grouserActive && uint32_t(now - grouserAt) >= COMMAND_TIMEOUT_MS) {
      grouser = 0; grouserActive = false;
    }
  }
  void mixed(float (&values)[4]) const {
    values[0] = limit(left + grouser);
    values[1] = limit(right - grouser);
    values[2] = limit(left - grouser);
    values[3] = limit(right + grouser);
  }
};
}
