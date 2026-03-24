#define PIN_LED_B   D0
#define PIN_LED_G   D1
#define PIN_LED_R   D2

#define PIN_TOUCH_0 D3
#define PIN_TOUCH_1 D4
#define PIN_TOUCH_2 D5

#define PIN_CHG     D6

#define PIN_L_FWD   D7
#define PIN_L_BWD   D8
#define PIN_R_FWD   D9
#define PIN_R_BWD   D10

#define L_ADJUST    0.93
#define R_ADJUST    1.00
#define B_LIMIT     0.7   // The limit on backwards speed
// Recent-forward reverse blocking:
// If command was forward-trending in the last 340ms, block backward-trending
// commands (within a cone), but allow turning behavior.
#define REV_BLOCK_WINDOW_MS  340UL
#define TREND_CONE_TAN       1.00f   // tan(45deg)
#define TREND_MIN_LINEAR     0.10f

enum LedMode { LED_OFF, LED_STATIC, LED_FADE };

struct {
  LedMode mode = LED_OFF;
  uint8_t targetB = 0, targetG = 0, targetR = 0;
  uint8_t staticB = 0, staticG = 0, staticR = 0;
  unsigned long startMs = 0;
  unsigned long periodMs = 1000;
} g_led;

static inline uint8_t clamp8(int v) {
  if (v < 0) return 0;
  if (v > 255) return 255;
  return (uint8_t)v;
}

static inline float clampf(float v, float lo, float hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

void setLedRawBGR(uint8_t b, uint8_t g, uint8_t r) {
  analogWrite(PIN_LED_B, b);
  analogWrite(PIN_LED_G, g);
  analogWrite(PIN_LED_R, r);
}

void setLedFloatBGR(float b, float g, float r) {
  uint8_t B = clamp8((int)(clampf(b, 0.0f, 1.0f) * 255.0f + 0.5f));
  uint8_t G = clamp8((int)(clampf(g, 0.0f, 1.0f) * 255.0f + 0.5f));
  uint8_t R = clamp8((int)(clampf(r, 0.0f, 1.0f) * 255.0f + 0.5f));
  setLedRawBGR(B, G, R);
}

void driveOneMotor(float val, uint8_t pinFwd, uint8_t pinBwd) {
  val = clampf(val, -1.0f, 1.0f);
  if (val >= 0.0f) {
    uint8_t pwm = clamp8((int)(val * 255.0f + 0.5f));
    analogWrite(pinFwd, pwm);
    analogWrite(pinBwd, 0);
  } else {
    uint8_t pwm = clamp8((int)((-val) * B_LIMIT * 255.0f + 0.5f));
    analogWrite(pinFwd, 0);
    analogWrite(pinBwd, pwm);
  }
}

void driveMotors(float left, float right) {
  driveOneMotor(L_ADJUST*left,  PIN_L_FWD, PIN_L_BWD);
  driveOneMotor(R_ADJUST*right, PIN_R_FWD, PIN_R_BWD);
}

static float triangle01(unsigned long elapsed, unsigned long period) {
  if (period == 0) return 0.0f;
  unsigned long t = elapsed % period;
  float x = (float)t / (float)period;
  float y = 1.0f - fabsf(2.0f * x - 1.0f);
  return clampf(y, 0.0f, 1.0f);
}

void updateLedFade() {
  if (g_led.mode != LED_FADE) return;
  unsigned long now = millis();
  float k = triangle01(now - g_led.startMs, g_led.periodMs);
  uint8_t B = (uint8_t)(g_led.targetB * k + 0.5f);
  uint8_t G = (uint8_t)(g_led.targetG * k + 0.5f);
  uint8_t R = (uint8_t)(g_led.targetR * k + 0.5f);
  setLedRawBGR(B, G, R);
}

// --------- idle handling state ----------
unsigned long lastMotorCmdMs = 0;
unsigned long lastLedCmdMs   = 0;
bool motorsTimeoutApplied    = false;
bool ledIsDefault            = false;
bool sawForwardTrend         = false;
unsigned long lastForwardTrendMs = 0;

// --------- telemetry state -------------
unsigned long lastTelemetryMs = 0;  // last time we printed T line
const unsigned long TELEMETRY_PERIOD_MS = 500;

void setDefaultLed() {
  g_led.mode = LED_FADE;
  g_led.targetB = 0;
  g_led.targetG = 0;
  g_led.targetR = 255;         // flashing red
  g_led.periodMs = 2000;       // 2s full in+out period
  g_led.startMs = millis();
  ledIsDefault = true;
}

void setup() {
  Serial.begin(115200);
  Serial.println("Starting setup...");

  pinMode(PIN_LED_B, OUTPUT);
  digitalWrite(PIN_LED_B, 0);
  pinMode(PIN_LED_G, OUTPUT);
  digitalWrite(PIN_LED_G, 0);
  pinMode(PIN_LED_R, OUTPUT);
  digitalWrite(PIN_LED_R, 0);

  pinMode(PIN_TOUCH_0, INPUT);
  pinMode(PIN_TOUCH_1, INPUT);
  pinMode(PIN_TOUCH_2, INPUT);

  pinMode(PIN_CHG, INPUT);  // high means charging

  pinMode(PIN_L_FWD, OUTPUT);
  digitalWrite(PIN_L_FWD, 0);
  pinMode(PIN_L_BWD, OUTPUT);
  digitalWrite(PIN_L_BWD, 0);
  pinMode(PIN_R_FWD, OUTPUT);
  digitalWrite(PIN_R_FWD, 0);
  pinMode(PIN_R_BWD, OUTPUT);
  digitalWrite(PIN_R_BWD, 0);

  setDefaultLed();
  const unsigned long now = millis();
  lastMotorCmdMs = now;
  lastLedCmdMs = now;
  lastTelemetryMs = now;

  Serial.println("Done!");
}

String buffer;

static inline void emitTelemetry() {
  // Raw ADC units. SAMD21 defaults to 10-bit (typically 0..1023).
  int t0 = analogRead(PIN_TOUCH_0);
  int t1 = analogRead(PIN_TOUCH_1);
  int t2 = analogRead(PIN_TOUCH_2);
  int chg = digitalRead(PIN_CHG) ? 1 : 0;

  // Single line, easy to parse
  Serial.print("T ");
  Serial.print("t0="); Serial.print(t0);
  Serial.print(" t1="); Serial.print(t1);
  Serial.print(" t2="); Serial.print(t2);
  Serial.print(" chg="); Serial.println(chg);
}

static inline void commandToLinearTurn(float left, float right, float &linear, float &turn) {
  linear = 0.5f * (left + right);
  turn = 0.5f * (right - left);
}

static inline bool isForwardTrending(float left, float right) {
  float linear, turn;
  commandToLinearTurn(left, right, linear, turn);
  if (linear <= TREND_MIN_LINEAR) return false;
  return fabsf(turn) <= (linear * TREND_CONE_TAN);
}

static inline bool isBackwardTrending(float left, float right) {
  float linear, turn;
  commandToLinearTurn(left, right, linear, turn);
  if (linear >= -TREND_MIN_LINEAR) return false;
  return fabsf(turn) <= ((-linear) * TREND_CONE_TAN);
}

void applyDriveCommand(float left, float right) {
  left = clampf(left, -1.0f, 1.0f);
  right = clampf(right, -1.0f, 1.0f);
  unsigned long now = millis();

  if (isForwardTrending(left, right)) {
    sawForwardTrend = true;
    lastForwardTrendMs = now;
  }

  bool reverseBlocked = sawForwardTrend && ((unsigned long)(now - lastForwardTrendMs) < REV_BLOCK_WINDOW_MS);
  if (reverseBlocked && isBackwardTrending(left, right)) {
    // Preserve turning intent, but strip reverse linear component.
    float linear, turn;
    commandToLinearTurn(left, right, linear, turn);
    if (linear < 0.0f) linear = 0.0f;

    float outLeft = clampf(linear - turn, -1.0f, 1.0f);
    float outRight = clampf(linear + turn, -1.0f, 1.0f);
    driveMotors(outLeft, outRight);
    return;
  }

  driveMotors(left, right);
}

void loop() {
  while (Serial.available()>0) {
    int c=Serial.read();
    if (c==-1) break;
    if (c=='\n') {
      handleCommand(buffer);
      buffer="";
    } else if (c!='\r') {
      buffer+=(char)c;
    }
  }

  updateLedFade();

  // idle safety: stop motors if no M in 5s
  unsigned long now = millis();
  if (!motorsTimeoutApplied && (now - lastMotorCmdMs > 5000UL)) {
    driveMotors(0.0f, 0.0f);
    motorsTimeoutApplied = true;
    sawForwardTrend = false;
    lastForwardTrendMs = 0;
  }

  // periodic telemetry every 0.5s
  if (now - lastTelemetryMs >= TELEMETRY_PERIOD_MS) {
    lastTelemetryMs = now;
    emitTelemetry();
  }
}

void handleCommand(String &cmd) {
  cmd.trim();
  if (cmd.length() == 0) return;

  String tokens[6];
  int ntok = 0;
  int start = 0;
  for (int i = 0; i <= cmd.length(); ++i) {
    if (i == cmd.length() || cmd[i] == ' ') {
      if (i > start) {
        if (ntok < 6) tokens[ntok++] = cmd.substring(start, i);
      }
      start = i + 1;
    }
  }
  if (ntok == 0) return;

  char type = tokens[0][0];
  if (type >= 'a' && type <= 'z') type = type - 'a' + 'A';

  switch (type) {
    case 'M': {
      if (ntok < 3) { Serial.println("ERR M needs 2 floats"); break; }
      float l = tokens[1].toFloat();
      float r = tokens[2].toFloat();
      applyDriveCommand(l, r);
      lastMotorCmdMs = millis();
      motorsTimeoutApplied = false;
      Serial.println("OK M");
    } break;

    case 'L': {
      if (ntok < 4) { Serial.println("ERR L needs 3 floats (B G R)"); break; }
      float b = clampf(tokens[1].toFloat(), 0.0f, 1.0f);
      float g = clampf(tokens[2].toFloat(), 0.0f, 1.0f);
      float r = clampf(tokens[3].toFloat(), 0.0f, 1.0f);
      g_led.mode = LED_STATIC;
      g_led.staticB = clamp8((int)(b * 255.0f + 0.5f));
      g_led.staticG = clamp8((int)(g * 255.0f + 0.5f));
      g_led.staticR = clamp8((int)(r * 255.0f + 0.5f));
      setLedRawBGR(g_led.staticB, g_led.staticG, g_led.staticR);
      lastLedCmdMs = millis();
      ledIsDefault = false;
      Serial.println("OK L");
    } break;

    case 'F': {
      if (ntok < 5) { Serial.println("ERR F needs 4 floats (B G R Tsec)"); break; }
      float b = clampf(tokens[1].toFloat(), 0.0f, 1.0f);
      float g = clampf(tokens[2].toFloat(), 0.0f, 1.0f);
      float r = clampf(tokens[3].toFloat(), 0.0f, 1.0f);
      float t = tokens[4].toFloat();
      unsigned long period = (unsigned long)(t * 1000.0f);
      if (period == 0) period = 1;

      g_led.mode = LED_FADE;
      g_led.targetB = clamp8((int)(b * 255.0f + 0.5f));
      g_led.targetG = clamp8((int)(g * 255.0f + 0.5f));
      g_led.targetR = clamp8((int)(r * 255.0f + 0.5f));
      g_led.periodMs = period;
      g_led.startMs = millis();
      lastLedCmdMs = millis();
      ledIsDefault = false;
      Serial.println("OK F");
    } break;

    default:
      Serial.println("ERR unknown cmd");
      break;
  }
}
