import argparse
import base64
import json
import logging
import queue
import signal
import ssl
import sys
import threading
import time
import zlib
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import cv2
import numpy as np
import paho.mqtt.client as mqtt

from camera_config import CameraProfileError, load_profile


FrameInfo = Tuple[np.ndarray, Dict[str, Any]]


def _normalize_tls_config(value: Any) -> Dict[str, Any]:
    if isinstance(value, bool):
        return {"enabled": value}
    if isinstance(value, dict):
        return {
            "enabled": bool(value.get("enabled", False)),
            "ca_cert": value.get("ca_cert") or value.get("ca_certs"),
            "client_cert": value.get("client_cert"),
            "client_key": value.get("client_key"),
            "insecure": bool(value.get("insecure", False)),
            "ciphers": value.get("ciphers"),
        }
    return {"enabled": False}


def _resolve_tls_paths(tls_cfg: Dict[str, Any]) -> Dict[str, Any]:
    def _resolve(path_value: Any) -> Optional[str]:
        if not path_value:
            return None
        raw = str(path_value).strip()
        if not raw:
            return None
        path = Path(raw)
        if not path.is_absolute():
            path = (Path(__file__).parent / path).resolve()
        if not path.exists():
            raise SystemExit(f"TLS file not found: {path}")
        return str(path)

    if not tls_cfg.get("enabled"):
        return tls_cfg
    resolved = dict(tls_cfg)
    resolved["ca_cert"] = _resolve(tls_cfg.get("ca_cert"))
    resolved["client_cert"] = _resolve(tls_cfg.get("client_cert"))
    resolved["client_key"] = _resolve(tls_cfg.get("client_key"))
    return resolved


def _apply_tls(client: mqtt.Client, mqtt_cfg: Dict[str, Any]) -> None:
    tls_cfg = _normalize_tls_config(mqtt_cfg.get("tls"))
    if not tls_cfg.get("enabled"):
        return
    tls_cfg = _resolve_tls_paths(tls_cfg)
    ca_cert = tls_cfg.get("ca_cert")
    client_cert = tls_cfg.get("client_cert")
    client_key = tls_cfg.get("client_key")
    ciphers = tls_cfg.get("ciphers")
    client.tls_set(
        ca_certs=ca_cert,
        certfile=client_cert,
        keyfile=client_key,
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
        ciphers=ciphers or None,
    )
    if tls_cfg.get("insecure"):
        client.tls_insecure_set(True)


class VideoStreamReceiver:
    """Subscribe to the publisher stream and render frames for quick testing."""

    def __init__(self, profile_name: str, config_path: Optional[str], window_title: Optional[str] = None):
        self.profile_name = profile_name
        self.config_path = config_path
        self.window_title = window_title or f"Video Receiver ({profile_name})"
        self._client: Optional[mqtt.Client] = None
        self._topic: Optional[str] = None
        self._frame_queue: "queue.Queue[FrameInfo]" = queue.Queue(maxsize=3)
        self._last_frame: Optional[np.ndarray] = None
        self._stop_event = threading.Event()

    def connect(self) -> None:
        try:
            profile = load_profile(self.profile_name, self.config_path)
        except CameraProfileError as exc:
            raise SystemExit(str(exc)) from exc

        mqtt_cfg = profile["mqtt"]
        broker = mqtt_cfg.get("broker")
        port = int(mqtt_cfg.get("port", 1883))
        keepalive = int(mqtt_cfg.get("keepalive", 60))
        username = mqtt_cfg.get("username")
        password = mqtt_cfg.get("password")
        self._topic = mqtt_cfg["topic"]

        client = mqtt.Client()
        if username:
            client.username_pw_set(username, password)
        _apply_tls(client, mqtt_cfg)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect

        logging.info("Connecting to MQTT broker %s:%s...", broker, port)
        client.connect(broker, port, keepalive)
        client.loop_start()
        self._client = client

    def run(self) -> None:
        if not self._client:
            self.connect()

        cv2.namedWindow(self.window_title, cv2.WINDOW_AUTOSIZE)
        logging.info("Press 'q' or Ctrl+C to exit the viewer.")

        try:
            while not self._stop_event.is_set():
                try:
                    frame, metadata = self._frame_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                annotated = frame.copy()
                self._draw_overlay(annotated, metadata)
                cv2.imshow(self.window_title, annotated)

                key = cv2.waitKey(1) & 0xFF
                if key == ord("q"):
                    logging.info("Exit requested via keyboard.")
                    break
        except KeyboardInterrupt:
            logging.info("Exit requested via Ctrl+C.")
        finally:
            self.close()

    def close(self) -> None:
        self._stop_event.set()
        if self._client:
            self._client.loop_stop()
            try:
                self._client.disconnect()
            except Exception:
                logging.debug("MQTT disconnect failed", exc_info=True)
            self._client = None
        cv2.destroyAllWindows()

    def _draw_overlay(self, frame: np.ndarray, metadata: Dict[str, Any]) -> None:
        text = (
            f"id={metadata.get('id')} "
            f"{'KEY' if metadata.get('keyframe') else 'delta'} "
            f"Δt={metadata.get('latency_ms', 'n/a')}ms"
        )
        cv2.putText(
            frame,
            text,
            (10, 20),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (0, 255, 0),
            1,
            cv2.LINE_AA,
        )

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: Dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("MQTT connection failed with result code %s", rc)
            return
        if not self._topic:
            logging.error("No topic configured; cannot subscribe.")
            return
        logging.info("MQTT connected. Subscribing to %s", self._topic)
        client.subscribe(self._topic)

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != 0:
            logging.warning("Unexpected MQTT disconnect (rc=%s).", rc)

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            packet = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            logging.warning("Received non-JSON payload; discarding.")
            return

        try:
            frame = self._decode_frame(packet)
        except ValueError as exc:
            logging.warning("Failed to decode frame: %s", exc)
            return

        metadata = {
            "id": packet.get("id"),
            "keyframe": bool(packet.get("keyframe")),
            "timestamp": packet.get("timestamp"),
        }
        if metadata["timestamp"] is not None:
            latency_ms = int((time.time() - float(metadata["timestamp"])) * 1000)
            metadata["latency_ms"] = latency_ms

        # Drop the oldest frame if we are falling behind to keep the viewer responsive.
        if self._frame_queue.full():
            try:
                self._frame_queue.get_nowait()
            except queue.Empty:
                pass
        self._frame_queue.put((frame, metadata))

    def _decode_frame(self, packet: Dict[str, Any]) -> np.ndarray:
        encoded = packet.get("data")
        if not encoded:
            raise ValueError("Missing frame data.")

        try:
            compressed = base64.b64decode(encoded)
            jpeg_bytes = zlib.decompress(compressed)
        except (base64.binascii.Error, zlib.error) as exc:
            raise ValueError(f"Compressed frame payload invalid: {exc}") from exc

        frame_array = np.frombuffer(jpeg_bytes, dtype=np.uint8)
        frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
        if frame is None:
            raise ValueError("cv2.imdecode returned None.")

        is_keyframe = bool(packet.get("keyframe"))
        if is_keyframe or self._last_frame is None:
            self._last_frame = frame
            return frame

        delta = frame.astype(np.int16) - 128
        base_frame = self._last_frame.astype(np.int16)
        reconstructed = np.clip(base_frame + delta, 0, 255).astype(np.uint8)
        self._last_frame = reconstructed
        return reconstructed


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test MQTT video reception and display the stream.")
    parser.add_argument("--profile", default="default", help="Camera profile name to load (default: default).")
    parser.add_argument("--config", help="Path to unified runtime config JSON.")
    parser.add_argument("--window-title", help="Custom window title for the OpenCV preview window.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO).")
    return parser.parse_args(argv)


def _configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)
    _configure_logging(args.log_level)

    receiver = VideoStreamReceiver(args.profile, args.config, args.window_title)

    def _handle_signal(_signum: int, _frame: Any) -> None:
        logging.info("Signal received; shutting down.")
        receiver.close()

    signal.signal(signal.SIGTERM, _handle_signal)
    receiver.run()


if __name__ == "__main__":
    main(sys.argv[1:])
