"""
Realtime CSI Client + Mini Management Control
=============================================

Mục đích:
- Không cần mở Management thật.
- Script này tự giả các lệnh JSON giống Management để server ESP mở COM.
- Đồng thời connect vào realtime port để vẽ CSI theo thời gian.

Yêu cầu server:
- Port control/management: 127.0.0.1:9200
- Port realtime viewer:    127.0.0.1:9201
- Giao thức JSON Lines, mỗi message kết thúc bằng \n.

Cài thư viện:
    pip install numpy pyqtgraph PyQt5

Chạy:
    python realtime_csi_client_with_control.py

Cách dùng nhanh:
1. Chạy ESP TCP server trước.
2. Chạy file này.
3. Bấm "Connect Control".
4. Bấm "Get COM" để xem COM server trả về.
5. Điền danh sách thiết bị/COM ở ô "Device connect list".
6. Bấm "Connect Listed Devices".
7. Bấm "Connect Realtime" để xem đồ thị.

Dòng connect có dạng:
    device_id,COM,baudrate

Ví dụ server thật:
    esp1,COM15,115200
    esp2,COM18,115200
    esp3,COM6,115200

Ví dụ fake stub theo MAC:
    00:1A:2B:3C:4D:5E,COM3,115200
    00:1C:C7:9A:01:6A,COM4,115200
    00:1D:2E:3F:40:51,COM5,115200
"""

from __future__ import annotations

import json
import queue
import socket
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Optional

import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtWidgets


# ==============================
# Cấu hình mặc định
# ==============================

CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 9200

REALTIME_HOST = "127.0.0.1"
REALTIME_PORT = 9201

WINDOW_SECONDS = 10
UPDATE_INTERVAL_MS = 50

DEFAULT_SUBCARRIERS = "0,1,2,10,20,30,40,50,63"

DEFAULT_DEVICE_LIST = """# Server thật: device_id,COM,baudrate
esp1,COM15,115200
esp2,COM18,115200
esp3,COM6,115200

# Fake stub nếu cần test bằng MAC thì dùng dạng này:
# 00:1A:2B:3C:4D:5E,COM3,115200
# 00:1C:C7:9A:01:6A,COM4,115200
# 00:1D:2E:3F:40:51,COM5,115200
"""


# Queue từ thread TCP sang GUI thread
realtime_packet_queue: queue.Queue[dict] = queue.Queue(maxsize=20_000)
log_queue: queue.Queue[str] = queue.Queue(maxsize=5_000)


def log(msg: str) -> None:
    text = time.strftime("%H:%M:%S") + "  " + msg
    try:
        log_queue.put_nowait(text)
    except queue.Full:
        pass
    print(text)


# ==============================
# TCP JSON Lines client
# ==============================

class JsonLineTcpClient:
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        on_message: Optional[Callable[[dict], None]] = None,
        reconnect: bool = True,
    ):
        self.name = name
        self.host = host
        self.port = port
        self.on_message = on_message
        self.reconnect = reconnect

        self.sock: Optional[socket.socket] = None
        self.running = False
        self.lock = threading.Lock()
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(
            target=self._run,
            name=f"{self.name}-thread",
            daemon=True,
        )
        self.thread.start()

    def _run(self) -> None:
        while self.running:
            try:
                log(f"[{self.name}] connecting {self.host}:{self.port} ...")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.connect((self.host, self.port))

                with self.lock:
                    self.sock = sock

                log(f"[{self.name}] connected")
                self._receive_loop(sock)

            except OSError as e:
                log(f"[{self.name}] socket error: {e}")
            except Exception as e:
                log(f"[{self.name}] error: {e}")
            finally:
                with self.lock:
                    if self.sock is not None:
                        try:
                            self.sock.close()
                        except Exception:
                            pass
                        self.sock = None

            if not self.reconnect:
                break

            if self.running:
                log(f"[{self.name}] reconnect after 2 seconds")
                time.sleep(2)

    def _receive_loop(self, sock: socket.socket) -> None:
        buffer = b""

        while self.running:
            chunk = sock.recv(65536)
            if not chunk:
                raise ConnectionError("server disconnected")

            buffer += chunk

            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue

                try:
                    msg = json.loads(line.decode("utf-8", errors="replace"))
                except json.JSONDecodeError as e:
                    log(f"[{self.name}] invalid JSON: {e}")
                    continue

                if self.on_message is not None:
                    self.on_message(msg)

    def send_json(self, msg: dict) -> bool:
        data = (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")

        with self.lock:
            sock = self.sock
            if sock is None:
                log(f"[{self.name}] not connected, cannot send: {msg}")
                return False

            try:
                sock.sendall(data)
                return True
            except Exception as e:
                log(f"[{self.name}] send error: {e}")
                try:
                    sock.close()
                except Exception:
                    pass
                self.sock = None
                return False

    def stop(self) -> None:
        self.running = False
        with self.lock:
            if self.sock is not None:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    self.sock.close()
                except Exception:
                    pass
                self.sock = None


# ==============================
# Parse CSI + buffer
# ==============================


def csi_to_amplitude(csi: object) -> Optional[np.ndarray]:
    """
    Hỗ trợ 2 dạng:
    1. [[Q0,I0], [Q1,I1], ...]
    2. [Q0,I0,Q1,I1,...]

    Với amplitude thì thứ tự Q/I hay I/Q không ảnh hưởng:
        amp = sqrt(I^2 + Q^2)
    """
    try:
        arr = np.asarray(csi, dtype=float)
    except Exception:
        return None

    if arr.ndim == 2 and arr.shape[1] >= 2:
        a = arr[:, 0]
        b = arr[:, 1]
        return np.sqrt(a * a + b * b)

    if arr.ndim == 1 and arr.size >= 2 and arr.size % 2 == 0:
        a = arr[0::2]
        b = arr[1::2]
        return np.sqrt(a * a + b * b)

    return None


def parse_subcarrier_selection(text: str, max_subcarriers: int) -> list[int]:
    text = text.strip().lower()

    if not text:
        return []

    if text == "all":
        return list(range(max_subcarriers))

    result: list[int] = []

    for part in text.split(","):
        part = part.strip()
        if not part:
            continue

        if ":" in part:
            nums = [x.strip() for x in part.split(":")]

            # 0:10 nghĩa là 0..10
            if len(nums) == 2:
                start = int(nums[0])
                end = int(nums[1])
                step = 1

            # 0:2:20 nghĩa là 0,2,4,...,20
            elif len(nums) == 3:
                start = int(nums[0])
                step = int(nums[1])
                end = int(nums[2])
                if step <= 0:
                    raise ValueError("step phải > 0")
            else:
                raise ValueError(f"Sai cú pháp subcarrier: {part}")

            for sc in range(start, end + 1, step):
                if 0 <= sc < max_subcarriers and sc not in result:
                    result.append(sc)

        else:
            sc = int(part)
            if 0 <= sc < max_subcarriers and sc not in result:
                result.append(sc)

    return result


@dataclass
class DeviceConnectConfig:
    device_id: str
    com: str
    baudrate: int


def parse_device_connect_list(text: str) -> list[DeviceConnectConfig]:
    configs: list[DeviceConnectConfig] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            raise ValueError(f"Dòng sai định dạng: {raw_line}")

        device_id = parts[0]
        com = parts[1]
        baudrate = int(parts[2]) if len(parts) >= 3 and parts[2] else 115200

        configs.append(DeviceConnectConfig(device_id, com, baudrate))

    return configs


class SourceBuffer:
    def __init__(self, name: str):
        self.name = name

        self.timestamps: deque[int] = deque()
        self.amps: deque[np.ndarray] = deque()

        self.last_timestamp: Optional[int] = None
        self.gap_times: deque[int] = deque()
        self.gaps_ms: deque[float] = deque()

        self.packet_count_by_second: dict[int, int] = defaultdict(int)
        self.max_subcarriers = 0

    def add_packet(self, timestamp_us: int, amp: np.ndarray) -> None:
        self.timestamps.append(timestamp_us)
        self.amps.append(amp)
        self.max_subcarriers = max(self.max_subcarriers, int(amp.size))

        if self.last_timestamp is not None:
            gap_us = timestamp_us - self.last_timestamp
            self.gap_times.append(timestamp_us)
            self.gaps_ms.append(gap_us / 1000.0)

        self.last_timestamp = timestamp_us

        sec_bin = timestamp_us // 1_000_000
        self.packet_count_by_second[sec_bin] += 1

        self.trim_old_data(timestamp_us)

    def trim_old_data(self, current_timestamp_us: int) -> None:
        min_ts = current_timestamp_us - WINDOW_SECONDS * 1_000_000

        while self.timestamps and self.timestamps[0] < min_ts:
            self.timestamps.popleft()
            self.amps.popleft()

        while self.gap_times and self.gap_times[0] < min_ts:
            self.gap_times.popleft()
            self.gaps_ms.popleft()

        min_sec = min_ts // 1_000_000
        for sec in list(self.packet_count_by_second.keys()):
            if sec < min_sec:
                del self.packet_count_by_second[sec]

    def get_time_axis_seconds(self) -> np.ndarray:
        if not self.timestamps:
            return np.array([])
        t0 = self.timestamps[0]
        return np.asarray([(t - t0) / 1_000_000.0 for t in self.timestamps], dtype=float)

    def get_amp_matrix(self) -> Optional[np.ndarray]:
        if not self.amps:
            return None
        try:
            return np.vstack(self.amps)
        except Exception:
            return None

    def get_gap_axis_seconds(self) -> np.ndarray:
        if not self.gap_times:
            return np.array([])
        t0 = self.timestamps[0] if self.timestamps else self.gap_times[0]
        return np.asarray([(t - t0) / 1_000_000.0 for t in self.gap_times], dtype=float)

    def get_packets_per_second_data(self) -> tuple[np.ndarray, np.ndarray]:
        if not self.packet_count_by_second:
            return np.array([]), np.array([])

        secs = sorted(self.packet_count_by_second.keys())
        x0 = secs[0]
        x = np.asarray([s - x0 for s in secs], dtype=float)
        y = np.asarray([self.packet_count_by_second[s] for s in secs], dtype=float)
        return x, y


# ==============================
# GUI
# ==============================

class RealtimeCSIClientWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Realtime CSI Viewer + Mini Management")

        self.control_client: Optional[JsonLineTcpClient] = None
        self.realtime_client: Optional[JsonLineTcpClient] = None

        self.buffers: dict[str, SourceBuffer] = {}
        self.source_widgets: dict[str, dict] = {}
        self.selected_subcarriers = [0, 1, 2, 10, 20, 30, 40, 50, 63]

        self._build_ui()

        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.update_gui)
        self.timer.start(UPDATE_INTERVAL_MS)

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        root = QtWidgets.QVBoxLayout(central)
        self.setCentralWidget(central)

        # ===== Control TCP row =====
        control_box = QtWidgets.QGroupBox("Mini Management Control")
        control_layout = QtWidgets.QGridLayout(control_box)

        self.control_host_input = QtWidgets.QLineEdit(CONTROL_HOST)
        self.control_port_input = QtWidgets.QLineEdit(str(CONTROL_PORT))
        self.realtime_host_input = QtWidgets.QLineEdit(REALTIME_HOST)
        self.realtime_port_input = QtWidgets.QLineEdit(str(REALTIME_PORT))

        self.btn_connect_control = QtWidgets.QPushButton("Connect Control")
        self.btn_get_com = QtWidgets.QPushButton("Get COM")
        self.btn_connect_devices = QtWidgets.QPushButton("Connect Listed Devices")
        self.btn_disconnect_devices = QtWidgets.QPushButton("Disconnect Listed Devices")
        self.btn_connect_realtime = QtWidgets.QPushButton("Connect Realtime")

        self.btn_connect_control.clicked.connect(self.connect_control)
        self.btn_get_com.clicked.connect(self.send_get_com)
        self.btn_connect_devices.clicked.connect(self.send_connect_devices)
        self.btn_disconnect_devices.clicked.connect(self.send_disconnect_devices)
        self.btn_connect_realtime.clicked.connect(self.connect_realtime)

        control_layout.addWidget(QtWidgets.QLabel("Control host"), 0, 0)
        control_layout.addWidget(self.control_host_input, 0, 1)
        control_layout.addWidget(QtWidgets.QLabel("Control port"), 0, 2)
        control_layout.addWidget(self.control_port_input, 0, 3)
        control_layout.addWidget(self.btn_connect_control, 0, 4)
        control_layout.addWidget(self.btn_get_com, 0, 5)

        control_layout.addWidget(QtWidgets.QLabel("Realtime host"), 1, 0)
        control_layout.addWidget(self.realtime_host_input, 1, 1)
        control_layout.addWidget(QtWidgets.QLabel("Realtime port"), 1, 2)
        control_layout.addWidget(self.realtime_port_input, 1, 3)
        control_layout.addWidget(self.btn_connect_realtime, 1, 4)

        self.device_text = QtWidgets.QPlainTextEdit()
        self.device_text.setPlainText(DEFAULT_DEVICE_LIST)
        self.device_text.setMaximumHeight(125)

        control_layout.addWidget(QtWidgets.QLabel("Device connect list"), 2, 0)
        control_layout.addWidget(self.device_text, 2, 1, 1, 5)
        control_layout.addWidget(self.btn_connect_devices, 3, 4)
        control_layout.addWidget(self.btn_disconnect_devices, 3, 5)

        root.addWidget(control_box)

        # ===== Subcarrier row =====
        sub_box = QtWidgets.QGroupBox("Plot Control")
        sub_layout = QtWidgets.QHBoxLayout(sub_box)
        self.subcarrier_input = QtWidgets.QLineEdit(DEFAULT_SUBCARRIERS)
        self.subcarrier_input.setPlaceholderText("Ví dụ: 0,1,2 hoặc 0:10 hoặc 0:2:20 hoặc all")
        self.btn_apply_subcarriers = QtWidgets.QPushButton("Apply Subcarriers")
        self.btn_apply_subcarriers.clicked.connect(self.apply_subcarriers)

        sub_layout.addWidget(QtWidgets.QLabel("Subcarriers:"))
        sub_layout.addWidget(self.subcarrier_input)
        sub_layout.addWidget(self.btn_apply_subcarriers)
        root.addWidget(sub_box)

        # ===== Scroll plot area =====
        self.scroll = QtWidgets.QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.plot_container = QtWidgets.QWidget()
        self.plot_layout = QtWidgets.QGridLayout(self.plot_container)
        self.plot_layout.setColumnStretch(0, 5)
        self.plot_layout.setColumnStretch(1, 2)
        self.plot_layout.setColumnStretch(2, 2)
        self.scroll.setWidget(self.plot_container)
        root.addWidget(self.scroll, stretch=1)

        # ===== Log =====
        self.log_text = QtWidgets.QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(150)
        root.addWidget(self.log_text)

    # ---------- TCP callbacks ----------

    def on_control_message(self, msg: dict) -> None:
        msg_type = msg.get("type")

        # Control port có thể trả status, com_list và cũng có thể trả csi_data.
        # Tránh vẽ trùng dữ liệu, csi_data từ control port chỉ log ngắn, không đưa vào plot.
        if msg_type == "csi_data":
            return

        log(f"[CONTROL RX] {msg}")

    def on_realtime_message(self, msg: dict) -> None:
        if msg.get("type") != "csi_data":
            return

        timestamp = msg.get("timestamp") or msg.get("esp_timestamp_us")
        if timestamp is None:
            timestamp = time.time_ns() // 1_000

        csi = msg.get("csi")
        amp = csi_to_amplitude(csi)
        if amp is None:
            return

        device_id = str(msg.get("device_id") or msg.get("source") or "UNKNOWN")

        # Nếu packet có antenna/rx/stream thì tách riêng nguồn.
        antenna = msg.get("antenna", None)
        if antenna is not None:
            source_name = f"{device_id}_ANT{antenna}"
        else:
            source_name = device_id

        packet = {
            "source": source_name,
            "timestamp": int(timestamp),
            "amp": amp,
            "seq": msg.get("seq"),
        }

        try:
            realtime_packet_queue.put_nowait(packet)
        except queue.Full:
            # Nếu GUI không kịp thì bỏ bớt packet realtime, không để trễ.
            try:
                realtime_packet_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                realtime_packet_queue.put_nowait(packet)
            except queue.Full:
                pass

    # ---------- Button handlers ----------

    def connect_control(self) -> None:
        host = self.control_host_input.text().strip()
        port = int(self.control_port_input.text().strip())

        if self.control_client is not None:
            self.control_client.stop()

        self.control_client = JsonLineTcpClient(
            name="CONTROL",
            host=host,
            port=port,
            on_message=self.on_control_message,
            reconnect=True,
        )
        self.control_client.start()

    def connect_realtime(self) -> None:
        host = self.realtime_host_input.text().strip()
        port = int(self.realtime_port_input.text().strip())

        if self.realtime_client is not None:
            self.realtime_client.stop()

        self.realtime_client = JsonLineTcpClient(
            name="REALTIME",
            host=host,
            port=port,
            on_message=self.on_realtime_message,
            reconnect=True,
        )
        self.realtime_client.start()

    def send_get_com(self) -> None:
        self.send_control_json({"type": "get_com_ports"})

    def send_connect_devices(self) -> None:
        try:
            configs = parse_device_connect_list(self.device_text.toPlainText())
        except Exception as e:
            log(f"[CONTROL] Lỗi parse danh sách device: {e}")
            return

        for cfg in configs:
            msg = {
                "type": "uart_control",
                "action": "connect",
                "device_id": cfg.device_id,
                "com": cfg.com,
                "baudrate": cfg.baudrate,
            }
            self.send_control_json(msg)
            time.sleep(0.02)

    def send_disconnect_devices(self) -> None:
        try:
            configs = parse_device_connect_list(self.device_text.toPlainText())
        except Exception as e:
            log(f"[CONTROL] Lỗi parse danh sách device: {e}")
            return

        for cfg in configs:
            msg = {
                "type": "uart_control",
                "action": "disconnect",
                "device_id": cfg.device_id,
            }
            self.send_control_json(msg)
            time.sleep(0.02)

    def send_control_json(self, msg: dict) -> None:
        if self.control_client is None:
            log("[CONTROL] Chưa connect control port")
            return
        ok = self.control_client.send_json(msg)
        if ok:
            log(f"[CONTROL TX] {msg}")

    def apply_subcarriers(self) -> None:
        max_sc = self.get_max_subcarriers()
        try:
            selected = parse_subcarrier_selection(self.subcarrier_input.text(), max_sc)
        except Exception as e:
            log(f"[PLOT] Lỗi chọn subcarrier: {e}")
            return

        if not selected:
            log("[PLOT] Không có subcarrier hợp lệ")
            return

        self.selected_subcarriers = selected
        log(f"[PLOT] Đang vẽ subcarrier: {selected}")

        for source_name in list(self.source_widgets.keys()):
            self.rebuild_subcarrier_curves(source_name)

    # ---------- Plot management ----------

    def get_max_subcarriers(self) -> int:
        max_sc = 64
        for buf in self.buffers.values():
            if buf.max_subcarriers > 0:
                max_sc = max(max_sc, buf.max_subcarriers)
        return max_sc

    def ensure_source_widgets(self, source_name: str) -> None:
        if source_name in self.source_widgets:
            return

        row = len(self.source_widgets) * 2

        label = QtWidgets.QLabel(f"<b>{source_name}</b>")
        self.plot_layout.addWidget(label, row, 0, 1, 3)

        sub_plot = pg.PlotWidget(title=f"{source_name} - Selected Subcarriers")
        pps_plot = pg.PlotWidget(title=f"{source_name} - Packets/s")
        gap_plot = pg.PlotWidget(title=f"{source_name} - Timestamp Gap")

        sub_plot.setLabel("left", "Amplitude")
        sub_plot.setLabel("bottom", "Time", units="s")
        sub_plot.showGrid(x=True, y=True)

        pps_plot.setLabel("left", "Packets/s")
        pps_plot.setLabel("bottom", "Second")
        pps_plot.showGrid(x=True, y=True)

        gap_plot.setLabel("left", "Gap", units="ms")
        gap_plot.setLabel("bottom", "Time", units="s")
        gap_plot.showGrid(x=True, y=True)

        self.plot_layout.addWidget(sub_plot, row + 1, 0)
        self.plot_layout.addWidget(pps_plot, row + 1, 1)
        self.plot_layout.addWidget(gap_plot, row + 1, 2)

        pps_curve = pps_plot.plot(symbol="o")
        gap_curve = gap_plot.plot()

        self.source_widgets[source_name] = {
            "label": label,
            "sub_plot": sub_plot,
            "pps_plot": pps_plot,
            "gap_plot": gap_plot,
            "pps_curve": pps_curve,
            "gap_curve": gap_curve,
            "sub_curves": {},
        }

        self.rebuild_subcarrier_curves(source_name)
        log(f"[PLOT] Thêm nguồn mới: {source_name}")

    def rebuild_subcarrier_curves(self, source_name: str) -> None:
        widgets = self.source_widgets[source_name]
        sub_plot: pg.PlotWidget = widgets["sub_plot"]
        sub_plot.clear()
        sub_plot.setTitle(f"{source_name} - Selected Subcarriers {self.selected_subcarriers}")
        sub_plot.setLabel("left", "Amplitude")
        sub_plot.setLabel("bottom", "Time", units="s")
        sub_plot.showGrid(x=True, y=True)

        sub_curves = {}
        total = max(1, len(self.selected_subcarriers))
        for idx, sc in enumerate(self.selected_subcarriers):
            curve = sub_plot.plot(pen=pg.intColor(idx, hues=total))
            sub_curves[sc] = curve
        widgets["sub_curves"] = sub_curves

    def consume_realtime_queue(self) -> None:
        max_per_tick = 5000
        count = 0

        while count < max_per_tick:
            try:
                packet = realtime_packet_queue.get_nowait()
            except queue.Empty:
                break

            source = packet["source"]
            timestamp = int(packet["timestamp"])
            amp = packet["amp"]

            if source not in self.buffers:
                self.buffers[source] = SourceBuffer(source)
                self.ensure_source_widgets(source)

            self.buffers[source].add_packet(timestamp, amp)
            count += 1

        if count == max_per_tick:
            log("[PLOT] Warning: GUI đang nhận quá nhiều packet mỗi tick")

    def update_gui(self) -> None:
        self.consume_realtime_queue()
        self.flush_logs()

        for source_name, buf in self.buffers.items():
            if source_name not in self.source_widgets:
                self.ensure_source_widgets(source_name)

            widgets = self.source_widgets[source_name]
            t = buf.get_time_axis_seconds()
            amp_matrix = buf.get_amp_matrix()

            if amp_matrix is not None and len(t) > 0:
                num_sc = amp_matrix.shape[1]
                for sc, curve in widgets["sub_curves"].items():
                    if sc < num_sc:
                        curve.setData(t, amp_matrix[:, sc])
                    else:
                        curve.setData([], [])

            x_pps, y_pps = buf.get_packets_per_second_data()
            widgets["pps_curve"].setData(x_pps, y_pps)

            x_gap = buf.get_gap_axis_seconds()
            y_gap = np.asarray(buf.gaps_ms, dtype=float)
            widgets["gap_curve"].setData(x_gap, y_gap)

    def flush_logs(self) -> None:
        updated = False
        while True:
            try:
                msg = log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_text.appendPlainText(msg)
            updated = True

        if updated:
            scrollbar = self.log_text.verticalScrollBar()
            scrollbar.setValue(scrollbar.maximum())

    def closeEvent(self, event) -> None:  # noqa: N802 - Qt method name
        if self.control_client is not None:
            self.control_client.stop()
        if self.realtime_client is not None:
            self.realtime_client.stop()
        event.accept()


def main() -> None:
    app = QtWidgets.QApplication([])
    pg.setConfigOptions(antialias=False)

    win = RealtimeCSIClientWindow()
    win.resize(1700, 950)
    win.show()

    app.exec_()


if __name__ == "__main__":
    main()
