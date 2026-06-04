# app/services/ethernet_manager.py
#
# Nexmon/ASUS Management.
# - tcp_config: cấu hình/kết nối TCP tới Nexmon-Collection.
# - csi_queue: hàng đợi dữ liệu CSI thật để CsiService ghi CSV khi session chạy.
#
# Web UI bấm Cấu hình & kết nối TCP ASUS -> backend connect Nexmon-Collection ngay.
# Nexmon có thể gửi CSI liên tục; backend chỉ đưa CSI vào csi_queue khi
# recording_enabled=True, tức là trong lúc START SESSION đang chạy.

from collections import deque
import queue
import threading
import time

from app.adapters.nexmon_tcp_client import NexmonTcpClient


NEXMON_COLLECTION_HOST = "127.0.0.1"
NEXMON_COLLECTION_PORT = 9100

CSI_QUEUE_MAXSIZE = 50000
RATE_WINDOW_US = 1_000_000
RATE_IDLE_RESET_SEC = 1.0


class EthernetManager:
    def __init__(self):
        # Cấu hình/trạng thái TCP, KHÔNG phải queue dữ liệu.
        self.tcp_config = {
            "host": NEXMON_COLLECTION_HOST,
            "port": NEXMON_COLLECTION_PORT,
            "connected": False,
        }

        # Alias cũ để tránh lỗi nếu còn code cũ gọi ethernet_manager.queue.
        self.queue = self.tcp_config

        # Queue dữ liệu CSI thật.
        self.csi_queue = queue.Queue(maxsize=CSI_QUEUE_MAXSIZE)

        self.client: NexmonTcpClient | None = None
        self.client_thread: threading.Thread | None = None
        self.client_lock = threading.Lock()
        self.client_running = False

        # Chỉ khi START SESSION mới cho CSI vào csi_queue để ghi CSV.
        self.recording_enabled = False

        self.devices = {
            "asus1": self._new_device(),
            "asus2": self._new_device(),
            "asus3": self._new_device(),
        }

        self._rate_windows = {
            "asus1": deque(),
            "asus2": deque(),
            "asus3": deque(),
        }

    def _new_device(self):
        return {
            "status": "DISCONNECTED",
            "packet_rate": 0,
            "dropped_packets": 0,
            "last_packet_at": None,       # timestamp trong packet, đơn vị us
            "last_packet_wall_at": None,  # time.monotonic() lúc ghi packet gần nhất
        }

    def _prune_rate_window(self, device_id: str, current_timestamp_us: int | None = None):
        if device_id not in self._rate_windows or current_timestamp_us is None:
            return

        window = self._rate_windows[device_id]
        cutoff = int(current_timestamp_us) - RATE_WINDOW_US

        while window and window[0] < cutoff:
            window.popleft()

        self.devices[device_id]["packet_rate"] = len(window)

    def refresh_all_rates(self):
        """
        Rate chính được tính trong update_packet_stat() bằng timestamp của packet.
        Hàm này chỉ reset rate về 0 nếu quá 1 giây theo đồng hồ máy tính không có packet mới.
        """
        now_wall = time.monotonic()

        for device_id, device in self.devices.items():
            last_wall = device.get("last_packet_wall_at")

            if last_wall is None or now_wall - float(last_wall) > RATE_IDLE_RESET_SEC:
                self._rate_windows[device_id].clear()
                device["packet_rate"] = 0

    def _refresh_device_display_status(self):
        for device in self.devices.values():
            if not self.tcp_config.get("connected"):
                device["status"] = "DISCONNECTED"
            elif device.get("packet_rate", 0) > 0:
                device["status"] = "RECEIVING"
            else:
                device["status"] = "CONNECTED"

    def get_status(self):
        self.refresh_all_rates()
        self._refresh_device_display_status()

        tcp_status = {
            **self.tcp_config,
            "csi_queue_size": self.csi_queue.qsize(),
            "csi_queue_maxsize": self.csi_queue.maxsize,
            "recording_enabled": self.recording_enabled,
        }

        # Trả cả tcp và queue để tương thích ws.py/UI cũ.
        return {
            "tcp": tcp_status,
            "queue": {
                **self.tcp_config,
                "size": self.csi_queue.qsize(),
                "maxsize": self.csi_queue.maxsize,
                "recording_enabled": self.recording_enabled,
            },
            "devices": self.devices,
        }

    def configure_tcp(self, host: str | None = None, port: int | None = None):
        if host is not None:
            self.tcp_config["host"] = host

        if port is not None:
            self.tcp_config["port"] = int(port)

        return {
            "status": "updated",
            "tcp": self.tcp_config,
        }

    def configure_queue(self, host: str | None = None, port: int | None = None):
        # Alias cũ.
        result = self.configure_tcp(host=host, port=port)
        return {
            "status": result["status"],
            "queue": self.tcp_config,
            "tcp": self.tcp_config,
        }

    def connect_collection(self):
        """
        Kết nối TCP tới Nexmon-Collection ngay khi Web bấm Cấu hình & kết nối TCP ASUS.
        Nếu đang có kết nối cũ, đóng kết nối cũ rồi mở kết nối mới theo host/port hiện tại.
        """
        old_thread = self.client_thread

        with self.client_lock:
            self.client_running = False
            if self.client is not None:
                self.client.close()
            self.client = None
            self.tcp_config["connected"] = False

        if old_thread and old_thread.is_alive():
            old_thread.join(timeout=1)

        with self.client_lock:
            host = self.tcp_config["host"]
            port = self.tcp_config["port"]

            self.client = NexmonTcpClient(host=host, port=port)

            try:
                self.client.connect()
            except Exception as e:
                self.client = None
                self.tcp_config["connected"] = False
                raise RuntimeError(f"Chưa kết nối được Nexmon-Collection {host}:{port}: {e}")

            self.tcp_config["connected"] = True
            self.client_running = True

            self.client_thread = threading.Thread(
                target=self._client_receive_loop,
                name="nexmon-collection-receiver",
                daemon=True,
            )
            self.client_thread.start()

        return {
            "status": "connected",
            "message": f"Đã kết nối TCP tới Nexmon-Collection {host}:{port}",
            "tcp": self.tcp_config,
            "queue": self.tcp_config,
        }

    def disconnect_collection(self):
        with self.client_lock:
            self.client_running = False
            if self.client is not None:
                self.client.close()
            self.client = None
            self.tcp_config["connected"] = False

        return {
            "status": "disconnected",
            "message": "Đã ngắt TCP Nexmon-Collection",
            "tcp": self.tcp_config,
            "queue": self.tcp_config,
        }

    def control(self, action: str, host: str | None = None, port: int | None = None):
        if action in {"configure_tcp", "configure_queue"}:
            self.configure_tcp(host=host, port=port)
            return self.connect_collection()

        if action == "disconnect_collection":
            return self.disconnect_collection()

        raise ValueError("action không hợp lệ")

    def set_tcp_connected(self, connected: bool):
        self.tcp_config["connected"] = bool(connected)

    def set_queue_connected(self, connected: bool):
        # Alias cũ.
        self.set_tcp_connected(connected)

    def set_recording_enabled(self, enabled: bool):
        self.recording_enabled = bool(enabled)

    def _client_receive_loop(self):
        """
        Đọc packet từ Nexmon-Collection liên tục.
        - Chưa START SESSION: đọc rồi bỏ qua, không đưa vào csi_queue.
        - Đang START SESSION: đưa packet vào csi_queue để CsiService ghi CSV.
        """
        while self.client_running:
            client = self.client
            if client is None:
                break

            packet = client.read_packet()

            if packet is None:
                if not client.connected:
                    break
                time.sleep(0.001)
                continue

            device_id = packet.get("device_id")
            if device_id not in self.devices:
                continue

            if self.recording_enabled:
                self.put_packet(packet)

        with self.client_lock:
            if self.client is not None and not self.client.connected:
                self.client = None

            self.client_running = False
            self.tcp_config["connected"] = False

    def put_packet(self, packet: dict):
        device_id = packet.get("device_id")

        if device_id not in self.devices:
            return False

        if not self.recording_enabled:
            return False

        try:
            self.csi_queue.put_nowait(packet)
            return True
        except queue.Full:
            self.devices[device_id]["dropped_packets"] = (
                self.devices[device_id].get("dropped_packets", 0) + 1
            )
            return False

    def get_packet(self, timeout: float = 0.1):
        try:
            return self.csi_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def update_packet_stat(self, device_id: str, timestamp_us: int | None = None):
        """
        Rate = số packet có timestamp nằm trong 1 giây gần nhất của chính dữ liệu CSI.
        CsiService gọi hàm này sau khi ghi CSV thành công.
        """
        if device_id not in self.devices:
            return

        if timestamp_us is None:
            return

        try:
            timestamp_us = int(timestamp_us)
        except (TypeError, ValueError):
            return

        device = self.devices[device_id]
        window = self._rate_windows[device_id]

        # Nếu timestamp bị nhảy lùi, reset cửa sổ để tránh đếm sai do dữ liệu không theo thứ tự.
        last_ts = device.get("last_packet_at")
        if last_ts is not None:
            try:
                if timestamp_us < int(last_ts):
                    window.clear()
            except (TypeError, ValueError):
                window.clear()

        window.append(timestamp_us)
        self._prune_rate_window(device_id, timestamp_us)

        device["last_packet_at"] = timestamp_us
        device["last_packet_wall_at"] = time.monotonic()

    def clear_csi_queue(self):
        while not self.csi_queue.empty():
            try:
                self.csi_queue.get_nowait()
            except queue.Empty:
                break

        for window in self._rate_windows.values():
            window.clear()

        for device in self.devices.values():
            device["packet_rate"] = 0
            device["dropped_packets"] = 0
            device["last_packet_at"] = None
            device["last_packet_wall_at"] = None


ethernet_manager = EthernetManager()
