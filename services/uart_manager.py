# app/services/uart_manager.py
#
# ESP/UART Management.
#
# Luồng đúng:
# 1. Management kết nối TCP tới ESP32-Collection bằng EspTcpClient.
# 2. ESP32-Collection gửi danh sách COM lên Management.
# 3. Web gọi GET /com để lấy danh sách COM từ Management.
# 4. Web POST /com/control để gửi port + baudrate xuống Management.
# 5. Management gửi lệnh uart_control qua TCP xuống ESP32-Collection.
#
# Lưu ý:
# - Không fake COM trong Management.
# - FAKE_COM_PORTS chỉ nằm ở collection_stub/esp32_collection_stub.py.
# - Baudrate do Web gửi xuống; backend chỉ chuyển tiếp xuống Collection.

from collections import deque
import queue
import threading
import time

from app.core.time_utils import unix_now_us

from app.adapters.esp_tcp_client import EspTcpClient


# Queue lớn hơn để giảm nguy cơ rớt packet khi CSI tốc độ cao.
# Nếu queue đầy, packet mới sẽ bị drop và đếm trong devices[device_id]["dropped_packets"].
CSI_QUEUE_MAXSIZE = 50000

# Rate tính theo timestamp trong packet, đơn vị micro giây.
RATE_WINDOW_US = 1_000_000
# Nếu hơn 1 giây theo đồng hồ máy tính không có packet mới thì reset rate về 0.
RATE_IDLE_RESET_SEC = 1.0


class UartManager:
    def __init__(self):
        self.tcp_state = {
            "connected": False,
        }

        # Chỉ khi START SESSION mới cho CSI vào csi_queue để ghi CSV.
        self.recording_enabled = False

        # Danh sách COM ban đầu rỗng. Chỉ cập nhật khi ESP32-Collection gửi com_list.
        self.available_ports: list[str] = []
        self.com_source = None
        self.com_updated_at = None

        # TCP client dùng chung để nhận com_list/status/csi_data từ ESP32-Collection.
        self.client: EspTcpClient | None = None
        self.client_thread: threading.Thread | None = None
        self.client_lock = threading.Lock()
        self.client_running = False

        self.csi_queue = queue.Queue(maxsize=CSI_QUEUE_MAXSIZE)

        self.devices = {
            "esp1": self._new_device(),
            "esp2": self._new_device(),
            "esp3": self._new_device(),
        }

        # Lưu timestamp các packet đã được lấy khỏi queue trong 1 giây gần nhất.
        self._rate_windows = {
            "esp1": deque(),
            "esp2": deque(),
            "esp3": deque(),
        }

    def _new_device(self):
        return {
            "connected": False,
            "status": "DISCONNECTED",
            "com": None,
            "baudrate": None,
            "packet_rate": 0,
            "dropped_packets": 0,
            "last_packet_at": None,       # timestamp trong packet, đơn vị us
            "last_packet_wall_at": None,  # time.monotonic() lúc ghi packet gần nhất
            "last_error": None,
        }

    def _calculate_packet_rate(self, window) -> int:
        """
        Tính tốc độ gói theo Hz từ timestamp trong packet.

        Không dùng trực tiếp len(window), vì với dữ liệu 200 Hz:
        t=0, 5ms, ..., 1000ms có thể thành 201 điểm timestamp
        nếu lấy cả hai biên của cửa sổ 1 giây.

        Rate đúng được tính theo số khoảng giữa các packet:
        rate = (N - 1) / duration_sec
        """
        n = len(window)
        if n <= 1:
            return n

        try:
            duration_us = int(window[-1]) - int(window[0])
        except Exception:
            return n

        if duration_us <= 0:
            return n

        return max(0, int(round((n - 1) * 1_000_000 / duration_us)))

    def _prune_rate_window(self, device_id: str, current_timestamp_us: int | None = None):
        """
        Giữ lại các timestamp nằm trong cửa sổ 1 giây gần nhất của chính dữ liệu CSI.
        current_timestamp_us là timestamp của packet mới nhất, đơn vị micro giây.
        """
        if device_id not in self._rate_windows or current_timestamp_us is None:
            return

        window = self._rate_windows[device_id]
        cutoff = int(current_timestamp_us) - RATE_WINDOW_US

        # Dùng <= để cửa sổ là (t-1s, t], tránh trường hợp 200 Hz bị đếm 201 gói
        # khi packet nằm đúng tại hai biên 0.000s và 1.000s.
        while window and window[0] <= cutoff:
            window.popleft()

        self.devices[device_id]["packet_rate"] = self._calculate_packet_rate(window)

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
        """
        Cập nhật status hiển thị cho Web UI.

        - DISCONNECTED: chưa mở COM hoặc TCP Collection đã ngắt
        - CONNECTING: đã gửi lệnh connect nhưng chưa có ack từ Collection
        - CONNECTED: Collection đã mở COM nhưng chưa có packet trong 1 giây gần nhất
        - RECEIVING: đang có packet được ghi ra CSV trong 1 giây gần nhất
        - ERROR: Collection báo lỗi mở/ngắt COM
        """
        for device_id, device in self.devices.items():
            current = str(device.get("status") or "DISCONNECTED").upper()

            if current == "ERROR":
                continue

            if not self.tcp_state.get("connected") or not device.get("connected"):
                if current != "CONNECTING":
                    device["status"] = "DISCONNECTED"
                continue

            if device.get("packet_rate", 0) > 0:
                device["status"] = "RECEIVING"
            else:
                device["status"] = "CONNECTED"

    def get_status(self):
        self.refresh_all_rates()
        self._refresh_device_display_status()

        return {
            "collection_connected": self.tcp_state["connected"],
            "ports": list(self.available_ports),
            "available_ports": list(self.available_ports),
            "com_source": self.com_source,
            "com_updated_at": self.com_updated_at,
            "tcp": {
                **self.tcp_state,
                "csi_queue_size": self.csi_queue.qsize(),
                "csi_queue_maxsize": self.csi_queue.maxsize,
                "recording_enabled": self.recording_enabled,
            },
            "uart": self.devices,
            "devices": self.devices,
        }

    def ensure_collection_connected(self):
        """
        Kết nối TCP tới ESP32-Collection nếu chưa kết nối.

        Hàm này không fake COM. Nếu Collection chưa chạy, hàm sẽ báo lỗi.
        """
        with self.client_lock:
            if self.client is not None and self.client.connected:
                self.tcp_state["connected"] = True
                return True

            self.client = EspTcpClient()

            try:
                self.client.connect()
            except Exception as e:
                self.tcp_state["connected"] = False
                self.client = None
                self.available_ports = []
                self.com_source = None
                self.com_updated_at = None
                raise RuntimeError(f"Chưa kết nối được ESP32-Collection: {e}")

            self.tcp_state["connected"] = True
            self.client_running = True
            print("Đã kết nối TCP tới ESP32-Collection")

            self.client_thread = threading.Thread(
                target=self._client_receive_loop,
                name="esp-collection-receiver",
                daemon=True,
            )
            self.client_thread.start()

        self.request_com_ports()
        return True

    def disconnect_collection(self):
        with self.client_lock:
            self.client_running = False

            if self.client is not None:
                self.client.close()

            self.client = None
            self.tcp_state["connected"] = False
            self.available_ports = []
            self.com_source = None
            self.com_updated_at = None

        for device in self.devices.values():
            device["connected"] = False
            device["status"] = "DISCONNECTED"
            device["last_error"] = None

        return {
            "status": "disconnected",
            "message": "Đã ngắt TCP ESP32-Collection",
        }

    def _client_receive_loop(self):
        """
        Nhận mọi message từ ESP32-Collection:
        - com_list / uart_status / control_ack: cập nhật trạng thái management
        - csi_data: đưa vào queue để CsiService ghi CSV khi session chạy
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

            msg_type = packet.get("type")

            if msg_type in {"com_list", "uart_status", "control_ack"}:
                self.handle_collection_message(packet)
                continue

            if msg_type not in {None, "csi_data"}:
                continue

            self.put_packet(packet)

        with self.client_lock:
            if self.client is not None and not self.client.connected:
                self.client = None

            self.tcp_state["connected"] = False
            self.client_running = False
            self.available_ports = []
            self.com_source = None
            self.com_updated_at = None

        for device in self.devices.values():
            if device.get("connected"):
                device["connected"] = False
                device["status"] = "DISCONNECTED"

    def request_com_ports(self):
        self.ensure_collection_connected()

        if self.client is None:
            raise RuntimeError("ESP32-Collection chưa kết nối TCP")

        ok = self.client.request_com_ports()
        if not ok:
            self.tcp_state["connected"] = False
            raise RuntimeError("Không gửi được yêu cầu lấy COM xuống ESP32-Collection")

        return {
            "status": "requested",
            "message": "Đã yêu cầu ESP32-Collection gửi danh sách COM",
        }

    def set_available_ports(self, ports: list[str] | None, source: str = "collection"):
        if ports is None:
            ports = []

        unique_ports = []
        for port in ports:
            port = str(port)
            if port and port not in unique_ports:
                unique_ports.append(port)

        self.available_ports = unique_ports
        self.com_source = source
        self.com_updated_at = unix_now_us()

    def _resolve_device_id(self, device_id: str | None):
        if device_id is None:
            raise ValueError("Thiếu device_id/uartId")

        if device_id not in self.devices:
            raise ValueError(f"Thiết bị ESP không hợp lệ: {device_id}")

        return device_id

    def connect_device(self, device_id: str, com: str, baudrate: int | None):
        device_id = self._resolve_device_id(device_id)

        if not self.tcp_state["connected"]:
            raise RuntimeError("ESP32-Collection chưa kết nối TCP")

        if not self.available_ports:
            raise RuntimeError("Chưa có danh sách COM từ ESP32-Collection")

        if not com:
            raise ValueError("Thiếu COM/port")

        if com not in self.available_ports:
            raise ValueError(f"COM không hợp lệ: {com}. Danh sách hiện có: {self.available_ports}")

        if baudrate is None:
            raise ValueError("Thiếu baudrate")

        if self.client is None:
            raise RuntimeError("ESP32-Collection chưa kết nối TCP")

        sent = self.client.send_uart_control(
            device_id=device_id,
            action="connect",
            com=com,
            baudrate=int(baudrate),
            enabled=True,
        )

        if not sent:
            self.tcp_state["connected"] = False
            raise RuntimeError("Không gửi được lệnh connect xuống ESP32-Collection")

        device = self.devices[device_id]
        device["com"] = com
        device["baudrate"] = int(baudrate)
        device["status"] = "CONNECTING"
        device["last_error"] = None

        return {
            "status": "sent",
            "message": f"Đã gửi lệnh connect {device_id} -> {com} xuống ESP32-Collection",
            "device_id": device_id,
            "config": device,
        }

    def disconnect_device(self, device_id: str):
        device_id = self._resolve_device_id(device_id)

        if self.client is not None and self.tcp_state["connected"]:
            self.client.send_uart_control(
                device_id=device_id,
                action="disconnect",
                enabled=False,
            )

        device = self.devices[device_id]
        device["connected"] = False
        device["status"] = "DISCONNECTED"
        device["last_error"] = None

        return {
            "status": "disconnected",
            "message": f"Đã ngắt {device_id}",
            "device_id": device_id,
            "config": device,
        }

    def handle_collection_message(self, message: dict):
        msg_type = message.get("type")

        if msg_type == "com_list":
            self.set_available_ports(message.get("ports"), source="collection")
            return

        if msg_type == "control_ack":
            return

        if msg_type == "uart_status":
            device_id = message.get("device_id")
            if device_id not in self.devices:
                return

            status = str(message.get("status", "")).lower()
            config = message.get("config") or {}
            device = self.devices[device_id]

            if config.get("com") is not None:
                device["com"] = config.get("com")

            if config.get("baudrate") is not None:
                device["baudrate"] = int(config.get("baudrate"))

            if status == "connected":
                device["connected"] = True
                device["status"] = "CONNECTED"
                device["last_error"] = None
            elif status == "disconnected":
                device["connected"] = False
                device["status"] = "DISCONNECTED"
                device["last_error"] = None
            elif status == "error":
                device["connected"] = False
                device["last_error"] = message.get("message")
                device["status"] = "ERROR"

    def control(
        self,
        action: str,
        device_id: str | None = None,
        com: str | None = None,
        baudrate: int | None = None,
    ):
        if action in {"connect_collection", "refresh_com_ports"}:
            return self.request_com_ports()

        if action == "disconnect_collection":
            return self.disconnect_collection()

        if action == "connect":
            return self.connect_device(
                device_id=self._resolve_device_id(device_id),
                com=com,
                baudrate=baudrate,
            )

        if action == "disconnect":
            return self.disconnect_device(
                device_id=self._resolve_device_id(device_id),
            )

        raise ValueError("action không hợp lệ")

    def set_tcp_connected(self, connected: bool):
        self.tcp_state["connected"] = bool(connected)

    # Giữ tên cũ để tránh lỗi nếu còn chỗ nào gọi set_queue_connected.
    def set_queue_connected(self, connected: bool):
        self.set_tcp_connected(connected)

    def set_recording_enabled(self, enabled: bool):
        self.recording_enabled = bool(enabled)

    def put_packet(self, packet: dict):
        device_id = packet.get("device_id")

        if device_id not in self.devices:
            print(self.devices)
            print(f"Received packet with unknown device_id {device_id}, type {packet.get('type')}")
            return False

        if not self.devices[device_id].get("connected", False):
      #      print(f"Received packet from {device_id} but device is not marked as connected, type {packet.get('type')}")
            return False

        # Chưa START SESSION thì không đưa CSI vào queue.
        # Backend vẫn đọc TCP để giữ trạng thái, nhưng không tích dữ liệu rác trước phiên thu.
        if not self.recording_enabled:
            return False

        try:
            # print(f"Nhận packet từ {device_id}, loại {packet.get('type')}, thời gian {packet.get('timestamp')}")
            self.csi_queue.put_nowait(packet)
            return True
        except queue.Full:
            # Khi thu CSI tốc độ cao mà writer không ghi kịp, queue sẽ đầy.
            # Đếm số packet bị drop để dễ debug/thống kê.
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


uart_manager = UartManager()
