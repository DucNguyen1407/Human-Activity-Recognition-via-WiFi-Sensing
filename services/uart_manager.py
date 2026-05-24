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


class UartManager:
    def __init__(self):
        self.queue = {
            "connected": False,
        }

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
            "last_packet_at": None,
            "last_error": None,
        }

    def _prune_rate_window(self, device_id: str, now: float | None = None):
        if device_id not in self._rate_windows:
            return

        if now is None:
            now = time.monotonic()

        window = self._rate_windows[device_id]
        while window and now - window[0] > 1.0:
            window.popleft()

        self.devices[device_id]["packet_rate"] = len(window)

    def refresh_all_rates(self):
        now = time.monotonic()
        for device_id in self.devices:
            self._prune_rate_window(device_id, now)

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

            if not self.queue.get("connected") or not device.get("connected"):
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
            "collection_connected": self.queue["connected"],
            "ports": list(self.available_ports),
            "available_ports": list(self.available_ports),
            "com_source": self.com_source,
            "com_updated_at": self.com_updated_at,
            "queue": {
                **self.queue,
                "size": self.csi_queue.qsize(),
                "maxsize": self.csi_queue.maxsize,
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
                self.queue["connected"] = True
                return True

            self.client = EspTcpClient()

            try:
                self.client.connect()
            except Exception as e:
                self.queue["connected"] = False
                self.client = None
                self.available_ports = []
                self.com_source = None
                self.com_updated_at = None
                raise RuntimeError(f"Chưa kết nối được ESP32-Collection: {e}")

            self.queue["connected"] = True
            self.client_running = True

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
            self.queue["connected"] = False
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

            self.queue["connected"] = False
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
            self.queue["connected"] = False
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

        if not self.queue["connected"]:
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
            self.queue["connected"] = False
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

        if self.client is not None and self.queue["connected"]:
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

    def set_queue_connected(self, connected: bool):
        self.queue["connected"] = connected

    def put_packet(self, packet: dict):
        device_id = packet.get("device_id")

        if device_id not in self.devices:
            return False

        if not self.devices[device_id].get("connected", False):
            return False

        try:
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

    def update_packet_stat(self, device_id: str):
        """
        Rate = số packet đã được lấy khỏi queue và ghi file thành công trong 1 giây gần nhất.
        CsiService gọi hàm này sau khi ghi CSV.
        """
        if device_id not in self.devices:
            return

        now = time.monotonic()
        self._rate_windows[device_id].append(now)
        self._prune_rate_window(device_id, now)
        self.devices[device_id]["last_packet_at"] = unix_now_us()

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


uart_manager = UartManager()
