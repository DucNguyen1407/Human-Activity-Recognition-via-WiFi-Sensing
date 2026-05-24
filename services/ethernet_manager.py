# app/services/ethernet_manager.py
#
# Nexmon Management.
# Web UI chỉ cấu hình host/port TCP client để kết nối tới Nexmon-Collection.
# asus1/asus2/asus3 do Collection gửi dữ liệu, Web không cần cấu hình riêng từng ID.

from collections import deque
import queue
import time

from app.core.time_utils import unix_now_us


NEXMON_COLLECTION_HOST = "127.0.0.1"
NEXMON_COLLECTION_PORT = 9100

# Queue lớn hơn để giảm nguy cơ rớt packet khi CSI tốc độ cao.
# Nếu queue đầy, packet mới sẽ bị drop và đếm trong devices[device_id]["dropped_packets"].
CSI_QUEUE_MAXSIZE = 50000


class EthernetManager:
    def __init__(self):
        self.queue = {
            "host": NEXMON_COLLECTION_HOST,
            "port": NEXMON_COLLECTION_PORT,
            "connected": False,
        }

        self.csi_queue = queue.Queue(maxsize=CSI_QUEUE_MAXSIZE)

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
            "status": "NO_DATA",
            "packet_rate": 0,
            "dropped_packets": 0,
            "last_packet_at": None,
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
        Cập nhật status ASUS cho Web UI.

        - NO_DATA: TCP ASUS chưa kết nối hoặc chưa có packet
        - WAITING: TCP ASUS đã kết nối nhưng thiết bị chưa gửi data
        - RECEIVING: đang có packet trong 1 giây gần nhất
        """
        for device in self.devices.values():
            if not self.queue.get("connected"):
                device["status"] = "NO_DATA"
            elif device.get("packet_rate", 0) > 0:
                device["status"] = "RECEIVING"
            else:
                device["status"] = "WAITING"

    def get_status(self):
        self.refresh_all_rates()
        self._refresh_device_display_status()

        return {
            "queue": {
                **self.queue,
                "size": self.csi_queue.qsize(),
                "maxsize": self.csi_queue.maxsize,
            },
            "devices": self.devices,
        }

    def configure_queue(self, host: str | None = None, port: int | None = None):
        if host is not None:
            self.queue["host"] = host

        if port is not None:
            self.queue["port"] = int(port)

        return {
            "status": "updated",
            "queue": self.queue,
        }

    def control(self, action: str, host: str | None = None, port: int | None = None):
        if action == "configure_queue":
            return self.configure_queue(host=host, port=port)

        raise ValueError("action không hợp lệ")

    def set_queue_connected(self, connected: bool):
        self.queue["connected"] = connected

    def put_packet(self, packet: dict):
        device_id = packet.get("device_id")

        if device_id not in self.devices:
            return False

        try:
            self.csi_queue.put_nowait(packet)
            return True
        except queue.Full:
            self.devices[device_id]["dropped_packets"] = self.devices[device_id].get("dropped_packets", 0) + 1
            return False

    def get_packet(self, timeout: float = 0.1):
        try:
            return self.csi_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def update_packet_stat(self, device_id: str):
        """
        Đếm rate bằng số packet đã được lấy ra khỏi queue trong 1 giây gần nhất.
        Hàm này được CsiService gọi sau khi packet được ghi file thành công.
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
            device["status"] = "NO_DATA"
            device["packet_rate"] = 0
            device["dropped_packets"] = 0
            device["last_packet_at"] = None


ethernet_manager = EthernetManager()
