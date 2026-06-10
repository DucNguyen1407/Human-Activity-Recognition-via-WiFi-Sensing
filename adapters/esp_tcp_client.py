# app/adapters/esp_tcp_client.py
#
# TCP client giao tiếp 2 chiều với ESP32-Collection.
# ESP32-Collection mở TCP server cố định, ví dụ: 127.0.0.1:9200
#
# Quy ước mapping đặt trực tiếp trong file này:
# - Collection có thể gửi device_id là MAC thật.
# - Adapter này map MAC -> esp1/esp2/esp3 trước khi đưa packet lên UartManager/CsiService.
# - Khi backend gửi lệnh xuống Collection, adapter map esp1/esp2/esp3 -> MAC.

import json
import socket
import threading
from typing import Optional


# Host/port TCP client của Management khi kết nối tới ESP32-Collection.
# Cấu hình này để cố định trong code, không đưa lên Web UI.
ESP_COLLECTION_HOST = "127.0.0.1"
ESP_COLLECTION_PORT = 9200


# Map ESP MAC thật về ID ngắn dùng trong backend/UI/CSV.
# Collection vẫn có thể gửi MAC; backend phía sau vẫn dùng esp1/esp2/esp3.
ESP_MAC_TO_ID = {
    "D0:CF:13:ED:2E:EC": "esp1",
    "D0:CF:13:EB:8A:9C": "esp2",  
    "D0:CF:13:EC:49:04": "esp3",
}
# # MAC giả
# ESP_MAC_TO_ID = {
#     "00:1A:2B:3C:4D:5E": "esp1",
#     "00:1C:C7:9A:01:6A": "esp2",
#     "00:1D:2E:3F:40:51": "esp3",
# }
# Map ngược để khi UI/backend gửi lệnh connect/disconnect bằng esp1/esp2/esp3,
# TCP client gửi xuống Collection bằng MAC thật.
ESP_ID_TO_MAC = {
    value: key
    for key, value in ESP_MAC_TO_ID.items()
}


def _normalize_device_id(value):
    """
    Chuẩn hóa device_id/MAC:
    - bỏ khoảng trắng đầu/cuối
    - viết hoa để match MAC ổn định
    """
    if value is None:
        return None

    return str(value).strip().upper()


def map_esp_mac_to_id(device_id):
    """
    Collection -> Backend:
    MAC thật -> esp1/esp2/esp3.
    Nếu không match mapping thì trả lại device_id đã strip để dễ debug.
    """
    if device_id is None:
        return None

    normalized = _normalize_device_id(device_id)
    return ESP_MAC_TO_ID.get(normalized, str(device_id).strip())


def map_esp_id_to_mac(device_id):
    """
    Backend -> Collection:
    esp1/esp2/esp3 -> MAC thật.
    Nếu device_id đã là MAC hoặc không có mapping thì giữ nguyên sau khi strip.
    """
    if device_id is None:
        return None

    short_id = str(device_id).strip()
    return ESP_ID_TO_MAC.get(short_id, short_id)


class EspTcpClient:
    def __init__(self, host: str = ESP_COLLECTION_HOST, port: int = ESP_COLLECTION_PORT):
        self.host = host
        self.port = port

        self.sock: Optional[socket.socket] = None
        self.buffer = b""
        self.write_lock = threading.Lock()
        self.connected = False

    def connect(self):
        """
        Kết nối tới TCP server của ESP32-Collection.
        """
        self.close()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(3)
        self.sock.connect((self.host, self.port))
        self.sock.settimeout(0.5)
        self.connected = True

    def send_message(self, message: dict) -> bool:
        """
        Gửi một message JSON line xuống ESP32-Collection.
        """
        if self.sock is None or not self.connected:
            return False

        data = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")

        try:
            with self.write_lock:
                self.sock.sendall(data)
            return True
        except Exception as e:
            print(f"[EspTcpClient] send_message error: {e}")
            self.connected = False
            return False

    def request_com_ports(self) -> bool:
        """
        Yêu cầu Collection gửi lại danh sách COM.
        """
        return self.send_message({
            "type": "get_com_ports",
            "source": "management",
        })

    def send_uart_control(
        self,
        device_id: str,
        action: str,
        com: str | None = None,
        baudrate: int | None = None,
        enabled: bool | None = None,
    ) -> bool:
        """
        Gửi lệnh connect/disconnect cho một ESP.

        UI/backend vẫn dùng esp1/esp2/esp3.
        Trước khi gửi xuống Collection, đổi thành MAC thật nếu có mapping.
        """
        message = {
            "type": "uart_control",
            "source": "management",
            "action": action,
            "device_id": map_esp_id_to_mac(device_id),
        }

        if com is not None:
            message["com"] = com

        if baudrate is not None:
            message["baudrate"] = baudrate

        if enabled is not None:
            message["enabled"] = enabled

        return self.send_message(message)

    def read_packet(self):
        """
        Đọc 1 packet/message JSON line từ TCP stream.

        Trả về:
        - dict nếu đọc được message hợp lệ
        - None nếu chưa có dữ liệu hoặc socket đã đóng
        """
        if self.sock is None or not self.connected:
            return None

        try:
            while b"\n" not in self.buffer:
                chunk = self.sock.recv(4096)

                if not chunk:
                    self.connected = False
                    return None

                self.buffer += chunk

            line, self.buffer = self.buffer.split(b"\n", 1)

            if not line.strip():
                return None

            packet = json.loads(line.decode("utf-8"))

            # Collection có thể gửi MAC thật; map về esp1/esp2/esp3 cho các tầng sau.
            if "device_id" in packet:
                packet["device_id"] = map_esp_mac_to_id(packet["device_id"])
                # print(f"Received packet from {packet['device_id']}, type {packet.get('type')}, thời gian {packet.get('timestamp')}")

            # Với CSI data thì source mặc định là esp.
            if packet.get("type") in (None, "csi_data"):
                packet["source"] = packet.get("source", "esp")  # Nếu packet có type là csi_data mà thiếu source thì mặc định là esp để dễ xử lý ở tầng trên.

            return packet

        except socket.timeout:
            return None

        except Exception as e:
            print(f"[EspTcpClient] read_packet error: {e}")
            self.connected = False
            return None

    def close(self):
        """
        Đóng socket.
        """
        self.connected = False

        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass

        self.sock = None
        self.buffer = b""
