# app/adapters/nexmon_tcp_client.py
#
# TCP client đọc dữ liệu từ Nexmon-Collection.
# Nexmon-Collection mở TCP server, ví dụ: 127.0.0.1:9100
#
# Quy ước mapping đặt trực tiếp trong file này:
# - Collection có thể gửi device_id là MAC thật.
# - Adapter này map MAC -> asus1/asus2/asus3 trước khi đưa packet lên
#   EthernetManager/CsiService.

import json
import socket
from typing import Optional


# Map ASUS MAC thật về ID ngắn dùng trong backend/UI/CSV.
# Collection vẫn có thể gửi MAC; backend phía sau vẫn dùng asus1/asus2/asus3.
ASUS_MAC_TO_ID = {
    "00:00:00:00:00:12": "asus1",
    "00:00:00:00:00:13": "asus2",
    "00:00:00:00:00:14": "asus3",
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


def map_asus_mac_to_id(device_id):
    """
    Collection -> Backend:
    MAC thật -> asus1/asus2/asus3.
    Nếu không match mapping thì trả lại device_id đã strip để dễ debug.
    """
    if device_id is None:
        return None

    normalized = _normalize_device_id(device_id)
    return ASUS_MAC_TO_ID.get(normalized, str(device_id).strip())


class NexmonTcpClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

        self.sock: Optional[socket.socket] = None
        self.buffer = b""
        self.connected = False

    def connect(self):
        """
        Kết nối tới TCP server của Nexmon-Collection.
        """
        self.close()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Timeout khi connect để tránh treo nếu collection chưa chạy.
        self.sock.settimeout(3)
        self.sock.connect((self.host, self.port))

        # Timeout ngắn khi đọc dữ liệu.
        self.sock.settimeout(0.5)
        self.connected = True

    def read_packet(self):
        """
        Đọc 1 packet JSON line từ TCP stream.

        Trả về:
        - dict nếu đọc được packet hợp lệ
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

            # Collection có thể gửi MAC thật; map về asus1/asus2/asus3 cho các tầng sau.
            if "device_id" in packet:
                packet["device_id"] = map_asus_mac_to_id(packet["device_id"])

            # Không ép thêm packet["source"]. CsiService gán source theo luồng ghi.
            return packet

        except socket.timeout:
            return None

        except Exception as e:
            print(f"[NexmonTcpClient] read_packet error: {e}")
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
