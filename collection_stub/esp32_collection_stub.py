# collection_stub/esp32_collection_stub.py
#
# Mô phỏng ESP32-Collection khi CHƯA có thiết bị thật.
# Collection dùng MAC thật cho device_id.
# CSI-IoT-system/EspTcpClient sẽ map MAC -> esp1/esp2/esp3.
#
# Format CSI gửi về Management:
# {
#   "type": "csi_data",
#   "device_id": "00:1A:2B:3C:4D:5E",
#   "seq": 123,
#   "timestamp": 1716023475123456,
#   "radio": {"rssi":-45,"channel":6,"agc_gain":1,"fft_gain":2,"noise_floor":-95},
#   "csi": [[q0,i0],[q1,i1],...]  # 64 cặp Q/I
# }

import random
import time

from tcp_stream_server import TcpStreamServer


FAKE_COM_PORTS = ["COM3", "COM4", "COM5", "COM6", "COM7", "COM8"]
CSI_INTERVAL_SEC = 0.002
CSI_PAIR_COUNT = 64

ESP_MACS = [
    "00:1A:2B:3C:4D:5E",
    "00:1C:C7:9A:01:6A",
    "00:1D:2E:3F:40:51",
]


def unix_now_us() -> int:
    return time.time_ns() // 1_000


def fake_csi_values() -> list[list[int]]:
    """Tạo fake CSI 64 cặp Q/I: [[q0,i0],[q1,i1],...]."""
    return [
        [random.randint(-128, 127), random.randint(-128, 127)]
        for _ in range(CSI_PAIR_COUNT)
    ]


def main():
    server = TcpStreamServer(
        host="127.0.0.1",
        port=9200,
        name="ESP32-Collection"
    )

    devices = {
        mac: {
            "connected": False,
            "com": None,
            "baudrate": 115200,
            "seq": random.randint(0, 4095),
        }
        for mac in ESP_MACS
    }

    def send_com_list():
        server.send_packet({
            "type": "com_list",
            "ports": FAKE_COM_PORTS,
        })

    def send_uart_status(device_id: str, status: str = "updated", message: str = ""):
        packet = {
            "type": "uart_status",
            "device_id": device_id,
            "status": status,
            "config": devices.get(device_id),
        }

        if message:
            packet["message"] = message

        server.send_packet(packet)

    def handle_message(message: dict):
        """
        Nhận lệnh từ Management.

        Message hỗ trợ:
        - {"type":"get_com_ports"}
        - {"type":"uart_control","action":"connect","device_id":"00:1A:2B:3C:4D:5E","com":"COM3","baudrate":115200}
        - {"type":"uart_control","action":"disconnect","device_id":"00:1A:2B:3C:4D:5E"}
        """
        print("[ESP32-Collection] RX:", message)

        msg_type = message.get("type")
        action = message.get("action")

        if msg_type == "get_com_ports" or action == "refresh_com_ports":
            send_com_list()
            return

        if msg_type not in {"uart_control", "configure_device"} and action not in {
            "connect",
            "disconnect",
            "configure_device",
        }:
            server.send_packet({
                "type": "control_ack",
                "status": "ignored",
                "message": "Message type/action không hỗ trợ",
                "raw": message,
            })
            return

        device_id = message.get("device_id") or message.get("uartId")
        if device_id is not None:
            device_id = str(device_id).strip().upper()

        if device_id not in devices:
            server.send_packet({
                "type": "uart_status",
                "status": "error",
                "message": f"Thiết bị không hợp lệ: {device_id}",
                "device_id": device_id,
            })
            return

        if action in {"disconnect"} or message.get("enabled") is False:
            devices[device_id]["connected"] = False
            send_uart_status(device_id, status="disconnected")
            return

        # connect/configure_device
        com = message.get("com") or message.get("port") or devices[device_id]["com"]
        baudrate = message.get("baudrate") or message.get("baudRate") or devices[device_id]["baudrate"]

        if com not in FAKE_COM_PORTS:
            send_uart_status(
                device_id,
                status="error",
                message=f"COM không nằm trong danh sách fake: {com}",
            )
            return

        devices[device_id]["com"] = com
        devices[device_id]["baudrate"] = int(baudrate)
        devices[device_id]["connected"] = True

        send_uart_status(device_id, status="connected")

    server.set_message_handler(handle_message)
    server.set_client_connected_handler(send_com_list)
    server.start()
    # Seq bắt đầu ngẫu nhiên trong khoảng 0..4095.
    # Mỗi gói gửi ra dùng seq hiện tại, sau đó tăng 1.
    # Khi seq = 4095 thì gói kế tiếp quay lại 0.

    last_com_list_time = 0.0

    while True:
        connected_devices = [
            device_id
            for device_id, cfg in devices.items()
            if cfg.get("connected")
        ]

        if not connected_devices:
            time.sleep(0.1)
            continue

        # Chọn ngẫu nhiên 1 ESP đang connected để gửi fake CSI
        device_id = random.choice(connected_devices)

        packet = {
            "type": "csi_data",
            "device_id": device_id,
            "seq": devices[device_id]["seq"],
            "timestamp": unix_now_us(),
            "radio": {
                "rssi": random.randint(-80, -30),
                "channel": random.choice([1, 6, 11]),
                "agc_gain": random.randint(0, 3),
                "fft_gain": random.randint(0, 3),
                "noise_floor": random.randint(-100, -85),
            },
            "csi": fake_csi_values(),
        }

        server.send_packet(packet)

        # Tăng seq riêng cho ESP vừa gửi
        # 4094 -> 4095 -> 0 -> 1 ...
        devices[device_id]["seq"] = (devices[device_id]["seq"] + 1) % 4096

        time.sleep(CSI_INTERVAL_SEC)


if __name__ == "__main__":
    main()
