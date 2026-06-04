# collection_stub/esp32_collection_stub_fake_realtime.py
#
# Fake ESP32-Collection để test khi CHƯA có phần cứng thật.
#
# Chạy 2 TCP server:
#   - 127.0.0.1:9200  Management/control: nhận lệnh get_com_ports, uart_control/connect/disconnect
#   - 127.0.0.1:9201  Realtime viewer: chỉ nhận bản sao csi_data để vẽ realtime
#
# Không dùng queue backlog:
#   - Có client thì gửi packet mới ngay
#   - Không có client thì bỏ qua packet
#   - Không gửi lại dữ liệu cũ khi client connect sau
#
# Giao thức JSON Lines, mỗi message kết thúc bằng \n.
# Packet CSI gửi ra:
# {
#   "type": "csi_data",
#   "device_id": "00:1A:2B:3C:4D:5E",      # MAC fake, giống hệ thật
#   "device_alias": "esp1",                 # alias để dễ nhìn
#   "seq": 123,
#   "timestamp": 1716023475123456,           # micro giây
#   "radio": {"rssi": -45, "channel": 6, "agc_gain": 1, "fft_gain": 2, "noise_floor": -95},
#   "csi": [[q0,i0], [q1,i1], ...]           # 64 cặp Q/I
# }

from __future__ import annotations

import argparse
import math
import random
import time
from typing import Any

from tcp_stream_server import TcpStreamServer


# ══════════════════════════════════════════════════════════
# CẤU HÌNH
# ══════════════════════════════════════════════════════════

TCP_HOST = "127.0.0.1"
CONTROL_TCP_PORT = 9200       # giống Management/Laptop2 cũ
REALTIME_TCP_PORT = 9201      # realtime viewer connect vào đây

FAKE_COM_PORTS = ["COM3", "COM4", "COM5", "COM6", "COM7", "COM8"]
DEFAULT_BAUDRATE = 115200
CSI_PAIR_COUNT = 64
SEQ_MODULO = 4096

# Mặc định fake gửi 200 packet/s cho MỖI ESP đang connected.
# Nếu 3 ESP đều connected thì tổng khoảng 600 packet/s.
DEFAULT_RATE_HZ_PER_DEVICE = 200.0

# MAC fake để giống hệ thật của bạn.
FAKE_DEVICES = [
    {"alias": "esp1", "mac": "00:1A:2B:3C:4D:5E", "default_com": "COM3", "channel": 1},
    {"alias": "esp2", "mac": "00:1C:C7:9A:01:6A", "default_com": "COM4", "channel": 6},
    {"alias": "esp3", "mac": "00:1D:2E:3F:40:51", "default_com": "COM5", "channel": 11},
]


def unix_now_us() -> int:
    return time.time_ns() // 1_000


def clamp_int8(x: float) -> int:
    return max(-128, min(127, int(round(x))))


def normalize_device_id(device_id: Any, alias_to_mac: dict[str, str]) -> str | None:
    """
    Cho phép gửi lệnh connect bằng MAC hoặc bằng esp1/esp2/esp3.
    Trả về MAC chuẩn nếu hợp lệ.
    """
    if device_id is None:
        return None

    text = str(device_id).strip()
    if not text:
        return None

    lower = text.lower()
    if lower in alias_to_mac:
        return alias_to_mac[lower]

    return text.upper()


def make_fake_csi(alias: str, seq: int, t_sec: float) -> list[list[int]]:
    """
    Tạo CSI fake 64 cặp [Q, I].

    Không dùng random hoàn toàn, mà tạo dạng biến thiên mượt theo thời gian
    để khi vẽ line/heatmap nhìn giống tín hiệu realtime hơn.
    """
    # Mỗi ESP lệch pha một chút để đồ thị không giống hệt nhau.
    alias_offset = {"esp1": 0.0, "esp2": 1.7, "esp3": 3.1}.get(alias, 0.0)

    pairs: list[list[int]] = []
    for sc in range(CSI_PAIR_COUNT):
        # Biên độ nền phụ thuộc subcarrier
        base_amp = 45.0 + 18.0 * math.sin(2.0 * math.pi * sc / CSI_PAIR_COUNT + alias_offset)

        # Biến thiên chậm theo thời gian, để line plot có dao động
        slow_fade = 10.0 * math.sin(2.0 * math.pi * 0.45 * t_sec + sc * 0.08 + alias_offset)

        # Tạo vùng nhiễu/chuyển động giả chạy theo thời gian trên các subcarrier
        moving_center = (CSI_PAIR_COUNT / 2.0) + 18.0 * math.sin(2.0 * math.pi * 0.12 * t_sec + alias_offset)
        width = 8.0
        motion_bump = 18.0 * math.exp(-((sc - moving_center) ** 2) / (2.0 * width * width))

        amp = max(3.0, base_amp + slow_fade + motion_bump + random.gauss(0.0, 2.5))

        # Pha cũng thay đổi theo thời gian và subcarrier
        phase = (
            2.0 * math.pi * 0.25 * t_sec
            + sc * 0.18
            + seq * 0.015
            + alias_offset
            + random.gauss(0.0, 0.035)
        )

        # Dữ liệu của bạn đang theo thứ tự [Q, I]
        i_val = clamp_int8(amp * math.cos(phase))
        q_val = clamp_int8(amp * math.sin(phase))
        pairs.append([q_val, i_val])

    return pairs


def main() -> None:
    parser = argparse.ArgumentParser(description="Fake ESP32 Collection with realtime CSI port")
    parser.add_argument(
        "--host",
        default=TCP_HOST,
        help="Host bind TCP server, mặc định 127.0.0.1",
    )
    parser.add_argument(
        "--control-port",
        type=int,
        default=CONTROL_TCP_PORT,
        help="Port Management/control, mặc định 9200",
    )
    parser.add_argument(
        "--realtime-port",
        type=int,
        default=REALTIME_TCP_PORT,
        help="Port realtime viewer, mặc định 9201",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE_HZ_PER_DEVICE,
        help="Số packet/s cho mỗi ESP connected, mặc định 200",
    )
    parser.add_argument(
        "--wait-management",
        action="store_true",
        help="Không auto-connect. Phải gửi uart_control/connect thì mới phát CSI.",
    )
    args = parser.parse_args()

    control_server = TcpStreamServer(
        host=args.host,
        port=args.control_port,
        name="ESP32-Control-Fake",
    )

    realtime_server = TcpStreamServer(
        host=args.host,
        port=args.realtime_port,
        name="ESP32-Realtime-Fake",
    )

    alias_to_mac = {d["alias"].lower(): d["mac"] for d in FAKE_DEVICES}
    mac_to_alias = {d["mac"]: d["alias"] for d in FAKE_DEVICES}
    mac_to_default_com = {d["mac"]: d["default_com"] for d in FAKE_DEVICES}
    mac_to_channel = {d["mac"]: d["channel"] for d in FAKE_DEVICES}

    # Nếu không dùng --wait-management thì tự connect cả 3 ESP ngay khi chạy,
    # để test realtime không cần mở Management thật.
    auto_connected = not args.wait_management

    devices: dict[str, dict[str, Any]] = {
        d["mac"]: {
            "alias": d["alias"],
            "connected": auto_connected,
            "com": d["default_com"] if auto_connected else None,
            "baudrate": DEFAULT_BAUDRATE,
            "seq": random.randint(0, SEQ_MODULO - 1),
            "next_send_time": time.perf_counter() + random.random() * 0.01,
            "sent": 0,
        }
        for d in FAKE_DEVICES
    }

    def send_to_control(packet: dict) -> bool:
        return control_server.send_packet(packet)

    def send_to_realtime(packet: dict) -> bool:
        return realtime_server.send_packet(packet)

    def send_com_list() -> None:
        send_to_control({"type": "com_list", "ports": FAKE_COM_PORTS})

    def make_uart_status(device_id: str, status: str, message: str = "") -> dict:
        cfg = devices.get(device_id)
        packet = {
            "type": "uart_status",
            "device_id": device_id,
            "device_alias": mac_to_alias.get(device_id),
            "status": status,
            "config": cfg,
        }
        if message:
            packet["message"] = message
        return packet

    def send_uart_status(device_id: str, status: str, message: str = "") -> None:
        send_to_control(make_uart_status(device_id, status, message))

    def set_device_connected(device_id: str, connected: bool, com: str | None = None, baudrate: int = DEFAULT_BAUDRATE) -> None:
        cfg = devices[device_id]
        cfg["connected"] = connected
        if connected:
            cfg["com"] = com or cfg["com"] or mac_to_default_com[device_id]
            cfg["baudrate"] = int(baudrate)
            cfg["next_send_time"] = time.perf_counter()
        else:
            cfg["connected"] = False

    def handle_message(message: dict) -> None:
        """
        Nhận lệnh giả từ Management hoặc realtime client control.

        Hỗ trợ:
        - {"type":"get_com_ports"}
        - {"type":"uart_control","action":"connect","device_id":"esp1","com":"COM3","baudrate":115200}
        - {"type":"uart_control","action":"disconnect","device_id":"esp1"}
        - {"type":"uart_control","action":"connect_all"}
        - {"type":"uart_control","action":"disconnect_all"}
        """
        print("[ESP32-Control-Fake] RX:", message)

        msg_type = message.get("type")
        action = str(message.get("action") or "").strip().lower()

        if msg_type == "get_com_ports" or action == "refresh_com_ports":
            send_com_list()
            return

        if action == "connect_all":
            for mac in devices:
                set_device_connected(mac, True, mac_to_default_com[mac], DEFAULT_BAUDRATE)
                send_uart_status(mac, "connected")
            return

        if action == "disconnect_all":
            for mac in devices:
                set_device_connected(mac, False)
                send_uart_status(mac, "disconnected")
            return

        if msg_type not in {"uart_control", "configure_device"} and action not in {
            "connect",
            "disconnect",
            "configure_device",
        }:
            send_to_control({
                "type": "control_ack",
                "status": "ignored",
                "message": "Message type/action không hỗ trợ",
                "raw": message,
            })
            return

        raw_device_id = message.get("device_id") or message.get("uartId")
        device_id = normalize_device_id(raw_device_id, alias_to_mac)

        if device_id not in devices:
            send_to_control({
                "type": "uart_status",
                "status": "error",
                "message": f"Thiết bị không hợp lệ: {raw_device_id}. Dùng esp1/esp2/esp3 hoặc MAC fake.",
                "device_id": raw_device_id,
            })
            return

        if action == "disconnect" or message.get("enabled") is False:
            set_device_connected(device_id, False)
            send_uart_status(device_id, "disconnected")
            return

        # connect/configure_device
        com = message.get("com") or message.get("port") or devices[device_id].get("com") or mac_to_default_com[device_id]
        baudrate = int(message.get("baudrate") or message.get("baudRate") or devices[device_id].get("baudrate") or DEFAULT_BAUDRATE)

        if com not in FAKE_COM_PORTS:
            send_uart_status(device_id, "error", f"COM không nằm trong danh sách fake: {com}")
            return

        set_device_connected(device_id, True, com, baudrate)
        send_uart_status(device_id, "connected")

    control_server.set_message_handler(handle_message)
    control_server.set_client_connected_handler(send_com_list)

    control_server.start()
    realtime_server.start()

    print("=" * 70)
    print("Fake ESP32 Collection đã chạy")
    print(f"Management/control : {args.host}:{args.control_port}")
    print(f"Realtime viewer    : {args.host}:{args.realtime_port}")
    print(f"Rate               : {args.rate:.1f} packet/s / ESP connected")
    print("Auto-connect       :", "ON" if auto_connected else "OFF, chờ lệnh connect")
    print("Device control có thể dùng esp1/esp2/esp3 hoặc MAC fake")
    print("=" * 70)

    interval_sec = 1.0 / max(1.0, args.rate)
    last_stats_time = time.perf_counter()
    last_sent_total = 0

    try:
        while True:
            now_perf = time.perf_counter()
            t_sec = time.time()
            sent_this_loop = 0

            for mac, cfg in devices.items():
                if not cfg.get("connected"):
                    continue

                # Gửi đủ tốc độ cho từng ESP. Nếu máy hơi trễ, vòng while sẽ bù nhẹ.
                # Giới hạn tối đa 5 packet/lần/ESP để tránh vòng lặp nghẹt nếu máy treo lâu.
                burst_count = 0
                while cfg["next_send_time"] <= now_perf and burst_count < 5:
                    alias = cfg["alias"]
                    seq = int(cfg["seq"])

                    packet = {
                        "type": "csi_data",
                        "device_id": mac,
                        "device_alias": alias,
                        "seq": seq,
                        "timestamp": unix_now_us(),
                        "radio": {
                            "rssi": random.randint(-75, -35),
                            "channel": mac_to_channel.get(mac, random.choice([1, 6, 11])),
                            "agc_gain": random.randint(0, 3),
                            "fft_gain": random.randint(0, 3),
                            "noise_floor": random.randint(-100, -85),
                        },
                        "csi": make_fake_csi(alias, seq, t_sec),
                    }

                    # Không queue: có client thì gửi, không có thì bỏ qua.
                    send_to_control(packet)
                    send_to_realtime(packet)

                    cfg["seq"] = (seq + 1) % SEQ_MODULO
                    cfg["sent"] += 1
                    cfg["next_send_time"] += interval_sec
                    burst_count += 1
                    sent_this_loop += 1

            now_stats = time.perf_counter()
            if now_stats - last_stats_time >= 1.0:
                total_sent = sum(int(cfg["sent"]) for cfg in devices.values())
                rate_total = (total_sent - last_sent_total) / (now_stats - last_stats_time)
                connected_names = [cfg["alias"] for cfg in devices.values() if cfg.get("connected")]
                # print(
                #     f"[FAKE] connected={connected_names} total_rate≈{rate_total:.1f} pkt/s "
                #     f"seq={{" + ", ".join(f"{cfg['alias']}:{cfg['seq']}" for cfg in devices.values()) + "}}"
                # )
                last_stats_time = now_stats
                last_sent_total = total_sent

            # Ngủ ngắn để CPU không full, nhưng vẫn đủ cho 200Hz/ESP.
            time.sleep(0.001 if sent_this_loop else 0.002)

    except KeyboardInterrupt:
        print("\nStopping fake ESP32 Collection...")
    finally:
        control_server.stop()
        realtime_server.stop()


if __name__ == "__main__":
    main()
