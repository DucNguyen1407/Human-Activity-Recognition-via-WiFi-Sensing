# collection_stub/nexmon_collection_stub.py
#
# Mô phỏng Nexmon-Collection.
# Collection gửi MAC thật lên CSI-IoT-system.
# CSI-IoT-system/NexmonTcpClient sẽ map MAC -> asus1/asus2/asus3.
#
# Format gửi về Management:
# {
#   "device_id": "02:1A:2B:3C:4D:5E",
#   "seq": 1,
#   "timestamp": 1716280000123456,
#   "bw": 20,
#   "agc": [0, 0, 0, 0],
#   "rssi": [2, 3, 4, 5],
#   "csi": {
#       "c0": [[q0,i0],... 64 cặp],
#       "c1": [[q0,i0],... 64 cặp],
#       "c2": [[q0,i0],... 64 cặp],
#       "c3": [[q0,i0],... 64 cặp]
#   }
# }

import random
import time

from tcp_stream_server import TcpStreamServer


CSI_PAIR_COUNT = 64
ANTENNA_COUNT = 4
CSI_INTERVAL_SEC = 0.02  # khoảng 50 packet/s tổng

ASUS_MACS = [
    "02:1A:2B:3C:4D:5E",
    "02:AA:BB:CC:DD:01",
    "02:11:22:33:44:55",
]


def unix_now_us() -> int:
    return time.time_ns() // 1_000


def fake_qi_pairs() -> list[list[int]]:
    """Tạo 64 cặp Q/I cho 1 antenna: [[q0,i0],[q1,i1],...]."""
    return [
        [random.randint(-128, 127), random.randint(-128, 127)]
        for _ in range(CSI_PAIR_COUNT)
    ]


def fake_nexmon_csi() -> dict:
    """Tạo CSI cho 4 antenna: c0/c1/c2/c3, mỗi antenna 64 cặp Q/I."""
    return {
        f"c{ant}": fake_qi_pairs()
        for ant in range(ANTENNA_COUNT)
    }


def fake_agc() -> list[int]:
    return [
        random.randint(0, 3)
        for _ in range(ANTENNA_COUNT)
    ]


def fake_rssi() -> list[int]:
    return [
        random.randint(-80, -30)
        for _ in range(ANTENNA_COUNT)
    ]


def main():
    server = TcpStreamServer(
        host="127.0.0.1",
        port=9100,
        name="Nexmon-Collection",
    )

    server.start()

    seq = 0

    while True:
        seq += 1
        device_id = random.choice(ASUS_MACS)

        packet = {
            "device_id": device_id,
            "seq": seq,
            "timestamp": unix_now_us(),
            "bw": 20,
            "agc": fake_agc(),
            "rssi": fake_rssi(),
            "csi": fake_nexmon_csi(),
        }

        server.send_packet(packet)
        time.sleep(CSI_INTERVAL_SEC)


if __name__ == "__main__":
    main()
