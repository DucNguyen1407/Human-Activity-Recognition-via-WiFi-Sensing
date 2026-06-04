# collection_stub/nexmon_collection_stub.py
#
# Mô phỏng Nexmon-Collection.
# Collection gửi MAC thật lên CSI-IoT-system.
# CSI-IoT-system/NexmonTcpClient sẽ map MAC -> asus1/asus2/asus3.
#
# Format gửi về Management hiện tại:
# {
#   "device_id": "02:1A:2B:3C:4D:5E",
#   "seq": 1,                       # 12 bit: 0..4095
#   "timestamp": 1716280000123456,  # unix time, microsecond
#   "bw": 20,
#   "ch": 157,
#   "agc": [0, 0, 0, 0],
#   "rssi": [2, 3, 4, 5],
#   "csi": {
#       "c0": [uint32_decimal_sub0, ..., uint32_decimal_sub63],
#       "c1": [uint32_decimal_sub0, ..., uint32_decimal_sub63],
#       "c2": [uint32_decimal_sub0, ..., uint32_decimal_sub63],
#       "c3": [uint32_decimal_sub0, ..., uint32_decimal_sub63]
#   }
# }
#
# Mỗi giá trị CSI trong c0/c1/c2/c3 là một số thập phân biểu diễn 4 byte.
# Stub này tạo giả Q/I dạng int16, rồi pack thành uint32:
#   bits  0..15  = Q dưới dạng unsigned 16-bit
#   bits 16..31  = I dưới dạng unsigned 16-bit

import random
import time

from tcp_stream_server import TcpStreamServer


CSI_SUBCARRIER_COUNT = 64
ANTENNA_COUNT = 4
SEQ_MODULO = 4096
CSI_INTERVAL_SEC = 0.002

NEXMON_CHANNEL = 157
NEXMON_BW = 20

ASUS_MACS = [
    "02:1A:2B:3C:4D:5E",
    "02:AA:BB:CC:DD:01",
    "02:11:22:33:44:55",
]


def unix_now_us() -> int:
    return time.time_ns() // 1_000


def pack_qi_to_uint32(q: int, i: int) -> int:
    """
    Pack 1 cặp Q/I thành 1 số uint32 để gửi trong JSON.

    Quy ước fake data:
    - Q là signed int16, lưu ở 16 bit thấp.
    - I là signed int16, lưu ở 16 bit cao.

    Backend đang lưu mỗi số này thành 4 byte binary bằng struct.pack("<I", value).
    """
    return (q & 0xFFFF) | ((i & 0xFFFF) << 16)


def fake_csi_uint32_values() -> list[int]:
    """
    Tạo 64 giá trị CSI cho 1 antenna.
    Mỗi giá trị là số thập phân biểu diễn 4 byte Q/I đã pack.
    """
    values = []
    for _ in range(CSI_SUBCARRIER_COUNT):
        q = random.randint(-32768, 32767)
        i = random.randint(-32768, 32767)
        values.append(pack_qi_to_uint32(q, i))
    return values


def fake_nexmon_csi() -> dict:
    """
    Tạo CSI cho 4 antenna: c0/c1/c2/c3.
    Mỗi antenna là mảng 64 số uint32 decimal.
    """
    return {
        f"c{ant}": fake_csi_uint32_values()
        for ant in range(ANTENNA_COUNT)
    }


def fake_agc() -> list[int]:
    return [
        random.randint(0, 255)
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

    seq = random.randint(0, SEQ_MODULO - 1)

    while True:
        device_id = random.choice(ASUS_MACS)

        packet = {
            "device_id": device_id,
            "seq": seq,
            "timestamp": unix_now_us(),
            "bw": NEXMON_BW,
            "ch": NEXMON_CHANNEL,
            "agc": fake_agc(),
            "rssi": fake_rssi(),
            "csi": fake_nexmon_csi(),
        }

        server.send_packet(packet)

        seq = (seq + 1) % SEQ_MODULO
        time.sleep(CSI_INTERVAL_SEC)


if __name__ == "__main__":
    main()
