import asyncio
import json
import random
import time

A_HOST = "127.0.0.1"
A_PORT = 9001

B_HOST = "127.0.0.1"
B_PORT = 9002

NUM_A_CLIENTS = 3
PACKETS_PER_CLIENT = 10

DEVICE_IDS = [
    "02:1A:2B:3C:4D:5E",
    "02:1A:2B:3C:4D:5F",
    "02:1A:2B:3C:4D:60",
]


def make_csi_array():
    """
    Tạo mảng CSI giả.
    Ở đây mỗi anten c0/c1/c2/c3 có 64 giá trị số nguyên.
    """
    return [random.randint(-30000, 30000) for _ in range(64)]


def make_packet(device_id: str, seq: int):
    packet = {
        "device_id": device_id,
        "seq": seq,
        "timestamp": int(time.time() * 1_000_000),
        "bw": 20,
        "ch": 157,
        "agc": [0, 0, 0, 0],
        "rssi": [
            random.randint(-80, -30),
            random.randint(-80, -30),
            random.randint(-80, -30),
            random.randint(-80, -30),
        ],
        "csi": {
            "c0": make_csi_array(),
            "c1": make_csi_array(),
            "c2": make_csi_array(),
            "c3": make_csi_array(),
        }
    }

    return packet


async def client_a_sender(client_index: int):
    device_id = DEVICE_IDS[client_index]
    reader, writer = await asyncio.open_connection(A_HOST, A_PORT)

    print(f"[A-CLIENT-{client_index + 1}] Đã kết nối tới server A")

    seq = random.randint(0, 4095)

    try:
        for i in range(PACKETS_PER_CLIENT):
            packet = make_packet(device_id, seq)

            line = json.dumps(packet, ensure_ascii=False) + "\n"
            writer.write(line.encode("utf-8"))
            await writer.drain()

            print(
                f"[A-CLIENT-{client_index + 1}] Gửi packet "
                f"device_id={device_id}, seq={seq}"
            )

            seq = (seq + 1) % 4096

            await asyncio.sleep(0.1)

    finally:
        writer.close()
        await writer.wait_closed()
        print(f"[A-CLIENT-{client_index + 1}] Đã đóng kết nối")


async def client_b_receiver():
    reader, writer = await asyncio.open_connection(B_HOST, B_PORT)

    print("[B-CLIENT] Đã kết nối tới server B, đang chờ dữ liệu forward...")

    total_expected = NUM_A_CLIENTS * PACKETS_PER_CLIENT
    received = 0

    try:
        while received < total_expected:
            line = await reader.readline()

            if not line:
                print("[B-CLIENT] Server B đã đóng kết nối")
                break

            try:
                packet = json.loads(line.decode("utf-8"))

                print(
                    f"[B-CLIENT] Nhận forward: "
                    f"device_id={packet.get('device_id')}, "
                    f"seq={packet.get('seq')}, "
                    f"timestamp={packet.get('timestamp')}, "
                    f"ch={packet.get('ch')}"
                )

            except Exception as e:
                print(f"[B-CLIENT] Lỗi parse JSON: {e}")
                print(line[:200])

            received += 1

    finally:
        writer.close()
        await writer.wait_closed()
        print(f"[B-CLIENT] Đã nhận {received}/{total_expected} gói")


async def main():
    # Kết nối client B trước để không bị mất gói đầu
    b_task = asyncio.create_task(client_b_receiver())

    await asyncio.sleep(0.5)

    a_tasks = [
        asyncio.create_task(client_a_sender(i))
        for i in range(NUM_A_CLIENTS)
    ]

    await asyncio.gather(*a_tasks)
    await b_task


if __name__ == "__main__":
    asyncio.run(main())