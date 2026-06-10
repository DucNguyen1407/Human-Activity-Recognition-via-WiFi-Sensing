import asyncio
import json

B_HOST = "127.0.0.1"
B_PORT = 9100


async def main():
    print(f"[B-CLIENT] Đang kết nối tới server B {B_HOST}:{B_PORT}...")

    reader, writer = await asyncio.open_connection(B_HOST, B_PORT)

    print("[B-CLIENT] Đã kết nối. Đang chờ nhận dữ liệu...")

    try:
        while True:
            line = await reader.readline()

            if not line:
                print("[B-CLIENT] Server đã ngắt kết nối")
                break

            text = line.decode("utf-8", errors="replace").strip()

            print("\n========== GÓI TIN NHẬN ĐƯỢC ==========")
            print(text)

            # Nếu muốn kiểm tra đây có phải JSON hợp lệ không
            try:
                packet = json.loads(text)

                print("---------- THÔNG TIN TÓM TẮT ----------")
                print("device_id :", packet.get("device_id"))
                print("seq       :", packet.get("seq"))
                print("timestamp :", packet.get("timestamp"))
                print("bw        :", packet.get("bw"))
                print("ch        :", packet.get("ch"))
                print("agc       :", packet.get("agc"))
                print("rssi      :", packet.get("rssi"))

                csi = packet.get("csi", {})
                print("csi keys  :", list(csi.keys()))

                for key in ["c0", "c1", "c2", "c3"]:
                    arr = csi.get(key, [])
                    print(f"{key} length :", len(arr))

            except json.JSONDecodeError:
                print("[WARNING] Gói nhận được không phải JSON hợp lệ")

    except KeyboardInterrupt:
        print("\n[B-CLIENT] Dừng bằng Ctrl+C")

    finally:
        writer.close()
        await writer.wait_closed()
        print("[B-CLIENT] Đã đóng kết nối")


if __name__ == "__main__":
    asyncio.run(main())