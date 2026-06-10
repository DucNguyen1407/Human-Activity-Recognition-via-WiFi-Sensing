"""
run_all.py

Chạy chung 2 phần trong 1 lệnh terminal:

1) esp_test1.py
   - TCP 9200: Management / Laptop2
   - TCP 9201: Realtime viewer

2) server2server.py
   - TCP 9001: Server A, nhận tối đa 3 client A
   - TCP 9100: Server B, 1 client nhận dữ liệu forward từ A

Cách chạy:
    python run_all.py

Lưu ý:
- Đặt file này cùng thư mục với esp_test1.py và server2server.py.
- Nếu port nào đang bị chiếm, chương trình tương ứng sẽ báo lỗi bind port.
"""

import asyncio
import threading
import traceback

import esp_test1
import server2server


def start_daemon_thread(name: str, target):
    """Start một server blocking socket trong thread nền."""

    def runner():
        try:
            target()
        except Exception:
            print(f"[{name}] Thread stopped because of error:")
            traceback.print_exc()

    thread = threading.Thread(target=runner, name=name, daemon=True)
    thread.start()
    return thread


async def main():
    print("Starting server2server...")
    start_daemon_thread("server-a-9001", server2server.server_a_thread)
    start_daemon_thread("server-b-9100", server2server.server_b_thread)

    print("Starting ESP Collection server...")
    print("- Management: 127.0.0.1:9200")
    print("- Realtime viewer: 127.0.0.1:9201")
    print("- Server A: 0.0.0.0:9001")
    print("- Server B: 0.0.0.0:9100")
    print("Run all servers OK. Press Ctrl+C to stop.\n")

    await esp_test1.TCPServerApp().run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exit")
