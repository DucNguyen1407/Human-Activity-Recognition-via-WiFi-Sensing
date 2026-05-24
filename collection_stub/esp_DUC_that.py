"""
Laptop 1 - ESP32-Collection TCP Server
======================================

Vai trò:
  ESP32 --UART/COM--> laptop1.py --TCP JSON Lines--> Backend Laptop 2

Phù hợp với backend hiện tại:
  - Laptop 1 chỉ gửi MAC thật trong device_id của gói csi_data.
  - Backend Laptop 2 sẽ map MAC -> esp1/esp2/esp3 trong esp_tcp_client.py.
  - JSON ESP gửi sang backend dùng field "timestamp", không dùng "esp_timestamp_us".
  - CSI gửi dạng 64 cặp Q/I: [[q0, i0], [q1, i1], ...].
  - TCP server mặc định mở 0.0.0.0:9200 để backend Laptop 2 kết nối tới.

Giao thức JSON Lines, mỗi message kết thúc bằng \n.

Backend -> Laptop 1:
  {"type":"get_com_ports"}
  {"type":"uart_control","action":"connect","device_id":"00:1A:2B:3C:4D:5E","com":"COM3","baudrate":115200}
  {"type":"uart_control","action":"disconnect","device_id":"00:1A:2B:3C:4D:5E"}

Laptop 1 -> Backend:
  {"type":"com_list","ports":["COM3","COM4"]}
  {"type":"uart_status","device_id":"00:1A:2B:3C:4D:5E","status":"connected","config":{...}}
  {"type":"uart_status","device_id":"00:1A:2B:3C:4D:5E","status":"disconnected"}
  {"type":"uart_status","device_id":"00:1A:2B:3C:4D:5E","status":"error","message":"..."}
  {"type":"csi_data","device_id":"00:1A:2B:3C:4D:5E","seq":123,"timestamp":...,"radio":{...},"csi":[[q,i],...]}

Cấu trúc frame binary ESP32 gửi qua UART, 155 bytes:
  [0:2]    magic_bytes = 0x55AA  (0x55, 0xAA)
  [2]      packet_length = 155
  [3:26]   Payload Header: MAC(6) Seq(4) ts_us(8) RSSI(1) CH(1) AGC(1) FFT(1) NF(1)
  [26:154] CSI Raw Data: 128 bytes, 64 cặp Q/I xen kẽ int8
  [154]    XOR Checksum: XOR(raw[0:154])
"""

import asyncio
import json
import logging
import struct
from typing import Optional

import serial.tools.list_ports

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ESP32-Collection] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ==========================================================
# CẤU HÌNH TCP / UART
# ==========================================================

# Laptop 1 mở server trên mọi interface để Laptop 2 kết nối qua IP LAN.
TCP_HOST = "0.0.0.0"
TCP_PORT = 9200

# Cho khớp các lựa chọn baudrate trên UI/backend hiện tại.
VALID_BAUDRATES = {115200, 460800, 921600, 1500000}
DEFAULT_BAUDRATE = 115200

# Queue gửi packet về backend. Tăng nếu CSI tốc độ cao.
TX_QUEUE_MAXSIZE = 50_000


# ==========================================================
# CẤU TRÚC FRAME BINARY 155 BYTES
# ==========================================================

HEADER_MAGIC = bytes([0x55, 0xAA])
TOTAL_FRAME_SIZE = 155

# <        : little-endian
# 6s       : MAC 6 bytes
# I        : uint32 seq
# Q        : uint64 timestamp us
# b        : int8 rssi
# B B B    : uint8 channel, agc_gain, fft_gain
# b        : int8 noise_floor
PAYLOAD_HEADER_FMT = "<6sIQbBBBb"
PAYLOAD_HEADER_SIZE = struct.calcsize(PAYLOAD_HEADER_FMT)  # 23 bytes

CSI_DATA_SIZE = 128
PAYLOAD_OFFSET = 3
CSI_OFFSET = PAYLOAD_OFFSET + PAYLOAD_HEADER_SIZE  # 26


# ==========================================================
# FRAME PARSER
# ==========================================================

def calculate_xor_checksum(data: bytes) -> int:
    """Tính XOR checksum trên raw[0:154]."""
    result = 0
    for b in data:
        result ^= b
    return result


def mac_bytes_to_string(mac_b: bytes) -> str:
    """Chuyển 6 bytes MAC thành chuỗi XX:XX:XX:XX:XX:XX."""
    return ":".join(f"{b:02X}" for b in mac_b)


def parse_packet(raw: bytes, fallback_device_id: str | None = None) -> Optional[dict]:
    """
    Parse một frame 155 bytes thành JSON-ready dict.

    Điểm quan trọng:
    - Không map MAC -> esp1 ở Laptop 1.
    - device_id gửi sang backend là MAC thật lấy từ frame ESP32.
    - timestamp dùng key "timestamp" để khớp CSV backend mới.
    - CSI chuyển từ 128 int8 phẳng thành 64 cặp [[q, i], ...].
    """
    if len(raw) != TOTAL_FRAME_SIZE:
        return None

    if raw[:2] != HEADER_MAGIC:
        return None

    if raw[2] != TOTAL_FRAME_SIZE:
        return None

    if calculate_xor_checksum(raw[:-1]) != raw[-1]:
        logger.warning("XOR checksum sai, bỏ frame")
        return None

    try:
        mac_b, seq, ts_us, rssi, ch, agc, fft, nf = struct.unpack_from(
            PAYLOAD_HEADER_FMT,
            raw,
            PAYLOAD_OFFSET,
        )
    except struct.error as e:
        logger.error("Lỗi unpack payload header: %s", e)
        return None

    mac_str = mac_bytes_to_string(mac_b)

    # Nếu frame không có MAC hợp lệ vì firmware test, vẫn fallback theo device_id lệnh connect.
    # Bình thường mac_str luôn có giá trị dạng XX:XX:XX:XX:XX:XX.
    device_id = mac_str or (fallback_device_id or "unknown")

    try:
        csi_raw = struct.unpack_from(f"<{CSI_DATA_SIZE}b", raw, CSI_OFFSET)
    except struct.error as e:
        logger.error("Lỗi unpack CSI raw: %s", e)
        return None

    # 128 số int8 -> 64 cặp Q/I.
    csi_pairs = [
        [int(csi_raw[i]), int(csi_raw[i + 1])]
        for i in range(0, CSI_DATA_SIZE, 2)
    ]

    return {
        "type": "csi_data",
        "device_id": device_id,
        "seq": int(seq),
        "timestamp": int(ts_us),
        "radio": {
            "rssi": int(rssi),
            "channel": int(ch),
            "agc_gain": int(agc),
            "fft_gain": int(fft),
            "noise_floor": int(nf),
        },
        "csi": csi_pairs,
    }


def find_frame(buf: bytearray):
    """
    Tìm một frame hợp lệ trong buffer streaming.

    Trả về:
      (frame_bytes, remaining_buffer)
      hoặc (None, buffer_còn_lại)
    """
    while True:
        start = buf.find(HEADER_MAGIC)

        if start == -1:
            # Không thấy magic, bỏ dữ liệu rác cũ để tránh buffer phình mãi.
            return None, bytearray()

        # Cần ít nhất 3 bytes để đọc length.
        if len(buf) - start < 3:
            return None, buf[start:]

        # Byte length không đúng thì đây là magic giả, dịch 1 byte rồi tìm tiếp.
        if buf[start + 2] != TOTAL_FRAME_SIZE:
            buf = buf[start + 1:]
            continue

        # Chưa đủ nguyên frame thì giữ lại từ magic để đọc tiếp chunk sau.
        if len(buf) - start < TOTAL_FRAME_SIZE:
            return None, buf[start:]

        frame = bytes(buf[start: start + TOTAL_FRAME_SIZE])
        remaining = bytearray(buf[start + TOTAL_FRAME_SIZE:])
        return frame, remaining


# ==========================================================
# SERIAL COLLECTOR - MỖI ESP/COM MỘT COLLECTOR
# ==========================================================

class SerialCollector:
    """
    Đọc CSI từ một COM port và đẩy packet JSON vào tx_queue.

    device_id ở đây là device_id backend gửi xuống khi connect.
    Với backend hiện tại có map ngược esp1 -> MAC, device_id thường sẽ là MAC.
    Nếu backend chưa map ngược, device_id có thể là esp1/esp2/esp3.
    Gói csi_data vẫn ưu tiên device_id là MAC thật lấy từ frame binary.
    """

    def __init__(self, device_id: str, com: str, baudrate: int, tx_queue: asyncio.Queue):
        self.device_id = str(device_id).strip()
        self.com = str(com).strip()
        self.baudrate = int(baudrate)
        self.tx_queue = tx_queue

        self.running = False
        self.task: Optional[asyncio.Task] = None

        self.stats = {
            "total": 0,
            "error": 0,
            "dropped": 0,
            "connected": False,
            "pkt_rate": 0.0,
        }

    async def run(self):
        import serial_asyncio
        import time

        buf = bytearray()
        rate_count = 0
        rate_t0 = time.monotonic()

        try:
            reader, _ = await serial_asyncio.open_serial_connection(
                url=self.com,
                baudrate=self.baudrate,
            )

            self.running = True
            self.stats["connected"] = True
            logger.info("[%s] Đã mở serial %s @ %d", self.device_id, self.com, self.baudrate)

            while self.running:
                chunk = await reader.read(4096)

                if not chunk:
                    logger.warning("[%s] Serial không còn dữ liệu, đóng collector", self.device_id)
                    break

                buf.extend(chunk)

                while True:
                    frame, buf = find_frame(buf)
                    if frame is None:
                        break

                    pkt = parse_packet(frame, fallback_device_id=self.device_id)

                    if pkt is None:
                        self.stats["error"] += 1
                        continue

                    self.stats["total"] += 1
                    rate_count += 1

                    now = time.monotonic()
                    elapsed = now - rate_t0
                    if elapsed >= 1.0:
                        self.stats["pkt_rate"] = round(rate_count / elapsed, 1)
                        rate_count = 0
                        rate_t0 = now

                    try:
                        self.tx_queue.put_nowait(pkt)
                    except asyncio.QueueFull:
                        self.stats["dropped"] += 1

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("[%s] Lỗi serial %s: %s", self.device_id, self.com, e)
            raise
        finally:
            self.running = False
            self.stats["connected"] = False
            self.stats["pkt_rate"] = 0.0
            logger.info("[%s] Serial %s đã đóng", self.device_id, self.com)

    def stop(self):
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()


# ==========================================================
# TCP SERVER APP
# ==========================================================

class TCPServerApp:
    """TCP server cho Backend Laptop 2 kết nối vào."""

    def __init__(self):
        self.collectors: dict[str, SerialCollector] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.tx_queue: asyncio.Queue = asyncio.Queue(maxsize=TX_QUEUE_MAXSIZE)
        self.current_writer: Optional[asyncio.StreamWriter] = None

    async def _send_json(self, writer: asyncio.StreamWriter, message: dict):
        writer.write(json.dumps(message, ensure_ascii=False).encode("utf-8") + b"\n")
        await writer.drain()

    def _list_com_ports(self) -> list[str]:
        ports = [
            p.device
            for p in sorted(serial.tools.list_ports.comports(), key=lambda x: x.device)
        ]
        return ports

    async def _send_com_list(self, writer: asyncio.StreamWriter):
        ports = self._list_com_ports()
        await self._send_json(writer, {"type": "com_list", "ports": ports})
        logger.info("Trả danh sách COM: %s", ports)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        logger.info("Backend Laptop 2 kết nối từ %s", addr)

        # Chỉ cho một backend active. Nếu backend mới kết nối, writer cũ sẽ không dùng nữa.
        self.current_writer = writer

        tx_task = asyncio.create_task(self._send_worker(writer), name="tcp-send-worker")

        try:
            # Gửi COM list ngay khi backend kết nối, giống stub fake cũ.
            await self._send_com_list(writer)

            while True:
                line = await reader.readline()

                if not line:
                    break

                if not line.strip():
                    continue

                try:
                    msg = json.loads(line.decode("utf-8").strip())
                except json.JSONDecodeError as e:
                    logger.warning("JSON không hợp lệ từ backend: %s", e)
                    continue

                await self._process_message(msg, writer)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Lỗi kết nối backend: %s", e)
        finally:
            tx_task.cancel()
            try:
                await tx_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

            if self.current_writer is writer:
                self.current_writer = None

            # Backend đóng TCP thì dừng collectors để nhả COM, tránh đọc không có nơi gửi.
            await self.stop_all_collectors()

            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

            logger.info("Backend Laptop 2 đã ngắt kết nối từ %s", addr)

    async def _send_worker(self, writer: asyncio.StreamWriter):
        """Gửi packet trong tx_queue về backend, gom batch để giảm syscall."""
        max_batch = 32

        while True:
            try:
                msg = await self.tx_queue.get()
                batch = [json.dumps(msg, ensure_ascii=False).encode("utf-8") + b"\n"]

                for _ in range(max_batch - 1):
                    try:
                        msg = self.tx_queue.get_nowait()
                        batch.append(json.dumps(msg, ensure_ascii=False).encode("utf-8") + b"\n")
                    except asyncio.QueueEmpty:
                        break

                writer.writelines(batch)
                await writer.drain()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Send worker dừng: %s", e)
                break

    async def _process_message(self, msg: dict, writer: asyncio.StreamWriter):
        msg_type = msg.get("type")
        action = msg.get("action")

        if msg_type == "get_com_ports" or action == "refresh_com_ports":
            await self._send_com_list(writer)
            return

        if msg_type == "uart_control":
            device_id = str(msg.get("device_id") or msg.get("uartId") or "").strip()
            action = str(msg.get("action") or "").strip()

            if action == "connect":
                com = str(msg.get("com") or msg.get("port") or "").strip()
                baudrate = int(msg.get("baudrate") or msg.get("baudRate") or DEFAULT_BAUDRATE)
                await self._do_connect(device_id, com, baudrate, writer)
                return

            if action == "disconnect":
                await self._do_disconnect(device_id, writer)
                return

            await self._send_json(writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "error",
                "message": f"uart_control action không hợp lệ: {action}",
            })
            return

        logger.warning("Message không hỗ trợ: %s", msg)

    async def _do_connect(
        self,
        device_id: str,
        com: str,
        baudrate: int,
        writer: asyncio.StreamWriter,
    ):
        if not device_id:
            await self._send_json(writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "error",
                "message": "Thiếu device_id",
            })
            return

        if not com:
            await self._send_json(writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "error",
                "message": "Thiếu COM/port",
            })
            return

        if baudrate not in VALID_BAUDRATES:
            await self._send_json(writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "error",
                "message": f"Baudrate {baudrate} không hợp lệ. Chọn: {sorted(VALID_BAUDRATES)}",
            })
            return

        if device_id in self.collectors:
            col = self.collectors[device_id]
            await self._send_json(writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "error",
                "message": f"{device_id} đã kết nối tới {col.com}",
                "config": {"com": col.com, "baudrate": col.baudrate},
            })
            return

        col = SerialCollector(device_id, com, baudrate, self.tx_queue)

        async def _run_and_notify():
            try:
                # Chạy collector. Nếu mở serial lỗi, exception sẽ nhảy xuống except.
                # Nhưng cần báo connected sau khi serial đã mở thành công. Ta báo ngay sau
                # khi collector.stats['connected'] được set true trong run().
                collector_task = asyncio.create_task(col.run(), name=f"serial-{device_id}")
                col.task = collector_task

                # Chờ tối đa 3s để biết serial có mở thành công không.
                for _ in range(300):
                    if collector_task.done():
                        # Nếu task chết sớm, lấy exception để báo lỗi.
                        exc = collector_task.exception()
                        if exc:
                            raise exc
                        break

                    if col.stats.get("connected"):
                        await self._notify_connected(device_id, com, baudrate)
                        break

                    await asyncio.sleep(0.01)
                else:
                    raise RuntimeError(f"Timeout khi mở serial {com}")

                await collector_task

            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error("[%s] Không mở/đọc được %s: %s", device_id, com, e)
                await self._notify_error(device_id, f"Không mở/đọc được {com}: {e}")
            finally:
                self.collectors.pop(device_id, None)
                self.tasks.pop(device_id, None)
                await self._notify_disconnected(device_id)
                logger.info("[%s] Collector đã dừng", device_id)

        task = asyncio.create_task(_run_and_notify(), name=f"collector-{device_id}")
        col.task = task
        self.collectors[device_id] = col
        self.tasks[device_id] = task

    async def _do_disconnect(self, device_id: str, writer: asyncio.StreamWriter):
        if not device_id:
            await self._send_json(writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "error",
                "message": "Thiếu device_id",
            })
            return

        col = self.collectors.get(device_id)
        task = self.tasks.get(device_id)

        if col is not None:
            col.stop()

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        self.collectors.pop(device_id, None)
        self.tasks.pop(device_id, None)

        await self._send_json(writer, {
            "type": "uart_status",
            "device_id": device_id,
            "status": "disconnected",
        })
        logger.info("[%s] Đã ngắt kết nối", device_id)

    async def _notify_connected(self, device_id: str, com: str, baudrate: int):
        if self.current_writer is None:
            return

        await self._send_json(self.current_writer, {
            "type": "uart_status",
            "device_id": device_id,
            "status": "connected",
            "config": {
                "connected": True,
                "com": com,
                "baudrate": baudrate,
            },
        })

    async def _notify_disconnected(self, device_id: str):
        if self.current_writer is None:
            return

        try:
            await self._send_json(self.current_writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "disconnected",
            })
        except Exception:
            pass

    async def _notify_error(self, device_id: str, message: str):
        if self.current_writer is None:
            return

        try:
            await self._send_json(self.current_writer, {
                "type": "uart_status",
                "device_id": device_id,
                "status": "error",
                "message": message,
            })
        except Exception:
            pass

    async def stop_all_collectors(self):
        if not self.collectors and not self.tasks:
            return

        logger.info("Dừng toàn bộ serial collectors...")

        for col in list(self.collectors.values()):
            col.stop()

        tasks = list(self.tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self.collectors.clear()
        self.tasks.clear()

        # Xóa queue cũ để lần kết nối sau không gửi packet tồn.
        while not self.tx_queue.empty():
            try:
                self.tx_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def run(self):
        server = await asyncio.start_server(self.handle_client, TCP_HOST, TCP_PORT)
        sockets = server.sockets or []
        for sock in sockets:
            host, port = sock.getsockname()[:2]
            logger.info("ESP32-Collection TCP server lắng nghe tại %s:%s", host, port)

        try:
            async with server:
                await server.serve_forever()
        finally:
            await self.stop_all_collectors()


async def main():
    app = TCPServerApp()
    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Đã dừng ESP32-Collection")
