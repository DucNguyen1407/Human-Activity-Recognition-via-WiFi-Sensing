"""
Laptop 1 - ESP32-Collection TCP Server
=======================================

Kiến trúc:
  ESP32 --UART--> laptop1.py --TCP JSON Lines--> laptop2.py

Giao thức JSON Lines (mỗi message một dòng kết thúc '\\n'):

  Nhận từ Management (laptop2):
    {"type":"get_com_ports"}
    {"type":"uart_control","action":"connect","device_id":"esp1","com":"COM3","baudrate":115200}
    {"type":"uart_control","action":"disconnect","device_id":"esp1"}

  Gửi về Management (laptop2):
    {"type":"com_list","ports":["COM3","COM4"]}
    {"type":"uart_status","device_id":"esp1","status":"connected","config":{...}}
    {"type":"uart_status","device_id":"esp1","status":"disconnected"}
    {"type":"uart_status","device_id":"esp1","status":"error","message":"..."}
    {"type":"csi_data","device_id":"esp1","seq":123,...}

Cấu trúc frame binary (155 bytes):
  [0:2]    magic_bytes = 0xAA55  (bytes: 0xAA, 0x55)
  [2]      packet_length = 155
  [3:26]   Payload Header: MAC(6) Seq(4) ts_us(8) RSSI(1) CH(1) AGC(1) FFT(1) NF(1)
  [26:154] CSI Raw Data: 128 bytes, 64 cặp (Q,I) xen kẽ int8
  [154]    XOR Checksum: XOR(raw[0:154])
"""

import asyncio
import json
import logging
import struct
from typing import Optional

import serial.tools.list_ports

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [Collection] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# CẤU HÌNH
# ══════════════════════════════════════════════════════════

TCP_HOST = "127.0.0.1"
TCP_PORT = 9200


# Baudrate hợp lệ
VALID_BAUDRATES = {115200, 460800, 921600}
DEFAULT_BAUDRATE = 115200

# ══════════════════════════════════════════════════════════
# MAP MAC → device_id
# Điền địa chỉ MAC thực tế của từng ESP32 vào đây.
# Key: chuỗi MAC dạng "XX:XX:XX:XX:XX:XX" (chữ hoa)
# Value: "esp1" | "esp2" | "esp3"
# ══════════════════════════════════════════════════════════

# bỏ mapping này đi, backend sẽ dùng MAC thật từ frame ESP, client sẽ map về esp1/esp2/esp3 nếu cần.
MAC_TO_DEVICE: dict[str, str] = {
    "AA:BB:CC:DD:EE:01": "esp1",
    "AA:BB:CC:DD:EE:02": "esp2",
    "AA:BB:CC:DD:EE:03": "esp3",
}

# ══════════════════════════════════════════════════════════
# CẤU TRÚC FRAME (155 bytes)
# ══════════════════════════════════════════════════════════

HEADER_MAGIC        = bytes([0xAA, 0x55])   # magic_bytes thực tế từ ESP: AA 55
TOTAL_FRAME_SIZE    = 155
PAYLOAD_HEADER_FMT  = "<6sIQbBBBb"
PAYLOAD_HEADER_SIZE = struct.calcsize(PAYLOAD_HEADER_FMT)  # 23 bytes
CSI_DATA_SIZE       = 128
PAYLOAD_OFFSET      = 3     # sau Header(2) + Length(1)
CSI_OFFSET          = PAYLOAD_OFFSET + PAYLOAD_HEADER_SIZE  # = 26


# Bật/tắt log từng gói CSI. Nếu packet rate cao, log sẽ rất nhiều.
LOG_CSI_PACKETS = True


def log_packet_event(stage: str, pkt: dict, raw_len: int | None = None):
    """
    Log ngắn gọn mỗi khi một gói CSI được nhận từ UART hoặc gửi qua TCP.

    stage:
      - "RX UART": vừa parse được frame từ cổng COM
      - "TX TCP" : vừa gửi JSON line sang Laptop 2
    """
    if not LOG_CSI_PACKETS:
        return

    if not isinstance(pkt, dict) or pkt.get("type") != "csi_data":
        return

    radio = pkt.get("radio") or {}
    csi = pkt.get("csi") or []

    logger.info(
        "%s | device=%s seq=%s ts_us=%s raw_len=%s csi_len=%s rssi=%s ch=%s nf=%s",
        stage,
        pkt.get("device_id"),
        pkt.get("seq"),
        pkt.get("esp_timestamp_us"),
        raw_len if raw_len is not None else "-",
        len(csi),
        radio.get("rssi"),
        radio.get("channel"),
        radio.get("noise_floor"),
    )


# ══════════════════════════════════════════════════════════
# PARSE & FRAME SYNC
# ══════════════════════════════════════════════════════════

def calculate_xor_checksum(data: bytes) -> int:
    """XOR tất cả bytes trong data. Áp dụng trên raw[0:154]."""
    result = 0
    for b in data:
        result ^= b
    return result


# def parse_packet(raw: bytes, device_id: str) -> Optional[dict]:
#     """
#     Xác thực và parse một frame 155 bytes.
#     Trả None nếu bất kỳ bước nào thất bại.
#     Trả dict JSON-ready nếu thành công.
#     """
#     # 1. Kích thước
#     if len(raw) != TOTAL_FRAME_SIZE:
#         return None

#     # 2. Check magic_bytes == 0xAA55
#     if raw[:2] != HEADER_MAGIC:
#         return None

#     # 3. Check packet_length == 155
#     if raw[2] != TOTAL_FRAME_SIZE:
#         return None

#     # 4. Check XOR checksum
#     if calculate_xor_checksum(raw[:-1]) != raw[-1]:
#         logger.warning("[%s] XOR checksum sai – gói bị nhiễu", device_id)
#         return None

#     # 5. Unpack Payload Header
#     try:
#         mac_b, seq, ts_us, rssi, ch, agc, fft, nf = struct.unpack_from(
#             PAYLOAD_HEADER_FMT, raw, PAYLOAD_OFFSET
#         )
#     except struct.error as e:
#         logger.error("[%s] Lỗi unpack header: %s", device_id, e)
#         return None

#     # 6. Lấy monitor_mac → map ra device_id nếu cần override
#     mac_str = ":".join(f"{b:02X}" for b in mac_b)
#     mapped_id = MAC_TO_DEVICE.get(mac_str, device_id)

#     # 7. Unpack CSI raw data – 128 int8 xen kẽ [Q0,I0,Q1,I1,...]
#     csi_raw = struct.unpack_from(f"<{CSI_DATA_SIZE}b", raw, CSI_OFFSET)
#     # Flatten thành list 128 phần tử: [Q0, I0, Q1, I1, ...]
#     csi_list = list(csi_raw)

#     return {
#         "type":           "csi_data",
#         "device_id":      mapped_id,
#         "seq":            seq,
#         "esp_timestamp_us": ts_us,
#         "radio": {
#             "rssi":        rssi,
#             "channel":     ch,
#             "agc_gain":    agc,
#             "fft_gain":    fft,
#             "noise_floor": nf,
#         },
#         "csi": csi_list,   # 128 phần tử = 64 cặp Q/I
#     }
def parse_packet(raw: bytes, device_id: str) -> Optional[dict]:
    """
    Xác thực và parse một frame 155 bytes.
    Trả None nếu bất kỳ bước nào thất bại.
    Trả dict JSON-ready nếu thành công.

    Lưu ý:
    - device_id gửi sang backend là MAC thật lấy từ frame ESP.
    - Backend/client esp_tcp_client.py sẽ map MAC -> esp1/esp2/esp3.
    """
    # 1. Kích thước
    if len(raw) != TOTAL_FRAME_SIZE:
        return None

    # 2. Check magic_bytes == 0xAA55
    if raw[:2] != HEADER_MAGIC:
        return None

    # 3. Check packet_length == 155
    if raw[2] != TOTAL_FRAME_SIZE:
        return None

    # 4. Check XOR checksum
    if calculate_xor_checksum(raw[:-1]) != raw[-1]:
        logger.warning("[%s] XOR checksum sai – gói bị nhiễu", device_id)
        return None

    # 5. Unpack Payload Header
    try:
        mac_b, seq, ts_us, rssi, ch, agc, fft, nf = struct.unpack_from(
            PAYLOAD_HEADER_FMT, raw, PAYLOAD_OFFSET
        )
    except struct.error as e:
        logger.error("[%s] Lỗi unpack header: %s", device_id, e)
        return None

    # 6. Lấy MAC thật từ frame ESP
    mac_str = ":".join(f"{b:02X}" for b in mac_b)

    # 7. Unpack CSI raw data – 128 int8 xen kẽ [Q0,I0,Q1,I1,...]
    csi_raw = struct.unpack_from(f"<{CSI_DATA_SIZE}b", raw, CSI_OFFSET)

    # Đổi thành 64 cặp Q/I: [[Q0,I0], [Q1,I1], ..., [Q63,I63]]
    csi_pairs = [
        [int(csi_raw[i]), int(csi_raw[i + 1])]
        for i in range(0, CSI_DATA_SIZE, 2)
    ]
    return {
        "type": "csi_data",
        "device_id": mac_str,   # gửi MAC thật sang backend/client
        "seq": seq,
        "timestamp": ts_us,
        "radio": {
            "rssi": rssi,
            "channel": ch,
            "agc_gain": agc,
            "fft_gain": fft,
            "noise_floor": nf,
        },
        "csi": csi_pairs,
    }


def find_frame(buf: bytearray):
    """
    Tìm frame hợp lệ trong buffer streaming.
    Dùng Header magic + Length field để xác định ranh giới.
    """
    while True:
        start = buf.find(HEADER_MAGIC)
        if start == -1:
            return None, bytearray()
        # Cần ít nhất 3 bytes để đọc Length
        if len(buf) - start < 3:
            return None, buf[start:]
        # Byte [2] là Length – phải đúng bằng TOTAL_FRAME_SIZE
        if buf[start + 2] != TOTAL_FRAME_SIZE:
            buf = buf[start + 1:]   # Header giả → skip 1 byte, tìm lại
            continue
        # Chờ đủ dữ liệu
        if len(buf) - start < TOTAL_FRAME_SIZE:
            return None, buf[start:]
        frame     = bytes(buf[start: start + TOTAL_FRAME_SIZE])
        remaining = bytearray(buf[start + TOTAL_FRAME_SIZE:])
        return frame, remaining


# ══════════════════════════════════════════════════════════
# SERIAL COLLECTOR – ĐỌC MỘT CỔNG COM
# ══════════════════════════════════════════════════════════

class SerialCollector:
    """
    Đọc CSI từ một cổng COM bất đồng bộ.
    Đẩy JSON dict vào out_queue để gửi TCP về Management.
    """

    def __init__(self, device_id: str, com: str, baudrate: int,
                 out_queue: asyncio.Queue):
        self.device_id = device_id
        self.com       = com
        self.baudrate  = baudrate
        self.out_queue = out_queue
        self.running   = False
        self.stats     = {
            "total": 0, "error": 0, "dropped": 0,
            "connected": False, "pkt_rate": 0.0,
        }
        self._task: Optional[asyncio.Task] = None

    async def run(self):
        import serial_asyncio
        import time
        self.running = True
        buf = bytearray()
        rate_count = 0
        rate_t0 = time.monotonic()

        try:
            reader, _ = await serial_asyncio.open_serial_connection(
                url=self.com, baudrate=self.baudrate
            )
            self.stats["connected"] = True
            logger.info("[%s] Serial mở %s @ %d baud",
                        self.device_id, self.com, self.baudrate)

            while self.running:
                # logger.debug("[%s] Đang chờ dữ liệu từ %s...", self.device_id, self.com)
                chunk = await reader.read(1)
                if not chunk:
                    break
                logger.info("[%s] RAW UART chunk len=%d hex=%s", self.device_id, len(chunk), chunk[:64].hex(" "))
                buf.extend(chunk)

                while True:
                    frame, buf = find_frame(buf)
                    if frame is None:
                        break
                    pkt = parse_packet(frame, self.device_id)
                    if pkt:
                        log_packet_event("RX UART", pkt, raw_len=len(frame))
                        self.stats["total"] += 1
                        rate_count += 1
                        # Cập nhật packet rate mỗi giây
                        now = time.monotonic()
                        elapsed = now - rate_t0
                        if elapsed >= 1.0:
                            self.stats["pkt_rate"] = round(rate_count / elapsed, 1)
                            rate_count = 0
                            rate_t0 = now
                        try:
                            self.out_queue.put_nowait(pkt)
                        except asyncio.QueueFull:
                            self.stats["dropped"] += 1
                    else:
                        self.stats["error"] += 1

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
        if self._task and not self._task.done():
            self._task.cancel()


# ══════════════════════════════════════════════════════════
# TCP SERVER APP – NHẬN LỆNH TỪ MANAGEMENT (LAPTOP 2)
# ══════════════════════════════════════════════════════════

class TCPServerApp:
    """
    Laptop 1 là TCP Server.
    Laptop 2 kết nối vào, gửi lệnh JSON Lines, nhận csi_data + status.

    Giao thức nhận:
      {"type":"get_com_ports"}
      {"type":"uart_control","action":"connect","device_id":"esp1","com":"COM3","baudrate":115200}
      {"type":"uart_control","action":"disconnect","device_id":"esp1"}

    Giao thức gửi:
      {"type":"com_list","ports":[...]}
      {"type":"uart_status","device_id":"esp1","status":"connected","config":{...}}
      {"type":"uart_status","device_id":"esp1","status":"disconnected"}
      {"type":"uart_status","device_id":"esp1","status":"error","message":"..."}
      {"type":"csi_data",...}
    """

    def __init__(self):
        # device_id → SerialCollector
        self.collectors: dict[str, SerialCollector] = {}
        self.tasks:      dict[str, asyncio.Task]    = {}
        # Queue gửi dữ liệu TCP về Management
        self.tx_queue    = asyncio.Queue(maxsize=10_000)
        self.current_writer: Optional[asyncio.StreamWriter] = None

    # ── Kết nối từ Management ─────────────────────────────

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        logger.info("Management kết nối từ %s", addr)
        self.current_writer = writer

        # Task gửi dữ liệu CSI về TCP
        tx_task = asyncio.create_task(self._send_worker(writer))

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line.decode().strip())
                    await self._process_message(msg, writer)
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.error("Lỗi kết nối Management: %s", e)
        finally:
            tx_task.cancel()
            self.current_writer = None
            logger.info("Management ngắt kết nối từ %s", addr)
            writer.close()
            await writer.wait_closed()

    # ── Send worker (batch flush) ─────────────────────────

    async def _send_worker(self, writer: asyncio.StreamWriter):
        """Gom nhiều gói từ tx_queue rồi flush 1 lần – giảm syscall."""
        MAX_BATCH = 32
        while True:
            try:
                msg = await self.tx_queue.get()
                msgs = [msg]
                batch = [json.dumps(msg, ensure_ascii=False).encode() + b"\n"]

                for _ in range(MAX_BATCH - 1):
                    try:
                        msg = self.tx_queue.get_nowait()
                        msgs.append(msg)
                        batch.append(json.dumps(msg, ensure_ascii=False).encode() + b"\n")
                    except asyncio.QueueEmpty:
                        break

                writer.writelines(batch)
                await writer.drain()

                for sent_msg in msgs:
                    log_packet_event("TX TCP", sent_msg)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Send worker dừng: %s", e)
                break

    # ── Xử lý lệnh từ Management ─────────────────────────

    async def _process_message(self, msg: dict,
                                writer: asyncio.StreamWriter):
        msg_type = msg.get("type")

        # File 01 – Management yêu cầu danh sách COM
        if msg_type == "get_com_ports":
            ports = [p.device for p in sorted(
                serial.tools.list_ports.comports(),
                key=lambda x: x.device
            )]
            resp = {"type": "com_list", "ports": ports}
            writer.write(json.dumps(resp, ensure_ascii=False).encode() + b"\n")
            await writer.drain()
            logger.info("Trả danh sách COM: %s", ports)

        # File 03/06 – Management điều khiển UART
        elif msg_type == "uart_control":
            action    = msg.get("action")
            device_id = msg.get("device_id", "")

            # File 03 – connect
            if action == "connect":
                com      = msg.get("com", "")
                baudrate = int(msg.get("baudrate", DEFAULT_BAUDRATE))
                await self._do_connect(device_id, com, baudrate, writer)

            # File 06 – disconnect
            elif action == "disconnect":
                await self._do_disconnect(device_id, writer)

            else:
                logger.warning("uart_control action không hợp lệ: %s", action)

        else:
            logger.warning("Loại message không hợp lệ: %s", msg_type)

    # ── Connect một ESP ───────────────────────────────────

    async def _do_connect(self, device_id: str, com: str, baudrate: int,
                          writer: asyncio.StreamWriter):
        """File 04 – báo connected | File 08 – báo error."""

        # Kiểm tra device đã connect chưa
        if device_id in self.collectors:
            err = {
                "type": "uart_status", "device_id": device_id,
                "status": "error",
                "message": f"{device_id} đã kết nối tới {self.collectors[device_id].com}",
            }
            writer.write(json.dumps(err, ensure_ascii=False).encode() + b"\n")
            await writer.drain()
            return

        if baudrate not in VALID_BAUDRATES:
            err = {
                "type": "uart_status", "device_id": device_id,
                "status": "error",
                "message": f"Baudrate {baudrate} không hợp lệ. Chọn: {sorted(VALID_BAUDRATES)}",
            }
            writer.write(json.dumps(err, ensure_ascii=False).encode() + b"\n")
            await writer.drain()
            return

        col  = SerialCollector(device_id, com, baudrate, self.tx_queue)

        async def _run_and_notify():
            try:
                # Thử mở serial – nếu lỗi sẽ raise ngay
                import serial_asyncio
                import time
                buf = bytearray()
                rate_count = 0
                rate_t0 = time.monotonic()

                reader, _ = await serial_asyncio.open_serial_connection(
                    url=com, baudrate=baudrate
                )
                col.stats["connected"] = True
                col.running = True

                # File 04 – báo connect thành công
                ok = {
                    "type":      "uart_status",
                    "device_id": device_id,
                    "status":    "connected",
                    "config":    {"com": com, "baudrate": baudrate},
                }
                if self.current_writer:
                    self.current_writer.write(
                        json.dumps(ok, ensure_ascii=False).encode() + b"\n"
                    )
                    await self.current_writer.drain()
                logger.info("[%s] Kết nối %s @ %d baud", device_id, com, baudrate)

                # Vòng đọc dữ liệu
                while col.running:
                    logger.debug("[%s] Đang chờ dữ liệu từ %s...", device_id, com)
                    chunk = await reader.read(4096)
                    if not chunk:
                        break
                    buf.extend(chunk)

                    while True:
                        frame, buf = find_frame(buf)
                        if frame is None:
                            break
                        pkt = parse_packet(frame, device_id)
                        if pkt:
                            log_packet_event("RX UART", pkt, raw_len=len(frame))
                            col.stats["total"] += 1
                            rate_count += 1
                            now = time.monotonic()
                            elapsed = now - rate_t0
                            if elapsed >= 1.0:
                                col.stats["pkt_rate"] = round(rate_count / elapsed, 1)
                                rate_count = 0
                                rate_t0 = now
                            try:
                                self.tx_queue.put_nowait(pkt)
                            except asyncio.QueueFull:
                                col.stats["dropped"] += 1
                        else:
                            col.stats["error"] += 1

            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error("[%s] Lỗi serial %s: %s", device_id, com, e)
                # File 08 – báo lỗi
                err_msg = {
                    "type":      "uart_status",
                    "device_id": device_id,
                    "status":    "error",
                    "message":   f"Không mở được {com}: {e}",
                }
                if self.current_writer:
                    try:
                        self.current_writer.write(
                            json.dumps(err_msg, ensure_ascii=False).encode() + b"\n"
                        )
                        await self.current_writer.drain()
                    except Exception:
                        pass
                # Dọn dẹp
                self.collectors.pop(device_id, None)
                self.tasks.pop(device_id, None)
                return
            finally:
                col.running = False
                col.stats["connected"] = False
                col.stats["pkt_rate"] = 0.0

            # Vòng lặp kết thúc bình thường (bị cancel hoặc stop)
            # → File 07: báo disconnected
            disc = {
                "type":      "uart_status",
                "device_id": device_id,
                "status":    "disconnected",
            }
            if self.current_writer:
                try:
                    self.current_writer.write(
                        json.dumps(disc, ensure_ascii=False).encode() + b"\n"
                    )
                    await self.current_writer.drain()
                except Exception:
                    pass
            self.collectors.pop(device_id, None)
            self.tasks.pop(device_id, None)
            logger.info("[%s] Collector đã dừng", device_id)

        task = asyncio.create_task(_run_and_notify(), name=f"col-{device_id}")
        col._task = task
        self.collectors[device_id] = col
        self.tasks[device_id]      = task

    # ── Disconnect một ESP ────────────────────────────────

    async def _do_disconnect(self, device_id: str,
                              writer: asyncio.StreamWriter):
        """File 06/07 – disconnect theo yêu cầu."""
        col  = self.collectors.pop(device_id, None)
        task = self.tasks.pop(device_id, None)

        if col is None:
            # Gửi trạng thái disconnected dù không tìm thấy
            disc = {
                "type":      "uart_status",
                "device_id": device_id,
                "status":    "disconnected",
            }
            writer.write(json.dumps(disc, ensure_ascii=False).encode() + b"\n")
            await writer.drain()
            return

        col.stop()
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        # File 07 – báo disconnected
        disc = {
            "type":      "uart_status",
            "device_id": device_id,
            "status":    "disconnected",
        }
        writer.write(json.dumps(disc, ensure_ascii=False).encode() + b"\n")
        await writer.drain()
        logger.info("[%s] Đã ngắt kết nối", device_id)

    # ── Entry point ───────────────────────────────────────

    async def run(self):
        server = await asyncio.start_server(
            self.handle_client, TCP_HOST, TCP_PORT
        )
        addr = server.sockets[0].getsockname()
        logger.info("Collection Server lắng nghe tại %s:%d", *addr)
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(TCPServerApp().run())