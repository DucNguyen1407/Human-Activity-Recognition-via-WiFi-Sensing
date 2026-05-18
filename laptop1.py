"""
Laptop 1 – TCP Server
Cắm trực tiếp các module RS485-USB, lắng nghe lệnh từ Laptop 2.

Thay đổi so với phiên bản cũ:
  • Kiến trúc đảo: Laptop 1 là SERVER, Laptop 2 là CLIENT.
  • get_ports: trả cổng thực tế từ OS (chỉ các cổng RS485 đang cắm).
  • Cấu trúc frame: Header(2) + Length(1) + Payload(23) + CSI(128) + Checksum(1) = 155 bytes.
  • XOR checksum bao phủ raw[0:-1] (toàn frame trừ byte cuối).
"""

import asyncio
import json
import logging
import struct
from dataclasses import asdict, dataclass
from typing import Optional

import serial.tools.list_ports

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [L1] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# CẤU HÌNH
# ══════════════════════════════════════════════════════════

TCP_HOST = "0.0.0.0"
TCP_PORT = 8888

# Baudrate hợp lệ – chỉ chấp nhận 2 giá trị này
VALID_BAUDRATES = {460800, 921600}
DEFAULT_BAUDRATE = 921600

# ══════════════════════════════════════════════════════════
# CẤU TRÚC FRAME (155 bytes)
#
#  [0:2]   Header magic  = 0xAA55
#  [2]     Length        = 155  (uint8, tổng số byte toàn frame)
#  [3:26]  Payload Header: MAC(6) Seq(4) ts_us(8) RSSI(1) CH(1) AGC(1) FFT(1) NF(1)
#  [26:154] CSI Raw Data : 128 bytes, 64 cặp (Q,I) xen kẽ int8
#  [154]   XOR Checksum : XOR(raw[0:154])
# ══════════════════════════════════════════════════════════

HEADER_MAGIC       = b'\xAA\x55'
TOTAL_FRAME_SIZE   = 155
PAYLOAD_HEADER_FMT  = "<6sIQbBBBb"
PAYLOAD_HEADER_SIZE = struct.calcsize(PAYLOAD_HEADER_FMT)  # 23 bytes
CSI_DATA_SIZE       = 128
PAYLOAD_OFFSET      = 3    # sau Header(2) + Length(1)
CSI_OFFSET          = PAYLOAD_OFFSET + PAYLOAD_HEADER_SIZE  # = 26


@dataclass
class CSIPacket:
    device_id:     str
    port:          str
    monitor_mac:   str
    sequence:      int
    timestamp_us:  int
    rssi:          int
    channel:       int
    agc_gain:      int
    fft_gain:      int
    noise_floor:   int
    i_array:       list
    q_array:       list
    n_subcarriers: int


# ══════════════════════════════════════════════════════════
# PARSE & FRAME SYNC
# ══════════════════════════════════════════════════════════

def calculate_xor_checksum(data: bytes) -> int:
    """XOR tất cả bytes trong data. Áp dụng trên raw[0:154]."""
    result = 0
    for b in data:
        result ^= b
    return result


def parse_packet(raw: bytes, device_id: str, port: str) -> Optional[CSIPacket]:
    """
    Xác thực và parse một frame 155 bytes.
    Trả None nếu bất kỳ bước nào thất bại.
    """
    # 1. Kích thước và Header magic
    if len(raw) != TOTAL_FRAME_SIZE or raw[:2] != HEADER_MAGIC:
        return None

    # 2. Trường Length tại byte [2]
    if raw[2] != TOTAL_FRAME_SIZE:
        return None

    # 3. XOR checksum: tính trên raw[0:154], so với raw[154]
    if calculate_xor_checksum(raw[:-1]) != raw[-1]:
        logger.warning("[%s] XOR checksum sai – gói bị nhiễu", device_id)
        return None

    # 4. Unpack Payload Header
    try:
        mac_b, seq, ts, rssi, ch, agc, fft, nf = struct.unpack_from(
            PAYLOAD_HEADER_FMT, raw, PAYLOAD_OFFSET
        )
    except struct.error as e:
        logger.error("[%s] Lỗi unpack header: %s", device_id, e)
        return None

    mac_str = ":".join(f"{b:02X}" for b in mac_b)

    # 5. Unpack CSI raw data – 128 int8 xen kẽ [Q0,I0,Q1,I1,...]
    csi_raw = struct.unpack_from(f"<{CSI_DATA_SIZE}b", raw, CSI_OFFSET)
    q_array = [csi_raw[i]     for i in range(0, CSI_DATA_SIZE, 2)]
    i_array = [csi_raw[i + 1] for i in range(0, CSI_DATA_SIZE, 2)]

    return CSIPacket(
        device_id=device_id, port=port, monitor_mac=mac_str,
        sequence=seq, timestamp_us=ts, rssi=rssi, channel=ch,
        agc_gain=agc, fft_gain=fft, noise_floor=nf,
        i_array=i_array, q_array=q_array, n_subcarriers=len(i_array),
    )


def find_frame(buf: bytearray):
    """
    Tìm frame hợp lệ trong buffer streaming.
    Dùng Header magic + Length field để xác định ranh giới – không dùng Footer.
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
    Đọc CSI từ một cổng COM bất đồng bộ, đẩy vào out_queue dưới dạng JSON.
    Thống kê: total (gói OK), error (lỗi parse/checksum), connected.
    """

    def __init__(self, device_id: str, port: str, baudrate: int,
                 out_queue: asyncio.Queue):
        self.device_id = device_id
        self.port      = port
        self.baudrate  = baudrate
        self.out_queue = out_queue
        self.running   = False
        self.stats     = {"total": 0, "error": 0, "connected": False}

    async def run(self):
        import serial_asyncio
        self.running = True
        buf = bytearray()
        try:
            reader, _ = await serial_asyncio.open_serial_connection(
                url=self.port, baudrate=self.baudrate
            )
            self.stats["connected"] = True
            logger.info("[%s] Serial mở @ %d baud", self.port, self.baudrate)

            while self.running:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buf.extend(chunk)
                while True:
                    frame, buf = find_frame(buf)
                    if frame is None:
                        break
                    pkt = parse_packet(frame, self.device_id, self.port)
                    if pkt:
                        self.stats["total"] += 1
                        try:
                            self.out_queue.put_nowait({"type": "csi_data",
                                                       "payload": asdict(pkt)})
                        except asyncio.QueueFull:
                            pass
                    else:
                        self.stats["error"] += 1
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("[%s] Lỗi serial: %s", self.port, e)
        finally:
            self.running = False
            self.stats["connected"] = False
            logger.info("[%s] Serial đã đóng", self.port)

    def stop(self):
        self.running = False


# ══════════════════════════════════════════════════════════
# TCP SERVER APP – NHẬN LỆNH TỪ LAPTOP 2
# ══════════════════════════════════════════════════════════

class TCPServerApp:
    """
    Laptop 1 là TCP Server.
    Laptop 2 kết nối vào, gửi lệnh JSON, nhận dữ liệu CSI + phản hồi.

    Luồng dữ liệu:
      ESP32 → Serial → SerialCollector → tx_queue → send_data() → TCP → Laptop 2
    Luồng lệnh:
      Laptop 2 → TCP → process_command() → SerialCollector.run()/stop()
    """

    def __init__(self):
        self.collectors: dict[str, SerialCollector] = {}
        self.tasks:      dict[str, asyncio.Task]    = {}
        self.tx_queue    = asyncio.Queue(maxsize=10_000)
        self.current_writer = None   # Writer của kết nối hiện tại

    # ── Xử lý một kết nối từ Laptop 2 ────────────────────

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        logger.info("Laptop 2 kết nối từ %s", addr)
        self.current_writer = writer

        # Task song song: gửi dữ liệu CSI lên TCP
        tx_task = asyncio.create_task(self._send_worker(writer))

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    cmd_data = json.loads(line.decode().strip())
                    await self._process_command(cmd_data, writer)
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.error("Lỗi kết nối client: %s", e)
        finally:
            tx_task.cancel()
            self.current_writer = None
            logger.info("Laptop 2 ngắt kết nối từ %s", addr)
            writer.close()
            await writer.wait_closed()

    async def _send_worker(self, writer: asyncio.StreamWriter):
        """Lấy message từ tx_queue và gửi qua TCP."""
        while True:
            try:
                msg = await self.tx_queue.get()
                writer.write(json.dumps(msg).encode() + b"\n")
                await writer.drain()
            except asyncio.CancelledError:
                break
            except Exception:
                break

    # ── Xử lý lệnh ───────────────────────────────────────

    async def _process_command(self, cmd_data: dict,
                                writer: asyncio.StreamWriter):
        cmd    = cmd_data.get("cmd")
        req_id = cmd_data.get("req_id")
        res    = {"req_id": req_id, "type": "response", "status": "ok"}

        # get_ports: trả tất cả cổng serial đang hiện diện trên hệ thống.
        # Frontend chỉ gọi sau khi người dùng cắm đủ 3 module RS485-USB.
        if cmd == "get_ports":
            ports = serial.tools.list_ports.comports()
            res["data"] = [
                {"port": p.device, "desc": p.description or ""}
                for p in sorted(ports, key=lambda x: x.device)
            ]

        # get_baudrates: trả 2 giá trị cố định + default
        elif cmd == "get_baudrates":
            res["data"] = {
                "baudrates": sorted(VALID_BAUDRATES),
                "default":   DEFAULT_BAUDRATE,
            }

        # start: khởi động collector trên một cổng
        elif cmd == "start":
            port    = cmd_data.get("port", "")
            baudrate = int(cmd_data.get("baudrate", DEFAULT_BAUDRATE))

            if baudrate not in VALID_BAUDRATES:
                res["status"] = "error"
                res["msg"]    = f"Baudrate {baudrate} không hợp lệ. Chọn: {sorted(VALID_BAUDRATES)}"
            elif port in self.collectors:
                res["status"] = "error"
                res["msg"]    = f"Cổng {port} đã có collector đang chạy"
            else:
                dev_id = f"ESP32_{port.replace('/', '_')}"
                col    = SerialCollector(dev_id, port, baudrate, self.tx_queue)
                task   = asyncio.create_task(col.run(), name=f"col-{port}")
                self.collectors[port] = col
                self.tasks[port]      = task
                res["msg"] = f"Đã khởi động {port} @ {baudrate} baud"
                logger.info("start: %s @ %d", port, baudrate)

        # stop: dừng collector của một cổng
        elif cmd == "stop":
            port = cmd_data.get("port", "")
            if port not in self.collectors:
                res["status"] = "error"
                res["msg"]    = f"Cổng {port} không có collector đang chạy"
            else:
                col  = self.collectors.pop(port)
                task = self.tasks.pop(port)
                col.stop()
                task.cancel()
                res["msg"] = f"Đã dừng {port}"
                logger.info("stop: %s", port)

        # status: trả thống kê tất cả collector
        elif cmd == "status":
            res["data"] = {
                p: c.stats for p, c in self.collectors.items()
            }

        else:
            res["status"] = "error"
            res["msg"]    = f"Lệnh không hợp lệ: '{cmd}'"

        writer.write(json.dumps(res).encode() + b"\n")
        await writer.drain()

    # ── Entry point ───────────────────────────────────────

    async def run(self):
        server = await asyncio.start_server(
            self.handle_client, TCP_HOST, TCP_PORT
        )
        addr = server.sockets[0].getsockname()
        logger.info("Laptop 1 (Server) lắng nghe tại %s:%d", *addr)
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(TCPServerApp().run())