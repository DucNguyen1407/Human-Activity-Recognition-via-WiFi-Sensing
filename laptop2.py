"""
Laptop 2 – FastAPI Server + TCP Client + Frontend tích hợp
===========================================================

Kiến trúc:
  ┌──────────────────────────────────────────────────────┐
  │                     Laptop 2                         │
  │                                                      │
  │  Browser ──HTTP──► FastAPI                           │
  │                      │                               │
  │              ┌───────┴────────┐                      │
  │              │  API Routes    │  (các hàm bên dưới)  │
  │              └───────┬────────┘                      │
  │                      │                               │
  │              Laptop2Client (singleton)               │
  │                      │                               │
  │                TCP ──┼──► Laptop 1 (Server)          │
  │                      │         ↑ lệnh JSON           │
  │                      │         ↓ csi_data + response │
  └──────────────────────────────────────────────────────┘

Chạy:
    pip install fastapi uvicorn
    python laptop2.py
    Mở trình duyệt: http://localhost:8000

Các hàm API (cho FastAPI router khác dùng):
    get_ports()       → list cổng COM từ Laptop 1
    get_baudrates()   → {baudrates, default}
    connect()         → kết nối TCP tới Laptop 1
    disconnect()      → ngắt TCP
    start_csi(port, baud) → ra lệnh Laptop 1 mở serial
    stop_csi(port)    → ra lệnh Laptop 1 dừng serial
    get_status()      → trạng thái collector + connection
    get_latest(port)  → CSIPacket mới nhất của 1 cổng
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [L2] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# CẤU HÌNH
# ══════════════════════════════════════════════════════════

LAPTOP1_IP   = "192.168.1.137"   # ← Đổi thành IP thực của Laptop 1
LAPTOP1_PORT = 8888
RPC_TIMEOUT  = 5.0               # Giây chờ phản hồi RPC


# ══════════════════════════════════════════════════════════
# DATACLASS CSIPacket (khớp với Laptop 1)
# ══════════════════════════════════════════════════════════

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
# LAPTOP2 CLIENT – QUẢN LÝ KẾT NỐI TCP TỚI LAPTOP 1
# ══════════════════════════════════════════════════════════

class Laptop2Client:
    """
    Quản lý kết nối TCP tới Laptop 1.

    Kết nối KHÔNG tự động – người dùng bấm "Connect" ở frontend.
    Các hàm rpc() chỉ hoạt động khi đã kết nối.

    Trạng thái:
      connected   : True khi TCP đang mở và reader/writer sẵn sàng.
      _writer     : StreamWriter hiện tại, None nếu chưa kết nối.
      _listen_task: Task đọc dữ liệu từ Laptop 1 (chạy khi connected).
    """

    def __init__(self):
        # Hàng đợi CSI packets cho module AI (module 3.3.3)
        self.queue: asyncio.Queue[CSIPacket] = asyncio.Queue(maxsize=10_000)
        # Gói mới nhất từ mỗi cổng (key=port)
        self.latest: dict[str, CSIPacket]    = {}

        # Trạng thái kết nối TCP
        self._writer:      Optional[asyncio.StreamWriter] = None
        self._listen_task: Optional[asyncio.Task]         = None
        self.connected:    bool = False

        # RPC state
        self._pending: dict[str, asyncio.Future] = {}
        self._req_id:  int = 0

    # ── Kết nối / Ngắt kết nối ───────────────────────────

    async def connect(self, host: str = LAPTOP1_IP,
                      port: int = LAPTOP1_PORT) -> dict:
        """
        Thiết lập kết nối TCP tới Laptop 1.
        Gọi từ endpoint POST /connect.

        Raises:
          RuntimeError nếu đã connected hoặc kết nối thất bại.
        """
        if self.connected:
            raise RuntimeError("Đã kết nối. Ngắt trước khi kết nối lại.")

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5.0
            )
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            raise RuntimeError(f"Không thể kết nối tới {host}:{port} – {e}") from e

        self._writer   = writer
        self.connected = True
        logger.info("Đã kết nối TCP tới Laptop 1 (%s:%d)", host, port)

        # Khởi động task lắng nghe dữ liệu từ Laptop 1
        self._listen_task = asyncio.create_task(
            self._listen(reader), name="l2-listener"
        )
        return {"status": "connected", "host": host, "port": port}

    async def disconnect(self) -> dict:
        """
        Ngắt kết nối TCP.
        Gọi từ endpoint POST /disconnect.
        """
        if not self.connected:
            raise RuntimeError("Chưa kết nối.")

        self.connected = False

        # Dừng listener
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        # Đóng TCP
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None

        # Hủy tất cả RPC đang chờ
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()

        logger.info("Đã ngắt kết nối TCP khỏi Laptop 1")
        return {"status": "disconnected"}

    # ── Listener – đọc dữ liệu từ Laptop 1 ──────────────

    async def _listen(self, reader: asyncio.StreamReader):
        """
        Vòng lặp đọc message từ Laptop 1.
        Mỗi message là một dòng JSON kết thúc '\n'.

        Loại message:
          type="csi_data"  → lưu vào queue và latest
          type="response"  → resolve Future RPC tương ứng
        """
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    data = json.loads(line.decode().strip())
                except json.JSONDecodeError:
                    continue

                if data.get("type") == "csi_data":
                    try:
                        pkt = CSIPacket(**data["payload"])
                        self.latest[pkt.port] = pkt
                        try:
                            self.queue.put_nowait(pkt)
                        except asyncio.QueueFull:
                            pass
                    except (TypeError, KeyError):
                        pass

                elif data.get("type") == "response":
                    rid = str(data.get("req_id", ""))
                    fut = self._pending.get(rid)
                    if fut and not fut.done():
                        fut.set_result(data)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Listener lỗi: %s", e)
        finally:
            # Kết nối bị cắt từ phía Laptop 1
            if self.connected:
                logger.warning("Mất kết nối từ phía Laptop 1")
                self.connected = False
                self._writer   = None

    # ── RPC – gửi lệnh và chờ phản hồi ──────────────────

    async def rpc(self, cmd: str, **kwargs) -> dict:
        """
        Gửi lệnh tới Laptop 1 và chờ phản hồi (RPC pattern).

        Raises:
          ConnectionError  nếu chưa kết nối.
          asyncio.TimeoutError nếu Laptop 1 không phản hồi.
        """
        if not self.connected or not self._writer:
            raise ConnectionError("Chưa kết nối tới Laptop 1.")

        self._req_id += 1
        rid     = str(self._req_id)
        payload = {"req_id": rid, "cmd": cmd, **kwargs}

        fut = asyncio.get_event_loop().create_future()
        self._pending[rid] = fut

        try:
            self._writer.write(json.dumps(payload).encode() + b"\n")
            await self._writer.drain()
            return await asyncio.wait_for(fut, timeout=RPC_TIMEOUT)
        except asyncio.TimeoutError:
            logger.error("RPC '%s' timeout", cmd)
            raise
        finally:
            self._pending.pop(rid, None)

    def get_connection_status(self) -> dict:
        """Trạng thái kết nối hiện tại, không cần gọi RPC."""
        return {
            "connected":  self.connected,
            "queue_size": self.queue.qsize(),
            "ports_active": list(self.latest.keys()),
        }


# Singleton – dùng chung toàn bộ ứng dụng
client = Laptop2Client()


# ══════════════════════════════════════════════════════════
# FASTAPI APP + LIFESPAN
# ══════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan: không tự kết nối TCP khi khởi động.
    Kết nối do người dùng kích hoạt qua POST /connect.
    """
    logger.info("FastAPI khởi động – chờ người dùng kết nối tới Laptop 1")
    yield
    # Cleanup: ngắt kết nối nếu còn
    if client.connected:
        await client.disconnect()
    logger.info("FastAPI shutdown")


app = FastAPI(
    title="CSI Manager – Laptop 2",
    description="Điều phối kết nối tới Laptop 1 và cung cấp dữ liệu CSI",
    version="1.0.0",
    lifespan=lifespan,
)


# ══════════════════════════════════════════════════════════
# CÁC HÀM API THUẦN PYTHON (cho FastAPI router khác import)
# ══════════════════════════════════════════════════════════
# Các hàm dưới đây không phụ thuộc FastAPI – chỉ là wrapper
# mỏng quanh client.rpc(). Router khác import trực tiếp.

async def get_ports() -> list:
    """Lấy danh sách cổng COM hiện có trên Laptop 1."""
    res = await client.rpc("get_ports")
    return res.get("data", [])

async def get_baudrates() -> dict:
    """Lấy baudrate hợp lệ và giá trị mặc định."""
    res = await client.rpc("get_baudrates")
    return res.get("data", {"baudrates": [460800, 921600], "default": 921600})

async def start_csi(port: str, baud: int = 921600) -> dict:
    """Ra lệnh Laptop 1 bắt đầu thu CSI từ cổng port."""
    return await client.rpc("start", port=port, baudrate=baud)

async def stop_csi(port: str) -> dict:
    """Ra lệnh Laptop 1 dừng thu CSI từ cổng port."""
    return await client.rpc("stop", port=port)

async def get_status() -> dict:
    """Trạng thái collector + kết nối."""
    conn = client.get_connection_status()
    if not client.connected:
        return {"connection": conn, "collectors": {}}
    try:
        res = await client.rpc("status")
        return {"connection": conn, "collectors": res.get("data", {})}
    except (ConnectionError, asyncio.TimeoutError):
        return {"connection": conn, "collectors": {}}

def get_latest(port: str) -> Optional[dict]:
    """CSIPacket mới nhất từ một cổng. Trả None nếu chưa có."""
    pkt = client.latest.get(port)
    return asdict(pkt) if pkt else None


# ══════════════════════════════════════════════════════════
# FASTAPI ENDPOINTS
# ══════════════════════════════════════════════════════════

# ── Kết nối / Ngắt ───────────────────────────────────────

class ConnectRequest(BaseModel):
    host: str = LAPTOP1_IP
    port: int = LAPTOP1_PORT

@app.post("/connect", tags=["Connection"])
async def api_connect(req: ConnectRequest = ConnectRequest()):
    """Kết nối TCP tới Laptop 1. Gọi từ nút Connect ở frontend."""
    try:
        return await client.connect(req.host, req.port)
    except RuntimeError as e:
        raise HTTPException(400, str(e))

@app.post("/disconnect", tags=["Connection"])
async def api_disconnect():
    """Ngắt kết nối TCP khỏi Laptop 1. Gọi từ nút Disconnect ở frontend."""
    try:
        return await client.disconnect()
    except RuntimeError as e:
        raise HTTPException(400, str(e))

@app.get("/connection-status", tags=["Connection"])
async def api_connection_status():
    """Trạng thái kết nối hiện tại (không gọi RPC)."""
    return client.get_connection_status()

# ── CSI Control ──────────────────────────────────────────

@app.get("/ports", tags=["CSI Control"])
async def api_get_ports():
    """Lấy danh sách cổng COM từ Laptop 1 (gọi sau khi connect)."""
    try:
        return {"ports": await get_ports()}
    except (ConnectionError, asyncio.TimeoutError) as e:
        raise HTTPException(503, str(e))

@app.get("/baudrates", tags=["CSI Control"])
async def api_get_baudrates():
    """Baudrate hợp lệ: 460800 và 921600."""
    try:
        return await get_baudrates()
    except (ConnectionError, asyncio.TimeoutError) as e:
        raise HTTPException(503, str(e))

class StartRequest(BaseModel):
    port:     str
    baudrate: int = 921600

@app.post("/start", tags=["CSI Control"])
async def api_start(req: StartRequest):
    """Ra lệnh Laptop 1 bắt đầu thu CSI từ một cổng."""
    try:
        return await start_csi(req.port, req.baudrate)
    except (ConnectionError, asyncio.TimeoutError) as e:
        raise HTTPException(503, str(e))

@app.post("/stop/{port}", tags=["CSI Control"])
async def api_stop(port: str):
    """Ra lệnh Laptop 1 dừng thu CSI từ một cổng."""
    try:
        return await stop_csi(port)
    except (ConnectionError, asyncio.TimeoutError) as e:
        raise HTTPException(503, str(e))

@app.get("/status", tags=["CSI Control"])
async def api_status():
    """Trạng thái collector trên Laptop 1 + kết nối."""
    return await get_status()

@app.get("/latest/{port}", tags=["CSI Data"])
async def api_latest(port: str):
    """CSIPacket mới nhất từ một cổng."""
    pkt = get_latest(port)
    if pkt is None:
        raise HTTPException(404, f"Chưa có dữ liệu từ cổng {port}")
    return pkt

# ── SSE stream ────────────────────────────────────────────

@app.get("/stream", tags=["CSI Data"])
async def api_stream():
    """
    Server-Sent Events: stream CSI packet liên tục.
    Frontend hoặc module AI subscribe vào endpoint này.
    """
    async def generator():
        while True:
            try:
                pkt = await asyncio.wait_for(client.queue.get(), timeout=5.0)
                yield f"data: {json.dumps(asdict(pkt))}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
            except asyncio.CancelledError:
                break

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ══════════════════════════════════════════════════════════
# FRONTEND – NHÚNG HTML TRỰC TIẾP
# ══════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse, tags=["Frontend"])
async def frontend():
    """Trả về trang quản lý CSI tích hợp sẵn."""
    return HTMLResponse(content=FRONTEND_HTML)


FRONTEND_HTML = r"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CSI Monitor</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500&display=swap');

  :root {
    --bg:       #0D0F12;
    --surface:  #151820;
    --surface2: #1C2030;
    --border:   #252A3A;
    --accent:   #00E5A0;
    --accent2:  #0070F3;
    --warn:     #FF6B35;
    --tx1:      #E8EAF0;
    --tx2:      #7A8099;
    --tx3:      #454B60;
    --mono:     'Space Mono', monospace;
    --sans:     'DM Sans', sans-serif;
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--tx1);
    font-family: var(--sans);
    font-size: 14px;
    min-height: 100vh;
    padding: 24px;
  }

  /* ── Header ── */
  .header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 28px;
    padding-bottom: 16px;
    border-bottom: 1px solid var(--border);
  }
  .header h1 {
    font-family: var(--mono);
    font-size: 18px;
    letter-spacing: 0.08em;
    color: var(--tx1);
  }
  .header h1 span { color: var(--accent); }

  /* ── Connection status pill ── */
  .status-pill {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-family: var(--mono);
    font-size: 11px;
    padding: 5px 12px;
    border-radius: 99px;
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--tx2);
    transition: all .3s;
    letter-spacing: 0.06em;
  }
  .status-pill.connected {
    border-color: var(--accent);
    color: var(--accent);
    background: rgba(0,229,160,.07);
  }
  .status-pill .dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: var(--tx3);
    transition: all .3s;
  }
  .status-pill.connected .dot {
    background: var(--accent);
    box-shadow: 0 0 8px var(--accent);
    animation: pulse-dot 2s ease-in-out infinite;
  }
  @keyframes pulse-dot {
    0%,100% { opacity:1; } 50% { opacity:.4; }
  }

  /* ── Layout ── */
  .grid {
    display: grid;
    grid-template-columns: 340px 1fr;
    gap: 16px;
  }

  /* ── Card ── */
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
  }
  .card-title {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.12em;
    color: var(--tx3);
    text-transform: uppercase;
    margin-bottom: 16px;
  }

  /* ── Form controls ── */
  label {
    display: block;
    font-size: 11px;
    color: var(--tx2);
    margin-bottom: 5px;
    letter-spacing: 0.04em;
  }
  select, input[type="text"] {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--tx1);
    font-family: var(--mono);
    font-size: 12px;
    padding: 8px 10px;
    outline: none;
    transition: border-color .2s;
    appearance: none;
  }
  select:focus, input:focus { border-color: var(--accent2); }
  select option { background: var(--surface2); }

  .form-row { margin-bottom: 14px; }

  /* ── Buttons ── */
  .btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 7px;
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.06em;
    padding: 9px 16px;
    border-radius: 6px;
    border: 1px solid transparent;
    cursor: pointer;
    transition: all .2s;
    user-select: none;
  }
  .btn:disabled { opacity: .4; cursor: not-allowed; }
  .btn-primary {
    background: var(--accent);
    color: #000;
    border-color: var(--accent);
  }
  .btn-primary:hover:not(:disabled) {
    background: #00ffb3;
    box-shadow: 0 0 20px rgba(0,229,160,.35);
  }
  .btn-danger {
    background: transparent;
    color: var(--warn);
    border-color: var(--warn);
  }
  .btn-danger:hover:not(:disabled) {
    background: rgba(255,107,53,.1);
  }
  .btn-ghost {
    background: var(--surface2);
    color: var(--tx2);
    border-color: var(--border);
  }
  .btn-ghost:hover:not(:disabled) {
    border-color: var(--tx2);
    color: var(--tx1);
  }
  .btn-full { width: 100%; }
  .btn-row {
    display: flex;
    gap: 8px;
    margin-top: 8px;
  }

  /* ── Divider ── */
  .divider {
    border: none;
    border-top: 1px solid var(--border);
    margin: 16px 0;
  }

  /* ── Port list ── */
  .port-list { display: flex; flex-direction: column; gap: 8px; }
  .port-item {
    display: grid;
    grid-template-columns: 1fr auto auto;
    align-items: center;
    gap: 8px;
    padding: 10px 12px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 7px;
    transition: border-color .2s;
  }
  .port-item.running { border-color: var(--accent); }
  .port-item.running .port-name { color: var(--accent); }
  .port-name {
    font-family: var(--mono);
    font-size: 12px;
    color: var(--tx1);
  }
  .port-desc {
    font-size: 10px;
    color: var(--tx3);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .port-stat {
    font-family: var(--mono);
    font-size: 10px;
    color: var(--tx2);
    text-align: right;
  }

  /* ── Stats grid ── */
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
    margin-bottom: 16px;
  }
  .stat-box {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 7px;
    padding: 10px 12px;
  }
  .stat-label {
    font-size: 10px;
    color: var(--tx3);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 4px;
  }
  .stat-value {
    font-family: var(--mono);
    font-size: 18px;
    color: var(--tx1);
    line-height: 1;
  }
  .stat-value.green { color: var(--accent); }
  .stat-value.orange { color: var(--warn); }

  /* ── Log ── */
  .log-box {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 7px;
    padding: 12px;
    height: 180px;
    overflow-y: auto;
    font-family: var(--mono);
    font-size: 11px;
    line-height: 1.8;
  }
  .log-entry { color: var(--tx2); }
  .log-entry.ok  { color: var(--accent); }
  .log-entry.err { color: var(--warn); }
  .log-entry.info{ color: var(--accent2); }

  /* ── Empty state ── */
  .empty {
    text-align: center;
    color: var(--tx3);
    font-size: 12px;
    padding: 20px 0;
    font-family: var(--mono);
  }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 4px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 99px; }
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <h1>CSI <span>Monitor</span></h1>
  <div class="status-pill" id="conn-pill">
    <span class="dot"></span>
    <span id="conn-label">NOT CONNECTED</span>
  </div>
</div>

<div class="grid">

  <!-- ── Left panel: Connection + Port control ── -->
  <div style="display:flex;flex-direction:column;gap:16px;">

    <!-- Connection card -->
    <div class="card">
      <div class="card-title">Laptop 1 Connection</div>

      <div class="form-row">
        <label>IP Address</label>
        <input type="text" id="inp-ip" value="192.168.1.137" placeholder="192.168.1.x">
      </div>

      <div class="btn-row">
        <button class="btn btn-primary btn-full" id="btn-connect" onclick="doConnect()">
          ⬡ CONNECT
        </button>
        <button class="btn btn-danger" id="btn-disconnect" onclick="doDisconnect()" disabled>
          ✕ DISC
        </button>
      </div>
    </div>

    <!-- Port control card -->
    <div class="card">
      <div class="card-title">Serial Port Control</div>

      <div class="form-row">
        <label>COM Port</label>
        <select id="sel-port">
          <option value="">— Connect first —</option>
        </select>
      </div>

      <div class="form-row">
        <label>Baudrate</label>
        <select id="sel-baud">
          <option value="921600">921600</option>
          <option value="460800">460800</option>
        </select>
      </div>

      <div class="btn-row">
        <button class="btn btn-ghost" id="btn-refresh" onclick="doRefreshPorts()" disabled
                style="gap:5px;">
          ↻ REFRESH PORTS
        </button>
        <button class="btn btn-primary" id="btn-start" onclick="doStartPort()" disabled>
          ▶ START
        </button>
      </div>
    </div>

    <!-- Active ports -->
    <div class="card">
      <div class="card-title">Active Ports</div>
      <div class="port-list" id="port-list">
        <div class="empty">No active ports</div>
      </div>
    </div>

  </div>

  <!-- ── Right panel: Stats + Log ── -->
  <div style="display:flex;flex-direction:column;gap:16px;">

    <!-- Stats -->
    <div class="card">
      <div class="card-title">System Stats</div>
      <div class="stats-grid">
        <div class="stat-box">
          <div class="stat-label">Connection</div>
          <div class="stat-value" id="stat-conn" style="font-size:13px">OFFLINE</div>
        </div>
        <div class="stat-box">
          <div class="stat-label">Total Packets</div>
          <div class="stat-value green" id="stat-total">0</div>
        </div>
        <div class="stat-box">
          <div class="stat-label">Parse Errors</div>
          <div class="stat-value orange" id="stat-errors">0</div>
        </div>
      </div>

      <!-- Per-port detail -->
      <div id="port-detail"></div>
    </div>

    <!-- Event log -->
    <div class="card" style="flex:1">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <div class="card-title" style="margin-bottom:0">Event Log</div>
        <button class="btn btn-ghost" onclick="clearLog()"
                style="padding:4px 10px;font-size:10px">CLEAR</button>
      </div>
      <div class="log-box" id="log-box"></div>
    </div>

  </div>
</div>

<script>
// ═══════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════
const state = {
  connected: false,
  ports: [],         // [{port, desc}] từ Laptop 1
  activePorts: {},   // port → {total, error, connected}
};

// ═══════════════════════════════════════════════
// Log
// ═══════════════════════════════════════════════
function log(msg, cls = "") {
  const box = document.getElementById("log-box");
  const now  = new Date().toLocaleTimeString("vi", {hour12: false});
  const el   = document.createElement("div");
  el.className = `log-entry ${cls}`;
  el.textContent = `[${now}] ${msg}`;
  box.prepend(el);
  // Giữ tối đa 200 dòng
  while (box.children.length > 200) box.removeChild(box.lastChild);
}
function clearLog() { document.getElementById("log-box").innerHTML = ""; }

// ═══════════════════════════════════════════════
// API helpers
// ═══════════════════════════════════════════════
async function api(method, path, body) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(path, opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

// ═══════════════════════════════════════════════
// Connect / Disconnect
// ═══════════════════════════════════════════════
async function doConnect() {
  const host = document.getElementById("inp-ip").value.trim();
  if (!host) { log("Nhập IP của Laptop 1 trước", "err"); return; }
  log(`Đang kết nối tới ${host}:8888 …`, "info");
  setConnectBusy(true);
  try {
    const res = await api("POST", "/connect", { host, port: 8888 });
    state.connected = true;
    updateConnectionUI(true);
    log(`✓ Đã kết nối – ${res.host}:${res.port}`, "ok");
    // Tự động lấy danh sách cổng
    await doRefreshPorts();
    await doRefreshBaudrates();
  } catch (e) {
    log(`✗ Kết nối thất bại: ${e.message}`, "err");
  }
  setConnectBusy(false);
}

async function doDisconnect() {
  log("Đang ngắt kết nối …", "info");
  try {
    await api("POST", "/disconnect");
    state.connected = false;
    state.activePorts = {};
    updateConnectionUI(false);
    resetPortSelect();
    renderPortList();
    renderPortDetail();
    log("Đã ngắt kết nối", "ok");
  } catch (e) {
    log(`Lỗi ngắt kết nối: ${e.message}`, "err");
  }
}

function setConnectBusy(busy) {
  document.getElementById("btn-connect").disabled = busy;
}

function updateConnectionUI(connected) {
  const pill  = document.getElementById("conn-pill");
  const label = document.getElementById("conn-label");
  pill.classList.toggle("connected", connected);
  label.textContent = connected ? "CONNECTED" : "NOT CONNECTED";
  document.getElementById("btn-connect").disabled    = connected;
  document.getElementById("btn-disconnect").disabled = !connected;
  document.getElementById("btn-refresh").disabled    = !connected;
  document.getElementById("btn-start").disabled      = !connected;
  document.getElementById("sel-port").disabled       = !connected;
  document.getElementById("sel-baud").disabled       = !connected;
  document.getElementById("stat-conn").textContent   = connected ? "ONLINE" : "OFFLINE";
  document.getElementById("stat-conn").style.color   = connected
    ? "var(--accent)" : "var(--tx2)";
}

// ═══════════════════════════════════════════════
// Port list
// ═══════════════════════════════════════════════
async function doRefreshPorts() {
  try {
    const res = await api("GET", "/ports");
    state.ports = res.ports || [];
    const sel = document.getElementById("sel-port");
    sel.innerHTML = state.ports.length
      ? state.ports.map(p =>
          `<option value="${p.port}">${p.port}${p.desc ? ' – ' + p.desc : ''}</option>`
        ).join("")
      : `<option value="">— No ports found —</option>`;
    log(`Tìm thấy ${state.ports.length} cổng COM`, state.ports.length ? "ok" : "");
  } catch (e) {
    log(`Lỗi lấy danh sách cổng: ${e.message}`, "err");
  }
}

async function doRefreshBaudrates() {
  try {
    const res = await api("GET", "/baudrates");
    const brates = res.baudrates || [460800, 921600];
    const def    = res.default || 921600;
    const sel    = document.getElementById("sel-baud");
    sel.innerHTML = brates.map(b =>
      `<option value="${b}" ${b === def ? "selected" : ""}>${b.toLocaleString()}</option>`
    ).join("");
  } catch { /* giữ giá trị mặc định */ }
}

function resetPortSelect() {
  document.getElementById("sel-port").innerHTML =
    `<option value="">— Connect first —</option>`;
  state.ports = [];
}

// ═══════════════════════════════════════════════
// Start / Stop serial
// ═══════════════════════════════════════════════
async function doStartPort() {
  const port = document.getElementById("sel-port").value;
  const baud = parseInt(document.getElementById("sel-baud").value);
  if (!port) { log("Chọn cổng COM trước", "err"); return; }
  try {
    const res = await api("POST", "/start", { port, baudrate: baud });
    log(`✓ ${res.msg || "Started " + port}`, "ok");
    state.activePorts[port] = { total: 0, error: 0, connected: true };
    renderPortList();
  } catch (e) {
    log(`✗ Start thất bại: ${e.message}`, "err");
  }
}

async function doStopPort(port) {
  try {
    const res = await api("POST", `/stop/${encodeURIComponent(port)}`);
    log(`✓ ${res.msg || "Stopped " + port}`, "ok");
    delete state.activePorts[port];
    renderPortList();
    renderPortDetail();
  } catch (e) {
    log(`✗ Stop thất bại: ${e.message}`, "err");
  }
}

// ═══════════════════════════════════════════════
// Render UI
// ═══════════════════════════════════════════════
function renderPortList() {
  const el     = document.getElementById("port-list");
  const ports  = Object.entries(state.activePorts);
  if (!ports.length) {
    el.innerHTML = `<div class="empty">No active ports</div>`;
    return;
  }
  el.innerHTML = ports.map(([port, stats]) => `
    <div class="port-item ${stats.connected ? "running" : ""}">
      <div>
        <div class="port-name">${port}</div>
        <div class="port-desc">${stats.connected ? "RUNNING" : "STOPPED"}</div>
      </div>
      <div class="port-stat">
        <div style="color:var(--accent)">${(stats.total||0).toLocaleString()} pkts</div>
        <div style="color:var(--warn)">${stats.error||0} err</div>
      </div>
      <button class="btn btn-danger" onclick="doStopPort('${port}')"
              style="padding:5px 10px;font-size:10px">STOP</button>
    </div>
  `).join("");
}

function renderPortDetail() {
  const el    = document.getElementById("port-detail");
  const ports = Object.keys(state.activePorts);
  if (!ports.length) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <hr class="divider">
    <div style="font-size:11px;color:var(--tx3);font-family:var(--mono);margin-bottom:10px;letter-spacing:.08em">PER-PORT DETAIL</div>
    <div style="display:flex;flex-direction:column;gap:6px">
      ${ports.map(p => {
        const s = state.activePorts[p] || {};
        return `
          <div style="display:flex;justify-content:space-between;padding:7px 10px;
                      background:var(--bg);border-radius:5px;border:1px solid var(--border)">
            <span style="font-family:var(--mono);font-size:11px;color:var(--tx1)">${p}</span>
            <div style="display:flex;gap:14px">
              <span style="font-family:var(--mono);font-size:11px;color:var(--accent)">
                ↓ ${(s.total||0).toLocaleString()}
              </span>
              <span style="font-family:var(--mono);font-size:11px;color:var(--warn)">
                ✗ ${s.error||0}
              </span>
            </div>
          </div>`;
      }).join("")}
    </div>`;
}

function updateStats() {
  let total = 0, errors = 0;
  for (const s of Object.values(state.activePorts)) {
    total  += s.total  || 0;
    errors += s.error  || 0;
  }
  document.getElementById("stat-total").textContent  = total.toLocaleString();
  document.getElementById("stat-errors").textContent = errors;
}

// ═══════════════════════════════════════════════
// Polling /status mỗi 1.5 giây
// ═══════════════════════════════════════════════
async function pollStatus() {
  if (!state.connected) return;
  try {
    const res = await api("GET", "/status");

    // Cập nhật trạng thái kết nối nếu bị mất
    if (!res.connection?.connected && state.connected) {
      state.connected = false;
      updateConnectionUI(false);
      log("Mất kết nối với Laptop 1!", "err");
      return;
    }

    // Cập nhật stats từng cổng
    const collectors = res.collectors || {};
    for (const [port, stats] of Object.entries(collectors)) {
      state.activePorts[port] = stats;
    }
    // Xóa cổng không còn trong collector
    for (const port of Object.keys(state.activePorts)) {
      if (!(port in collectors)) delete state.activePorts[port];
    }

    renderPortList();
    renderPortDetail();
    updateStats();
  } catch { /* bỏ qua lỗi poll đơn lẻ */ }
}

// Polling kết nối status mỗi 2s
async function pollConnection() {
  try {
    const res = await api("GET", "/connection-status");
    const wasConnected = state.connected;
    state.connected = res.connected;
    if (wasConnected !== res.connected) {
      updateConnectionUI(res.connected);
      if (!res.connected) {
        log("Phát hiện ngắt kết nối từ polling", "err");
        state.activePorts = {};
        renderPortList();
        renderPortDetail();
      }
    }
  } catch { /* ignore */ }
}

// ═══════════════════════════════════════════════
// Init
// ═══════════════════════════════════════════════
updateConnectionUI(false);
setInterval(pollStatus,     1500);
setInterval(pollConnection, 2000);
log("CSI Monitor sẵn sàng. Nhập IP và bấm CONNECT.", "info");
</script>
</body>
</html>"""


# ══════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        "laptop2:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )