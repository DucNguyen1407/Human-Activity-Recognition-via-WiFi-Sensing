"""
Laptop 2 - FastAPI Server + TCP Client + Frontend tích hợp
===========================================================

Kiến trúc:
  Browser ──HTTP──► FastAPI (laptop2.py)
                        │
                    Laptop2Client (singleton)
                        │
                   TCP JSON Lines ──► Laptop 1 / ESP32-Collection

Giao thức JSON Lines với Laptop 1:
  Gửi đi:
    {"type":"get_com_ports"}
    {"type":"uart_control","action":"connect","device_id":"esp1","com":"COM3","baudrate":115200}
    {"type":"uart_control","action":"disconnect","device_id":"esp1"}

  Nhận về:
    {"type":"com_list","ports":["COM3","COM4"]}
    {"type":"uart_status","device_id":"esp1","status":"connected","config":{...}}
    {"type":"uart_status","device_id":"esp1","status":"disconnected"}
    {"type":"uart_status","device_id":"esp1","status":"error","message":"..."}
    {"type":"csi_data","device_id":"esp1","seq":123,...}

Chạy:
    pip install fastapi uvicorn
    python laptop2.py
    Mở trình duyệt: http://localhost:8000
"""

import asyncio
import collections
import json
import logging
import math
import time
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Management] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# CẤU HÌNH – chỉnh trực tiếp tại đây, không đưa lên frontend
# ══════════════════════════════════════════════════════════

LAPTOP1_IP   = "192.168.1.137"   # ← Đổi thành IP thực của Laptop 1
LAPTOP1_PORT = 8888


# ══════════════════════════════════════════════════════════
# LAPTOP2 CLIENT – QUẢN LÝ KẾT NỐI TCP & ĐIỀU KHIỂN UART
# ══════════════════════════════════════════════════════════

class Laptop2Client:
    """
    Quản lý kết nối TCP tới Laptop 1 và điều phối 3 ESP32.

    State per ESP:
      esp_state[device_id] = {
        "status":    "disconnected" | "connected" | "error",
        "com":       "COM3",
        "baudrate":  115200,
        "message":   "...",       # khi error
        "pkt_total": int,
        "pkt_rate":  float,       # gói/giây, cửa sổ 1s
      }
    """

    DEVICE_IDS = ["esp1", "esp2", "esp3"]

    def __init__(self):
        # Queue CSI packets cho module AI / SSE
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=30_000)
        # Gói mới nhất từ mỗi device
        self.latest: dict[str, dict] = {}

        # Trạng thái TCP
        self._writer:      Optional[asyncio.StreamWriter] = None
        self._listen_task: Optional[asyncio.Task]         = None
        self.connected:    bool = False

        # Trạng thái từng ESP
        self.esp_state: dict[str, dict] = {
            did: {
                "status":    "disconnected",
                "com":       "",
                "baudrate":  115200,
                "message":   "",
                "pkt_total": 0,
                "pkt_rate":  0.0,
            }
            for did in self.DEVICE_IDS
        }

        # Danh sách COM ports nhận từ Laptop 1
        self.com_ports: list[str] = []

        # Packet rate tracking (cửa sổ 1 giây)
        self._pkt_counter: dict[str, int]   = {}
        self._pkt_rate:    dict[str, float] = {}
        self._rate_t0:     float = time.monotonic()

        # Stability tracking (sliding window 200 gói)
        self._STABILITY_WINDOW = 200
        self._pending_csi:  dict[str, collections.deque] = {}
        self._amp_history:  dict[str, collections.deque] = {}
        self._rssi_history: dict[str, collections.deque] = {}
        self._amp_task:     Optional[asyncio.Task]       = None

        self._l2_dropped: int = 0

        # Future chờ com_list từ Laptop 1
        self._com_list_future: Optional[asyncio.Future] = None

    # ── Kết nối TCP tới Laptop 1 ──────────────────────────

    async def connect(self) -> dict:
        if self.connected:
            raise RuntimeError("Đã kết nối. Ngắt trước khi kết nối lại.")

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(LAPTOP1_IP, LAPTOP1_PORT),
                timeout=5.0
            )
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            raise RuntimeError(
                f"Không thể kết nối tới {LAPTOP1_IP}:{LAPTOP1_PORT} – {e}"
            ) from e

        self._writer   = writer
        self.connected = True
        logger.info("Đã kết nối TCP tới Laptop 1 (%s:%d)", LAPTOP1_IP, LAPTOP1_PORT)

        self._listen_task = asyncio.create_task(
            self._listen(reader), name="l2-listener"
        )
        self._amp_task = asyncio.create_task(
            self._amp_worker(), name="l2-amp-worker"
        )
        return {"status": "connected", "host": LAPTOP1_IP, "port": LAPTOP1_PORT}

    async def disconnect(self) -> dict:
        if not self.connected:
            raise RuntimeError("Chưa kết nối.")

        self.connected = False

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self._amp_task and not self._amp_task.done():
            self._amp_task.cancel()
            try:
                await self._amp_task
            except asyncio.CancelledError:
                pass
        self._amp_task = None

        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None

        # Reset trạng thái ESP về disconnected
        for did in self.DEVICE_IDS:
            self.esp_state[did]["status"] = "disconnected"
            self.esp_state[did]["pkt_rate"] = 0.0

        logger.info("Đã ngắt kết nối TCP khỏi Laptop 1")
        return {"status": "disconnected"}

    # ── Listener – đọc dữ liệu từ Laptop 1 ──────────────

    async def _listen(self, reader: asyncio.StreamReader):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    data = json.loads(line.decode().strip())
                except json.JSONDecodeError:
                    continue

                msg_type = data.get("type")

                # File 02 – nhận danh sách COM
                if msg_type == "com_list":
                    self.com_ports = data.get("ports", [])
                    if self._com_list_future and not self._com_list_future.done():
                        self._com_list_future.set_result(self.com_ports)
                    logger.info("Nhận danh sách COM: %s", self.com_ports)

                # File 04/07/08 – nhận trạng thái UART
                elif msg_type == "uart_status":
                    did    = data.get("device_id", "")
                    status = data.get("status", "")
                    if did in self.esp_state:
                        self.esp_state[did]["status"]  = status
                        self.esp_state[did]["message"] = data.get("message", "")
                        if status == "connected":
                            cfg = data.get("config", {})
                            self.esp_state[did]["com"]      = cfg.get("com", "")
                            self.esp_state[did]["baudrate"] = cfg.get("baudrate", 115200)
                        elif status in ("disconnected", "error"):
                            self.esp_state[did]["pkt_rate"] = 0.0
                    logger.info("uart_status [%s] → %s", did, status)

                # File 05 – nhận CSI data
                elif msg_type == "csi_data":
                    did = data.get("device_id", "")
                    self.latest[did] = data

                    # Cập nhật tổng gói
                    if did in self.esp_state:
                        self.esp_state[did]["pkt_total"] += 1

                    # Packet rate (cửa sổ 1 giây)
                    now     = time.monotonic()
                    elapsed = now - self._rate_t0
                    self._pkt_counter[did] = self._pkt_counter.get(did, 0) + 1
                    if elapsed >= 1.0:
                        for d, cnt in self._pkt_counter.items():
                            rate = round(cnt / elapsed, 1)
                            self._pkt_rate[d] = rate
                            if d in self.esp_state:
                                self.esp_state[d]["pkt_rate"] = rate
                        self._pkt_counter.clear()
                        self._rate_t0 = now

                    # Stability deques
                    radio = data.get("radio", {})
                    rssi  = radio.get("rssi", 0)
                    nf    = radio.get("noise_floor", 0)
                    csi   = data.get("csi", [])
                    # Tách Q/I từ list phẳng: [Q0,I0,Q1,I1,...]
                    n = len(csi) // 2
                    q_arr = csi[0::2][:n]
                    i_arr = csi[1::2][:n]

                    if did not in self._pending_csi:
                        self._pending_csi[did] = collections.deque()
                    self._pending_csi[did].append((i_arr, q_arr))

                    if did not in self._rssi_history:
                        self._rssi_history[did] = collections.deque(
                            maxlen=self._STABILITY_WINDOW
                        )
                    self._rssi_history[did].append((rssi, nf))

                    # Đưa vào queue SSE (circular buffer)
                    try:
                        self.queue.put_nowait(data)
                    except asyncio.QueueFull:
                        try:
                            self.queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        try:
                            self.queue.put_nowait(data)
                        except asyncio.QueueFull:
                            pass
                        self._l2_dropped += 1

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Listener lỗi: %s", e)
        finally:
            if self.connected:
                logger.warning("Mất kết nối từ phía Laptop 1")
                self.connected = False
                self._writer   = None
                for did in self.DEVICE_IDS:
                    self.esp_state[did]["status"]   = "disconnected"
                    self.esp_state[did]["pkt_rate"] = 0.0

    # ── Gửi lệnh tới Laptop 1 ────────────────────────────

    async def _send(self, msg: dict):
        if not self.connected or not self._writer:
            raise ConnectionError("Chưa kết nối tới Laptop 1.")
        self._writer.write(json.dumps(msg, ensure_ascii=False).encode() + b"\n")
        await self._writer.drain()

    async def request_com_ports(self) -> list[str]:
        """Gửi get_com_ports, chờ com_list phản hồi (timeout 5s)."""
        if not self.connected:
            raise ConnectionError("Chưa kết nối.")
        loop = asyncio.get_event_loop()
        self._com_list_future = loop.create_future()
        await self._send({"type": "get_com_ports"})
        try:
            return await asyncio.wait_for(self._com_list_future, timeout=5.0)
        except asyncio.TimeoutError:
            return self.com_ports   # trả cache cũ nếu timeout
        finally:
            self._com_list_future = None

    async def uart_connect(self, device_id: str, com: str, baudrate: int):
        """Gửi uart_control connect."""
        await self._send({
            "type":      "uart_control",
            "action":    "connect",
            "device_id": device_id,
            "com":       com,
            "baudrate":  baudrate,
        })

    async def uart_disconnect(self, device_id: str):
        """Gửi uart_control disconnect."""
        await self._send({
            "type":      "uart_control",
            "action":    "disconnect",
            "device_id": device_id,
        })

    # ── Amplitude Worker ─────────────────────────────────

    async def _amp_worker(self):
        while True:
            try:
                W = self._STABILITY_WINDOW
                for did, pending in list(self._pending_csi.items()):
                    if did not in self._amp_history:
                        self._amp_history[did] = collections.deque(maxlen=W)
                    while pending:
                        i_arr, q_arr = pending.popleft()
                        n = min(len(i_arr), len(q_arr))
                        mean_a = (
                            sum(math.hypot(i_arr[k], q_arr[k]) for k in range(n)) / n
                            if n > 0 else 0.0
                        )
                        self._amp_history[did].append(mean_a)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("_amp_worker lỗi: %s", e)
            await asyncio.sleep(0.1)

    # ── Stability ─────────────────────────────────────────

    def get_stability(self) -> dict:
        def _stats(seq):
            n = len(seq)
            if n == 0:
                return 0.0, 0.0
            m = sum(seq) / n
            s = math.sqrt(sum((x - m) ** 2 for x in seq) / n) if n > 1 else 0.0
            return m, s

        result = {}
        for did in self.DEVICE_IDS:
            amps = list(self._amp_history.get(did, []))
            rf   = list(self._rssi_history.get(did, []))
            n_samples = len(amps)

            if n_samples < 10:
                result[did] = {
                    "score": None, "grade": "WARMING UP",
                    "n_samples": n_samples, "need": 10,
                }
                continue

            mean_amp, std_amp = _stats(amps)
            amp_cv    = std_amp / mean_amp if mean_amp > 0 else 1.0
            amp_score = max(0.0, 1.0 - min(amp_cv, 1.0)) * 60

            if rf:
                rssis = [r for r, _ in rf]
                nfs   = [n for _, n in rf]
                mean_rssi, rssi_std = _stats(rssis)
                mean_nf,   _        = _stats(nfs)
                snr_proxy = mean_rssi - mean_nf
            else:
                mean_rssi = rssi_std = mean_nf = snr_proxy = 0.0

            snr_part  = min(max(snr_proxy / 25.0, 0.0), 1.0) * 28
            stab_part = max(0.0, 1.0 - min(rssi_std / 10.0, 1.0)) * 12
            rf_score  = snr_part + stab_part
            score     = round(amp_score + rf_score, 1)

            if score >= 80:   grade = "STABLE"
            elif score >= 60: grade = "GOOD"
            elif score >= 40: grade = "FAIR"
            elif score >= 20: grade = "UNSTABLE"
            else:             grade = "CRITICAL"

            result[did] = {
                "score": score, "grade": grade, "n_samples": n_samples,
                "amp_cv": round(amp_cv, 4), "amp_cv_pct": round(amp_cv * 100, 1),
                "mean_amp": round(mean_amp, 1), "std_amp": round(std_amp, 1),
                "amp_score": round(amp_score, 1),
                "mean_rssi": round(mean_rssi, 1), "mean_nf": round(mean_nf, 1),
                "snr_proxy_db": round(snr_proxy, 1), "rssi_std": round(rssi_std, 2),
                "rf_score": round(rf_score, 1),
            }
        return result


# Singleton
client = Laptop2Client()


# ══════════════════════════════════════════════════════════
# FASTAPI APP + LIFESPAN
# ══════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("FastAPI khởi động – chờ người dùng kết nối tới Laptop 1 (%s:%d)",
                LAPTOP1_IP, LAPTOP1_PORT)
    yield
    if client.connected:
        await client.disconnect()
    logger.info("FastAPI shutdown")


app = FastAPI(
    title="CSI Manager – Laptop 2",
    description="Điều phối kết nối tới Laptop 1 và cung cấp dữ liệu CSI",
    version="2.0.0",
    lifespan=lifespan,
)


# ══════════════════════════════════════════════════════════
# FASTAPI ENDPOINTS
# ══════════════════════════════════════════════════════════

# ── Kết nối TCP ──────────────────────────────────────────

@app.post("/connect", tags=["Connection"])
async def api_connect():
    """Kết nối TCP tới Laptop 1 (IP/Port cấu hình trong code)."""
    try:
        return await client.connect()
    except RuntimeError as e:
        raise HTTPException(400, str(e))

@app.post("/disconnect", tags=["Connection"])
async def api_disconnect():
    """Ngắt kết nối TCP khỏi Laptop 1."""
    try:
        return await client.disconnect()
    except RuntimeError as e:
        raise HTTPException(400, str(e))

@app.get("/connection-status", tags=["Connection"])
async def api_connection_status():
    """Trạng thái kết nối TCP."""
    return {
        "connected":   client.connected,
        "host":        LAPTOP1_IP,
        "port":        LAPTOP1_PORT,
        "l2_dropped":  client._l2_dropped,
        "queue_size":  client.queue.qsize(),
    }

# ── COM Ports ─────────────────────────────────────────────

@app.get("/com-ports", tags=["UART Control"])
async def api_com_ports():
    """Yêu cầu Laptop 1 trả danh sách cổng COM hiện có."""
    try:
        ports = await client.request_com_ports()
        return {"ports": ports}
    except (ConnectionError, asyncio.TimeoutError) as e:
        raise HTTPException(503, str(e))

# ── UART Control ──────────────────────────────────────────

class UartConnectRequest(BaseModel):
    device_id: str  # "esp1" | "esp2" | "esp3"
    com:       str  # "COM3"
    baudrate:  int = 115200

@app.post("/uart/connect", tags=["UART Control"])
async def api_uart_connect(req: UartConnectRequest):
    """Yêu cầu Laptop 1 kết nối UART tới một ESP32."""
    if req.device_id not in client.DEVICE_IDS:
        raise HTTPException(400, f"device_id phải là: {client.DEVICE_IDS}")
    try:
        await client.uart_connect(req.device_id, req.com, req.baudrate)
        return {"sent": True, "device_id": req.device_id}
    except ConnectionError as e:
        raise HTTPException(503, str(e))

class UartDisconnectRequest(BaseModel):
    device_id: str

@app.post("/uart/disconnect", tags=["UART Control"])
async def api_uart_disconnect(req: UartDisconnectRequest):
    """Yêu cầu Laptop 1 ngắt UART của một ESP32."""
    if req.device_id not in client.DEVICE_IDS:
        raise HTTPException(400, f"device_id phải là: {client.DEVICE_IDS}")
    try:
        await client.uart_disconnect(req.device_id)
        return {"sent": True, "device_id": req.device_id}
    except ConnectionError as e:
        raise HTTPException(503, str(e))

# ── Status & Data ─────────────────────────────────────────

@app.get("/esp-status", tags=["CSI Data"])
async def api_esp_status():
    """Trạng thái realtime của cả 3 ESP32."""
    return {"devices": client.esp_state}

@app.get("/stability", tags=["CSI Data"])
async def api_stability():
    """Đánh giá ổn định tín hiệu CSI (window 200 gói) cho 3 ESP."""
    return {"stability": client.get_stability()}

@app.get("/stream", tags=["CSI Data"])
async def api_stream():
    """Server-Sent Events: stream CSI packet liên tục."""
    async def generator():
        while True:
            try:
                pkt = await asyncio.wait_for(client.queue.get(), timeout=5.0)
                yield f"data: {json.dumps(pkt, ensure_ascii=False)}\n\n"
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
    """Trang quản lý CSI tích hợp sẵn."""
    return HTMLResponse(content=FRONTEND_HTML)


FRONTEND_HTML = r"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CSI IoT Collection Dashboard</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Inter:wght@300;400;500;600&display=swap');

  :root {
    --bg:        #1a1a1a;
    --panel:     #222222;
    --border:    #3a3a3a;
    --border2:   #444444;
    --accent-g:  #00c853;
    --accent-g2: #00e676;
    --accent-r:  #e53935;
    --accent-r2: #ff5252;
    --accent-o:  #ff6d00;
    --accent-b:  #00b0ff;
    --tx1:       #e0e0e0;
    --tx2:       #9e9e9e;
    --tx3:       #616161;
    --mono:      'Share Tech Mono', monospace;
    --sans:      'Inter', sans-serif;
    --header-h:  36px;
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--tx1);
    font-family: var(--sans);
    font-size: 13px;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  /* ════ TOPBAR ════ */
  .topbar {
    height: var(--header-h);
    background: #111;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    padding: 0 12px;
    gap: 16px;
    flex-shrink: 0;
  }
  .topbar-title {
    font-family: var(--mono);
    font-size: 13px;
    color: var(--tx1);
    letter-spacing: .04em;
    white-space: nowrap;
  }
  .topbar-sep { width: 1px; height: 18px; background: var(--border2); }
  .topbar-indicators {
    display: flex; align-items: center; gap: 16px; margin-left: auto;
  }
  .indicator {
    display: flex; align-items: center; gap: 5px;
    font-family: var(--mono); font-size: 10px;
    color: var(--tx3); letter-spacing: .06em;
  }
  .indicator .dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--tx3); transition: background .3s;
  }
  .indicator.on .dot  { background: var(--accent-g); box-shadow: 0 0 6px var(--accent-g); }
  .indicator.on       { color: var(--tx2); }
  .indicator.err .dot { background: var(--accent-r); box-shadow: 0 0 6px var(--accent-r); }
  .indicator.err      { color: var(--accent-r); }

  /* ════ LAYOUT ════ */
  .workspace { display: flex; flex: 1; overflow: hidden; }

  /* ════ LEFT COLUMN ════ */
  .left-col {
    width: 210px; min-width: 210px;
    background: var(--panel);
    border-right: 1px solid var(--border);
    display: flex; flex-direction: column;
    overflow-y: auto; overflow-x: hidden;
  }

  .sec-hdr {
    display: flex; align-items: center; justify-content: space-between;
    padding: 7px 10px; background: #1e1e1e;
    border-bottom: 1px solid var(--border);
    font-family: var(--mono); font-size: 10px;
    color: var(--tx2); letter-spacing: .08em;
    text-transform: uppercase; flex-shrink: 0;
  }
  .sec-hdr button {
    font-family: var(--mono); font-size: 9px; padding: 2px 8px;
    background: #333; border: 1px solid var(--border2); border-radius: 3px;
    color: var(--tx2); cursor: pointer; letter-spacing: .04em; transition: all .2s;
  }
  .sec-hdr button:hover { background: #444; color: var(--tx1); }
  .sec-hdr button:disabled { opacity: .35; cursor: not-allowed; }

  /* TCP connect box */
  .tcp-box {
    padding: 6px 10px; background: #1c1c1c;
    border-bottom: 1px solid var(--border); flex-shrink: 0;
  }
  .tcp-label {
    font-size: 9px; color: var(--tx3); font-family: var(--mono);
    margin-bottom: 5px; letter-spacing: .04em;
  }
  .tcp-host {
    font-size: 9px; color: var(--tx3); font-family: var(--mono);
    margin-top: 4px; text-align: center;
  }

  /* UART panel */
  .uart-panel {
    border-bottom: 1px solid var(--border);
    padding: 8px 10px; flex-shrink: 0;
  }
  .uart-hdr {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 6px;
  }
  .uart-label {
    font-family: var(--mono); font-size: 10px;
    color: var(--tx2); letter-spacing: .06em;
  }
  .uart-status {
    display: flex; align-items: center; gap: 4px;
    font-family: var(--mono); font-size: 9px; letter-spacing: .04em;
  }
  .uart-status .sdot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--tx3); flex-shrink: 0;
  }
  .uart-status.disconnected { color: var(--tx3); }
  .uart-status.connected    { color: var(--accent-g); }
  .uart-status.connected .sdot {
    background: var(--accent-g); box-shadow: 0 0 5px var(--accent-g);
    animation: blink 2s infinite;
  }
  .uart-status.error        { color: var(--accent-r); }
  .uart-status.error .sdot  { background: var(--accent-r); }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.4} }

  /* 2-col form */
  .form-grid {
    display: grid; grid-template-columns: 1fr 1fr;
    gap: 4px 6px; margin-bottom: 6px;
  }
  .form-item label {
    display: block; font-size: 9px; color: var(--tx3);
    letter-spacing: .06em; text-transform: uppercase; margin-bottom: 3px;
  }
  select {
    width: 100%; background: #1a1a1a;
    border: 1px solid var(--border2); border-radius: 3px;
    color: var(--tx1); font-family: var(--mono); font-size: 11px;
    padding: 4px 6px; outline: none; appearance: none;
    cursor: pointer; transition: border-color .2s;
  }
  select:focus { border-color: var(--accent-b); }
  select option { background: #222; }
  select:disabled { opacity: .35; cursor: not-allowed; }

  /* Buttons */
  .btn-row-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 5px; }
  .btn {
    font-family: var(--mono); font-size: 10px; letter-spacing: .04em;
    padding: 5px 0; border-radius: 3px; border: none;
    cursor: pointer; transition: all .15s; text-align: center; font-weight: 600;
  }
  .btn:disabled { opacity: .35; cursor: not-allowed; }
  .btn-g { background: var(--accent-g); color: #000; }
  .btn-g:hover:not(:disabled) {
    background: var(--accent-g2); box-shadow: 0 0 10px rgba(0,200,83,.4);
  }
  .btn-r { background: var(--accent-r); color: #fff; }
  .btn-r:hover:not(:disabled) {
    background: var(--accent-r2); box-shadow: 0 0 10px rgba(229,57,53,.4);
  }

  /* Error msg */
  .uart-err {
    font-family: var(--mono); font-size: 9px; color: var(--accent-r);
    background: rgba(229,57,53,.08); border: 1px solid rgba(229,57,53,.2);
    border-radius: 3px; padding: 3px 6px; margin-top: 5px;
    display: none; word-break: break-all; line-height: 1.4;
  }
  .uart-err.show { display: block; }

  /* Rate */
  .rate-row {
    display: flex; align-items: center;
    padding: 5px 10px; border-bottom: 1px solid #2a2a2a; gap: 6px;
  }
  .rate-row:last-child { border-bottom: none; }
  .rate-did { font-family: var(--mono); font-size: 11px; color: var(--tx2); width: 36px; flex-shrink: 0; }
  .rate-lbl { font-size: 9px; color: var(--tx3); flex: 1; }
  .rate-val {
    font-family: var(--mono); font-size: 17px; font-weight: 700;
    color: var(--tx1); text-align: right; min-width: 48px; line-height: 1;
  }
  .rate-val.active { color: var(--accent-g); }
  .rate-unit { font-family: var(--mono); font-size: 9px; color: var(--tx3); width: 16px; }

  /* Footer total */
  .left-footer {
    padding: 7px 10px; border-top: 1px solid var(--border); background: #1c1c1c;
    display: flex; justify-content: space-between; align-items: center; flex-shrink: 0;
  }

  /* ════ RIGHT COLUMN ════ */
  .right-col {
    flex: 1; display: flex; flex-direction: column;
    overflow: hidden; background: var(--bg);
  }
  .log-hdr {
    display: flex; align-items: center; justify-content: space-between;
    padding: 7px 14px; background: #1e1e1e;
    border-bottom: 1px solid var(--border); flex-shrink: 0;
  }
  .log-hdr-title {
    font-family: var(--mono); font-size: 10px; color: var(--tx2);
    letter-spacing: .08em; text-transform: uppercase;
  }
  .log-hdr-right {
    display: flex; align-items: center; gap: 10px;
    font-family: var(--mono); font-size: 9px; color: var(--tx3);
  }
  .log-hdr-right button {
    font-family: var(--mono); font-size: 9px; padding: 2px 8px;
    background: #333; border: 1px solid var(--border2); border-radius: 3px;
    color: var(--tx2); cursor: pointer; transition: all .2s;
  }
  .log-hdr-right button:hover { background: #444; color: var(--tx1); }

  .log-body {
    flex: 1; overflow-y: auto; padding: 6px 0;
    font-family: var(--mono); font-size: 11px; line-height: 1.7;
  }
  .log-entry { padding: 0 14px; color: var(--tx3); white-space: pre-wrap; word-break: break-all; }
  .log-entry .ts { color: #4a5060; }
  .log-entry.ok   { color: var(--accent-g); }
  .log-entry.err  { color: var(--accent-r); }
  .log-entry.info { color: var(--accent-b); }
  .log-entry.warn { color: var(--accent-o); }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: #3a3a3a; border-radius: 99px; }
  ::-webkit-scrollbar-thumb:hover { background: #555; }
</style>
</head>
<body>

<!-- TOPBAR -->
<div class="topbar">
  <span class="topbar-title">CSI IoT Collection Dashboard</span>
  <div class="topbar-sep"></div>
  <div class="topbar-indicators">
    <div class="indicator" id="ind-tcp">
      <span class="dot"></span><span id="ind-tcp-lbl">TCP: OFF</span>
    </div>
    <div class="indicator" id="ind-esp1s">
      <span class="dot"></span><span>ESP1: <span id="ind-esp1-lbl">OFF</span></span>
    </div>
    <div class="indicator" id="ind-esp2s">
      <span class="dot"></span><span>ESP2: <span id="ind-esp2-lbl">OFF</span></span>
    </div>
    <div class="indicator" id="ind-esp3s">
      <span class="dot"></span><span>ESP3: <span id="ind-esp3-lbl">OFF</span></span>
    </div>
  </div>
</div>

<!-- WORKSPACE -->
<div class="workspace">

  <!-- LEFT COLUMN -->
  <div class="left-col">

    <!-- UART / ESP32 header -->
    <div class="sec-hdr">
      <span>UART / ESP32</span>
      <button id="btn-refresh-com" onclick="doRefreshPorts()" disabled>↻ Refresh COM</button>
    </div>

    <!-- TCP connect box -->
    <div class="tcp-box">
      <div class="tcp-label">Kết nối TCP tới Laptop 1</div>
      <div class="btn-row-2">
        <button class="btn btn-g" id="btn-tcp-connect" onclick="doConnect()">Kết nối</button>
        <button class="btn btn-r" id="btn-tcp-disc" onclick="doDisconnect()" disabled>Ngắt</button>
      </div>
      <div class="tcp-host" id="tcp-host-lbl"></div>
    </div>

    <!-- UART 1 / ESP1 -->
    <div class="uart-panel">
      <div class="uart-hdr">
        <span class="uart-label">UART 1 / ESP1</span>
        <div class="uart-status disconnected" id="st-esp1">
          <span class="sdot"></span><span id="st-esp1-lbl">NOT_CONNECTED</span>
        </div>
      </div>
      <div class="form-grid">
        <div class="form-item">
          <label>PORT</label>
          <select id="com-esp1" disabled><option value="">--</option></select>
        </div>
        <div class="form-item">
          <label>BAUDRATE</label>
          <select id="baud-esp1" disabled>
            <option value="115200">115200</option>
            <option value="460800">460800</option>
            <option value="921600">921600</option>
          </select>
        </div>
      </div>
      <div class="btn-row-2">
        <button class="btn btn-g" id="btn-conn-esp1" onclick="doEspConnect('esp1')" disabled>Kết nối</button>
        <button class="btn btn-r" id="btn-disc-esp1" onclick="doEspDisconnect('esp1')" disabled>Ngắt</button>
      </div>
      <div class="uart-err" id="err-esp1"></div>
    </div>

    <!-- UART 2 / ESP2 -->
    <div class="uart-panel">
      <div class="uart-hdr">
        <span class="uart-label">UART 2 / ESP2</span>
        <div class="uart-status disconnected" id="st-esp2">
          <span class="sdot"></span><span id="st-esp2-lbl">NOT_CONNECTED</span>
        </div>
      </div>
      <div class="form-grid">
        <div class="form-item">
          <label>PORT</label>
          <select id="com-esp2" disabled><option value="">--</option></select>
        </div>
        <div class="form-item">
          <label>BAUDRATE</label>
          <select id="baud-esp2" disabled>
            <option value="115200">115200</option>
            <option value="460800">460800</option>
            <option value="921600">921600</option>
          </select>
        </div>
      </div>
      <div class="btn-row-2">
        <button class="btn btn-g" id="btn-conn-esp2" onclick="doEspConnect('esp2')" disabled>Kết nối</button>
        <button class="btn btn-r" id="btn-disc-esp2" onclick="doEspDisconnect('esp2')" disabled>Ngắt</button>
      </div>
      <div class="uart-err" id="err-esp2"></div>
    </div>

    <!-- UART 3 / ESP3 -->
    <div class="uart-panel">
      <div class="uart-hdr">
        <span class="uart-label">UART 3 / ESP3</span>
        <div class="uart-status disconnected" id="st-esp3">
          <span class="sdot"></span><span id="st-esp3-lbl">NOT_CONNECTED</span>
        </div>
      </div>
      <div class="form-grid">
        <div class="form-item">
          <label>PORT</label>
          <select id="com-esp3" disabled><option value="">--</option></select>
        </div>
        <div class="form-item">
          <label>BAUDRATE</label>
          <select id="baud-esp3" disabled>
            <option value="115200">115200</option>
            <option value="460800">460800</option>
            <option value="921600">921600</option>
          </select>
        </div>
      </div>
      <div class="btn-row-2">
        <button class="btn btn-g" id="btn-conn-esp3" onclick="doEspConnect('esp3')" disabled>Kết nối</button>
        <button class="btn btn-r" id="btn-disc-esp3" onclick="doEspDisconnect('esp3')" disabled>Ngắt</button>
      </div>
      <div class="uart-err" id="err-esp3"></div>
    </div>

    <!-- Rate ESP -->
    <div class="sec-hdr" style="border-top:1px solid var(--border);">
      <span>Rate ESP</span>
      <span style="color:var(--tx3);font-size:9px;">packets/s</span>
    </div>
    <div>
      <div class="rate-row">
        <span class="rate-did">ESP1</span>
        <span class="rate-lbl">rate</span>
        <span class="rate-val" id="rate-esp1">0</span>
        <span class="rate-unit">Hz</span>
      </div>
      <div class="rate-row">
        <span class="rate-did">ESP2</span>
        <span class="rate-lbl">rate</span>
        <span class="rate-val" id="rate-esp2">0</span>
        <span class="rate-unit">Hz</span>
      </div>
      <div class="rate-row">
        <span class="rate-did">ESP3</span>
        <span class="rate-lbl">rate</span>
        <span class="rate-val" id="rate-esp3">0</span>
        <span class="rate-unit">Hz</span>
      </div>
    </div>

    <div style="flex:1;"></div>

    <!-- Footer -->
    <div class="left-footer">
      <span style="font-family:var(--mono);font-size:9px;color:var(--tx3);">TOTAL PKTS</span>
      <span style="font-family:var(--mono);font-size:13px;color:var(--accent-g);font-weight:700;"
            id="total-pkts">0</span>
    </div>

  </div><!-- /left-col -->

  <!-- RIGHT COLUMN -->
  <div class="right-col">
    <div class="log-hdr">
      <span class="log-hdr-title">LOG REALTIME</span>
      <div class="log-hdr-right">
        <span id="log-count-lbl">0 entries</span>
        <button onclick="clearLog()">CLEAR</button>
      </div>
    </div>
    <div class="log-body" id="log-body"></div>
  </div>

</div><!-- /workspace -->

<script>
// ═══════════════════
// State
// ═══════════════════
const DEVICES = ["esp1","esp2","esp3"];
const state = {
  connected: false,
  esp: {
    esp1: {status:"disconnected",pkt_rate:0,pkt_total:0,message:"",com:"",baudrate:115200},
    esp2: {status:"disconnected",pkt_rate:0,pkt_total:0,message:"",com:"",baudrate:115200},
    esp3: {status:"disconnected",pkt_rate:0,pkt_total:0,message:"",com:"",baudrate:115200},
  },
};
let logCount = 0;

// ═══════════════════
// Log
// ═══════════════════
function log(msg, cls="") {
  const body = document.getElementById("log-body");
  const now  = new Date().toLocaleTimeString("vi",{hour12:false});
  const el   = document.createElement("div");
  el.className = "log-entry " + cls;
  el.innerHTML = `<span class="ts">[${now}]</span> ${esc(msg)}`;
  body.appendChild(el);
  body.scrollTop = body.scrollHeight;
  logCount++;
  document.getElementById("log-count-lbl").textContent = logCount + " entries";
  while (body.children.length > 500) body.removeChild(body.firstChild);
}
function esc(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}
function clearLog() {
  document.getElementById("log-body").innerHTML = "";
  logCount = 0;
  document.getElementById("log-count-lbl").textContent = "0 entries";
}

// ═══════════════════
// API
// ═══════════════════
async function api(method, path, body) {
  const opts = {method, headers:{"Content-Type":"application/json"}};
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(path, opts);
  if (!res.ok) {
    const err = await res.json().catch(()=>({detail:res.statusText}));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

// ═══════════════════
// TCP
// ═══════════════════
async function doConnect() {
  document.getElementById("btn-tcp-connect").disabled = true;
  log("Đang kết nối tới Laptop 1 …","info");
  try {
    const res = await api("POST","/connect");
    state.connected = true;
    updateTopbarTCP(true);
    log("✓ Kết nối TCP thành công – "+res.host+":"+res.port,"ok");
    await doRefreshPorts();
  } catch(e) {
    log("✗ Kết nối thất bại: "+e.message,"err");
    document.getElementById("btn-tcp-connect").disabled = false;
  }
}

async function doDisconnect() {
  log("Đang ngắt kết nối …","info");
  try {
    await api("POST","/disconnect");
    state.connected = false;
    updateTopbarTCP(false);
    DEVICES.forEach(d => {
      state.esp[d] = {status:"disconnected",pkt_rate:0,pkt_total:0,message:"",com:"",baudrate:115200};
      updateEspUI(d);
    });
    resetCom();
    updateTotal();
    log("Đã ngắt kết nối TCP","warn");
  } catch(e) {
    log("Lỗi ngắt kết nối: "+e.message,"err");
  }
}

function updateTopbarTCP(on) {
  const ind = document.getElementById("ind-tcp");
  ind.className = "indicator"+(on?" on":"");
  document.getElementById("ind-tcp-lbl").textContent = on?"TCP: ON":"TCP: OFF";
  document.getElementById("btn-tcp-connect").disabled = on;
  document.getElementById("btn-tcp-disc").disabled    = !on;
  document.getElementById("btn-refresh-com").disabled = !on;
  DEVICES.forEach(d => {
    document.getElementById("com-"+d).disabled  = !on;
    document.getElementById("baud-"+d).disabled = !on;
    const s = state.esp[d];
    document.getElementById("btn-conn-"+d).disabled = !on || s.status==="connected";
    document.getElementById("btn-disc-"+d).disabled = !on || s.status!=="connected";
  });
}

// ═══════════════════
// COM
// ═══════════════════
async function doRefreshPorts() {
  try {
    const res = await api("GET","/com-ports");
    const ports = res.ports || [];
    DEVICES.forEach(d => {
      const sel = document.getElementById("com-"+d);
      const cur = sel.value;
      sel.innerHTML = ports.length
        ? ports.map(p=>`<option value="${p}"${p===cur?" selected":""}>${p}</option>`).join("")
        : `<option value="">— Không có cổng —</option>`;
    });
    log("Tìm thấy "+ports.length+" cổng COM: "+(ports.join(", ")||"—"),"ok");
  } catch(e) {
    log("Lỗi lấy COM: "+e.message,"err");
  }
}

function resetCom() {
  DEVICES.forEach(d => {
    document.getElementById("com-"+d).innerHTML = `<option value="">--</option>`;
  });
}

// ═══════════════════
// ESP connect/disc
// ═══════════════════
async function doEspConnect(did) {
  const com      = document.getElementById("com-"+did).value;
  const baudrate = parseInt(document.getElementById("baud-"+did).value);
  if (!com) { log("["+did+"] Chọn cổng COM trước","warn"); return; }
  document.getElementById("btn-conn-"+did).disabled = true;
  log("["+did+"] Đang kết nối "+com+" @ "+baudrate+" baud …","info");
  try {
    await api("POST","/uart/connect",{device_id:did,com,baudrate});
  } catch(e) {
    log("["+did+"] ✗ Lỗi: "+e.message,"err");
    document.getElementById("btn-conn-"+did).disabled = false;
  }
}

async function doEspDisconnect(did) {
  document.getElementById("btn-disc-"+did).disabled = true;
  log("["+did+"] Đang ngắt kết nối …","info");
  try {
    await api("POST","/uart/disconnect",{device_id:did});
  } catch(e) {
    log("["+did+"] ✗ Lỗi: "+e.message,"err");
    document.getElementById("btn-disc-"+did).disabled = false;
  }
}

// ═══════════════════
// Update UI per ESP
// ═══════════════════
function updateEspUI(did) {
  const s       = state.esp[did];
  const stEl    = document.getElementById("st-"+did);
  const stLbl   = document.getElementById("st-"+did+"-lbl");
  const errEl   = document.getElementById("err-"+did);
  const rateEl  = document.getElementById("rate-"+did);
  const indEl   = document.getElementById("ind-"+did+"s");
  const indLbl  = document.getElementById("ind-"+did+"-lbl");
  const btnConn = document.getElementById("btn-conn-"+did);
  const btnDisc = document.getElementById("btn-disc-"+did);

  const LBL = {disconnected:"NOT_CONNECTED",connected:"CONNECTED",error:"ERROR"};
  stEl.className   = "uart-status "+s.status;
  stLbl.textContent = LBL[s.status] || s.status.toUpperCase();

  if (indEl) {
    indEl.className = "indicator"+(s.status==="connected"?" on":s.status==="error"?" err":"");
    if (indLbl) indLbl.textContent = s.status==="connected"?"ON":s.status==="error"?"ERR":"OFF";
  }

  const rate = s.pkt_rate || 0;
  rateEl.textContent = rate.toFixed(1);
  rateEl.className   = "rate-val"+(rate>0?" active":"");

  if (s.status==="error" && s.message) {
    errEl.textContent = s.message;
    errEl.classList.add("show");
  } else {
    errEl.classList.remove("show");
  }

  const ok = state.connected;
  btnConn.disabled = s.status==="connected" ? true  : !ok;
  btnDisc.disabled = s.status==="connected" ? !ok   : true;
}

function updateTotal() {
  let t = 0;
  DEVICES.forEach(d => t += state.esp[d].pkt_total||0);
  document.getElementById("total-pkts").textContent = t.toLocaleString("vi");
}

// ═══════════════════
// Polling 1.5s
// ═══════════════════
let _prev = {};

async function poll() {
  try {
    const res = await api("GET","/connection-status");
    const was  = state.connected;
    state.connected = res.connected;
    document.getElementById("tcp-host-lbl").textContent = res.host+":"+res.port;

    if (was && !res.connected) {
      updateTopbarTCP(false);
      log("⚠ Mất kết nối TCP với Laptop 1!","err");
      DEVICES.forEach(d => {
        state.esp[d] = {status:"disconnected",pkt_rate:0,pkt_total:0,message:"",com:"",baudrate:115200};
        updateEspUI(d);
      });
      resetCom();
    }
  } catch {}

  if (!state.connected) return;

  try {
    const res = await api("GET","/esp-status");
    const devs = res.devices || {};
    DEVICES.forEach(d => {
      const sv = devs[d]; if (!sv) return;
      const prev = _prev[d] || {};
      if (prev.status !== sv.status) {
        if (sv.status==="connected")
          log("["+d+"] ✓ Đã kết nối "+sv.com+" @ "+sv.baudrate+" baud","ok");
        else if (sv.status==="disconnected")
          log("["+d+"] Đã ngắt kết nối","warn");
        else if (sv.status==="error")
          log("["+d+"] ✗ Lỗi: "+sv.message,"err");
      }
      state.esp[d] = sv;
      updateEspUI(d);
    });
    _prev = JSON.parse(JSON.stringify(devs));
    updateTotal();
  } catch {}
}

// ═══════════════════
// Init
// ═══════════════════
updateTopbarTCP(false);
DEVICES.forEach(d => updateEspUI(d));
// Hiển thị IP ngay khi load
api("GET","/connection-status").then(r=>{
  document.getElementById("tcp-host-lbl").textContent = r.host+":"+r.port;
}).catch(()=>{});
setInterval(poll, 1500);
log("CSI IoT Collection Dashboard khởi động. Nhấn [Kết nối] để bắt đầu.","info");
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