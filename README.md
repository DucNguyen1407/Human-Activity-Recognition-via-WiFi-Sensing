# Human Activity Recognition via WiFi Sensing

A real-time CSI (Channel State Information) data collection system using 3 ESP32 nodes communicating over RS-485, processed on a laptop server using Python asyncio, and transmitted through a TCP Server–Client architecture to support Human Activity Recognition (HAR).

---

## Table of Contents

- [System Overview](#system-overview)
- [Hardware Architecture](#hardware-architecture)
- [Software Architecture](#software-architecture)
- [Directory Structure](#directory-structure)
- [File Descriptions](#file-descriptions)
- [Binary Frame Protocol](#binary-frame-protocol)
- [TCP JSON Lines Protocol](#tcp-json-lines-protocol)
- [Installation & Running](#installation--running)
- [Data Flow Diagram](#data-flow-diagram)

---

## System Overview

The system consists of two main roles:

| Role | Description |
|------|-------------|
| **Collection (Laptop 1)** | Reads binary CSI frames from 3 ESP32 nodes via COM ports (RS-485 → USB), parses frames, and opens a TCP Server to forward data to Management |
| **Management (Laptop 2)** | Connects via TCP to Collection, receives CSI packets, enqueues them into a thread-safe queue, coordinates CSV recording, and serves the Web UI |

---

## Hardware Architecture

```
┌─────────────┐     UART / RS-485     ┌──────────────────────────────┐
│   ESP32 #1  │ ─────────────────────►│                              │
├─────────────┤                        │   TTL–RS-485 Module (x3)     │
│   ESP32 #2  │ ─────────────────────►│        │                     │
├─────────────┤                        │   RS-485–USB Module (x3)     │
│   ESP32 #3  │ ─────────────────────►│        │                     │
└─────────────┘                        │   COM ports (USB) ×3         │
                                       └────────────┬─────────────────┘
                                                    │
                                           ┌────────▼────────┐
                                           │    Laptop 1     │
                                           │  (Collection)   │
                                           │  esp_real.py    │
                                           └────────┬────────┘
                                                    │  TCP :9200 (JSON Lines)
                                           ┌────────▼────────┐
                                           │    Laptop 2     │
                                           │  (Management)   │
                                           │  FastAPI Server │
                                           └─────────────────┘
```

**Hardware notes:**
- **ESP32**: WiFi CSI sensor node, transmits binary frames over UART
- **TTL–RS-485 Module**: Converts ESP32 UART signal to RS-485 for long-distance transmission
- **RS-485–USB Module**: Converts RS-485 back to USB so the laptop can receive via COM port
- Each ESP32 uses its own dedicated module pair → 3 separate COM ports on Laptop 1

---

## Software Architecture

```
Collection side (esp_real.py)                  Management side (Laptop 2)
──────────────────────────────                 ────────────────────────────────────────
                                               ┌────────────────────────────────────────┐
 [COM3]  SerialCollector(esp1)                 │   EspTcpClient          UartManager    │
 [COM4]  SerialCollector(esp2)   TCP :9200     │   ─────────────         ────────────── │
 [COM5]  SerialCollector(esp3) ──────────────► │   read_packet()  ──►   csi_queue       │
          │                                    │   (JSON Lines)         (Queue 50,000)  │
          │ asyncio read UART                  │                        put_packet()    │
          │ parse binary → dict                │                        get_packet()    │
          │                                    └─────────────────────────────┬──────────┘
 TCPServerApp                                                                │
 ──────────────────────                                                      ▼
 asyncio.start_server(:9200) ◄── control commands                      CsiService
 handle_client()                                                        writes CSV per session
 send_management_packet()
 broadcast_realtime(:9201) ──────────────────────────────► Realtime Viewer
```

---

## Directory Structure

```
app/
├── collection/
│   ├── esp_real.py              ← Core: asyncio UART reader + TCP Server
│   ├── tcp_stream_server.py     ← TCP Server wrapper (threading-based)
│   ├── server2server.py         ← Relay: Server A → Client B forwarding
│   ├── clientB.py               ← TCP asyncio client receiving data
│   ├── esp_fake.py              ← Fake data generator (for testing)
│   ├── asus_fake_bin.py         ← Fake binary stream (ASUS test)
│   └── asus_fake_csv.py         ← Fake CSV stream (ASUS test)
│
├── adapters/
│   ├── esp_tcp_client.py        ← TCP Client: Management → Collection
│   ├── nexmon_tcp_client.py     ← TCP Client for Nexmon CSI
│   └── webcam_adapter.py        ← Webcam adapter
│
├── services/
│   ├── uart_manager.py          ← Queue management + 3 ESP device state
│   ├── csi_service.py           ← CSV writer consuming from queue
│   ├── recording_service.py     ← Session recording coordinator
│   ├── session_service.py       ← Session lifecycle management
│   ├── camera_service.py        ← Camera recording service
│   ├── ethernet_manager.py      ← Ethernet / Nexmon CSI management
│   └── scenario_audio_service.py← Audio scenario playback
│
├── api/                         ← FastAPI routes (REST + WebSocket)
├── core/
│   ├── config.py                ← Path configuration
│   └── time_utils.py            ← Time utilities (unix_now_us)
├── resources/
│   ├── audio/                   ← Audio guidance files (.wav)
│   └── scenarios/               ← Action scenario definitions (.json)
├── main.py                      ← FastAPI application entry point
└── README.md
```

---

## File Descriptions

### 1. `collection/esp_real.py` — Core: UART Reader + TCP Server

The central file of the data collection layer, running on **Laptop 1 (Collection)**.

#### Key components

| Component | Description |
|-----------|-------------|
| `parse_packet(raw, device_id)` | Validates and parses a 155-byte binary frame into a Python `dict` |
| `find_frame(buf)` | Scans a streaming byte buffer for a valid frame using magic bytes + length field |
| `calculate_xor_checksum(data)` | Computes XOR checksum over `raw[0:154]` for frame integrity check |
| `SerialCollector` | Asyncio class that reads raw bytes from a single COM port |
| `TCPServerApp` | Asyncio TCP server: receives commands from Management, streams CSI data back |

#### Processing pipeline

```
COM port (RS-485 / USB)
    │
    ▼  serial_asyncio.open_serial_connection()
reader.read(4096)  →  buf.extend(chunk)
    │
    ▼  find_frame(buf)           ← locate magic bytes 0xAA 0x55
frame (155 bytes)
    │
    ▼  parse_packet(frame, device_id)
    │    1. Validate size == 155
    │    2. Check magic bytes == 0xAA 0x55
    │    3. Check length field == 155
    │    4. XOR checksum validation
    │    5. struct.unpack_from() → mac, seq, ts_us, rssi, ch, agc, fft, nf
    │    6. Unpack 128 × int8 CSI raw → 64 [Q, I] pairs
    ▼
{
    "type":        "csi_data",
    "device_id":   "AA:BB:CC:DD:EE:FF",   ← real MAC from frame
    "seq":         12345,
    "timestamp":   1234567890123,          ← ts_us from ESP32
    "radio": {
        "rssi": -60, "channel": 6,
        "agc_gain": 0, "fft_gain": 0, "noise_floor": -95
    },
    "csi": [[Q0,I0], [Q1,I1], ..., [Q63,I63]]   ← 64 pairs
}
    │
    ▼  send_management_packet()  →  TCP :9200  →  Laptop 2
    ▼  broadcast_realtime()      →  TCP :9201  →  Realtime Viewer
```

#### TCP Server ports

| Port | Client | Purpose |
|------|--------|---------|
| `:9200` | Management (Laptop 2) | UART control commands + CSI data stream |
| `:9201` | Realtime Viewer | Read-only CSI mirror, no control |

---

### 2. `collection/tcp_stream_server.py` — TCP Server Wrapper

A lightweight, threading-based TCP server used as a reusable wrapper for the Collection stub.

| Method | Description |
|--------|-------------|
| `start()` | Binds and listens, spawns an accept loop thread |
| `send_packet(dict)` | Serializes a dict to a JSON line and sends to the connected client |
| `set_message_handler(fn)` | Registers a callback for inbound JSON commands from Management |
| `set_client_connected_handler(fn)` | Registers a callback fired immediately when a client connects |

All communication follows the **JSON Lines** convention: one JSON object per line, terminated by `\n`.

---

### 3. `collection/server2server.py` — Relay: Server A → Client B

A relay architecture where multiple clients connect to Server A, and all messages are forwarded to a single Client B.

```
Client A1 ─┐
Client A2 ──┤  Server A (:9001)  ──►  Client B (:9100)
Client A3 ─┘  (up to 3 clients)        (1 client only)
```

| Function | Description |
|----------|-------------|
| `server_a_thread()` | TCP server accepting up to 3 clients, one thread per client |
| `server_b_thread()` | TCP server accepting exactly 1 client (the downstream receiver) |
| `forward_to_b(data)` | Forwards raw bytes from any Client A to Client B |

All messages are JSON Lines; the relay forwards them byte-for-byte without modification.

---

### 4. `collection/clientB.py` — TCP asyncio Client

An asyncio client that connects to Server B and receives forwarded CSI data.

**Behavior:**
- Connects via `asyncio.open_connection()` to `127.0.0.1:9100`
- Reads lines with `reader.readline()`, decodes UTF-8
- Parses JSON → Python `dict`
- Prints a compact summary: `device_id`, `seq`, `timestamp`, `rssi`, CSI array lengths per subcarrier

This file serves as the foundation for integrating downstream processing logic (e.g., feeding an `asyncio.Queue` for ML inference pipelines).

---

### 5. `adapters/esp_tcp_client.py` — TCP Client: Management → Collection

A TCP client running on **Laptop 2 (Management)** that connects to the Collection TCP server.

#### Key functions

| Function | Description |
|----------|-------------|
| `EspTcpClient.connect()` | Establishes TCP connection to `127.0.0.1:9200` |
| `read_packet()` | Reads one JSON line; auto-maps `device_id` (MAC → esp1/2/3) |
| `send_message(dict)` | Sends a JSON line command to Collection |
| `request_com_ports()` | Requests Collection to report available COM ports |
| `send_uart_control(...)` | Sends a connect / disconnect command for a specific ESP |
| `map_esp_mac_to_id(mac)` | Collection sends real MAC → map to `esp1` / `esp2` / `esp3` |
| `map_esp_id_to_mac(id)` | Backend sends `esp1` → map back to real MAC before sending to Collection |

#### MAC address mapping (real hardware)

```python
ESP_MAC_TO_ID = {
    "D0:CF:13:ED:2E:EC": "esp1",
    "D0:CF:13:EB:8A:9C": "esp2",
    "D0:CF:13:EC:49:04": "esp3",
}
```

---

### 6. `services/uart_manager.py` — Queue Management + Device State

The central management layer on **Laptop 2**. Receives packets from `EspTcpClient` and dispatches them into a thread-safe queue consumed by `CsiService`.

#### Key components

| Component | Description |
|-----------|-------------|
| `csi_queue` | `queue.Queue(maxsize=50,000)` — packet buffer |
| `put_packet(packet)` | Validates device ID, checks recording flag, then `put_nowait()` |
| `get_packet(timeout)` | Called by `CsiService` to dequeue packets for CSV writing |
| `update_packet_stat()` | Computes real-time packet rate (Hz) using a 1-second sliding window over CSI timestamps |
| `connect_device()` | Sends `uart_control connect` to Collection via TCP |
| `disconnect_device()` | Sends `uart_control disconnect` to Collection |
| `handle_collection_message()` | Processes `com_list`, `uart_status`, `control_ack` messages |
| `_client_receive_loop()` | Background thread that continuously reads packets from `EspTcpClient` |

#### Queue behavior

| Condition | Behavior |
|-----------|----------|
| `recording_enabled = False` | Packet silently dropped — no data accumulated before session start |
| Queue full (`queue.Full`) | Packet dropped, `dropped_packets` counter incremented |
| Normal | Packet placed in `csi_queue` with `put_nowait()` |

`CSI_QUEUE_MAXSIZE = 50,000` provides approximately **250 seconds** of buffer at 200 Hz.

#### Device state machine

```
DISCONNECTED ──(uart_control connect)──► CONNECTING
CONNECTING   ──(uart_status connected)──► CONNECTED
CONNECTED    ──(packet received in 1s)──► RECEIVING
RECEIVING    ──(no packet for 1s)────────► CONNECTED
CONNECTED    ──(TCP lost / disconnect)──► DISCONNECTED
CONNECTED    ──(serial error)───────────► ERROR
```

---

## Binary Frame Protocol

Every frame transmitted from an ESP32 has a **fixed size of 155 bytes**:

```
Offset   Size   Type      Field              Description
──────   ────   ────      ─────              ───────────────────────────────────
0        2      uint8[2]  magic_bytes        0xAA 0x55  (synchronization marker)
2        1      uint8     packet_length      Always 155
3        6      uint8[6]  mac_address        ESP32 MAC address (big-endian bytes)
9        4      uint32    seq                Packet sequence number (little-endian)
13       8      uint64    ts_us              ESP32 timestamp in microseconds
21       1      int8      rssi               Received signal strength (dBm)
22       1      uint8     channel            WiFi channel number
23       1      uint8     agc_gain           Automatic gain control value
24       1      uint8     fft_gain           FFT gain value
25       1      int8      noise_floor        Noise floor (dBm)
26       128    int8[128] csi_raw            CSI data: 64 interleaved (Q, I) pairs
154      1      uint8     xor_checksum       XOR of raw[0:154]
```

**Struct format string**: `"<6sIQbBBBb"` (little-endian, applied at offset 3)

**Frame validation steps** (in order, inside `parse_packet()`):

1. `len(raw) == 155`
2. `raw[:2] == b'\xAA\x55'`
3. `raw[2] == 155`
4. `XOR(raw[0:154]) == raw[154]`

**CSI data unpacking:**

```python
# Unpack 128 signed int8 values
csi_raw = struct.unpack_from("<128b", raw, offset=26)

# Group into 64 [Q, I] pairs
csi_pairs = [
    [int(csi_raw[i]), int(csi_raw[i + 1])]
    for i in range(0, 128, 2)
]
# Result: [[Q0,I0], [Q1,I1], ..., [Q63,I63]]
```

---

## TCP JSON Lines Protocol

All TCP communication uses **JSON Lines**: each message is a single JSON object on one line, terminated by `\n`.

### Collection → Management

```jsonc
// Available COM ports
{"type": "com_list", "ports": ["COM3", "COM4", "COM5"]}

// ESP connected successfully
{"type": "uart_status", "device_id": "esp1", "status": "connected",
 "config": {"com": "COM3", "baudrate": 115200}}

// ESP disconnected
{"type": "uart_status", "device_id": "esp1", "status": "disconnected"}

// ESP error
{"type": "uart_status", "device_id": "esp1", "status": "error",
 "message": "Failed to open COM3: [Errno 13] Permission denied"}

// CSI data packet
{
  "type": "csi_data",
  "device_id": "D0:CF:13:ED:2E:EC",
  "seq": 12345,
  "timestamp": 1234567890123,
  "radio": {
    "rssi": -60, "channel": 6,
    "agc_gain": 0, "fft_gain": 0, "noise_floor": -95
  },
  "csi": [[-3, 5], [2, -1], ..., [0, 4]]
}
```

### Management → Collection

```jsonc
// Request COM port list
{"type": "get_com_ports", "source": "management"}

// Connect an ESP to a COM port
{"type": "uart_control", "source": "management", "action": "connect",
 "device_id": "D0:CF:13:ED:2E:EC", "com": "COM3", "baudrate": 115200}

// Disconnect an ESP
{"type": "uart_control", "source": "management", "action": "disconnect",
 "device_id": "D0:CF:13:ED:2E:EC"}
```

---

## Installation & Running

### Requirements

- Python >= 3.10
- Physical wiring: 3 × ESP32 → 3 × TTL-RS485 module → 3 × RS485-USB module → Laptop 1 (COM ports)

### Install dependencies

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

**Key dependencies:**

| Package | Version | Purpose |
|---------|---------|---------|
| `pyserial` | 3.5 | UART / COM port communication |
| `pyserial-asyncio` | 0.6 | Asyncio wrapper for serial I/O |
| `fastapi` | 0.136.0 | Web API server (Management side) |
| `uvicorn` | 0.44.0 | ASGI server |
| `numpy` | 2.4.4 | CSI signal processing |
| `pandas` | 3.0.2 | CSV read/write |

### Run Collection (Laptop 1)

```bash
.venv\Scripts\activate
python -m app.collection.esp_real
```

The server will listen on:
- `:9200` — Management TCP channel (JSON Lines, bidirectional)
- `:9201` — Realtime viewer TCP channel (read-only CSI stream)

### Run Management (Laptop 2)

```bash
.venv\Scripts\activate
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Configure ESP32 MAC addresses

Edit the mapping in `adapters/esp_tcp_client.py`:

```python
ESP_MAC_TO_ID = {
    "D0:CF:13:ED:2E:EC": "esp1",   # ← real MAC of ESP32 node 1
    "D0:CF:13:EB:8A:9C": "esp2",   # ← real MAC of ESP32 node 2
    "D0:CF:13:EC:49:04": "esp3",   # ← real MAC of ESP32 node 3
}
```

### Supported baudrates

| Baudrate | Notes |
|----------|-------|
| `115200` | Default — most stable |
| `460800` | High speed |
| `921600` | Very high speed — verify RS-485 cable quality |

---

## Data Flow Diagram

```
ESP32 #1 ──UART──► COM3 ─┐
ESP32 #2 ──UART──► COM4 ──┤──► SerialCollector (asyncio)
ESP32 #3 ──UART──► COM5 ─┘         │
                               parse_packet()
                          binary 155B → Python dict
                                    │
                          TCPServerApp (:9200)
                          asyncio TCP Server
                                    │  JSON Lines \n
                                    ▼
                         EspTcpClient.read_packet()
                         map MAC → esp1 / esp2 / esp3
                                    │
                         UartManager._client_receive_loop()
                                    │
                     put_packet() ──► csi_queue (Queue, 50k max)
                                    │
                         CsiService.get_packet()
                                    │
                           Write CSV per session
                                    │
                      Web UI (FastAPI + WebSocket)
```

---

## Author

**Duc Nguyen** — Final Year Thesis: *Human Activity Recognition via WiFi CSI Sensing*