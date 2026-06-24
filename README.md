# Human Activity Recognition via WiFi Sensing

A real-time CSI (Channel State Information) data collection system using 3 ESP32 nodes communicating over RS-485, processed on a laptop server with Python asyncio, and transmitted through a TCP Server–Client architecture to support Human Activity Recognition (HAR).

---

## Table of Contents

- [System Overview](#system-overview)
- [Hardware Setup](#hardware-setup)
- [Software Architecture](#software-architecture)
- [Directory Structure](#directory-structure)
- [Key Files](#key-files)
- [Binary Frame Format](#binary-frame-format)
- [TCP Communication Protocol](#tcp-communication-protocol)
- [Installation & Running](#installation--running)

---

## System Overview

| Role | Description |
|------|-------------|
| **Collection (Laptop 1)** | Reads binary CSI frames from 3 ESP32 nodes via COM ports (RS-485 → USB), parses frames, and streams data to Management over TCP |
| **Management (Laptop 2)** | Connects to Collection via TCP, enqueues CSI packets, coordinates CSV recording, and serves the Web UI |

---

## Hardware Setup

### Wiring Diagram

![Wiring Diagram](docs/images/wiring_diagram.png)

> *Place your wiring diagram at `docs/images/wiring_diagram.png`*

### Hardware Components

| Component | Quantity | Role |
|-----------|----------|------|
| ESP32 | 3 | WiFi CSI sensor nodes |
| TTL–RS-485 module | 3 | Convert ESP32 UART → RS-485 |
| RS-485–USB module | 3 | Convert RS-485 → USB COM port |
| Laptop 1 (Collection) | 1 | Receive & forward CSI data |
| Laptop 2 (Management) | 1 | Record sessions, serve Web UI |

Each ESP32 uses one dedicated TTL–RS-485 + RS-485–USB module pair, resulting in **3 separate COM ports** on Laptop 1.

### Photos

**Module close-up:**

![Module Close-up](docs/images/module_closeup.jpg)

> *Place a close-up photo of one module at `docs/images/module_closeup.jpg`*

**Full room setup:**

![Room Overview](docs/images/room_overview.jpg)

> *Place a full-room overview photo at `docs/images/room_overview.jpg`*

---

## Software Architecture

The system is split into two layers that communicate over TCP on port `9200` using **JSON Lines** (one JSON object per line, terminated by `\n`).

| Layer | Main file | Responsibility |
|-------|-----------|----------------|
| **Collection** | `collection/esp_real.py` | asyncio UART reader, binary parser, TCP server |
| **Adapter** | `adapters/esp_tcp_client.py` | TCP client, MAC → device ID mapping |
| **Management** | `services/uart_manager.py` | Queue, device state machine, CSV coordination |

---

## Directory Structure

```
app/
├── collection/
│   ├── esp_real.py              ← Core: UART reader + TCP server (asyncio)
│   ├── tcp_stream_server.py     ← TCP server wrapper (threading)
│   ├── server2server.py         ← Relay: Server A → Client B
│   └── clientB.py               ← TCP asyncio client
├── adapters/
│   └── esp_tcp_client.py        ← TCP client: Management → Collection
├── services/
│   ├── uart_manager.py          ← Queue + device state management
│   ├── csi_service.py           ← CSV writer
│   └── recording_service.py     ← Session coordinator
├── api/                         ← FastAPI routes (REST + WebSocket)
├── core/
│   ├── config.py                ← Path configuration
│   └── time_utils.py            ← Timestamp utilities
├── resources/
│   ├── audio/                   ← Guidance audio files (.wav)
│   └── scenarios/               ← Action scenario definitions (.json)
└── main.py                      ← FastAPI entry point
```

---

## Key Files

### `collection/esp_real.py`
Runs on Laptop 1. Opens a serial connection to each COM port using `serial_asyncio`, reads raw bytes, finds and validates 155-byte binary frames, parses them into Python dicts, then forwards the data as JSON Lines over TCP to Management.

### `collection/tcp_stream_server.py`
A reusable threading-based TCP server that handles one client at a time. Registers callbacks for incoming JSON commands and exposes a `send_packet()` method for outbound data.

### `collection/server2server.py`
A relay bridge: **Server A** accepts up to 3 clients (one per ESP32) on port `9001`; all incoming messages are forwarded to the single **Client B** connected on port `9100`.

### `collection/clientB.py`
An asyncio TCP client that connects to the relay's Server B, receives JSON Lines, parses each packet into a Python dict, and prints a compact summary. Serves as the base for feeding data into downstream queues or ML pipelines.

### `adapters/esp_tcp_client.py`
Runs on Laptop 2. Connects to `esp_real.py`'s TCP server, maps real ESP32 MAC addresses to logical IDs (`esp1`, `esp2`, `esp3`), and provides `read_packet()` / `send_uart_control()` methods.

### `services/uart_manager.py`
The queue hub on Laptop 2. Receives packets from `EspTcpClient` and places them into a `queue.Queue` (capacity 50 000). Tracks per-device packet rates, drop counts, and connection states. Exposes `get_packet()` for the CSV writer.

---

## Binary Frame Format

Each ESP32 transmits fixed-size **155-byte** frames:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 2 | `magic_bytes` | `0xAA 0x55` — sync marker |
| 2 | 1 | `packet_length` | Always `155` |
| 3 | 6 | `mac_address` | ESP32 MAC address |
| 9 | 4 | `seq` | Packet sequence number (uint32) |
| 13 | 8 | `ts_us` | Timestamp in microseconds (uint64) |
| 21 | 1 | `rssi` | Signal strength in dBm (int8) |
| 22 | 1 | `channel` | WiFi channel (uint8) |
| 23 | 2 | `agc`, `fft` | AGC gain, FFT gain (uint8 each) |
| 25 | 1 | `noise_floor` | Noise floor in dBm (int8) |
| 26 | 128 | `csi_raw` | 64 interleaved (Q, I) pairs — int8 each |
| 154 | 1 | `xor_checksum` | XOR of bytes `[0:154]` |

Frame validation (in order): size == 155 → magic bytes → length field → XOR checksum.

---

## TCP Communication Protocol

All messages between Collection and Management use **JSON Lines**.

**Collection → Management**

| Message type | Key fields |
|---|---|
| `com_list` | `ports: ["COM3", "COM4", "COM5"]` |
| `uart_status` | `device_id, status: connected/disconnected/error` |
| `csi_data` | `device_id, seq, timestamp, radio{rssi, channel, ...}, csi[[Q,I]×64]` |

**Management → Collection**

| Message type | Key fields |
|---|---|
| `get_com_ports` | _(no extra fields)_ |
| `uart_control` | `action: connect/disconnect, device_id, com, baudrate` |

---

## Installation & Running

### Requirements

- Python >= 3.10
- Physical wiring: 3 × ESP32 → TTL-RS485 → RS485-USB → Laptop 1

### Install

```bash
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

### Run Collection (Laptop 1)

```bash
python -m app.collection.esp_real
```

Listens on `:9200` (Management) and `:9201` (Realtime viewer).

### Run Management (Laptop 2)

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Configure ESP32 MAC addresses

Edit `adapters/esp_tcp_client.py`:

```python
ESP_MAC_TO_ID = {
    "D0:CF:13:ED:2E:EC": "esp1",
    "D0:CF:13:EB:8A:9C": "esp2",
    "D0:CF:13:EC:49:04": "esp3",
}
```

---

## Author

**Duc Nguyen** — Final Year Thesis: *Human Activity Recognition via WiFi CSI Sensing*