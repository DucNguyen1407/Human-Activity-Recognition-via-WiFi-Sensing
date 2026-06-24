# Human Activity Recognition via WiFi Sensing

Hệ thống thu thập dữ liệu CSI (Channel State Information) từ 3 node ESP32 thông qua giao tiếp RS-485, xử lý trên máy tính (laptop/server) bằng Python, và truyền tải qua kiến trúc TCP Server–Client để phục vụ bài toán nhận dạng hoạt động con người (Human Activity Recognition).

---

## Mục lục

- [Tổng quan hệ thống](#tổng-quan-hệ-thống)
- [Kiến trúc phần cứng](#kiến-trúc-phần-cứng)
- [Kiến trúc phần mềm](#kiến-trúc-phần-mềm)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Mô tả chi tiết từng file](#mô-tả-chi-tiết-từng-file)
- [Giao thức frame binary](#giao-thức-frame-binary)
- [Giao thức TCP JSON Lines](#giao-thức-tcp-json-lines)
- [Cài đặt & Chạy](#cài-đặt--chạy)
- [Sơ đồ luồng dữ liệu](#sơ-đồ-luồng-dữ liệu)

---

## Tổng quan hệ thống

Hệ thống gồm hai phần chính:

| Vai trò | Mô tả |
|---------|-------|
| **Collection (Laptop 1)** | Đọc dữ liệu CSI binary từ 3 ESP32 qua cổng COM (RS-485 → USB), parse frame, mở TCP Server để gửi dữ liệu sang Management |
| **Management (Laptop 2)** | Kết nối TCP tới Collection, nhận packet CSI, đưa vào queue, điều phối ghi CSV, phục vụ Web UI |

---

## Kiến trúc phần cứng

```
┌─────────────┐     UART/RS-485     ┌──────────────────────────┐
│   ESP32 #1  │ ──────────────────► │                          │
├─────────────┤                     │   Module TTL–RS-485      │
│   ESP32 #2  │ ──────────────────► │   (x3 module)            │
├─────────────┤                     │        │                  │
│   ESP32 #3  │ ──────────────────► │   Module RS-485–USB      │
└─────────────┘                     │        │                  │
                                    │   COM port (USB)          │
                                    └──────────┬───────────────┘
                                               │
                                      ┌────────▼────────┐
                                      │   Laptop 1      │
                                      │  (Collection)   │
                                      │  esp_real.py    │
                                      └────────┬────────┘
                                               │  TCP :9200 (JSON Lines)
                                      ┌────────▼────────┐
                                      │   Laptop 2      │
                                      │  (Management)   │
                                      │  FastAPI Server │
                                      └─────────────────┘
```

**Chú thích phần cứng:**
- **ESP32**: Node cảm biến WiFi CSI, truyền dữ liệu dạng binary frame qua UART
- **Module TTL–RS-485**: Chuyển đổi tín hiệu UART của ESP32 sang chuẩn RS-485 để truyền xa
- **Module RS-485–USB**: Chuyển đổi RS-485 sang USB để laptop nhận qua cổng COM
- Mỗi ESP32 sử dụng một bộ module riêng → 3 cổng COM trên laptop

---

## Kiến trúc phần mềm

```
Collection (esp_real.py)              Management (Laptop 2)
─────────────────────────             ──────────────────────────────────────
                                      ┌─────────────────────────────────────┐
 [COM3] SerialCollector(esp1)         │  EspTcpClient          UartManager  │
 [COM4] SerialCollector(esp2)  TCP    │  ─────────────         ──────────── │
 [COM5] SerialCollector(esp3) ──────► │  read_packet()  ──►   csi_queue     │
         │ asyncio read UART          │  (JSON Lines)         (Queue 50000) │
         │ parse binary → dict        │                        put_packet()  │
         │                            │                        get_packet()  │
 TCPServerApp                         └─────────────────────────────────────┘
 ─────────────────────                                │
 asyncio.start_server(:9200) ◄──── lệnh điều khiển   │
 handle_client()                                      ▼
 send_management_packet()                       CsiService
 broadcast_realtime(:9201)                      ghi CSV theo session
```

---

## Cấu trúc thư mục

```
app/
├── collection/
│   ├── esp_real.py              ← Core: asyncio UART reader + TCP Server
│   ├── tcp_stream_server.py     ← TCP Server wrapper (threading)
│   ├── server2server.py         ← Forward Server A → Client B
│   ├── clientB.py               ← TCP asyncio client nhận dữ liệu
│   ├── esp_fake.py              ← Fake data generator (test)
│   ├── asus_fake_bin.py         ← Fake binary (test ASUS)
│   └── asus_fake_csv.py         ← Fake CSV (test ASUS)
│
├── adapters/
│   ├── esp_tcp_client.py        ← TCP Client: Management → Collection
│   ├── nexmon_tcp_client.py     ← TCP Client cho Nexmon CSI
│   └── webcam_adapter.py        ← Webcam adapter
│
├── services/
│   ├── uart_manager.py          ← Queue & quản lý 3 ESP device
│   ├── csi_service.py           ← Ghi CSV từ queue
│   ├── recording_service.py     ← Điều phối phiên thu
│   ├── session_service.py       ← Quản lý session
│   ├── camera_service.py        ← Camera service
│   ├── ethernet_manager.py      ← Ethernet/Nexmon
│   └── scenario_audio_service.py← Phát âm thanh kịch bản
│
├── api/                         ← FastAPI routes (REST + WebSocket)
├── core/
│   ├── config.py                ← Cấu hình đường dẫn
│   └── time_utils.py            ← Tiện ích thời gian (unix_now_us)
├── resources/
│   ├── audio/                   ← File âm thanh hướng dẫn
│   └── scenarios/               ← File kịch bản JSON
├── main.py                      ← Entry point FastAPI server
└── README.md
```

---

## Mô tả chi tiết từng file

### 1. `collection/esp_real.py` — **Core: UART Reader + TCP Server**

File trung tâm của phần thu thập dữ liệu, chạy trên **Laptop 1 (Collection)**.

**Chức năng chính:**

| Thành phần | Mô tả |
|------------|-------|
| `parse_packet(raw, device_id)` | Parse binary frame 155 bytes → Python `dict` |
| `find_frame(buf)` | Tìm và đồng bộ frame trong buffer streaming |
| `calculate_xor_checksum(data)` | Tính XOR checksum để xác thực frame |
| `SerialCollector` | Asyncio class đọc UART từ một cổng COM |
| `TCPServerApp` | Asyncio TCP server, nhận lệnh & gửi CSI cho Management |

**Luồng xử lý:**
```
COM port (RS-485/USB)
    │
    ▼ serial_asyncio.open_serial_connection()
reader.read(4096)  →  buf.extend(chunk)
    │
    ▼ find_frame(buf)   ← tìm magic bytes 0xAA55
frame (155 bytes)
    │
    ▼ parse_packet(frame, device_id)
    │   1. Kiểm tra kích thước == 155
    │   2. Kiểm tra magic bytes == 0xAA 0x55
    │   3. Kiểm tra length field == 155
    │   4. XOR checksum validation
    │   5. struct.unpack_from() → mac, seq, ts_us, rssi, ch, agc, fft, nf
    │   6. Unpack 128 int8 CSI → 64 cặp [Q, I]
    ▼
dict {
    "type": "csi_data",
    "device_id": "AA:BB:CC:DD:EE:FF",  ← MAC thật từ frame
    "seq": 12345,
    "timestamp": 1234567890,            ← ts_us từ ESP32
    "radio": {"rssi": -60, "channel": 6, ...},
    "csi": [[Q0,I0], [Q1,I1], ..., [Q63,I63]]  ← 64 cặp
}
    │
    ▼ send_management_packet()  →  TCP :9200 → Laptop 2
    ▼ broadcast_realtime()      →  TCP :9201 → Realtime Viewer
```

**TCP Server ports:**
- `:9200` — Management (Laptop 2), điều khiển UART và nhận CSI
- `:9201` — Realtime viewer (đọc CSI trực tiếp, không điều khiển)

---

### 2. `collection/tcp_stream_server.py` — **TCP Server Wrapper**

TCP server đơn giản dùng `threading`, dùng làm mẫu/wrapper cho Collection stub.

**Chức năng:**
- `TcpStreamServer.start()`: mở TCP server, chờ client kết nối
- `send_packet(dict)`: gửi một packet JSON line cho client
- `set_message_handler(fn)`: đăng ký callback xử lý lệnh từ client
- `set_client_connected_handler(fn)`: callback khi client vừa kết nối
- Giao tiếp theo JSON Lines: mỗi message một dòng kết thúc `\n`

---

### 3. `collection/server2server.py` — **Forward Server A → Client B**

Kiến trúc relay: nhiều client gửi lên Server A, Server A forward sang Client B duy nhất.

```
Client A1 ─┐
Client A2 ──┤ Server A (:9001) ──► Client B (:9100)
Client A3 ─┘     (tối đa 3)         (1 client)
```

**Chức năng:**
- `server_a_thread()`: TCP server nhận tối đa 3 client, mỗi client một thread
- `server_b_thread()`: TCP server chỉ nhận đúng 1 client (phía nhận)
- `forward_to_b(data)`: gửi raw bytes từ bất kỳ client A nào sang client B
- Giao tiếp JSON Lines, forward giữ nguyên format

---

### 4. `collection/clientB.py` — **TCP asyncio Client**

Client asyncio kết nối tới Server B để nhận dữ liệu CSI được forward.

**Chức năng:**
- Kết nối `asyncio.open_connection()` tới `127.0.0.1:9100`
- Đọc từng dòng `reader.readline()`, decode UTF-8
- Parse JSON → Python `dict`
- In tóm tắt: `device_id`, `seq`, `timestamp`, `rssi`, độ dài từng subcarrier CSI

**Mở rộng**: file này là nền để tích hợp logic đưa packet vào `asyncio.Queue` cho các tầng xử lý tiếp theo.

---

### 5. `adapters/esp_tcp_client.py` — **TCP Client: Management kết nối vào Collection**

TCP client chạy ở phía **Management (Laptop 2)**, kết nối ngược vào TCP Server của Collection.

**Chức năng chính:**

| Hàm | Mô tả |
|-----|-------|
| `EspTcpClient.connect()` | Kết nối TCP tới `127.0.0.1:9200` |
| `read_packet()` | Đọc một JSON line, tự động map `device_id` (MAC → esp1/2/3) |
| `send_message(dict)` | Gửi lệnh JSON line xuống Collection |
| `request_com_ports()` | Yêu cầu Collection gửi danh sách COM |
| `send_uart_control(...)` | Gửi lệnh connect/disconnect một ESP |
| `map_esp_mac_to_id(mac)` | Collection gửi MAC thật → map về `esp1/esp2/esp3` |
| `map_esp_id_to_mac(id)` | Backend gửi `esp1` → map về MAC thật trước khi gửi xuống |

**MAC mapping** (cấu hình thực tế):
```python
ESP_MAC_TO_ID = {
    "D0:CF:13:ED:2E:EC": "esp1",
    "D0:CF:13:EB:8A:9C": "esp2",
    "D0:CF:13:EC:49:04": "esp3",
}
```

---

### 6. `services/uart_manager.py` — **Queue & Quản lý 3 ESP Device**

Tầng quản lý trung tâm ở phía **Management**. Nhận packet từ `EspTcpClient` và đưa vào queue.

**Chức năng chính:**

| Thành phần | Mô tả |
|------------|-------|
| `csi_queue` | `queue.Queue(maxsize=50000)` — buffer CSI packets |
| `put_packet(packet)` | Kiểm tra device hợp lệ, recording enabled, rồi `put_nowait()` vào queue |
| `get_packet(timeout)` | `CsiService` lấy packet từ queue để ghi CSV |
| `update_packet_stat()` | Tính packet rate (Hz) theo sliding window 1 giây |
| `connect_device()` | Gửi lệnh `uart_control connect` xuống Collection qua TCP |
| `disconnect_device()` | Gửi lệnh `uart_control disconnect` |
| `handle_collection_message()` | Xử lý `com_list`, `uart_status`, `control_ack` từ Collection |
| `_client_receive_loop()` | Thread liên tục đọc packet từ `EspTcpClient` |

**Queue behavior:**
- Nếu `recording_enabled = False` → packet bị drop (không tích dữ liệu trước phiên thu)
- Nếu queue đầy → `queue.Full` exception → tăng `dropped_packets` counter
- `CSI_QUEUE_MAXSIZE = 50000` — đủ buffer ~250 giây ở 200 Hz

**Device states:**

```
DISCONNECTED → (gửi uart_control connect) → CONNECTING
CONNECTING   → (nhận uart_status connected) → CONNECTED
CONNECTED    → (có packet trong 1s) → RECEIVING
RECEIVING/CONNECTED → (ngắt TCP hoặc disconnect) → DISCONNECTED
CONNECTED    → (lỗi serial) → ERROR
```

---

## Giao thức Frame Binary

Mỗi frame từ ESP32 có kích thước cố định **155 bytes**:

```
Offset  Size  Field           Mô tả
──────  ────  ─────────────   ─────────────────────────────────────
0       2     magic_bytes     0xAA 0x55  (sync marker)
2       1     packet_length   155        (luôn == 155)
3       6     MAC address     6 bytes, địa chỉ MAC của ESP32
9       4     seq             uint32, số thứ tự packet
13      8     ts_us           uint64, timestamp microseconds từ ESP32
21      1     rssi            int8, tín hiệu RSSI (dBm)
22      1     channel         uint8, kênh WiFi
23      1     agc_gain        uint8, AGC gain
24      1     fft_gain        uint8, FFT gain
25      1     noise_floor     int8, sàn nhiễu (dBm)
26      128   CSI raw data    128 x int8, 64 cặp (Q, I) xen kẽ
154     1     XOR checksum    XOR của raw[0:154]
```

**Struct format**: `"<6sIQbBBBb"` (little-endian)

**Xác thực frame** (theo thứ tự trong `parse_packet()`):
1. `len(raw) == 155`
2. `raw[:2] == b'\xAA\x55'`
3. `raw[2] == 155`
4. `XOR(raw[0:154]) == raw[154]`

**CSI parsing:**
```python
# 128 int8 → 64 cặp [Q, I]
csi_raw = struct.unpack_from("<128b", raw, offset=26)
csi_pairs = [[csi_raw[i], csi_raw[i+1]] for i in range(0, 128, 2)]
# Kết quả: [[Q0,I0], [Q1,I1], ..., [Q63,I63]]
```

---

## Giao thức TCP JSON Lines

Tất cả communication qua TCP dùng **JSON Lines**: mỗi message là một JSON object trên một dòng, kết thúc bằng `\n`.

### Collection → Management (Laptop 1 → Laptop 2)

```jsonc
// Danh sách COM ports
{"type": "com_list", "ports": ["COM3", "COM4", "COM5"]}

// Trạng thái ESP: connected
{"type": "uart_status", "device_id": "esp1", "status": "connected",
 "config": {"com": "COM3", "baudrate": 115200}}

// Trạng thái ESP: disconnected
{"type": "uart_status", "device_id": "esp1", "status": "disconnected"}

// Trạng thái ESP: error
{"type": "uart_status", "device_id": "esp1", "status": "error",
 "message": "Không mở được COM3: [Errno 13] Permission denied"}

// CSI Data packet
{
  "type": "csi_data",
  "device_id": "D0:CF:13:ED:2E:EC",
  "seq": 12345,
  "timestamp": 1234567890123,
  "radio": {"rssi": -60, "channel": 6, "agc_gain": 0, "fft_gain": 0, "noise_floor": -95},
  "csi": [[-3, 5], [2, -1], ..., [0, 4]]
}
```

### Management → Collection (Laptop 2 → Laptop 1)

```jsonc
// Yêu cầu danh sách COM
{"type": "get_com_ports", "source": "management"}

// Lệnh kết nối ESP
{"type": "uart_control", "source": "management", "action": "connect",
 "device_id": "D0:CF:13:ED:2E:EC", "com": "COM3", "baudrate": 115200}

// Lệnh ngắt kết nối ESP
{"type": "uart_control", "source": "management", "action": "disconnect",
 "device_id": "D0:CF:13:ED:2E:EC"}
```

---

## Cài đặt & Chạy

### Yêu cầu

- Python >= 3.10
- Kết nối vật lý: 3 ESP32 → 3 module TTL-RS485 → 3 module RS485-USB → Laptop 1

### Cài đặt thư viện

```bash
# Tạo virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows

# Cài dependencies
pip install -r requirements.txt
```

**Thư viện quan trọng:**

| Package | Phiên bản | Vai trò |
|---------|-----------|---------|
| `pyserial` | 3.5 | Giao tiếp UART/COM |
| `pyserial-asyncio` | 0.6 | Asyncio wrapper cho serial |
| `fastapi` | 0.136.0 | Web API server (Management) |
| `uvicorn` | 0.44.0 | ASGI server |
| `numpy` | 2.4.4 | Xử lý tín hiệu CSI |
| `pandas` | 3.0.2 | Ghi/đọc CSV |

### Chạy Collection (Laptop 1)

```bash
# Kích hoạt venv
.venv\Scripts\activate

# Chạy TCP server thu CSI
python -m app.collection.esp_real
```

Server sẽ lắng nghe:
- `:9200` — Management TCP (JSON Lines)
- `:9201` — Realtime viewer TCP

### Chạy Management (Laptop 2)

```bash
# Kích hoạt venv
.venv\Scripts\activate

# Chạy FastAPI server
python -m app.main
# hoặc
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Cấu hình MAC ESP32

Sửa mapping MAC trong `adapters/esp_tcp_client.py`:

```python
ESP_MAC_TO_ID = {
    "D0:CF:13:ED:2E:EC": "esp1",  # ← Địa chỉ MAC thật của ESP32 #1
    "D0:CF:13:EB:8A:9C": "esp2",  # ← Địa chỉ MAC thật của ESP32 #2
    "D0:CF:13:EC:49:04": "esp3",  # ← Địa chỉ MAC thật của ESP32 #3
}
```

---

## Sơ đồ luồng dữ liệu

```
ESP32 #1 ──UART──► COM3 ─┐
ESP32 #2 ──UART──► COM4 ─┤──► SerialCollector (asyncio)
ESP32 #3 ──UART──► COM5 ─┘         │
                               parse_packet()
                               binary 155B → dict
                                    │
                             TCPServerApp (:9200)
                             asyncio TCP Server
                                    │  JSON Lines \n
                                    ▼
                            EspTcpClient.read_packet()
                            map MAC → esp1/esp2/esp3
                                    │
                            UartManager._client_receive_loop()
                                    │
                             put_packet() → csi_queue (Queue 50k)
                                    │
                            CsiService.get_packet()
                                    │
                              Ghi CSV theo session
                                    │
                            Web UI (FastAPI + WebSocket)
```

---

## Baudrate hỗ trợ

| Baudrate | Ghi chú |
|----------|---------|
| `115200` | Mặc định, ổn định nhất |
| `460800` | Tốc độ cao |
| `921600` | Tốc độ rất cao, cần kiểm tra chất lượng RS-485 |

---

## Tác giả

**Duc Nguyen** — Thesis Project: Human Activity Recognition via WiFi CSI Sensing