# iot_laptop_server/
# ├── app/
# │   ├── api/
# │   │   ├── [sessions.py](http://sessions.py/)              # API start/stop phiên thu
# │   │   ├── [config_crenario.py](http://configscenario.py/)                # API trả danh sách scenario cho UI
# │   │   ├── [camera.py](http://camera.py/)                # API camera preview/select
# │   │   ├── [ethernet.py](http://ethernet.py/)              # API quản lý Nexmon/asus source
# │   │   ├── [uart.py](http://uart.py/)                  # API quản lý ESP/uart source
# │   │   └── [ws.py](http://ws.py/)                    # WebSocket realtime status
# │   │
# │   ├── services/
# │   │   ├── recording_service.py     # System Management chính
# │   │   ├── ethernet_manager.py      # Nexmon Management: host/port + asus1/2/3 status
# │   │   ├── uart_manager.py          # ESP Management: host/port + esp1/2/3 status
# │   │   ├── csi_service.py           # CSI Management: đọc TCP client, ghi 6 file CSI
# │   │   ├── camera_service.py        # Camera Management: preview + ghi video
# │   │   ├── session_service.py       # Tạo session folder + session_config.json
# │   │   └── scenario_audio_service.py# Scenario + audio cue + action_events.csv
# │   │
# │   ├── adapters/
# │   │   ├── nexmon_tcp_client.py     # TCP client đọc dữ liệu từ Nexmon-Collection
# │   │   ├── esp_tcp_client.py        # TCP client đọc dữ liệu từ ESP32-Collection
# │   │   └── webcam_adapter.py        # Adapter OpenCV camera
# │   │
# │   ├── core/
# │   │   ├── [config.py](http://config.py/)     # Đường dẫn config/data/audio/session
# │   │   └── time_utils.py            # Hàm thời gian: utc_now_iso, perf_now
# │   ├── resources/
# │   │   ├── audio/
# │   │   └── scenarios/ action_event.josn  # cấu hình các kịch bản
# │   └──  collection/               # Phần trống/mẫu cho nhóm Collection
# │   │   ├── tcp_stream_server.py         # Class TCP server mẫu dùng chung
# │   │   ├── asus_fake_bin.py    # Mẫu Nexmon-Collection gửi asus1/2/3
# │   │   └── esp_fake_bin.py     # Mẫu ESP32-Collection gửi esp1/2/3
# │   │   └── 
# │   │
# │   ├── ui/
# │   │   ├── static/
# │   │   └── templates/
# │   │       └── index.html           # Web UI
# │   │
# │   └── [main.py](http://main.py/)    └──                  # FastAPI entrypoint
# │
# ├── data/
# │   └── phòng_setup_phiên_người_vị trí_ số lần lặp_tên kịch bản_ tháng ngày_ giờ phút giây/
# │       ├── session_config.json   // file log ra cấu hình của 1 lần thu
# │       ├── action_events.csv     // file đánh dấu các thông số timestamp star và timestamp end 
#                                      của môi hành động trong chuỗi dữ liệu dài
# │       ├── video.mp4             // video 
# │       ├── video_index.csv       // file log timestamp của các frame video
# │       ├── raw_asus1.bin         // file dữ liệu của asus 1 ( binnary little-endian)
# │       ├── raw_asus2.bin        // file dữ liệu của asus 2 ( binnary little-endian)
# │       ├── raw_asus3.bin         // file dữ liệu của asus 3 ( binnary little-endian)
# │       ├── raw_esp1.bin          // file dữ liệu của esp 1 ( binnary little-endian)
# │       ├── raw_esp2.bin         // file dữ liệu của esp 2 ( binnary little-endian)
# │       └──raw_esp3.bin          // file dữ liệu của esp 3 ( binnary little-endian)
# │        
# │
# ├── tests/
# │   ├── test_api_ethernet.py
# │   ├── test_api_uart.py
# │   ├── test_tcp_clients.py
# │   └── test_session_flow.py
# │
# └── requirements.txt




# |--- voice2 ---|           |--- voice1 ---|
#                |beep|                      |beep|
# |------ duration_sec ------|------ duration_sec ------|
# ghi                        ghi                        ghi


file json asus gửi sang : 
{
  "device_id": "02:1A:2B:3C:4D:5E",    // MAC của Monitor
  "seq": 1,                                                // 12 bit: 0 -4095 
  "timestamp": 1716280000123456,   // us thời gian thực unix
  "bw": 20,                                                // bawdwidth 
  "ch": 157,                                                         // channel
  "agc": [0, 0, 0, 0],
  "rssi": [2, 3, 4, 5],
  "csi": {
    "c0": [ 1223, 5256, …..    ],     // mảng 64 giá trị subcarrier bao gồm Q/I 4byte viết dưới dạng thập phân của từng anten
    "c1": [    ],
    "c2": [    ],
    "c3": [    ]
  }
}
file json esp gửi sang : 
{
 "type": "csi_data",
 "device_id": "00:1A:2B:3C:4D:5E",  // MAC ví dụ 
 "seq": 123,
 "timestamp": 1716023475123456,
 "radio": {
   "rssi": -45,
   "channel": 6,
   "agc_gain": 1,
   "fft_gain": 2,
   "noise_floor": -95
 },
 "csi": [[12, -3], [5, 8],.....] 64 cặp Q/I
}


asus lưu file: 1044B
- Seq: 2B
- Timestamp: 8B
- Channel: 2B
- agc_gain0: 1B
- agc_gain1: 1B
- agc_gain2: 1B
- agc_gain3: 1B
- Rssi0: 1B
- Rssi1: 1B
- Rssi2: 1B
- Rssi3: 1B
- antten0_sub0: 4 byte
- antten0_sub1 4 byte
...
- antten0_subN: 4 byte
- antten1_sub0: 4 byte
- antten1_sub1 4 byte
...
- antten1_subN: 4 byte
...
- antten3_sub0: 4 byte
- antten3_sub1 4 byte
...
- antten3_subN: 4 byte


seq        uint16   2 byte
timestamp  uint64   8 byte
channel    uint16   2 byte
agc0       uint8    1 byte
agc1       uint8    1 byte
agc2       uint8    1 byte
agc3       uint8    1 byte
rssi0      int8     1 byte
rssi1      int8     1 byte
rssi2      int8     1 byte
rssi3      int8     1 byte
c0: 64 giá trị * 4 byte   uint32
c1: 64 giá trị * 4 byte
c2: 64 giá trị * 4 byte
c3: 64 giá trị * 4 byte

esp32 lưu vào file: 144B
- Seq: 2B
- Timestamp: 8B
- Channel: 2B
- agc_gain: 1B
- fft_gain: 1B
- noise: 1B
- Rssi: 1B
- sub0_q: 1 byte  // ảo
- sub0_i: 1 byte  // thực
...
- subN_q: 1 byte
- subN_i: 1 byte

seq          uint16   2 byte
timestamp    uint64   8 byte
channel      uint16   2 byte
agc_gain     uint8    1 byte
fft_gain     uint8    1 byte
noise_floor  int8     1 byte
rssi         int8     1 byte
csi Q I Q I mỗi giá trị 1byte int8 * 128