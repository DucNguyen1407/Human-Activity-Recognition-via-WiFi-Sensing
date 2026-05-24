# iot_laptop_server/
# ├── app/
# │   ├── api/
# │   │   ├── [sessions.py](http://sessions.py/)              # API start/stop phiên thu
# │   │   ├── [config.py](http://config.py/)                # API trả danh sách scenario cho UI
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
# │   │   ├── [config.py](http://config.py/)                # Đường dẫn config/data/audio/session
# │   │   └── time_utils.py            # Hàm thời gian: utc_now_iso, perf_now
# │   │
# │   ├── ui/
# │   │   ├── static/
# │   │   └── templates/
# │   │       └── index.html           # Web UI
# │   │
# │   ├── [main.py](http://main.py/)    └──                  # FastAPI entrypoint
# │   └──  collection_stub/                 # Phần trống/mẫu cho nhóm Collection
# │       ├── [README.md](http://readme.md/)                    # Quy ước giao tiếp giữa Collection và Server ---- trong file word trên nhóm rồi 
# │       ├── tcp_stream_server.py         # Class TCP server mẫu dùng chung
# │       ├── nexmon_collection_stub.py    # Mẫu Nexmon-Collection gửi asus1/2/3
# │       └── esp32_collection_stub.py     # Mẫu ESP32-Collection gửi esp1/2/3
# │       └── esp_DUC_that.py     # code bên đức chạy esp
# │
# ├── data/
# │   ├── scripts/
# │   │   └── action_scenarios.json    # Kịch bản hành động
# │   ├── assets/
# │   │   └── audio/
# │   │       └── *.wav                # Audio cue
# │   └── sessions/
# │       └── <session_id>/
# │           ├── session_config.json
# │           ├── action_events.csv
# │           ├── video.mp4
# │           ├── video_index.csv
# │           ├── raw_asus1.csv
# │           ├── raw_asus2.csv
# │           ├── raw_asus3.csv
# │           ├── raw_esp1.csv
# │           ├── raw_esp2.csv
# │           ├── raw_esp3.csv
# │           └── segments/
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