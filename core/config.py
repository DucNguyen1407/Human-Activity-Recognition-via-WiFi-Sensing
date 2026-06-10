from pathlib import Path

# Project root: iot_laptop_server/
BASE_DIR = Path(__file__).resolve().parents[2]

# app/
APP_DIR = BASE_DIR / "app"

# data/ nằm cùng cấp với app/
DATA_DIR = BASE_DIR / "data"

# Dữ liệu lưu ở ổ D
# DATA_DIR = Path(r"D:\data")

# Lưu session trực tiếp trong data/
# Ví dụ: data/1_1_1_1_1_ngoi_dung_0604_210000/
SESSIONS_DIR = DATA_DIR

# Tài nguyên chạy chương trình để trong app/resources/
# app/resources/audio/
# app/resources/scenarios/action_scenarios.json

RESOURCES_DIR = APP_DIR / "resources"
AUDIO_DIR = RESOURCES_DIR / "audio"
SCENARIO_DIR = RESOURCES_DIR / "scenarios"
ACTION_SCENARIOS_PATH = SCENARIO_DIR / "action_scenarios.json"

# Tạo các thư mục cần thiết nếu chưa có
DATA_DIR.mkdir(parents=True, exist_ok=True)
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
RESOURCES_DIR.mkdir(parents=True, exist_ok=True)
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
SCENARIO_DIR.mkdir(parents=True, exist_ok=True)

CAMERA_CONFIG = {
    "width": 1280,
    "height": 720,
    "fps": 30,
}
# CAMERA_CONFIG = {
#     "height": 480,
#     "fps": 20,
# }