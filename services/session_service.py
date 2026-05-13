
# tạo service quản lý session, tạo thư mục lưu trữ dữ liệu cho mỗi session, lưu config session vào file JSON trong thư mục session đó
# Kết quả session sẽ có tên dạng: ngoi_dung_ngoi_pos1_20260427_153000
import json
from datetime import datetime
from pathlib import Path

from app.core.config import SESSIONS_DIR
from app.core.time_utils import utc_now_iso


class SessionService:
    def create_session(self, session_config: dict):
        scenario = session_config["scenario"]
        position_id = session_config["position_id"]

        session_id = (
            f"{scenario}_"
            f"pos{position_id}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        session_dir = SESSIONS_DIR / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        segments_dir = session_dir / "segments"
        segments_dir.mkdir(parents=True, exist_ok=True)

        full_config = {
            "session_id": session_id,
            "start_time_utc": utc_now_iso(),
            "status": "running",
            **session_config,
            "output_files": {
                "raw_eth": "raw_eth.csv",
                "raw_uart": "raw_uart.csv",
                "video": "video.mp4",
                "video_index": "video_index.csv",
                "action_events": "action_events.csv",
                "segments_dir": "segments"
            }
        }

        config_path = session_dir / "session_config.json"
        config_path.write_text(
            json.dumps(full_config, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        return full_config, session_dir