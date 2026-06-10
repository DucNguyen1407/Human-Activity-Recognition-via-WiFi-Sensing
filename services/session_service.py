# app/services/session_service.py
#
# Tạo thư mục session và ghi session_config.json.
# Format session_id:
# room_setup_session_person_position_repeat_scenario_MMDD_HHMMSS

import json
from datetime import datetime
from pathlib import Path

from app.core.config import SESSIONS_DIR
from app.core.time_utils import unix_now_us


class SessionService:
    def create_session(self, session_config: dict):
        scenario = session_config["scenario"]
        room_id = session_config["room_id"]
        setup_id = session_config["setup_id"]
        session_no = session_config["session_no"]
        person_id = session_config["person_id"]
        position_id = session_config["position_id"]
        repeat_count = session_config["repeat_count"]

        now = datetime.now()
        date_part = now.strftime("%m%d")
        time_part = now.strftime("%H%M%S")

        session_id = (
            f"{room_id}_"
            f"{setup_id}_"
            f"{session_no}_"
            f"{person_id}_"
            f"{position_id}_"
            f"{repeat_count}_"
            f"{scenario}_"
            f"{date_part}_"
            f"{time_part}"
        )

        session_dir = SESSIONS_DIR / session_id
        session_dir.mkdir(parents=True, exist_ok=True)


        capture = session_config.get("capture", {})
        output_files = {
            "raw_asus1": "raw_asus1.bin",
            "raw_asus2": "raw_asus2.bin",
            "raw_asus3": "raw_asus3.bin",
            "raw_esp1": "raw_esp1.bin",
            "raw_esp2": "raw_esp2.bin",
            "raw_esp3": "raw_esp3.bin",
            "action_events": "action_events.csv",
        }

        if capture.get("camera", True):
            output_files["video"] = "video.mp4"
            output_files["video_index"] = "video_index.csv"

        full_config = {
            "session_id": session_id,
            "start_time_unix_us": unix_now_us(),
            "status": "running",
            **session_config,
            "output_files": output_files,
        }

        config_path = session_dir / "session_config.json"
        config_path.write_text(
            json.dumps(full_config, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        return full_config, session_dir
