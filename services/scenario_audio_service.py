# app/services/scenario_audio_service.py
# - ScenarioService: đọc action_scenarios.json và build action_plan
# - AudioCueService: phát audio cue và ghi action_events.csv

import csv
import json
import time
from pathlib import Path
from typing import Callable, Any

import pygame

from app.core.config import ACTION_SCENARIOS_PATH, AUDIO_DIR
from app.core.time_utils import unix_now_us, perf_now


class ScenarioService:
    def __init__(self, scenario_file: Path = ACTION_SCENARIOS_PATH):
        self.scenario_file = scenario_file

    def load_all(self):
        if not self.scenario_file.exists():
            raise FileNotFoundError(f"Không tìm thấy file kịch bản: {self.scenario_file}")

        with open(self.scenario_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_scenario(self, scenario_name: str):
        scenarios = self.load_all()

        for item in scenarios:
            if item.get("scenario") == scenario_name:
                return item

        raise ValueError(f"Không tìm thấy scenario: {scenario_name}")

    def build_action_plan(self, scenario_name: str, repeat_count: int, position_id: int):
        scenario = self.get_scenario(scenario_name)
        actions = scenario["actions"]

        action_plan = []
        action_index = 0

        for repeat_index in range(1, repeat_count + 1):
            for action in actions:
                action_index += 1
                action_plan.append({
                    "action_index": action_index,
                    "repeat_index": repeat_index,
                    "position_id": position_id,
                    "scenario": scenario_name,
                    "order": action["order"],
                    "voice_file": action["voice_file"],
                    "action_name": action["action_name"],
                    "duration_sec": float(action["duration_sec"]),
                })

        return action_plan


class AudioCueService:
    def __init__(self, session_dir: Path):
        self.session_dir = Path(session_dir)
        self.action_file = self.session_dir / "action_events.csv"
        pygame.mixer.init()

        if not self.action_file.exists():
            with open(self.action_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "action_index",
                    "repeat_index",
                    "position_id",
                    "scenario",
                    "action_name",
                    "voice_file",
                    "start_elapsed_us",
                    "end_elapsed_us",
                    "start_unix_us",
                    "end_unix_us",
                ])

    def run_action_plan(
        self,
        action_plan: list,
        session_t0: float | None = None,
        stop_event=None,
        packet_progress_getter: Callable[[], Any] | None = None,
        packet_target: int = 1000,
    ) -> dict:
        """
        Chạy action plan.

        Điều kiện sang action tiếp theo:
        - đủ duration_sec lấy từ action_scenarios.json
        - và cả 3 ESP đều tăng thêm ít nhất packet_target bước seq so với đầu action

        packet_progress_getter có thể trả về:
        - dict {"esp1": x, "esp2": y, "esp3": z}: kiểm tra từng ESP riêng biệt
        - int: giữ tương thích kiểu cũ, kiểm tra tổng/giá trị đơn

        Nếu stop_event được set giữa chừng, action hiện tại vẫn được ghi với end time
        tại lúc dừng, rồi trả stopped_midway=True.
        """
        if session_t0 is None:
            session_t0 = perf_now()

        if not action_plan:
            return {"stopped_midway": False}

        if stop_event is not None and stop_event.is_set():
            return {"stopped_midway": True}

        if not self._play_voice("chuan_bi.wav", stop_event=stop_event):
            return {"stopped_midway": True}

        if not self._sleep_interruptible(1.0, stop_event=stop_event):
            return {"stopped_midway": True}

        def packet_enough(start_progress, current_progress) -> bool:
            if packet_progress_getter is None or packet_target <= 0:
                return True

            # Kiểu mới: dict tiến trình riêng từng ESP.
            # Điều kiện đúng: mọi ESP trong start_progress đều phải tăng >= packet_target.
            if isinstance(start_progress, dict) and isinstance(current_progress, dict):
                for device_id, start_value in start_progress.items():
                    current_value = current_progress.get(device_id, start_value)
                    try:
                        delta = int(current_value) - int(start_value)
                    except (TypeError, ValueError):
                        delta = 0

                    if delta < packet_target:
                        return False

                return True

            # Tương thích kiểu cũ: int progress.
            try:
                return int(current_progress) - int(start_progress) >= packet_target
            except (TypeError, ValueError):
                return False

        for item in action_plan:
            if stop_event is not None and stop_event.is_set():
                return {"stopped_midway": True}

            duration_sec = float(item["duration_sec"])
            action_start_perf = perf_now()
            start_elapsed_us = int((action_start_perf - session_t0) * 1_000_000)
            start_unix_us = unix_now_us()
            action_end_target_perf = action_start_perf + duration_sec

            packet_start = packet_progress_getter() if packet_progress_getter else 0

            # Voice và beep cũng nằm trong duration_sec.
            if not self._play_voice(item["voice_file"], stop_event=stop_event):
                self._write_current_action_end(item, session_t0, start_elapsed_us, start_unix_us)
                return {"stopped_midway": True}

            if not self._beep(stop_event=stop_event):
                self._write_current_action_end(item, session_t0, start_elapsed_us, start_unix_us)
                return {"stopped_midway": True}

            while True:
                if stop_event is not None and stop_event.is_set():
                    self._write_current_action_end(item, session_t0, start_elapsed_us, start_unix_us)
                    return {"stopped_midway": True}

                duration_ok = perf_now() >= action_end_target_perf

                if packet_progress_getter is None or packet_target <= 0:
                    packet_ok = True
                else:
                    packet_current = packet_progress_getter()
                    packet_ok = packet_enough(packet_start, packet_current)

                if duration_ok and packet_ok:
                    break

                time.sleep(0.02)

            action_end_perf = perf_now()
            end_elapsed_us = int((action_end_perf - session_t0) * 1_000_000)
            end_unix_us = unix_now_us()
            self._write_action_event(
                item=item,
                start_elapsed_us=start_elapsed_us,
                end_elapsed_us=end_elapsed_us,
                start_unix_us=start_unix_us,
                end_unix_us=end_unix_us,
            )

        if not self._play_voice("ket_thuc.wav", stop_event=stop_event):
            return {"stopped_midway": True}

        return {"stopped_midway": False}

    def _write_current_action_end(self, item, session_t0, start_elapsed_us, start_unix_us):
        action_end_perf = perf_now()
        end_elapsed_us = int((action_end_perf - session_t0) * 1_000_000)
        end_unix_us = unix_now_us()
        self._write_action_event(
            item=item,
            start_elapsed_us=start_elapsed_us,
            end_elapsed_us=end_elapsed_us,
            start_unix_us=start_unix_us,
            end_unix_us=end_unix_us,
        )

    def _sleep_interruptible(self, seconds: float, stop_event=None) -> bool:
        end_at = perf_now() + seconds
        while perf_now() < end_at:
            if stop_event is not None and stop_event.is_set():
                return False
            time.sleep(min(0.05, max(0.0, end_at - perf_now())))
        return True

    def _play_voice(self, voice_file: str, stop_event=None) -> bool:
        path = AUDIO_DIR / voice_file
        if not path.exists():
            raise FileNotFoundError(f"Không tìm thấy file âm thanh: {path}")

        pygame.mixer.music.load(str(path))
        pygame.mixer.music.play()

        while pygame.mixer.music.get_busy():
            if stop_event is not None and stop_event.is_set():
                pygame.mixer.music.stop()
                return False
            time.sleep(0.05)

        return True

    def _beep(self, stop_event=None) -> bool:
        beep_path = AUDIO_DIR / "beep.wav"
        if not beep_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file beep: {beep_path}")

        sound = pygame.mixer.Sound(str(beep_path))
        channel = sound.play()

        if channel is not None:
            while channel.get_busy():
                if stop_event is not None and stop_event.is_set():
                    channel.stop()
                    return False
                time.sleep(0.01)

        return True

    def _write_action_event(self, item, start_elapsed_us, end_elapsed_us, start_unix_us, end_unix_us):
        with open(self.action_file, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                item["action_index"],
                item["repeat_index"],
                item["position_id"],
                item["scenario"],
                item["action_name"],
                item["voice_file"],
                start_elapsed_us,
                end_elapsed_us,
                start_unix_us,
                end_unix_us,
            ])
