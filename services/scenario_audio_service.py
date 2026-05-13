# scenario_audio_service.py
# Gộp từ scenario_service.py + audio_cue_service.py
# - ScenarioService: đọc action_scenarios.json và build action_plan
# - AudioCueService: phát audio cue và ghi action_events.csv

import csv
import json
import time
from pathlib import Path

import pygame

from app.core.config import ACTION_SCENARIOS_PATH, AUDIO_DIR
from app.core.time_utils import utc_now_iso, perf_now


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
                    "duration_sec": float(action["duration_sec"])
                })

        return action_plan


class AudioCueService:
    def __init__(self, session_dir: Path):
        self.session_dir = session_dir
        self.action_file = session_dir / "action_events.csv"
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
                    "start_sec",
                    "end_sec",
                    "start_time_utc",
                    "end_time_utc"
                ])

    def run_action_plan(self, action_plan: list, session_t0: float | None = None):
        if session_t0 is None:
            session_t0 = perf_now()

        for item in action_plan:
            voice_file = item["voice_file"]
            duration_sec = float(item["duration_sec"])

            self._play_voice(voice_file)
            self._beep()

            start_sec = perf_now() - session_t0
            start_time_utc = utc_now_iso()

            time.sleep(duration_sec)

            end_sec = perf_now() - session_t0
            end_time_utc = utc_now_iso()

            self._write_action_event(
                item=item,
                start_sec=start_sec,
                end_sec=end_sec,
                start_time_utc=start_time_utc,
                end_time_utc=end_time_utc
            )

    def _play_voice(self, voice_file: str):
        path = AUDIO_DIR / voice_file

        if not path.exists():
            raise FileNotFoundError(f"Không tìm thấy file âm thanh: {path}")

        pygame.mixer.music.load(str(path))
        pygame.mixer.music.play()

        while pygame.mixer.music.get_busy():
            time.sleep(0.05)

    def _beep(self):
        beep_path = AUDIO_DIR / "beep.wav"

        if not beep_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file beep: {beep_path}")

        sound = pygame.mixer.Sound(str(beep_path))
        sound.play()
        time.sleep(0.25)

    def _write_action_event(self, item, start_sec, end_sec, start_time_utc, end_time_utc):
        with open(self.action_file, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                item["action_index"],
                item["repeat_index"],
                item["position_id"],
                item["scenario"],
                item["action_name"],
                item["voice_file"],
                f"{start_sec:.6f}",
                f"{end_sec:.6f}",
                start_time_utc,
                end_time_utc
            ])
