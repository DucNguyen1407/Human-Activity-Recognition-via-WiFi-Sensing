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
                    "start_elapsed_us",
                    "end_elapsed_us",
                    "start_unix_us",
                    "end_unix_us"
                ])

    # def run_action_plan(self, action_plan: list, session_t0: float | None = None):
    #     if session_t0 is None:
    #         session_t0 = perf_now()

    #     for item in action_plan:
    #         voice_file = item["voice_file"]
    #         duration_sec = float(item["duration_sec"])

    #         self._play_voice(voice_file)
    #         self._beep()

    #         start_elapsed_us = int((perf_now() - session_t0) * 1_000_000)
    #         start_unix_us = unix_now_us()

    #         time.sleep(duration_sec)

    #         end_elapsed_us = int((perf_now() - session_t0) * 1_000_000)
    #         end_unix_us = unix_now_us()

    #         self._write_action_event(
    #             item=item,
    #             start_elapsed_us=start_elapsed_us,
    #             end_elapsed_us=end_elapsed_us,
    #             start_unix_us=start_unix_us,
    #             end_unix_us=end_unix_us
    #         )
    # def run_action_plan(self, action_plan: list, session_t0: float | None = None):
    #     """
    #     Timeline mới cho mỗi action:

    #     start action
    #     ↓
    #     lấy mốc start_elapsed_us / start_unix_us
    #     ↓
    #     phát voice_file
    #     ↓
    #     phát beep
    #     ↓
    #     chờ cho đủ duration_sec tính từ mốc start action
    #     ↓
    #     lấy mốc end_elapsed_us / end_unix_us
    #     ↓
    #     ghi action_events.csv
    #     ↓
    #     sang action tiếp theo

    #     Ví dụ duration_sec = 5:
    #     - 0.00s: start action, lấy timestamp start
    #     - 0.00s -> 1.20s: phát voice
    #     - 1.20s -> 1.45s: phát beep
    #     - 1.45s -> 5.00s: chờ phần còn lại
    #     - 5.00s: end action, ghi CSV, sang action tiếp theo

    #     Như vậy duration_sec = 5 đã bao gồm thời gian phát voice + beep.
    #     """

    #     # session_t0 là mốc bắt đầu session theo perf_now().
    #     # elapsed_us = perf_now hiện tại - session_t0.
    #     # Dùng để đồng bộ action với CSI/video theo thời gian tương đối trong session.
    #     if session_t0 is None:
    #         session_t0 = perf_now()

    #     for item in action_plan:
    #         voice_file = item["voice_file"]
    #         duration_sec = float(item["duration_sec"])

    #         # =========================
    #         # 1. START ACTION
    #         # =========================
    #         # Lấy mốc bắt đầu action TRƯỚC khi phát voice.
    #         # Từ thời điểm này bắt đầu tính duration_sec.
    #         action_start_perf = perf_now()

    #         start_elapsed_us = int((action_start_perf - session_t0) * 1_000_000)
    #         start_unix_us = unix_now_us()

    #         # Mốc kết thúc lý tưởng của action.
    #         # Ví dụ duration_sec = 5 thì action_end_target_perf = start + 5 giây.
    #         action_end_target_perf = action_start_perf + duration_sec

    #         # =========================
    #         # 2. PHÁT VOICE
    #         # =========================
    #         # Voice nằm trong duration_sec.
    #         # Nếu voice dài 1.2s thì 1.2s đó đã được tính vào 5s.
    #         self._play_voice(voice_file)

    #         # =========================
    #         # 3. PHÁT BEEP
    #         # =========================
    #         # Beep cũng nằm trong duration_sec.
    #         self._beep()

    #         # =========================
    #         # 4. CHỜ ĐỦ duration_sec TỪ START
    #         # =========================
    #         # Sau voice + beep, nếu vẫn chưa đủ duration_sec thì chờ tiếp.
    #         # Ví dụ:
    #         # duration_sec = 5
    #         # voice + beep = 1.45s
    #         # còn lại = 3.55s
    #         while True:
    #             now = perf_now()
    #             remaining = action_end_target_perf - now

    #             if remaining <= 0:
    #                 break

    #             # Sleep ngắn để thời gian kết thúc không bị lệch nhiều.
    #             # Không sleep một cục quá dài để dễ kiểm soát timing hơn.
    #             time.sleep(min(0.05, remaining))

    #         # =========================
    #         # 5. END ACTION
    #         # =========================
    #         # Lấy mốc kết thúc sau khi đã đủ duration_sec.
    #         action_end_perf = perf_now()

    #         end_elapsed_us = int((action_end_perf - session_t0) * 1_000_000)
    #         end_unix_us = unix_now_us()

    #         # Nếu voice + beep dài hơn duration_sec thì sẽ bị quá thời gian.
    #         # Ví dụ voice 6s, duration_sec 5s thì không thể kết thúc đúng 5s.
    #         actual_duration_sec = action_end_perf - action_start_perf
    #         if actual_duration_sec > duration_sec + 0.05:
    #             print(
    #                 f"[AudioCueService] Warning: action '{item['action_name']}' "
    #                 f"duration bị vượt. target={duration_sec:.3f}s, "
    #                 f"actual={actual_duration_sec:.3f}s"
    #             )

    #         # =========================
    #         # 6. GHI CSV
    #         # =========================
    #         # CSV vẫn ghi một dòng đầy đủ sau khi action kết thúc.
    #         # Nhưng start_elapsed_us/start_unix_us đã được lấy ngay từ đầu action,
    #         # nên nhãn action vẫn bắt đầu đúng trước voice.
    #         self._write_action_event(
    #             item=item,
    #             start_elapsed_us=start_elapsed_us,
    #             end_elapsed_us=end_elapsed_us,
    #             start_unix_us=start_unix_us,
    #             end_unix_us=end_unix_us,
    #         )
    def run_action_plan(self, action_plan: list, session_t0: float | None = None):
        """
        Timeline:

        1. Phát chuan_bi.wav một lần trước toàn bộ chuỗi action.
        2. Chờ thêm 1 giây sau khi đọc chuẩn bị xong.
        3. Với mỗi action:
        - Ghi start trước khi phát voice action.
        - Phát voice_file.
        - Phát beep.
        - Chờ cho đủ duration_sec tính từ mốc start.
        - Ghi end vào action_events.csv.
        4. Phát ket_thuc.wav một lần sau khi chạy xong toàn bộ chuỗi action.

        Lưu ý:
        - chuan_bi.wav không ghi vào action_events.csv.
        - 1 giây chờ sau chuẩn bị cũng không ghi vào action_events.csv.
        - duration_sec chỉ tính từ lúc start của từng action.
        """

        if session_t0 is None:
            session_t0 = perf_now()

        # Nếu không có action nào thì thoát luôn.
        # Tránh phát chuẩn bị/kết thúc khi không có chuỗi hành động để chạy.
        if not action_plan:
            return

        prepare_voice_file = "chuan_bi.wav"
        finish_voice_file = "ket_thuc.wav"

        # ============================================================
        # 1. PHÁT CHUẨN BỊ TRƯỚC TOÀN BỘ CHUỖI ACTION
        # ============================================================
        # Hàm _play_voice() sẽ chờ file chuan_bi.wav phát xong rồi mới đi tiếp.
        self._play_voice(prepare_voice_file)

        # ============================================================
        # 2. CHỜ 1 GIÂY SAU KHI ĐỌC CHUẨN BỊ XONG
        # ============================================================
        # Sau 1 giây này mới bắt đầu action đầu tiên.
        time.sleep(1.0)

        # ============================================================
        # 3. CHẠY TOÀN BỘ ACTION
        # ============================================================
        for item in action_plan:
            voice_file = item["voice_file"]
            duration_sec = float(item["duration_sec"])

            # ------------------------------------------------------------
            # START ACTION
            # ------------------------------------------------------------
            # Lấy mốc start TRƯỚC khi phát voice action.
            # Từ mốc này bắt đầu tính duration_sec.
            action_start_perf = perf_now()

            start_elapsed_us = int((action_start_perf - session_t0) * 1_000_000)
            start_unix_us = unix_now_us()

            # Action sẽ kết thúc tại start + duration_sec.
            # Ví dụ duration_sec = 5 thì action kéo dài đúng 5 giây,
            # bao gồm cả thời gian phát voice + beep.
            action_end_target_perf = action_start_perf + duration_sec

            # ------------------------------------------------------------
            # PHÁT VOICE ACTION
            # ------------------------------------------------------------
            self._play_voice(voice_file)

            # ------------------------------------------------------------
            # PHÁT BEEP
            # ------------------------------------------------------------
            self._beep()

            # ------------------------------------------------------------
            # CHỜ CHO ĐỦ duration_sec TỪ MỐC START
            # ------------------------------------------------------------
            # Nếu voice + beep mất 1.5s và duration_sec = 5s,
            # đoạn này sẽ chờ thêm khoảng 3.5s.
            # Nếu voice + beep đã vượt quá 5s thì không chờ nữa.
            while True:
                now = perf_now()
                remaining = action_end_target_perf - now

                if remaining <= 0:
                    break

                time.sleep(min(0.05, remaining))

            # ------------------------------------------------------------
            # END ACTION
            # ------------------------------------------------------------
            action_end_perf = perf_now()

            end_elapsed_us = int((action_end_perf - session_t0) * 1_000_000)
            end_unix_us = unix_now_us()

            actual_duration_sec = action_end_perf - action_start_perf
            if actual_duration_sec > duration_sec + 0.05:
                print(
                    f"[AudioCueService] Warning: action '{item['action_name']}' "
                    f"bị vượt thời gian. target={duration_sec:.3f}s, "
                    f"actual={actual_duration_sec:.3f}s"
                )

            self._write_action_event(
                item=item,
                start_elapsed_us=start_elapsed_us,
                end_elapsed_us=end_elapsed_us,
                start_unix_us=start_unix_us,
                end_unix_us=end_unix_us,
            )

        # ============================================================
        # 4. PHÁT KẾT THÚC SAU TOÀN BỘ CHUỖI ACTION
        # ============================================================
        self._play_voice(finish_voice_file)

    def _play_voice(self, voice_file: str):
        path = AUDIO_DIR / voice_file

        if not path.exists():
            raise FileNotFoundError(f"Không tìm thấy file âm thanh: {path}")

        pygame.mixer.music.load(str(path))
        pygame.mixer.music.play()

        while pygame.mixer.music.get_busy():
            time.sleep(0.05)

    # def _beep(self):
    #     beep_path = AUDIO_DIR / "beep.wav"

    #     if not beep_path.exists():
    #         raise FileNotFoundError(f"Không tìm thấy file beep: {beep_path}")

    #     sound = pygame.mixer.Sound(str(beep_path))
    #     sound.play()
    #     time.sleep(0.25)
    def _beep(self):
        """
        Phát beep.wav và chờ beep phát xong.

        Lý do:
        - Beep cũng được tính vào duration_sec.
        - Nếu beep.wav dài/ngắn hơn 0.25s, code vẫn đo đúng theo file thật.
        """
        beep_path = AUDIO_DIR / "beep.wav"

        if not beep_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file beep: {beep_path}")

        sound = pygame.mixer.Sound(str(beep_path))
        channel = sound.play()

        # Chờ beep phát xong thật sự.
        # Nếu channel is None thì pygame không phát được, bỏ qua để tránh crash.
        if channel is not None:
            while channel.get_busy():
                time.sleep(0.01)

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
                end_unix_us
            ])
