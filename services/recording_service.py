# app/services/recording_service.py
#
# Điều phối toàn bộ session:
# - tạo session folder
# - start/stop CSI collection
# - ghi video nếu capture.camera = true
# - chạy scenario/audio và ghi action_events.csv

import asyncio
import threading
import time
import traceback

from app.api.ws import broadcast_state
from app.core.time_utils import perf_now
from app.services.camera_service import VideoService, camera_manager
from app.services.csi_service import CsiService
from app.services.scenario_audio_service import AudioCueService, ScenarioService
from app.services.session_service import SessionService

from app.core.config import CAMERA_CONFIG

main_loop = None

# DEFAULT_CAMERA_CONFIG = {
#     "fps": 20,
#     "width": 640,
#     "height": 480,
# }


def set_main_loop(loop):
    global main_loop
    main_loop = loop
    print("MAIN LOOP SET:", main_loop)


def update_state(data: dict):
    """
    Ghi log nội bộ và gọi broadcast.

    WebSocket hiện chỉ gửi status/rate của 6 thiết bị, nên không cần giữ
    current_state session trong ws.py nữa. Các message session chỉ dùng để
    debug ở backend.
    """
    print("UPDATE STATE:", data)

    if main_loop is None:
        print("MAIN LOOP IS NONE")
        return

    asyncio.run_coroutine_threadsafe(broadcast_state(), main_loop)


def record_video(session_dir, camera_cfg, stop_event, video_ready_event, session_t0):
    video = VideoService(
        session_dir=session_dir,
        fps=camera_cfg["fps"],
        width=camera_cfg["width"],
        height=camera_cfg["height"],
        session_t0=session_t0,
    )

    frame_interval = 1.0 / camera_cfg["fps"]
    first_frame_written = False

    try:
        video.open()
        print("Video recording started")
        update_state({"message": "Video recording started"})

        while not stop_event.is_set():
            loop_start = time.perf_counter()
            frame = camera_manager.get_frame()

            if frame is not None:
                video.write_frame(frame)

                if not first_frame_written:
                    first_frame_written = True
                    video_ready_event.set()
                    print("Video ready: first frame written")
                    update_state({"camera_ready": True, "message": "Video ready"})
            else:
                update_state({"message": "Không có frame từ camera_manager"})

            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0, frame_interval - elapsed))

    finally:
        video.close()
        print("Video recording stopped")
        update_state({"camera_ready": False, "message": "Video recording stopped"})


class RecordingService:
    def __init__(self):
        self.thread = None
        self.is_running = False
        self.stop_requested = False

    def start(self, session_config: dict):
        if self.is_running:
            return {
                "status": "already_running",
                "message": "A session is already running",
            }

        if not session_config.get("scenario"):
            return {
                "status": "error",
                "message": "Missing scenario",
            }

        self.stop_requested = False
        self.thread = threading.Thread(
            target=self._run,
            args=(session_config,),
            daemon=True,
        )
        self.thread.start()
        self.is_running = True

        update_state({
            "running": True,
            "scenario": session_config["scenario"],
            "position_id": session_config["position_id"],
            "repeat_count": session_config["repeat_count"],
            "message": "Session starting",
            "error": None,
        })

        return {
            "status": "started",
            "message": "Session started",
        }

    def stop(self):
        if not self.is_running:
            return {
                "status": "not_running",
                "message": "No session is running",
            }

        self.stop_requested = True
        update_state({"message": "Stop requested"})

        return {
            "status": "stopping",
            "message": "Stopping current session",
        }

    def _run(self, session_config: dict):
        stop_video_event = threading.Event()
        video_ready_event = threading.Event()
        video_thread = None
        csi_service = None

        try:
            print("THREAD STARTED")
            print("CONFIG FROM UI:", session_config)

            action_plan = ScenarioService().build_action_plan(
                scenario_name=session_config["scenario"],
                repeat_count=session_config["repeat_count"],
                position_id=session_config["position_id"],
            )

            print("Total actions:", len(action_plan))
            update_state({"message": f"Total actions: {len(action_plan)}"})

            session_info, session_dir = SessionService().create_session(session_config)
            print("Session ID:", session_info["session_id"])
            print("Session dir:", session_dir)

            update_state({
                "session_id": session_info["session_id"],
                "session_dir": str(session_dir),
                "message": "Session created",
            })

            session_t0 = perf_now()

            csi_service = CsiService(session_dir, session_t0)
            csi_service.start_csi_collection()

            print("CSI collection started")
            update_state({"csi_ready": True, "message": "CSI collection started"})

            capture = session_config.get("capture", {})
            camera_enabled = capture.get("camera", True)

            if camera_enabled:
                camera_cfg = {
                    # **DEFAULT_CAMERA_CONFIG,
                    **CAMERA_CONFIG,
                    "camera_index": camera_manager.selected_camera_index,
                }

                if not camera_manager.running:
                    camera_manager.start(
                        width=camera_cfg["width"],
                        height=camera_cfg["height"],
                        fps=camera_cfg["fps"],
                    )

                video_thread = threading.Thread(
                    target=record_video,
                    args=(session_dir, camera_cfg, stop_video_event, video_ready_event, session_t0),
                    daemon=True,
                )
                video_thread.start()

                print("Waiting for video ready...")
                update_state({"message": "Waiting for video ready"})

                if not video_ready_event.wait(timeout=10):
                    stop_video_event.set()
                    if video_thread:
                        video_thread.join()
                    raise RuntimeError("Camera chưa ghi được frame đầu tiên sau 10 giây")

                time.sleep(0.5)

            update_state({"message": "Running action plan"})

            audio = AudioCueService(session_dir)
            audio.run_action_plan(action_plan, session_t0=session_t0)

            print("Done")
            print("Action events:", session_dir / "action_events.csv")
            print("CSI data:")
            print(" -", session_dir / "raw_asus1.csv")
            print(" -", session_dir / "raw_asus2.csv")
            print(" -", session_dir / "raw_asus3.csv")
            print(" -", session_dir / "raw_esp1.csv")
            print(" -", session_dir / "raw_esp2.csv")
            print(" -", session_dir / "raw_esp3.csv")
            if camera_enabled:
                print("Video:", session_dir / "video.mp4")
                print("Video index:", session_dir / "video_index.csv")

            update_state({"message": "Done"})

        except Exception as e:
            traceback.print_exc()
            update_state({"error": str(e), "message": f"Error: {str(e)}"})

        finally:
            stop_video_event.set()

            if video_thread:
                video_thread.join()

            if csi_service:
                csi_service.stop_csi_collection()

            self.is_running = False
            update_state({
                "running": False,
                "camera_ready": False,
                "csi_ready": False,
                "current_action": None,
                "message": "Done",
            })
