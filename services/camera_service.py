# camera_service.py
# Gộp từ camera_manager.py + video_service.py
# - CameraManager: quản lý preview camera, chọn camera, lấy frame mới nhất
# - VideoService: ghi video.mp4 và video_index.csv trong session

from pathlib import Path
import threading
import time

import cv2

from app.adapters.webcam_adapter import WebcamAdapter
from app.core.time_utils import unix_now_us, perf_now
from app.core.config import CAMERA_CONFIG


class VideoService:
    def __init__(
        self,
        session_dir: Path,
        fps: int | None = None,
        width: int | None = None,
        height: int | None = None,
        session_t0: float | None = None
    ):
        self.session_dir = session_dir

        # Nếu không truyền config riêng thì lấy từ CAMERA_CONFIG chung.
        self.fps = fps if fps is not None else CAMERA_CONFIG["fps"]
        self.width = width if width is not None else CAMERA_CONFIG["width"]
        self.height = height if height is not None else CAMERA_CONFIG["height"]

        self.video_path = session_dir / "video.mp4"
        self.index_path = session_dir / "video_index.csv"

        self.writer = None
        self.frame_no = 0
        self.session_t0 = session_t0 if session_t0 is not None else perf_now()

        if not self.index_path.exists():
            self.index_path.write_text(
                "frame_no,timestamp_unix_us,elapsed_us\n",
                encoding="utf-8"
            )

    def open(self):
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        self.writer = cv2.VideoWriter(
            str(self.video_path),
            fourcc,
            self.fps,
            (self.width, self.height)
        )

        if not self.writer.isOpened():
            raise RuntimeError("Không mở được VideoWriter để ghi video.mp4")

    def write_frame(self, frame):
        if self.writer is None:
            raise RuntimeError("VideoService chưa được open()")

        self.frame_no += 1
        elapsed_us = int((perf_now() - self.session_t0) * 1_000_000)

        frame = cv2.resize(frame, (self.width, self.height))
        self.writer.write(frame)

        with open(self.index_path, "a", encoding="utf-8") as f:
            f.write(f"{self.frame_no},{unix_now_us()},{elapsed_us}\n")

    def close(self):
        if self.writer:
            self.writer.release()
            self.writer = None


class CameraManager:
    def __init__(self):
        self.adapter = None
        self.thread = None
        self.lock = threading.Lock()

        self.running = False
        self.selected_camera_index = 0

        # self.width = 640
        # self.height = 480
        # self.fps = 20
        self.width = CAMERA_CONFIG["width"]
        self.height = CAMERA_CONFIG["height"]
        self.fps = CAMERA_CONFIG["fps"]

        self.latest_frame = None

    def list_cameras(self, max_index=5):
        available = []

        for index in range(max_index):
            cam = WebcamAdapter(camera_index=index)

            try:
                cam.open()
                ok, frame = cam.read_frame()

                if ok and frame is not None:
                    available.append(index)

            except Exception:
                pass

            finally:
                cam.close()

        return available

    def select_camera(self, cam_index: int):
        if self.running:
            self.stop()

        self.selected_camera_index = cam_index

        return {
            "status": "success",
            "cam_index": cam_index
        }
    def start(self, width=None, height=None, fps=None):
        if self.running:
            return {
                "status": "already_running",
                "cam_index": self.selected_camera_index
            }

        # Nếu không truyền tham số thì lấy từ CAMERA_CONFIG chung.
        self.width = width if width is not None else CAMERA_CONFIG["width"]
        self.height = height if height is not None else CAMERA_CONFIG["height"]
        self.fps = fps if fps is not None else CAMERA_CONFIG["fps"]

        self.adapter = WebcamAdapter(
            camera_index=self.selected_camera_index,
            width=self.width,
            height=self.height,
            fps=self.fps
        )

        self.adapter.open()

        self.running = True

        self.thread = threading.Thread(
            target=self._capture_loop,
            daemon=True
        )
        self.thread.start()

        return {
            "status": "started",
            "cam_index": self.selected_camera_index,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
        }

    # def _capture_loop(self):
    #     frame_interval = 1.0 / self.fps

    #     while self.running:
    #         loop_start = time.perf_counter()

    #         ok, frame = self.adapter.read_frame()

    #         if ok and frame is not None:
    #             with self.lock:
    #                 self.latest_frame = frame.copy()

    #         elapsed = time.perf_counter() - loop_start
    #         time.sleep(max(0, frame_interval - elapsed))
    def _capture_loop(self):
        frame_interval = 1.0 / self.fps
        fail_count = 0

        while self.running:
            loop_start = time.perf_counter()

            ok, frame = self.adapter.read_frame()

            if ok and frame is not None:
                fail_count = 0
                with self.lock:
                    self.latest_frame = frame.copy()
            else:
                fail_count += 1

                # Nếu lỗi liên tục thì thử mở lại camera
                if fail_count >= 30:
                    print("Camera lỗi liên tục, thử mở lại camera...")

                    try:
                        self.adapter.close()
                        time.sleep(0.5)
                        self.adapter.open()
                        fail_count = 0
                    except Exception as e:
                        print(f"Không mở lại được camera: {e}")
                        time.sleep(1)

            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0, frame_interval - elapsed))

    def stop(self):
        self.running = False

        if self.thread:
            self.thread.join(timeout=2)

        if self.adapter:
            self.adapter.close()

        self.thread = None
        self.adapter = None

        with self.lock:
            self.latest_frame = None

        return {
            "status": "stopped"
        }

    def get_frame(self):
        with self.lock:
            if self.latest_frame is None:
                return None

            return self.latest_frame.copy()

    def status(self):
        return {
            "running": self.running,
            "cam_index": self.selected_camera_index,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
            "has_frame": self.latest_frame is not None
        }


# Instance dùng chung cho preview camera.
camera_manager = CameraManager()

# Alias để code mới có thể import tên camera_service nếu muốn.
camera_service = camera_manager
