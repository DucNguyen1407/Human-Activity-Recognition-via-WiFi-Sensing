import cv2

from app.core.config import CAMERA_CONFIG


class WebcamAdapter:
    def __init__(
        self,
        camera_index: int = 0,
        width: int | None = None,
        height: int | None = None,
        fps: int | None = None,
    ):
        self.camera_index = camera_index

        # Nếu không truyền width/height/fps thì lấy từ config chung.
        self.width = width if width is not None else CAMERA_CONFIG["width"]
        self.height = height if height is not None else CAMERA_CONFIG["height"]
        self.fps = fps if fps is not None else CAMERA_CONFIG["fps"]

        self.cap = None

    def open(self):
        # Dùng DirectShow để ổn định hơn với webcam ngoài trên Windows.
        self.cap = cv2.VideoCapture(self.camera_index, cv2.CAP_DSHOW)

        if not self.cap.isOpened():
            raise RuntimeError(f"Không mở được camera index={self.camera_index}")

        # Set cấu hình camera theo config chung.
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        self.cap.set(cv2.CAP_PROP_FPS, self.fps)

        # MJPG thường ổn định hơn YUY2 với webcam USB.
        self.cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))

        # Giảm buffer để tránh delay/đơ frame.
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    def read_frame(self):
        if self.cap is None:
            return False, None

        ok, frame = self.cap.read()

        if not ok or frame is None:
            return False, None

        return True, frame

    def close(self):
        if self.cap is not None:
            self.cap.release()
            self.cap = None