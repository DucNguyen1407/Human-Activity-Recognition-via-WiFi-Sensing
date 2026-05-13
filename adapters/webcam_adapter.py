


import cv2
class WebcamAdapter:
    def __init__(self, camera_index: int = 0):
        self.camera_index = camera_index
        self.cap = None

    def open(self):
        self.cap = cv2.VideoCapture(self.camera_index)

        if not self.cap.isOpened():
            raise RuntimeError(f"Không mở được camera index={self.camera_index}")

    def read_frame(self):
        if self.cap is None:
            return False, None

        return self.cap.read()

    def close(self):
        if self.cap:
            self.cap.release()
            self.cap = None