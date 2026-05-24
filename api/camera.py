import cv2
import time

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.services.camera_service import camera_manager

router = APIRouter()


class SelectCameraRequest(BaseModel):
    cam_index: int


class VideoControlRequest(BaseModel):
    enabled: bool


@router.get("/cameras")
def list_cameras():
    return {
        "cameras": camera_manager.list_cameras()
    }


@router.patch("/camera/select")
def select_camera(payload: SelectCameraRequest):
    return camera_manager.select_camera(payload.cam_index)


@router.patch("/video")
def control_video(payload: VideoControlRequest):
    if payload.enabled:
        # width/height/fps dùng giá trị mặc định trong camera_service.py
        return camera_manager.start()

    return camera_manager.stop()


@router.get("/video_feed")
def video_feed():
    def generate():
        while True:
            frame = camera_manager.get_frame()

            if frame is None:
                time.sleep(0.05)
                continue

            ret, buffer = cv2.imencode(".jpg", frame)

            if not ret:
                continue

            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n" +
                buffer.tobytes() +
                b"\r\n"
            )

    return StreamingResponse(
        generate(),
        media_type="multipart/x-mixed-replace; boundary=frame"
    )
