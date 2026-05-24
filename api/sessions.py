# API quản lý session

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.recording_service import RecordingService

router = APIRouter()

recorder = RecordingService()


class CaptureConfig(BaseModel):
    """
    Cấu hình phần dữ liệu tùy chọn khi START SESSION.
    ESP và ASUS không nằm ở đây vì đã được cấu hình riêng qua /com/control
    và /ethernet/control.
    """
    camera: bool = True


class StartSessionRequest(BaseModel):
    room_id: int = Field(..., ge=1)
    setup_id: int = Field(..., ge=1)
    session_no: int = Field(..., ge=1)
    person_id: int = Field(..., ge=1)
    position_id: int = Field(..., ge=1)
    repeat_count: int = Field(..., ge=1)
    scenario: str
    capture: CaptureConfig = CaptureConfig()


@router.post("/start")
def start_session(config: StartSessionRequest):
    return recorder.start(config.model_dump())


@router.post("/stop")
def stop_session():
    return recorder.stop()
