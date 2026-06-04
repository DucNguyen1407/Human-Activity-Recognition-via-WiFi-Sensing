# app/api/ethernet.py
#
# API quản lý ASUS/Nexmon Collection.
# Web UI POST host/port xuống backend khi người dùng bấm Lưu/Kết nối TCP ASUS.
# Không dùng GET /ethernet nữa vì realtime status/rate ASUS lấy qua WebSocket /ws/status.
# Không dùng protocol và không cấu hình riêng từng asus trên Web.

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.ethernet_manager import ethernet_manager

router = APIRouter()


class EthernetControlRequest(BaseModel):
    action: str
    host: Optional[str] = None
    port: Optional[int] = None


@router.post("/ethernet/control")
def control_ethernet(payload: EthernetControlRequest):
    try:
        return ethernet_manager.control(
            action=payload.action,
            host=payload.host,
            port=payload.port,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
