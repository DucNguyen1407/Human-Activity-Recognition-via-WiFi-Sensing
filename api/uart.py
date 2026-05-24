# app/api/uart.py
#
# API quản lý UART/ESP.
# Endpoint theo tài liệu giao diện:
# - GET  /com
# - POST /com/control
#
# Web không cấu hình host/port TCP ESP32-Collection.
# Host/port này nằm cố định trong app/adapters/esp_tcp_client.py.

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.uart_manager import uart_manager

router = APIRouter()


class UartControlRequest(BaseModel):
    """
    Refresh/Kết nối TCP Collection và yêu cầu danh sách COM:
    {
        "action": "refresh_com_ports"
    }

    Connect ESP:
    {
        "action": "connect",
        "uartId": "esp1",
        "port": "COM3",
        "baudRate": 115200
    }

    Disconnect ESP:
    {
        "action": "disconnect",
        "uartId": "esp1"
    }
    """
    action: str
    uartId: Optional[str] = None
    device_id: Optional[str] = None
    port: Optional[str] = None
    com: Optional[str] = None
    baudRate: Optional[int] = None
    baudrate: Optional[int] = None


@router.get("/com")
def get_com_info():
    """
    GET /com chỉ dùng để lấy trạng thái TCP ESP32-Collection
    và danh sách COM do Collection gửi lên.

    Trạng thái/rate esp1/esp2/esp3 được cập nhật qua WebSocket /ws/status,
    nên endpoint này không trả devices nữa để tránh trùng dữ liệu và tránh UI bị ghi đè.
    """
    status = uart_manager.get_status()
    connected = bool(status.get("collection_connected"))

    return {
        "collection_connected": connected,
        "ports": list(status.get("ports", [])) if connected else [],
        "available_ports": list(status.get("available_ports", [])) if connected else [],
        "com_source": status.get("com_source"),
        "com_updated_at": status.get("com_updated_at"),
    }


@router.post("/com/control")
def control_uart(payload: UartControlRequest):
    try:
        device_id = payload.device_id or payload.uartId
        com = payload.com or payload.port
        baudrate = payload.baudrate or payload.baudRate

        return uart_manager.control(
            action=payload.action,
            device_id=device_id,
            com=com,
            baudrate=baudrate,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
