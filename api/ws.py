# app/api/ws.py
#
# WebSocket realtime status.
# Gửi realtime cho Web UI:
# - trạng thái và packet rate của 6 thiết bị
# - trạng thái TCP ASUS Collection để hiển thị badge ASUS TCP

import asyncio
from typing import Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.uart_manager import uart_manager
from app.services.ethernet_manager import ethernet_manager

router = APIRouter()

clients: Set[WebSocket] = set()
client_locks: Dict[WebSocket, asyncio.Lock] = {}


def _compact_devices(status: dict, device_ids: list[str]) -> dict:
    """Lấy gọn status + packet_rate cho danh sách thiết bị."""
    devices = status.get("devices", {}) if isinstance(status, dict) else {}
    compact = {}

    for device_id in device_ids:
        device = devices.get(device_id, {})
        compact[device_id] = {
            "status": device.get("status", "DISCONNECTED"),
            "packet_rate": device.get("packet_rate", 0),
        }

    return compact


def build_ws_state() -> dict:
    """
    Payload duy nhất gửi lên frontend qua /ws/status.
    Không gửi session_id, session_dir, queue size, COM, baudrate, host, port.
    Chỉ thêm asus_tcp.connected để badge ASUS TCP biết ON/OFF.
    """
    uart_manager.refresh_all_rates()
    ethernet_manager.refresh_all_rates()

    esp_status = uart_manager.get_status()
    asus_status = ethernet_manager.get_status()

    return {
        "esp": _compact_devices(esp_status, ["esp1", "esp2", "esp3"]),
        "asus": _compact_devices(asus_status, ["asus1", "asus2", "asus3"]),
        "asus_tcp": {
            "connected": bool((asus_status.get("tcp") or asus_status.get("queue") or {}).get("connected", False))
        },
    }


async def _send_state_to_client(client: WebSocket) -> bool:
    lock = client_locks.get(client)
    if lock is None:
        return False

    try:
        async with lock:
            await client.send_json(build_ws_state())
        return True
    except Exception:
        return False


async def broadcast_state():
    """Gửi trạng thái/rate mới nhất tới tất cả client."""
    dead_clients = []

    for client in list(clients):
        ok = await _send_state_to_client(client)
        if not ok:
            dead_clients.append(client)

    for client in dead_clients:
        clients.discard(client)
        client_locks.pop(client, None)


@router.websocket("/ws/status")
async def websocket_status(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    client_locks[websocket] = asyncio.Lock()

    try:
        await _send_state_to_client(websocket)

        while True:
            await asyncio.sleep(1)
            ok = await _send_state_to_client(websocket)
            if not ok:
                break

    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(websocket)
        client_locks.pop(websocket, None)
