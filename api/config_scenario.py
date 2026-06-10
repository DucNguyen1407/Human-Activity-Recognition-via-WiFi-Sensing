# app/api/config_scenario.py
# API trả danh sách kịch bản cho UI.
# Chỉ trả danh sách tên scenario, không dùng label.

import json

from fastapi import APIRouter

from app.core.config import ACTION_SCENARIOS_PATH

router = APIRouter()


@router.get("/scenarios")
def get_scenarios():
    with open(ACTION_SCENARIOS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    scenarios = [item["scenario"] for item in data]

    return {
        "scenarios": scenarios
    }