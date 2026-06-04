# app/api/config_scenario.py
# API trả danh sách kịch bản cho UI.
# Chỉ trả danh sách tên scenario, không dùng label.

import json
from pathlib import Path

from fastapi import APIRouter

router = APIRouter()

SCENARIO_PATH = Path("data/scripts/action_scenarios.json")


@router.get("/scenarios")
def get_scenarios():
    with open(SCENARIO_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    scenarios = [item["scenario"] for item in data]

    return {
        "scenarios": scenarios
    }