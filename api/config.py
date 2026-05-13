# File này chỉ đọc danh sách kịch bản, không đọc session_config.json. - 🆕  API trả scenario cho UI
import json
from pathlib import Path
from fastapi import APIRouter

router = APIRouter()

SCENARIO_PATH = Path("data/scripts/action_scenarios.json")


@router.get("/scenarios")
def get_scenarios():
    with open(SCENARIO_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    scenarios = []

    for item in data:
        scenarios.append({
            "name": item["scenario"],
            "label": item["scenario"]
        })

    return {
        "scenarios": scenarios
    }