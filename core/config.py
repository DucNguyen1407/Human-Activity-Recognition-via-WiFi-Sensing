

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = BASE_DIR / "data"
CONFIG_DIR = DATA_DIR / "config"
SCRIPT_DIR = DATA_DIR / "scripts"
AUDIO_DIR = DATA_DIR / "assets" / "audio"
SESSIONS_DIR = DATA_DIR / "sessions"

CONFIG_DIR.mkdir(parents=True, exist_ok=True)
SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

SESSION_CONFIG_PATH = CONFIG_DIR / "session_config.json"
ACTION_SCENARIOS_PATH = SCRIPT_DIR / "action_scenarios.json"