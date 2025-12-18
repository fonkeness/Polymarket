from pathlib import Path
import yaml

_BASE_DIR = Path(__file__).resolve().parents[1]  # Polymarket_client
_RULES_PATH = _BASE_DIR / "config" / "rules.yaml"
_cache = None

def load_rules() -> dict:
    global _cache
    if _cache is None:
        with _RULES_PATH.open("r", encoding="utf-8") as f:
            _cache = yaml.safe_load(f) or {}
    return _cache
