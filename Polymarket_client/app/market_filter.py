from pathlib import Path
import yaml


class MarketFilter:
    def __init__(self, config_path: Path):
        self.mode = None
        self.whitelist = set()
        self._load_config(config_path)

    def _load_config(self, config_path: Path):
        if not config_path.exists():
            raise FileNotFoundError(f"markets.yaml not found: {config_path}")

        with config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        self.mode = config.get("mode", "all")

        if self.mode not in {"all", "whitelist"}:
            raise ValueError(f"Invalid markets mode: {self.mode}")

        if self.mode == "whitelist":
            whitelist = config.get("whitelist", [])
            if not isinstance(whitelist, list):
                raise ValueError("whitelist must be a list")
            self.whitelist = set(whitelist)

    def is_market_allowed(self, token_id: str) -> bool:
        if self.mode == "all":
            return True

        if self.mode == "whitelist":
            return token_id in self.whitelist

        return False  # на всякий случай
