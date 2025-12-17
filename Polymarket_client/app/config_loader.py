from pathlib import Path
import yaml
import pprint


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    # Определяем корень Polymarket_client
    base_dir = Path(__file__).resolve().parents[1]

    config_dir = base_dir / "config"

    rules_path = config_dir / "rules.yaml"
    markets_path = config_dir / "markets.yaml"
    settings_path = config_dir / "settings.yaml"

    print("Base dir:", base_dir)
    print("Config dir:", config_dir)
    print()

    rules = load_yaml(rules_path)
    markets = load_yaml(markets_path)
    settings = load_yaml(settings_path)

    print("=== RULES ===")
    pprint.pprint(rules)
    print()

    print("=== MARKETS ===")
    pprint.pprint(markets)
    print()

    print("=== SETTINGS ===")
    pprint.pprint(settings)
    print()


if __name__ == "__main__":
    main()
