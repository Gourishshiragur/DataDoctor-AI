import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai.provider_router import offline_infer


def test_offline_sql_fallback_produces_select():
    text = offline_infer("give me top customers", system="You generate SQL")
    assert "SELECT" in text.upper()


def test_offline_repair_fallback_mentions_repair():
    text = offline_infer("how should I fix this column", system="repair advisor")
    assert "repair" in text.lower() or "imputed" in text.lower()


def test_offline_default_fallback_mentions_offline():
    text = offline_infer("hello", system="")
    assert "offline" in text.lower()


if __name__ == "__main__":
    test_offline_sql_fallback_produces_select()
    test_offline_repair_fallback_mentions_repair()
    test_offline_default_fallback_mentions_offline()
    print("provider router tests passed")
