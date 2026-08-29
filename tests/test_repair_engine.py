import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai.repair_engine import repair
from pipeline import quality


def test_repair_removes_duplicates():
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    repaired, actions = repair(df, explain=False)
    assert len(repaired) == 2
    assert any(a["issue"] == "duplicate_rows" for a in actions)


def test_repair_imputes_nulls():
    df = pd.DataFrame({"a": [1.0, None, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]})
    repaired, actions = repair(df, explain=False)
    assert repaired["a"].isna().sum() == 0
    assert any("null" in a["issue"] for a in actions)


def test_repair_fixes_negative_quantity():
    df = pd.DataFrame({"quantity": [1, 2, -5, 3, 4, 5, 6, 7, 8, 9]})
    repaired, actions = repair(df, explain=False)
    assert (repaired["quantity"] >= 0).all()
    assert any("negative" in a["issue"] for a in actions)


def test_repair_caps_outliers():
    values = list(range(1, 100)) + [10000]  # one extreme outlier
    df = pd.DataFrame({"amount": values})
    repaired, actions = repair(df, explain=False)
    assert repaired["amount"].max() < 10000
    assert any("outlier" in a["issue"] for a in actions)


def test_repair_normalizes_categorical_casing():
    df = pd.DataFrame({"type": ["Savings", "savings", "SAVINGS", "Checking"] * 3})
    repaired, actions = repair(df, explain=False)
    assert repaired["type"].nunique() == 2  # Savings, Checking
    assert any("variant" in a["issue"] for a in actions)


def test_repaired_data_passes_quality_checks():
    df = pd.DataFrame({
        "quantity": [1, -2, 3, None, 5, 6, 7, 8, 9, 10],
        "type": ["A", "a", "B", "b", "A", "A", "B", "B", "A", "B"],
    })
    repaired, _ = repair(df, explain=False)
    results = {r["check_name"]: r["passed"] for r in quality.run_all_checks(repaired)}
    assert results["duplicate_rows"] is True
    assert results["negative_values_where_invalid"] is True
    assert results["categorical_consistency"] is True


if __name__ == "__main__":
    test_repair_removes_duplicates()
    test_repair_imputes_nulls()
    test_repair_fixes_negative_quantity()
    test_repair_caps_outliers()
    test_repair_normalizes_categorical_casing()
    test_repaired_data_passes_quality_checks()
    print("repair engine tests passed")
