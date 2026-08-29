import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import quality


def test_null_threshold_check_fails_above_threshold():
    df = pd.DataFrame({"a": [1, None, None, None, None]})
    passed, details = quality.check_null_thresholds(df, threshold_pct=15.0)
    assert passed is False
    assert "a" in details["offending_columns"]


def test_duplicate_check():
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    passed, details = quality.check_duplicate_rows(df)
    assert passed is False
    assert details["duplicate_rows"] == 1


def test_negative_values_check():
    df = pd.DataFrame({"quantity": [1, -3, 5], "age": [30, 40, -1]})
    passed, details = quality.check_negative_values(df)
    assert passed is False
    assert details["offending_columns"]["quantity"] == 1
    assert details["offending_columns"]["age"] == 1


def test_categorical_consistency_check():
    df = pd.DataFrame({"type": ["Savings", "savings", "Checking", "CHECKING", "Savings"]})
    passed, details = quality.check_categorical_consistency(df)
    assert passed is False
    assert details["offending_columns"]["type"]["raw_variants"] == 4


def test_run_all_checks_returns_all():
    df = pd.DataFrame({"a": [1, 2, 3]})
    results = quality.run_all_checks(df)
    names = {r["check_name"] for r in results}
    assert names == set(quality.CHECKS)


if __name__ == "__main__":
    test_null_threshold_check_fails_above_threshold()
    test_duplicate_check()
    test_negative_values_check()
    test_categorical_consistency_check()
    test_run_all_checks_returns_all()
    print("quality tests passed")
