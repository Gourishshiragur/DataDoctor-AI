import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import profiling


def test_profile_detects_nulls_and_duplicates():
    df = pd.DataFrame({
        "a": [1, 2, None, 4, 4],
        "b": ["x", "y", "z", "w", "w"],
    })
    profile = profiling.profile_dataframe(df)
    assert profile["row_count"] == 5
    assert profile["columns"]["a"]["null_count"] == 1
    assert profile["duplicate_rows"] == 1


def test_quality_score_penalizes_nulls():
    clean = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    dirty = pd.DataFrame({"a": [1, None, None, None, 5]})
    clean_score = profiling.quality_score(profiling.profile_dataframe(clean))
    dirty_score = profiling.quality_score(profiling.profile_dataframe(dirty))
    assert clean_score > dirty_score


def test_quality_score_bounds():
    df = pd.DataFrame({"a": [None] * 20})
    score = profiling.quality_score(profiling.profile_dataframe(df))
    assert 0.0 <= score <= 100.0


if __name__ == "__main__":
    test_profile_detects_nulls_and_duplicates()
    test_quality_score_penalizes_nulls()
    test_quality_score_bounds()
    print("profiling tests passed")
