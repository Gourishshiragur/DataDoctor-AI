from core.databricks_executor import configured
from core.models import new_id

def test_new_id_has_prefix():
    assert new_id("run").startswith("run-")

def test_databricks_is_optional():
    assert isinstance(configured(), bool)
