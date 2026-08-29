"""dbx_enterprise/unity_catalog.py — helpers for Unity Catalog metadata (catalogs,
schemas, lineage) via the Databricks SDK. Lazily imported so Demo Mode never needs this
package. Mode-aware like connection.py/jobs.py — pass mode="demo" or "enterprise", or
it uses whichever mode is currently active."""
from __future__ import annotations

from typing import Optional

from config.settings import current_mode, get_databricks_config, load_settings


def _get_workspace_client(mode: Optional[str] = None):
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise RuntimeError("databricks-sdk is not installed. Run: pip install databricks-sdk") from e

    settings = load_settings()
    mode = mode or current_mode(settings)
    cfg = get_databricks_config(settings, mode)
    return WorkspaceClient(host=cfg["workspace_url"], token=cfg["token"])


def list_catalogs(mode: Optional[str] = None) -> list[str]:
    client = _get_workspace_client(mode)
    return [c.name for c in client.catalogs.list()]


def list_schemas(catalog: str, mode: Optional[str] = None) -> list[str]:
    client = _get_workspace_client(mode)
    return [s.name for s in client.schemas.list(catalog_name=catalog)]


def list_tables(catalog: str, schema: str, mode: Optional[str] = None) -> list[str]:
    client = _get_workspace_client(mode)
    return [t.name for t in client.tables.list(catalog_name=catalog, schema_name=schema)]


def get_table_lineage(catalog: str, schema: str, table: str, mode: Optional[str] = None) -> dict:
    """Placeholder for Unity Catalog lineage API — structure depends on SDK version.
    Returns an empty lineage graph if unavailable, so the UI degrades gracefully."""
    try:
        client = _get_workspace_client(mode)
        full_name = f"{catalog}.{schema}.{table}"
        # NOTE: lineage APIs vary by SDK version / workspace tier; wrap defensively.
        info = client.tables.get(full_name=full_name)
        return {"table": full_name, "comment": getattr(info, "comment", None)}
    except Exception as e:  # noqa: BLE001
        return {"table": f"{catalog}.{schema}.{table}", "error": str(e)}
