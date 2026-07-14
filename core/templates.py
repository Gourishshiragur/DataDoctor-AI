"""
Pre-built pipeline templates so a new pipeline doesn't have to be built
step-by-step from a blank canvas every time. Each template returns a list
of step dicts shaped exactly like core.models.Step.__dict__, ready to drop
into Pipeline Builder's draft_steps.
"""
from core.models import Step, new_id


def _steps(*defs):
    steps = []
    name_to_id = {}
    for d in defs:
        sid = new_id("step")
        name_to_id[d["name"]] = sid
        steps.append(
            Step(
                id=sid,
                name=d["name"],
                step_type=d["type"],
                engine=d.get("engine", "pyspark"),
                code=d["code"],
                depends_on=[name_to_id[dep] for dep in d.get("depends_on", [])],
            ).__dict__
        )
    return steps


def get_templates() -> dict:
    return {
        "Bronze → Silver → Gold batch": {
            "description": "Classic medallion batch load: raw ingest, cleanse/dedup, curated publish.",
            "steps": _steps(
                {
                    "name": "Bronze: load raw",
                    "type": "source",
                    "code": "df = spark.read.format('json').load('/mnt/bronze/landing')",
                },
                {
                    "name": "Silver: cleanse & dedup",
                    "type": "transform",
                    "code": (
                        "from pyspark.sql import functions as F\n"
                        "clean_df = df.dropDuplicates(['id']).na.drop(subset=['id'])"
                    ),
                    "depends_on": ["Bronze: load raw"],
                },
                {
                    "name": "Gold: publish curated",
                    "type": "sink",
                    "code": "clean_df.write.format('delta').mode('overwrite').save('/mnt/gold/curated')",
                    "depends_on": ["Silver: cleanse & dedup"],
                },
            ),
        },
        "Idempotent micro-batch upsert": {
            "description": "Batch-tracked Delta MERGE upsert, safe to retry without duplicating rows.",
            "steps": _steps(
                {
                    "name": "Load batch",
                    "type": "source",
                    "code": "df = spark.read.json(f'/mnt/landing/batch_{batch_id}*.json')",
                },
                {
                    "name": "Merge upsert",
                    "type": "transform",
                    "code": (
                        "target.alias('t').merge(df.alias('s'), 't.id = s.id')"
                        ".whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()"
                    ),
                    "depends_on": ["Load batch"],
                },
                {
                    "name": "Reconcile counts",
                    "type": "sink",
                    "code": "assert df.count() == target.toDF().filter(df.id.isin(...)).count()",
                    "depends_on": ["Merge upsert"],
                },
            ),
        },
        "SCD Type 2 dimension load": {
            "description": "Close-and-open version pattern for slowly changing dimensions.",
            "steps": _steps(
                {
                    "name": "Load source changes",
                    "type": "source",
                    "code": "changes_df = spark.read.table('staging.entity_changes')",
                },
                {
                    "name": "Close current versions",
                    "type": "transform",
                    "code": (
                        "target.alias('t').merge(changes_df.alias('s'), "
                        "'t.entity_id = s.entity_id AND t.is_current = true')"
                        ".whenMatchedUpdate(condition='t.status <> s.status', "
                        "set={'is_current': 'false', 'effective_to': 's.event_time'}).execute()"
                    ),
                    "depends_on": ["Load source changes"],
                },
                {
                    "name": "Insert new versions",
                    "type": "sink",
                    "code": "changes_df.write.format('delta').mode('append').save('/mnt/gold/dim_entity')",
                    "depends_on": ["Close current versions"],
                },
            ),
        },
        "SQL-only aggregation pipeline": {
            "description": "Lightweight pipeline for teams that prefer SQL over PySpark for transforms.",
            "steps": _steps(
                {
                    "name": "Extract source table",
                    "type": "source",
                    "engine": "sql",
                    "code": "SELECT * FROM raw.events WHERE event_date = current_date()",
                },
                {
                    "name": "Aggregate daily metrics",
                    "type": "transform",
                    "engine": "sql",
                    "code": (
                        "SELECT device_id, date_trunc('day', event_time) AS day, COUNT(*) AS events\n"
                        "FROM extracted GROUP BY device_id, date_trunc('day', event_time)"
                    ),
                    "depends_on": ["Extract source table"],
                },
                {
                    "name": "Publish to reporting table",
                    "type": "sink",
                    "engine": "sql",
                    "code": "INSERT OVERWRITE TABLE reporting.daily_device_metrics SELECT * FROM aggregated",
                    "depends_on": ["Aggregate daily metrics"],
                },
            ),
        },
    }
