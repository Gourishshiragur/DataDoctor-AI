# Databricks notebook source
# MAGIC %md
# MAGIC # DataDoctorAI â€” Bronze/Silver/Gold Job (Databricks-native)
# MAGIC Mirrors the logic in `pipeline/bronze.py`, `pipeline/silver.py`, `pipeline/gold.py`
# MAGIC but runs natively as a Databricks Job against Unity Catalog tables, for teams that
# MAGIC prefer the transformation to execute in-workspace rather than being orchestrated
# MAGIC from the Streamlit app.
# pyright: reportUndefinedVariable=false
# pyright: reportMissingImports=false

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("source_path", "/Volumes/main/default/landing/dataset.csv")
dbutils.widgets.text("dataset_name", "dataset")
dbutils.widgets.text("datadoctor_run_id", "")
dbutils.widgets.text("bronze_table", "bronze")
dbutils.widgets.text("silver_table", "silver")
dbutils.widgets.text("gold_table", "gold")
dbutils.widgets.text("batch_size", "50000")
dbutils.widgets.text("optimize_after_load", "True")
dbutils.widgets.text("vacuum_after_load", "False")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")
dataset_name = dbutils.widgets.get("dataset_name")
datadoctor_run_id = dbutils.widgets.get("datadoctor_run_id")
bronze_prefix = dbutils.widgets.get("bronze_table")
silver_prefix = dbutils.widgets.get("silver_table")
gold_prefix = dbutils.widgets.get("gold_table")
batch_size = int(dbutils.widgets.get("batch_size"))
optimize_after_load = dbutils.widgets.get("optimize_after_load").lower() == "true"
vacuum_after_load = dbutils.widgets.get("vacuum_after_load").lower() == "true"
# ---------------------------------------------------------------------------
# Persistent execution status
# Dashboard reads this Delta table independently of Streamlit page state.
# ---------------------------------------------------------------------------
status_table = f"{catalog}.{schema}.datadoctor_pipeline_status"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {status_table} (
    run_id STRING,
    dataset STRING,
    stage STRING,
    state STRING,
    rows_in BIGINT,
    rows_out BIGINT,
    message STRING,
    updated_at TIMESTAMP
) USING DELTA
""")

def _publish_stage(stage: str, state: str, rows_in=0, rows_out=0, message=""):
    safe_run_id = str(datadoctor_run_id or "")
    if not safe_run_id:
        return

    spark.sql(f"""
    DELETE FROM {status_table}
    WHERE run_id = '{safe_run_id.replace("'", "''")}'
    AND stage = '{str(stage).replace("'", "''")}'
    """)

    row_id = safe_run_id.replace("'", "''")
    row_dataset = str(dataset_name).replace("'", "''")
    row_stage = str(stage).replace("'", "''")
    row_state = str(state).replace("'", "''")
    row_message = str(message).replace("'", "''")

    spark.sql(f"""
    INSERT INTO {status_table}
    VALUES (
        '{row_id}',
        '{row_dataset}',
        '{row_stage}',
        '{row_state}',
        {int(rows_in or 0)},
        {int(rows_out or 0)},
        '{row_message}',
        current_timestamp()
    )
    """)

_publish_stage("source", "success", message="Source accepted by Databricks")

# batch_size controls target file size on write â€” mirrors the app-side pipeline
# setting of the same name (settings["pipeline"]["batch_size"]) instead of a fixed
# Spark default, so a Job run behaves consistently with what Settings says.
# Configure file sizing if the runtime supports it.
# Databricks Serverless / Spark Connect blocks some Spark configs,
# while classic clusters allow them.

try:
    spark.conf.set("spark.sql.files.maxRecordsPerFile", str(batch_size))
    print(f"âœ“ Enabled maxRecordsPerFile={batch_size}")
except Exception as ex:
    print(f"âš  Runtime doesn't support maxRecordsPerFile. Continuing... ({ex})")


def _finalize_table(full_table_name: str):
    """Applies the Delta housekeeping flags from settings["pipeline"] after each write."""
    if optimize_after_load:
        spark.sql(f"OPTIMIZE {full_table_name}")
    if vacuum_after_load:
        spark.sql(f"VACUUM {full_table_name}")


# COMMAND ----------
from pyspark.sql import functions as F

# Bronze: raw ingestion + metadata
_publish_stage("bronze", "running", message="Ingesting raw data into Bronze")

raw_df = spark.read.option("header", True).option("inferSchema", True).csv(source_path)
bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp())

bronze_table = f"{catalog}.{schema}.{bronze_prefix}__{dataset_name}"
bronze_df.write.mode("overwrite").saveAsTable(bronze_table)
_finalize_table(bronze_table)

bronze_count = bronze_df.count()
_publish_stage(
    "bronze",
    "success",
    rows_in=bronze_count,
    rows_out=bronze_count,
    message=f"Bronze ingestion complete: {bronze_count:,} rows",
)

# COMMAND ----------
# Silver: quality checks + self-healing repair (simplified Spark equivalent)

_publish_stage(
    "profiling",
    "running",
    rows_in=bronze_count,
    message="Profiling Bronze data and detecting quality issues",
)

silver_df = bronze_df.drop("_ingested_at")

numeric_cols = [
    f.name
    for f in silver_df.schema.fields
    if str(f.dataType) in ("DoubleType", "IntegerType", "LongType", "FloatType")
]

_publish_stage(
    "profiling",
    "success",
    rows_in=bronze_count,
    message=f"Profiled {len(silver_df.columns):,} columns; {len(numeric_cols):,} numeric columns detected",
)

_publish_stage(
    "quality",
    "running",
    rows_in=bronze_count,
    message="Running data quality checks",
)

for c in numeric_cols:
    median_values = silver_df.approxQuantile(c, [0.5], 0.05)
    median = median_values[0] if median_values else None

    if median is not None:
        silver_df = silver_df.fillna({c: median})

before_dedup_count = silver_df.count()
silver_df = silver_df.dropDuplicates()

_publish_stage(
    "quality",
    "success",
    rows_in=bronze_count,
    rows_out=before_dedup_count,
    message="Quality checks completed",
)

_publish_stage(
    "repair",
    "running",
    rows_in=before_dedup_count,
    message="Applying automated data repairs",
)

after_repair_count = silver_df.count()

_publish_stage(
    "repair",
    "success",
    rows_in=before_dedup_count,
    rows_out=after_repair_count,
    message="Automated repair completed",
)

_publish_stage(
    "silver",
    "running",
    rows_in=bronze_count,
    message="Writing cleaned Silver dataset",
)

silver_table = f"{catalog}.{schema}.{silver_prefix}__{dataset_name}"
silver_df.write.mode("overwrite").saveAsTable(silver_table)
_finalize_table(silver_table)

silver_count = silver_df.count()

_publish_stage(
    "silver",
    "success",
    rows_in=bronze_count,
    rows_out=silver_count,
    message=f"Silver dataset ready: {silver_count:,} rows",
)

# COMMAND ----------
# Gold: business aggregates (adjust group column per dataset)

_publish_stage(
    "gold",
    "running",
    rows_in=silver_count,
    message="Building Gold business aggregates",
)

string_cols = [
    f.name for f in silver_df.schema.fields if str(f.dataType) == "StringType"
]
group_col = string_cols[0] if string_cols else None

if group_col and numeric_cols:
    agg_exprs = [F.sum(c).alias(f"sum_{c}") for c in numeric_cols[:4]] + [
        F.avg(c).alias(f"avg_{c}") for c in numeric_cols[:4]
    ]
    gold_df = silver_df.groupBy(group_col).agg(*agg_exprs)
else:
    gold_df = silver_df

gold_table = f"{catalog}.{schema}.{gold_prefix}__{dataset_name}"
gold_df.write.mode("overwrite").saveAsTable(gold_table)
_finalize_table(gold_table)

gold_count = gold_df.count()

_publish_stage(
    "gold",
    "success",
    rows_in=silver_count,
    rows_out=gold_count,
    message=f"Gold dataset ready: {gold_count:,} rows",
)

print(f"Bronze: {bronze_table}\nSilver: {silver_table}\nGold: {gold_table}")



