"""
Pre-built pipeline templates for DataDoctor AI.

These templates allow users to create realistic enterprise-style
data pipelines without building every pipeline step manually from
a blank canvas.

Each template returns a list of step dictionaries shaped exactly
like core.models.Step.to_dict(), ready to load into Pipeline
Builder's draft_steps state.
"""

from core.models import Step, new_id


# ============================================================
# Template step builder
# ============================================================

def _steps(
    *definitions,
) -> list:
    """
    Convert pipeline-template definitions into serialized
    Step objects.

    Dependencies are declared using readable step names and
    converted internally into generated step IDs.

    Dependencies must refer to steps defined earlier in the
    template. This prevents invalid forward dependencies and
    keeps the generated pipeline dependency graph valid.
    """

    steps = []

    name_to_id = {}


    for definition in definitions:


        name = definition[
            "name"
        ]


        # ----------------------------------------------------
        # Prevent duplicate names
        # ----------------------------------------------------

        if name in name_to_id:

            raise ValueError(
                (
                    "Duplicate pipeline-template "
                    f"step name: '{name}'."
                )
            )


        step_id = new_id(
            "step"
        )


        name_to_id[
            name
        ] = step_id


        dependency_ids = []


        # ----------------------------------------------------
        # Convert dependency names into generated IDs
        # ----------------------------------------------------

        for dependency_name in (
            definition.get(
                "depends_on",
                [],
            )
        ):


            if (
                dependency_name
                not in name_to_id
            ):

                raise ValueError(
                    (
                        f"Step '{name}' depends on "
                        f"unknown or future step "
                        f"'{dependency_name}'. "
                        "Dependencies must be defined "
                        "before dependent steps."
                    )
                )


            dependency_ids.append(

                name_to_id[
                    dependency_name
                ]

            )


        # ----------------------------------------------------
        # Create model
        # ----------------------------------------------------

        step = Step(

            id=step_id,

            name=name,

            step_type=(
                definition[
                    "type"
                ]
            ),

            engine=(
                definition.get(
                    "engine",
                    "pyspark",
                )
            ),

            code=(
                definition[
                    "code"
                ]
            ),

            depends_on=(
                dependency_ids
            ),

        )


        steps.append(

            step.to_dict()

        )


    return steps


# ============================================================
# Built-in enterprise pipeline templates
# ============================================================

def get_templates() -> dict:
    """
    Return all built-in DataDoctor AI pipeline templates.

    Every template contains:

    description:
        Human-readable information displayed in Pipeline
        Builder.

    steps:
        Pipeline steps serialized as dictionaries.
    """

    return {


        # ====================================================
        # BRONZE → SILVER → GOLD
        # ====================================================

        "Bronze → Silver → Gold batch": {

            "description": (
                "Classic medallion batch pipeline with raw "
                "ingestion, data cleansing, deduplication, "
                "validation, and curated Delta publication."
            ),

            "steps": _steps(


                {
                    "name": (
                        "Bronze: load raw"
                    ),

                    "type": (
                        "source"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "df = (\n"
                        "    spark.read\n"
                        "    .format('json')\n"
                        "    .load(\n"
                        "        '/mnt/bronze/landing'\n"
                        "    )\n"
                        ")"
                    ),
                },


                {
                    "name": (
                        "Silver: cleanse & dedup"
                    ),

                    "type": (
                        "transform"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "from pyspark.sql import "
                        "functions as F\n\n"
                        "clean_df = (\n"
                        "    df\n"
                        "    .dropDuplicates(['id'])\n"
                        "    .na.drop(\n"
                        "        subset=['id']\n"
                        "    )\n"
                        ")"
                    ),

                    "depends_on": [
                        "Bronze: load raw",
                    ],
                },


                {
                    "name": (
                        "Gold: publish curated"
                    ),

                    "type": (
                        "sink"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "(\n"
                        "    clean_df.write\n"
                        "    .format('delta')\n"
                        "    .mode('overwrite')\n"
                        "    .save(\n"
                        "        '/mnt/gold/curated'\n"
                        "    )\n"
                        ")"
                    ),

                    "depends_on": [
                        "Silver: cleanse & dedup",
                    ],
                },


            ),

        },


        # ====================================================
        # IDEMPOTENT MICRO-BATCH
        # ====================================================

        "Idempotent micro-batch upsert": {

            "description": (
                "Batch-controlled Delta MERGE pipeline "
                "designed for safe retries, idempotent "
                "processing, duplicate prevention, and "
                "source-to-target reconciliation."
            ),

            "steps": _steps(


                {
                    "name": (
                        "Load batch"
                    ),

                    "type": (
                        "source"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "df = (\n"
                        "    spark.read\n"
                        "    .format('json')\n"
                        "    .load(\n"
                        "        f'/mnt/landing/"
                        "batch_{batch_id}*.json'\n"
                        "    )\n"
                        ")"
                    ),
                },


                {
                    "name": (
                        "Merge upsert"
                    ),

                    "type": (
                        "transform"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "(\n"
                        "    target.alias('t')\n"
                        "    .merge(\n"
                        "        df.alias('s'),\n"
                        "        't.id = s.id'\n"
                        "    )\n"
                        "    .whenMatchedUpdateAll()\n"
                        "    .whenNotMatchedInsertAll()\n"
                        "    .execute()\n"
                        ")"
                    ),

                    "depends_on": [
                        "Load batch",
                    ],
                },


                {
                    "name": (
                        "Reconcile counts"
                    ),

                    "type": (
                        "sink"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "source_ids = (\n"
                        "    df\n"
                        "    .select('id')\n"
                        "    .dropDuplicates()\n"
                        ")\n\n"
                        "matched_count = (\n"
                        "    target.toDF()\n"
                        "    .join(\n"
                        "        source_ids,\n"
                        "        on='id',\n"
                        "        how='inner'\n"
                        "    )\n"
                        "    .select('id')\n"
                        "    .dropDuplicates()\n"
                        "    .count()\n"
                        ")\n\n"
                        "expected_count = (\n"
                        "    source_ids.count()\n"
                        ")\n\n"
                        "assert (\n"
                        "    matched_count\n"
                        "    == expected_count\n"
                        "), (\n"
                        "    f'Reconciliation failed: '\n"
                        "    f'expected={expected_count}, '\n"
                        "    f'actual={matched_count}'\n"
                        ")"
                    ),

                    "depends_on": [
                        "Merge upsert",
                    ],
                },


            ),

        },


        # ====================================================
        # SCD TYPE 2
        # ====================================================

        "SCD Type 2 dimension load": {

            "description": (
                "Enterprise close-and-open version pattern "
                "for maintaining historical slowly changing "
                "dimension records."
            ),

            "steps": _steps(


                {
                    "name": (
                        "Load source changes"
                    ),

                    "type": (
                        "source"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "changes_df = (\n"
                        "    spark.read.table(\n"
                        "        'staging.entity_changes'\n"
                        "    )\n"
                        ")"
                    ),
                },


                {
                    "name": (
                        "Close current versions"
                    ),

                    "type": (
                        "transform"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "(\n"
                        "    target.alias('t')\n"
                        "    .merge(\n"
                        "        changes_df.alias('s'),\n"
                        "        't.entity_id = '\n"
                        "        's.entity_id AND '\n"
                        "        't.is_current = true'\n"
                        "    )\n"
                        "    .whenMatchedUpdate(\n"
                        "        condition=(\n"
                        "            't.status <> '\n"
                        "            's.status'\n"
                        "        ),\n"
                        "        set={\n"
                        "            'is_current': (\n"
                        "                'false'\n"
                        "            ),\n"
                        "            'effective_to': (\n"
                        "                's.event_time'\n"
                        "            ),\n"
                        "        },\n"
                        "    )\n"
                        "    .execute()\n"
                        ")"
                    ),

                    "depends_on": [
                        "Load source changes",
                    ],
                },


                {
                    "name": (
                        "Insert new versions"
                    ),

                    "type": (
                        "sink"
                    ),

                    "engine": (
                        "pyspark"
                    ),

                    "code": (
                        "from pyspark.sql import "
                        "functions as F\n\n"
                        "new_versions_df = (\n"
                        "    changes_df\n"
                        "    .withColumn(\n"
                        "        'effective_from',\n"
                        "        F.col('event_time')\n"
                        "    )\n"
                        "    .withColumn(\n"
                        "        'effective_to',\n"
                        "        F.lit(None)\n"
                        "        .cast('timestamp')\n"
                        "    )\n"
                        "    .withColumn(\n"
                        "        'is_current',\n"
                        "        F.lit(True)\n"
                        "    )\n"
                        ")\n\n"
                        "(\n"
                        "    new_versions_df.write\n"
                        "    .format('delta')\n"
                        "    .mode('append')\n"
                        "    .save(\n"
                        "        '/mnt/gold/dim_entity'\n"
                        "    )\n"
                        ")"
                    ),

                    "depends_on": [
                        "Close current versions",
                    ],
                },


            ),

        },


        # ====================================================
        # SQL-ONLY AGGREGATION
        # ====================================================

        "SQL-only aggregation pipeline": {

            "description": (
                "Lightweight SQL pipeline for teams that "
                "prefer SQL-based extraction, transformation, "
                "aggregation, and reporting."
            ),

            "steps": _steps(


                {
                    "name": (
                        "Extract source table"
                    ),

                    "type": (
                        "source"
                    ),

                    "engine": (
                        "sql"
                    ),

                    "code": (
                        "SELECT\n"
                        "    *\n"
                        "FROM raw.events\n"
                        "WHERE event_date = "
                        "current_date()"
                    ),
                },


                {
                    "name": (
                        "Aggregate daily metrics"
                    ),

                    "type": (
                        "transform"
                    ),

                    "engine": (
                        "sql"
                    ),

                    "code": (
                        "SELECT\n"
                        "    device_id,\n"
                        "    date_trunc(\n"
                        "        'day',\n"
                        "        event_time\n"
                        "    ) AS day,\n"
                        "    COUNT(*) AS events\n"
                        "FROM extracted\n"
                        "GROUP BY\n"
                        "    device_id,\n"
                        "    date_trunc(\n"
                        "        'day',\n"
                        "        event_time\n"
                        "    )"
                    ),

                    "depends_on": [
                        "Extract source table",
                    ],
                },


                {
                    "name": (
                        "Publish to reporting table"
                    ),

                    "type": (
                        "sink"
                    ),

                    "engine": (
                        "sql"
                    ),

                    "code": (
                        "INSERT OVERWRITE TABLE\n"
                        "    reporting."
                        "daily_device_metrics\n"
                        "SELECT\n"
                        "    *\n"
                        "FROM aggregated"
                    ),

                    "depends_on": [
                        "Aggregate daily metrics",
                    ],
                },


            ),

        },


    }