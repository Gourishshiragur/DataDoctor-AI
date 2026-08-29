"""ai/prompt_library.py — centralized prompt templates used across the app."""

SQL_SYSTEM_PROMPT = (
    "You are a SQL expert generating DuckDB/Spark-SQL compatible queries. "
    "Given a natural language question and a table schema, return ONLY the SQL query, "
    "no explanation, no markdown fences."
)

REPAIR_SYSTEM_PROMPT = (
    "You are a data quality engineer. Given a column name, its data type, and a description "
    "of a detected issue (nulls, duplicates, outliers, type mismatch), propose the single best "
    "repair action and a one-sentence justification. Be concise and specific."
)

BUSINESS_SYSTEM_PROMPT = (
    "You are a business intelligence analyst. Given aggregate KPI numbers from the Gold layer "
    "of a data pipeline, write a short (3-5 sentence) plain-English summary highlighting the "
    "most important trend or risk. Avoid restating every number — focus on what matters."
)

PIPELINE_PLAN_SYSTEM_PROMPT = (
    "You are a data pipeline architect. Given a raw dataset's column names and dtypes, propose "
    "a Bronze -> Silver -> Gold transformation plan as a short bullet list: what cleaning rules "
    "apply to Silver, and what business aggregates belong in Gold."
)


def sql_prompt(question: str, schema: str) -> str:
    return f"Schema:\n{schema}\n\nQuestion: {question}\n\nSQL:"


def repair_prompt(column: str, dtype: str, issue: str, sample_values: list) -> str:
    return (
        f"Column: {column}\nType: {dtype}\nIssue detected: {issue}\n"
        f"Sample affected values: {sample_values[:5]}\n\nWhat repair action should be taken?"
    )


def business_prompt(kpis: dict) -> str:
    lines = "\n".join(f"- {k}: {v}" for k, v in kpis.items())
    return f"Gold layer KPIs:\n{lines}\n\nWrite the business summary."


def pipeline_plan_prompt(columns_dtypes: dict) -> str:
    lines = "\n".join(f"- {c}: {t}" for c, t in columns_dtypes.items())
    return f"Raw columns:\n{lines}\n\nPropose the Bronze -> Silver -> Gold plan."
