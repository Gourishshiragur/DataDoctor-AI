"""
config/demo.py
---------------
Everything needed to run DataDoctorAI with zero external services:
- Registry of bundled demo datasets (retail, banking, healthcare, ecommerce, manufacturing)
- Each dataset is deliberately "dirty" (nulls, duplicates, bad types, outliers) so the
  Quality + Repair engines have real work to do.
"""
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATASETS_DIR = ROOT_DIR / "datasets"

DEMO_DATASETS = {
    "retail": {
        "label": "🛍️ Retail — Store Transactions",
        "file": DATASETS_DIR / "retail" / "transactions.csv",
        "description": "Point-of-sale transactions with missing prices, duplicate order IDs, and inconsistent store codes.",
    },
    "banking": {
        "label": "🏦 Banking — Customer Accounts",
        "file": DATASETS_DIR / "banking" / "accounts.csv",
        "description": "Account & transaction records with malformed IBANs, null balances, and negative-age anomalies.",
    },
    "healthcare": {
        "label": "🏥 Healthcare — Patient Visits",
        "file": DATASETS_DIR / "healthcare" / "visits.csv",
        "description": "Patient visit logs with inconsistent date formats, missing vitals, and outlier readings.",
    },
    "ecommerce": {
        "label": "🛒 E-Commerce — Order Events",
        "file": DATASETS_DIR / "ecommerce" / "orders.csv",
        "description": "Order + shipping events with duplicate emails, currency mismatches, and null shipping dates.",
    },
    "manufacturing": {
        "label": "🏭 Manufacturing — Sensor & QA Logs",
        "file": DATASETS_DIR / "manufacturing" / "sensor_qa.csv",
        "description": "Machine sensor + QA logs with sensor dropouts, out-of-range readings, and shift code typos.",
    },
}

DEMO_MODE_NOTICE = (
    "Demo Mode ? Free execution ? DuckDB fallback"
)
