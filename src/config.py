from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_RAW_DIR = ROOT_DIR / "data" / "raw"
DATA_PROCESSED_DIR = ROOT_DIR / "data" / "processed"
SQL_DIR = ROOT_DIR / "sql"
OUTPUTS_CHARTS_DIR = ROOT_DIR / "outputs" / "charts"
OUTPUTS_DASHBOARD_DIR = ROOT_DIR / "outputs" / "dashboard"
OUTPUTS_REPORTS_DIR = ROOT_DIR / "outputs" / "reports"
DOCS_DIR = ROOT_DIR / "docs"
NOTEBOOKS_DIR = ROOT_DIR / "notebooks"
DB_PATH = DATA_PROCESSED_DIR / "railway_maintenance.db"

RANDOM_SEED = 42
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"

for path in [
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    SQL_DIR,
    OUTPUTS_CHARTS_DIR,
    OUTPUTS_DASHBOARD_DIR,
    OUTPUTS_REPORTS_DIR,
    DOCS_DIR,
    NOTEBOOKS_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)
