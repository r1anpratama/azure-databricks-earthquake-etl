# Azure Databricks ETL Pipeline — Medallion Architecture

**ETL Pipeline:** USGS Earthquake CSV → Bronze (Raw) → Silver (Clean) → Gold (Analytics + ML Features)

[![CI](https://github.com/r1anpratama/azure-databricks-earthquake-etl/actions/workflows/ci.yml/badge.svg)](https://github.com/r1anpratama/azure-databricks-earthquake-etl/actions/workflows/ci.yml)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://community.cloud.databricks.com)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-0078D4?style=flat&logo=delta&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **✅ Databricks Community Edition (Free) Compatible** — Runs on Serverless SQL Warehouse with Unity Catalog managed tables. No DBFS, no compute clusters needed.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                           │
├──────────────┬─────────────────┬─────────────────┬────────────────┤
│   EXTERNAL   │     BRONZE      │     SILVER      │     GOLD       │
│   SOURCE     │   (Raw Layer)   │  (Clean Layer)  │ (Analytics)    │
├──────────────┼─────────────────┼─────────────────┼────────────────┤
│              │                 │                 │                │
│  USGS FDSNWS │  Schema enforce │  Deduplicate    │  Daily stats   │
│  Earthquake  │  Quality check │  Null filter    │  Monthly stats │
│  API (CSV)   │  Audit columns  │  Geo enrich     │  Regional risk │
│              │  Batch ID      │  Mag classify   │  Mag distrib   │
│              │  Delta table   │  Depth classify │  ML features   │
│              │  UC managed    │  Temporal enrich│  Lag features  │
│              │                 │  Partitioned    │  Rolling windows│
│              │                 │  Delta table    │  5 Gold tables │
└──────────────┴─────────────────┴─────────────────┴────────────────┘
                         │                │                │
                    spark.table    spark.table    spark.table
                   (UC managed)   (UC managed)   (UC managed)
```

---

## Notebooks

| # | Notebook | Layer | Description |
|---|----------|-------|-------------|
| 1 | `01_bronze_ingestion.py` | 🥉 Bronze | USGS CSV → Delta table with schema enforcement, DQ framework, batch ID |
| 2 | `02_silver_transform.py` | 🥈 Silver | Dedup, null filter, geo/mag/depth/temporal enrichment, partitioned Delta |
| 3 | `03_gold_analytics.py` | 🥇 Gold | 5 Gold tables: daily, monthly, regional, mag dist, ML features |

### Run Order
```
01_bronze_ingestion → 02_silver_transform → 03_gold_analytics
```

---

## Gold Layer Tables

| Table | Grain | Features |
|-------|-------|----------|
| `gold_daily_stats` | Daily | Rolling 7d/30d avg magnitude, event count, significant/major events |
| `gold_monthly_stats` | Monthly | MoM delta, shallow/intermediate/deep breakdown, std magnitude |
| `gold_regional_stats` | Region × Month | Geographic risk comparison, max/avg magnitude per region |
| `gold_mag_distribution` | Mag Category | Statistical summary (min, max, avg, std) per magnitude class |
| `gold_ml_features` | Event-level | Lagged magnitude/depth, time-since-prev, rolling 10-event stats, 95th percentile |

---

## Data Quality Framework

### Bronze Layer Checks

| Rule ID | Check | Column | Severity |
|---------|-------|--------|----------|
| DQ001 | no_null_id | id | FAIL |
| DQ002 | no_null_time | time | FAIL |
| DQ003 | valid_latitude | latitude | FAIL |
| DQ004 | valid_longitude | longitude | FAIL |
| DQ005 | valid_magnitude | mag | FAIL |
| DQ006 | no_null_magnitude | mag | WARN |
| DQ007 | no_null_depth | depth | WARN |
| DQ008 | no_null_place | place | WARN |

### Silver Layer Checks

| Check | Description |
|-------|-------------|
| no_duplicates | No duplicate event IDs after dedup |
| no_null_critical | No nulls in time, lat, lon, mag |
| valid_lat | All latitudes in [-90, 90] |
| valid_lon | All longitudes in [-180, 180] |
| valid_mag | All magnitudes in [0, 10] |
| valid_depth | All depths ≥ 0 |
| region_classified | All events assigned a region |

---

## Enrichment Details

### Geographic Region Classification
Bounding-box classification for major tectonic zones:
- Sunda Arc (Sumatra–Java–Bali)
- Japan
- California
- Alaska
- New Zealand
- South America
- Other

### Magnitude Categories

| Range | Category |
|-------|----------|
| < 3.0 | micro |
| 3.0–3.9 | minor |
| 4.0–4.9 | light |
| 5.0–5.9 | moderate |
| 6.0–6.9 | strong |
| 7.0–7.9 | major |
| ≥ 8.0 | great |

### Depth Classification

| Range (km) | Class |
|------------|-------|
| 0–70 | shallow |
| 71–300 | intermediate |
| > 300 | deep |

---

## ML Feature Engineering

The `gold_ml_features` table provides event-level features for ML models:

- **Lag features:** `prev_event_mag`, `prev_event_depth`, `time_since_prev_event`
- **Rolling window features:** `rolling_10_event_avg_mag`, `rolling_10_event_max_mag`, `rolling_10_event_count`
- **Statistical features:** `mag_95th_percentile_region`
- **Temporal features:** `event_year`, `event_month`, `event_quarter`
- **Classification features:** `mag_category`, `depth_class`, `region`

---

## Configuration

Edit `config/config.yaml`:

```yaml
catalog: main
schema: default
tables:
  bronze: main.default.bronze_earthquake_events
  silver: main.default.silver_earthquake_events
  gold_daily: main.default.gold_daily_stats
  # ...
```

---

## Running on Databricks Community Edition

### Prerequisites
1. Databricks Community Edition account (free)
2. Serverless Starter Warehouse (auto-provisioned)
3. Git repo imported as Git Folder in workspace

### Steps
1. Import repo: Workspace → Create → Git Folder → `https://github.com/r1anpratama/azure-databricks-earthquake-etl.git`
2. Open `notebooks/01_bronze_ingestion.py`
3. Attach compute → Serverless Starter Warehouse
4. Run All (Ctrl+Shift+A)
5. Repeat for `02_silver_transform.py` and `03_gold_analytics.py`

### Supported Platforms
- ✅ Databricks Community Edition (Serverless SQL Warehouse)
- ✅ Databricks Premium/Enterprise (All-Purpose Compute or SQL Warehouse)
- ✅ Azure Databricks
- ✅ AWS Databricks

---

## Project Structure

```
azure-databricks-earthquake-etl/
├── notebooks/
│   ├── 01_bronze_ingestion.py    # Bronze: USGS CSV → Delta (UC)
│   ├── 02_silver_transform.py    # Silver: Dedup, enrich, partitioned Delta
│   └── 03_gold_analytics.py      # Gold: 5 analytics tables + ML features
├── config/
│   └── config.yaml               # Pipeline configuration
├── .github/
│   └── workflows/
│       └── ci.yml                # CI: lint, test, YAML validation
├── pyproject.toml                # Python project config (ruff, mypy)
├── requirements.txt              # Python dependencies
├── Makefile                      # Common commands
├── Dockerfile                    # Containerization
└── README.md                     # This file
```

---

## CI/CD

GitHub Actions pipeline:
1. **Lint** — ruff (import sorting, unused imports, style)
2. **YAML validation** — config syntax check
3. **Integration test** — Spark pipeline smoke test with sample data

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Compute | Databricks Serverless SQL Warehouse |
| Storage | Delta Lake (Unity Catalog managed tables) |
| Processing | Apache Spark (PySpark) |
| Source | USGS FDSNWS API (CSV) |
| Language | Python 3.11 |
| CI/CD | GitHub Actions |
| Linting | ruff, mypy |

---

## License

MIT