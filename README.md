# Azure Databricks ETL Pipeline — Medallion Architecture

**ETL Pipeline:** USGS Earthquake CSV → Bronze (Raw) → Silver (Clean) → Gold (Analytics)

[![CI](https://github.com/r1anpratama/azure-databricks-earthquake-etl/actions/workflows/ci.yml/badge.svg)](https://github.com/r1anpratama/azure-databricks-earthquake-etl/actions/workflows/ci.yml)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://community.cloud.databricks.com)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-0078D4?style=flat&logo=delta&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MEDALLION ARCHITECTURE                    │
├──────────────┬─────────────────┬─────────────────┬──────────────┤
│   EXTERNAL   │     BRONZE      │     SILVER      │     GOLD     │
│              │   (Raw Delta)   │  (Clean Delta)  │  (Aggregated) │
│  ┌────────┐  │  ┌───────────┐  │  ┌───────────┐  │  ┌────────┐  │
│  │  USGS  │──┼─▶│ Events    │──┼─▶│ Events    │──┼─▶│ Daily  │  │
│  │ API    │  │  │  (Raw)    │  │  │  (Clean)  │  │  │ Stats  │  │
│  │ (CSV)  │  │  │           │  │  │           │  │  └────────┘  │
│  └────────┘  │  │·InferSchema│  │  │·Validate  │  │  ┌────────┐  │
│              │  │·Timestamp  │  │  │·Dedup     │  │  │Monthly │  │
│              │  │·Source meta│  │  │·Partition │  │  │ Stats  │  │
│              │  └───────────┘  │  │·Enrich    │  │  └────────┘  │
│              │                 │  └───────────┘  │  ┌────────┐  │
│              │                 │                 │  │Mag Dist│  │
│              │                 │                 │  └────────┘  │
└──────────────┴─────────────────┴─────────────────┴──────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │     QUALITY FRAMEWORK         │
                  │  ┌──────┐  ┌────────┐  ┌────┐ │
                  │  │Not   │  │ Range  │  │Fresh│ │
                  │  │Null  │  │ Check  │  │ness │ │
                  │  └──────┘  └────────┘  └────┘ │
                  └───────────────────────────────┘
```

## Pipeline Design

| Layer | Format | Purpose | Operations |
|-------|--------|---------|------------|
| **Bronze** | Delta (Raw) | Immutable raw ingestion | Schema inference, source metadata, no transform |
| **Silver** | Delta (Partitioned) | Clean, validated events | Dedup, type casting, magnitude classification, region extraction, quality filters |
| **Gold** | Delta (Aggregated) | Analytics-ready tables | Daily stats, monthly rollups, magnitude distribution, region-level metrics |

## Key Features

### ✅ Medallion Architecture
Industry-standard data lakehouse pattern. Bronze preserves raw data, Silver cleans, Gold serves analytics.

### ✅ Delta Lake
ACID transactions, schema enforcement, time travel, and scalable metadata handling.

### ✅ Data Quality Framework
Custom `DataQuality` class runs checks at every layer:
- **Not-null**: Detects missing critical columns
- **Range checks**: Validates lat/lon/mag values
- **Freshness**: Alerts on stale data (>1 year)
- **Schema conformity**: Catches column drift
- **Configurable severity**: FAIL vs WARN

### ✅ Parameterized by Config
All paths, thresholds, and rules live in `config/config.yaml`. No hardcoded values.

### ✅ CI/CD Pipeline (GitHub Actions)
| Job | Tools |
|-----|-------|
| **Lint** | Ruff, mypy, YAML validation |
| **Test** | PySpark unit tests (pytest) |
| **Integration** | Full Bronze→Silver→Gold simulation |

### ✅ Docker Support
Reproducible environment with multi-stage Docker build.

### ✅ Makefile
Common commands: `make lint`, `make test`, `make validate`, `make docker-build`.

## Repository Structure

```
azure-databricks-earthquake-etl/
├── .github/workflows/ci.yml    # GitHub Actions CI
├── config/config.yaml           # Pipeline configuration
├── src/
│   ├── bronze.py                # Raw ingestion module
│   ├── silver.py                # Cleaning & enrichment module
│   ├── gold.py                  # Aggregation module
│   ├── quality.py               # Data quality framework
│   └── utils.py                 # Shared utilities
├── notebooks/
│   ├── bronze_ingestion.py      # Databricks notebook (Bronze)
│   ├── silver_cleaning.py       # Databricks notebook (Silver)
│   └── gold_analytics.py        # Databricks notebook (Gold)
├── tests/
│   ├── test_bronze.py           # Bronze unit tests
│   ├── test_silver.py           # Silver unit tests
│   └── test_quality.py          # Quality framework tests
├── Dockerfile                   # Multi-stage Docker build
├── Makefile                     # Common commands
├── pyproject.toml               # Python tool config (ruff, mypy, pytest)
├── requirements.txt             # Python dependencies
├── environment.yml              # Conda environment
└── README.md                    # This file
```

## Quick Start

### Local Setup
```bash
# Clone
git clone https://github.com/r1anpratama/azure-databricks-earthquake-etl.git
cd azure-databricks-earthquake-etl

# Option A: pip
pip install -r requirements.txt

# Option B: conda
conda env create -f environment.yml
conda activate earthquake-etl

# Run tests
make test

# Full validation
make validate
```

### Databricks Workspace
1. Import `notebooks/` directory via Databricks UI → Workspace → Import
2. Attach to cluster (Runtime 10.4+ LTS, Delta enabled)
3. Run Bronze → Silver → Gold in order
4. Verify outputs at:
   - `DBFS:/mnt/earthquake_analytics/bronze/events`
   - `DBFS:/mnt/earthquake_analytics/silver/events`
   - `DBFS:/mnt/earthquake_analytics/gold/`

### Docker
```bash
make docker-build
make docker-test
```

## Skills Demonstrated

| Skill | Evidence |
|-------|----------|
| **ETL Pipeline Design** | Bronze→Silver→Gold medallion architecture |
| **Delta Lake** | ACID transactions, partitioned tables, time travel |
| **PySpark** | DataFrame API, window functions, aggregations |
| **Data Quality** | Rule-based framework with FAIL/WARN severity |
| **Config Management** | YAML-driven pipeline parameters |
| **CI/CD** | GitHub Actions — lint, test, integration |
| **Testing** | PySpark unit tests with pytest fixtures |
| **Containerization** | Multi-stage Docker build |
| **DevOps** | Makefile, pyproject.toml, pre-commit |
| **Data Warehousing** | Partition strategy, star-schema ready aggregates |

## Data Sources

- **USGS Earthquake Catalog**: [earthquake.usgs.gov/fdsnws/event/1/query](https://earthquake.usgs.gov/fdsnws/event/1/query)
  - Magnitude ≥ 2.5, sorted by time
  - Format: CSV with header
  - Schema: 22 columns (time, lat, lon, mag, depth, place, type, status, ...)

## Background

Built as a portfolio project for **Data Engineer** role applications (targeting EY). Demonstrates production-grade data engineering practices on the **Microsoft Azure Databricks** platform, aligned with the medallion architecture pattern used in modern data lakehouses.

## Author

**Rian Pratama** — Data Engineer @ BMKG | M.Sc. candidate @ National Central University, Taiwan

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/ri-anpratama)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/r1anpratama)
[![Google Scholar](https://img.shields.io/badge/Scholar-4285F4?style=flat&logo=googlescholar&logoColor=white)](https://scholar.google.com/citations?user=BYbHHKYAAAAJ)
