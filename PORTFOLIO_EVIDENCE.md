# Databricks Medallion Architecture — Portfolio Evidence
**Project:** USGS Earthquake ETL Pipeline (Bronze → Silver → Gold)  
**Platform:** Databricks Community Edition (Free) — Serverless SQL Warehouse  
**Catalog:** `workspace.default.*` (Unity Catalog managed Delta tables)  
**Date:** 2026-07-08  
**GitHub:** https://github.com/r1anpratama/azure-databricks-earthquake-etl

---

## Evidence Summary

| Layer | Notebook | Target Table | Rows | Key Features |
|-------|----------|--------------|------|--------------|
| 🥉 Bronze | `01_bronze_ingestion.py` | `workspace.default.bronze_earthquake_events` | **1,875** | 22-field explicit schema, 8-rule DQ framework (5 FAIL/3 WARN), batch audit columns (_ingested_at, _source, _batch_id MD5) |
| 🥈 Silver | `02_silver_transform.py` | `workspace.default.silver_earthquake_events` | **1,875** | Window dedup, 7 tectonic zone enrichment, mag/depth classification, temporal features, partitioned by year/month |
| 🥇 Gold | `03_gold_analytics.py` | 5 Gold tables | **~1,875** | Daily/monthly/regional/mag_dist + ML features with lagged/rolling window features |

---

## Bronze Layer Evidence

**Table:** `workspace.default.bronze_earthquake_events`  
**Rows:** 1,875 | **Columns:** 25 (22 source + 3 audit)  
**Quality Checks:** 8/8 passed (0 FAIL, 0 WARN)

### Sample Data
| time | latitude | longitude | depth | mag | magType | _batch_id |
|------|----------|-----------|-------|-----|---------|-----------|
| 2026-07-08T13:01:10.819Z | -6.4643 | 68.7025 | 10 | 5.4 | mww | af7e1ba02257 |
| 2026-07-08T12:30:44.744Z | 55.851 | -158.227 | 45.5 | 2.5 | ml | af7e1ba02257 |
| 2026-07-08T12:18:59.673Z | -23.5549 | -174.9716 | 10 | 5.4 | mb | af7e1ba02257 |

### Data Quality Report
| Rule | Check | Column | Severity | Failures | Status |
|------|-------|--------|----------|----------|--------|
| DQ001 | no_null_id | id | FAIL | 0 | ✅ |
| DQ002 | no_null_time | time | FAIL | 0 | ✅ |
| DQ003 | valid_latitude | latitude | FAIL | 0 | ✅ |
| DQ004 | valid_longitude | longitude | FAIL | 0 | ✅ |
| DQ005 | valid_magnitude | mag | FAIL | 0 | ✅ |
| DQ006 | no_null_magnitude | mag | WARN | 0 | ✅ |
| DQ007 | no_null_depth | depth | WARN | 0 | ✅ |
| DQ008 | no_null_place | place | WARN | 0 | ✅ |

---

## Silver Layer Evidence

**Table:** `workspace.default.silver_earthquake_events`  
**Rows:** 1,875 (0 dropped)  
**Partitioning:** `event_year`, `event_month`

### Region Distribution (7 tectonic zones)
| Region | Count | % |
|--------|-------|---|
| Other | 1,396 | 74.5% |
| Alaska | 201 | 10.7% |
| California | 99 | 5.3% |
| Japan | 83 | 4.4% |
| South America | 68 | 3.6% |
| **Sunda Arc** | **21** | **1.1%** |
| New Zealand | 7 | 0.4% |

### Enrichments
- **Magnitude Category:** micro, minor, light, moderate, strong, major, great
- **Depth Class:** shallow (0-70), intermediate (71-300), deep (>300)
- **Temporal:** event_year, event_month, event_quarter

### Quality Checks: 7/7 passed ✅

---

## Gold Layer Evidence

### 5 Gold Tables Created

| Table | Grain | Features |
|-------|-------|----------|
| `gold_daily_stats` | Daily | Rolling 7d/30d avg magnitude, event count, significant/major events |
| `gold_monthly_stats` | Monthly | MoM delta, depth breakdown, std magnitude |
| `gold_regional_stats` | Region × Month | Geographic risk comparison |
| `gold_mag_distribution` | Mag Category | Statistical summary per class |
| `gold_ml_features` | Event-level | **Lag features + rolling windows + percentiles** |

### Regional Risk Ranking (Key Analytics Output)

| Region | Total Events | Max Mag | Avg Mag | Significant (≥5.0) |
|--------|--------------|---------|---------|---------------------|
| Other | 1,396 | 7.5 | 3.94 | 139 |
| Alaska | 201 | 5.3 | 2.88 | 1 |
| California | 99 | 5.59 | 2.98 | 2 |
| Japan | 83 | 6.9 | 4.58 | 11 |
| South America | 68 | 5.5 | 4.46 | 9 |
| **Sunda Arc** | **21** | **5.3** | **4.58** | **5** |
| New Zealand | 7 | 5.0 | 4.56 | 1 |

---

## Technical Stack

| Component | Technology |
|-----------|------------|
| Compute | Databricks Serverless SQL Warehouse |
| Storage | Delta Lake (Unity Catalog managed tables) |
| Processing | PySpark (DataFrame API) |
| Schema | Explicit StructType (no inference) |
| Quality | Custom rule-based framework (FAIL/WARN/INFO) |
| CI/CD | GitHub Actions (ruff lint + integration smoke test) |
| Version Control | Git (GitHub) |

---

## Repository Structure

```
azure-databricks-earthquake-etl/
├── notebooks/
│   ├── 01_bronze_ingestion.py    # Bronze: USGS CSV → Delta + DQ
│   ├── 02_silver_transform.py    # Silver: Dedup + enrich + partition
│   └── 03_gold_analytics.py      # Gold: 5 tables + ML features
├── config/
│   └── config.yaml               # Pipeline configuration
├── .github/workflows/
│   └── ci.yml                    # CI: lint + validation + smoke test
├── pyproject.toml                # Ruff + mypy config
└── README.md                     # Full architecture documentation
```

---

## How to Run

1. Import repo as Git Folder in Databricks Workspace
2. Open `notebooks/01_bronze_ingestion.py` → Attach Serverless Starter Warehouse → Run All
3. Open `notebooks/02_silver_transform.py` → Run All
4. Open `notebooks/03_gold_analytics.py` → Run All

---

## Contact

**Rian Pratama**  
Data Scientist, BMKG (2020–present) | M.Sc. EEW STGNN, NCU Taiwan  
GitHub: r1anpratama | LinkedIn: ri-anpratama  
Email: rian8pratama@gmail.com