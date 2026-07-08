# Databricks Medallion Architecture — Portfolio Evidence
**Project:** USGS Earthquake ETL Pipeline (Bronze → Silver → Gold)
**Platform:** Databricks Community Edition (Free) — Serverless SQL Warehouse
**Catalog:** `workspace.default.*` (Unity Catalog managed Delta tables)
**Date:** 2026-07-08
**GitHub:** https://github.com/r1anpratama/azure-databricks-earthquake-etl

---

## Architecture Overview

```
┌──────────────┬─────────────────┬─────────────────┬────────────────────────┐
│   EXTERNAL   │     BRONZE      │     SILVER      │        GOLD            │
│   SOURCE     │   (Raw Layer)   │  (Clean Layer)  │    (Analytics + ML)    │
├──────────────┼─────────────────┼─────────────────┼────────────────────────┤
│              │                 │                 │                        │
│ USGS FDSNWS  │ Schema enforce  │ Dedup (window)  │ 5 Gold tables:        │
│ Earthquake   │ 22-field schema │ Null filter     │  • daily_stats        │
│ API (CSV)    │ DQ framework    │ Geo enrich      │  • monthly_stats      │
│ 1,875 rows   │ Audit columns   │ Mag classify    │  • regional_stats     │
│              │ Batch ID (MD5)  │ Depth classify  │  • mag_distribution   │
│              │ Delta (UC)      │ Temporal enrich │  • ml_features        │
│              │                 │ Partitioned     │  Lag + rolling features│
└──────────────┴─────────────────┴─────────────────┴────────────────────────┘
```

---

## Layer 1 — Bronze (Raw Ingestion)

**Notebook:** `notebooks/01_bronze_ingestion.py`

| Metric | Value |
|--------|-------|
| Source | USGS FDSNWS API (`format=csv&minmagnitude=2.5`) |
| Target Table | `workspace.default.bronze_earthquake_events` |
| **Rows Ingested** | **1,875** |
| **Columns** | **25** (22 source + 3 audit) |
| Schema | Explicit 22-field StructType (no inference) |
| Data Quality | 8 rules (5 FAIL, 3 WARN) |
| Audit Columns | `_ingested_at`, `_source`, `_batch_id` (MD5) |
| Storage | Delta Lake managed table (Unity Catalog) |

### Sample Data (first 15 rows)

| time | latitude | longitude | depth | mag | magType | region |
|------|----------|-----------|-------|-----|---------|--------|
| 2026-07-08T13:01:10.819Z | -6.4643 | 68.7025 | 10 | 5.4 | mww | Other |
| 2026-07-08T12:30:44.744Z | 55.851 | -158.227 | 45.5 | 2.5 | ml | Alaska |
| 2026-07-08T12:18:59.673Z | -23.5549 | -174.9716 | 10 | 5.4 | mb | South America |
| 2026-07-08T11:42:51.921Z | -20.3518 | 168.6887 | 10 | 5.2 | mwr | Other |
| 2026-07-08T11:23:04.023Z | -6.1834 | 150.91 | 25.5 | 5.0 | mb | Other |

### Data Quality Report — Bronze

| Rule ID | Check | Column | Severity | Failures | Status |
|---------|-------|--------|----------|----------|--------|
| DQ001 | no_null_id | id | FAIL | 0 | ✅ |
| DQ002 | no_null_time | time | FAIL | 0 | ✅ |
| DQ003 | valid_latitude | latitude | FAIL | 0 | ✅ |
| DQ004 | valid_longitude | longitude | FAIL | 0 | ✅ |
| DQ005 | valid_magnitude | mag | FAIL | 0 | ✅ |
| DQ006 | no_null_magnitude | mag | WARN | 0 | ✅ |
| DQ007 | no_null_depth | depth | WARN | 0 | ✅ |
| DQ008 | no_null_place | place | WARN | 0 | ✅ |

**Result:** 8/8 checks passed — 0 FAIL, 0 WARN

---

## Layer 2 — Silver (Cleansing & Enrichment)

**Notebook:** `notebooks/02_silver_transform.py`

| Metric | Value |
|--------|-------|
| Input | `workspace.default.bronze_earthquake_events` (1,875) |
| Target Table | `workspace.default.silver_earthquake_events` |
| **Rows Output** | **1,875** |
| Rows Dropped | 0 |
| Dedup Removed | 0 (no duplicates in source) |
| Null-Filter Removed | 0 |
| Partitioning | `event_year`, `event_month` |

### Enrichments Applied

| Feature | Method | Values |
|---------|--------|--------|
| **Region** | Bounding box (7 tectonic zones) | Sunda Arc, Japan, California, Alaska, New Zealand, South America, Other |
| **Magnitude Category** | Range-based | micro, minor, light, moderate, strong, major, great |
| **Depth Class** | Range-based | shallow (0-70), intermediate (71-300), deep (>300) |
| **Temporal** | `year()`, `month()`, `quarter()` | event_year, event_month, event_quarter |

### Region Distribution

| Region | Count | % of Total |
|--------|-------|------------|
| Other | 1,396 | 74.5% |
| Alaska | 201 | 10.7% |
| California | 99 | 5.3% |
| Japan | 83 | 4.4% |
| South America | 68 | 3.6% |
| **Sunda Arc** | **21** | **1.1%** |
| New Zealand | 7 | 0.4% |

### Data Quality Report — Silver

| Check | Description | Status |
|-------|-------------|--------|
| no_duplicates | No duplicate event IDs after window dedup | ✅ |
| no_null_critical | No nulls in time, lat, lon, mag | ✅ |
| valid_lat | All latitudes in [-90, 90] | ✅ |
| valid_lon | All longitudes in [-180, 180] | ✅ |
| valid_mag | All magnitudes in [0, 10] | ✅ |
| valid_depth | All depths ≥ 0 | ✅ |
| region_classified | All events assigned region ≠ "Unknown" | ✅ |

**Result:** 7/7 checks passed

---

## Layer 3 — Gold (Analytics + ML Features)

**Notebook:** `notebooks/03_gold_analytics.py`

### Gold Tables Created

| Table | Grain | Rows | Key Features |
|-------|-------|------|--------------|
| `gold_daily_stats` | Daily | ~30 | Rolling 7d/30d avg mag, event count, significant/major events |
| `gold_monthly_stats` | Monthly | ~12 | MoM delta, depth breakdown, std magnitude |
| `gold_regional_stats` | Region × Month | ~84 | Geographic risk comparison per region/month |
| `gold_mag_distribution` | Mag Category | 7 | Statistical summary (min, max, avg, std) per class |
| `gold_ml_features` | Event-level | 1,875 | **Lag features + rolling windows + percentiles** |

### Regional Risk Ranking (Gold Output)

| Region | Total Events | Max Mag | Avg Mag | Significant (≥5.0) |
|--------|--------------|---------|---------|---------------------|
| Other | 1,396 | 7.5 | 3.94 | 139 |
| Alaska | 201 | 5.3 | 2.88 | 1 |
| California | 99 | 5.59 | 2.98 | 2 |
| Japan | 83 | 6.9 | 4.58 | 11 |
| South America | 68 | 5.5 | 4.46 | 9 |
| **Sunda Arc** | **21** | **5.3** | **4.58** | **5** |
| New Zealand | 7 | 5.0 | 4.56 | 1 |

### ML Feature Engineering (`gold_ml_features`)

| Feature | Description |
|---------|-------------|
| `prev_event_mag` | Magnitude of previous event in same region |
| `prev_event_depth` | Depth of previous event in same region |
| `time_since_prev_event` | Days since last event in region |
| `rolling_10_event_avg_mag` | 10-event rolling average magnitude |
| `rolling_10_event_max_mag` | 10-event rolling max magnitude |
| `rolling_10_event_count` | Events in last 10-window |
| `mag_95th_percentile_region` | 95th percentile magnitude per region |

---

## Technical Stack

| Component | Technology |
|-----------|------------|
| **Compute** | Databricks Serverless SQL Warehouse (Free tier) |
| **Storage** | Delta Lake — Unity Catalog managed tables |
| **Processing** | Apache Spark 3.5 (PySpark) |
| **Source** | USGS FDSNWS Earthquake API (CSV) |
| **Language** | Python 3.11 |
| **Orchestration** | Notebook-based (01 → 02 → 03) |
| **CI/CD** | GitHub Actions (ruff lint + PySpark integration test) |
| **Version Control** | Git → Databricks Git Folder sync |

---

## Key Achievements

- ✅ **Production-grade Medallion Architecture** on Databricks Community Edition (Free)
- ✅ **Unity Catalog managed Delta tables** — ACID, time travel, schema enforcement
- ✅ **Explicit schema enforcement** (22 fields) — no silent schema drift
- ✅ **Data Quality Framework** — 15 total checks (8 Bronze + 7 Silver) with FAIL/WARN severity
- ✅ **Geographic enrichment** — 7 tectonic zones with bounding-box classification
- ✅ **ML-ready Gold layer** — lagged features, rolling windows, percentile features
- ✅ **Zero DBFS, zero Hive Metastore** — fully UC-native, works on Free tier
- ✅ **CI/CD pipeline** — lint + integration smoke test on every push

---

## Screenshots for Portfolio

| Layer | Screenshot Reference | Key Visuals |
|-------|---------------------|-------------|
| **Bronze** | `img_4d42501ed562.jpg` | 1,875 rows × 25 cols, table `workspace.default.bronze_earthquake_events`, sample data |
| **Silver** | `img_20809357c543.jpg` | Summary (1875 in/out), region breakdown table, 0 rows dropped |
| **Gold** | `img_40e10d12df46.jpg` | Regional Risk Ranking table, 7 regions with total_events/max_mag/avg_mag/significant_events |

---

## Repository

**GitHub:** https://github.com/r1anpratama/azure-databricks-earthquake-etl

```
azure-databricks-earthquake-etl/
├── notebooks/
│   ├── 01_bronze_ingestion.py    # Bronze: schema enforcement + DQ + audit
│   ├── 02_silver_transform.py    # Silver: dedup + enrich + partitioned Delta
│   └── 03_gold_analytics.py      # Gold: 5 tables + ML features
├── config/
│   └── config.yaml               # All table paths (workspace.default.*)
├── .github/workflows/ci.yml      # Lint + integration test
├── pyproject.toml                # ruff config
└── README.md                     # Full architecture documentation
```

---

*Generated: 2026-07-08 | Portfolio evidence for Data Engineer applications*