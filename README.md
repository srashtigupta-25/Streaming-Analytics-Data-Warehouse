# Streaming Analytics Data Warehouse

**Authors:** Srashti Gupta, Sai Venkatesh Varun Muruganandam Murali Krishnan  
**Course:** Practicum II — Fall 2025

---

## Overview

This project builds an end-to-end streaming analytics data warehouse for **SportsTV Germany**, a sports streaming platform operating across multiple European markets. Raw subscriber transaction data (~98,000 events) is extracted from a local SQLite operational database and a CSV feed, transformed through a chunked ETL pipeline in R, and loaded into a **cloud-hosted MySQL star schema** partitioned by year. A fully automated R Markdown report then queries the warehouse to surface business insights on viewing patterns by sport, country, and time period.

---

## What We Built

- **Designed a star schema** with a partitioned fact table (`fact_streaming_summary`) and a staging layer, optimized with 8 composite indexes for analytical query performance
- **Built a chunked ETL pipeline** in R that processes 98,000+ streaming transactions in batches of 5,000, enriching each record with sport and country dimensions via SQLite lookups before bulk-inserting into cloud MySQL
- **Automated a business analysis report** in R Markdown that queries the warehouse and generates visualizations on sport popularity, weekly viewing trends, country-level engagement, and day-of-week patterns
- **Engineered for reproducibility** — replaced a 116MB binary SQLite file with versioned source CSVs and a `setup_db.R` rebuild script, keeping the full repo under 5MB

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Storage (operational) | SQLite (local) |
| Data Warehouse | MySQL (Aiven cloud) |
| ETL & Analysis | R — `DBI`, `RMySQL`, `RSQLite` |
| Reporting | R Markdown → HTML |
| Schema Design | SQL — star schema with range partitioning |

---

## Star Schema

```
                        ┌─────────────────────────────────┐
                        │      fact_streaming_summary     │
                        │─────────────────────────────────│
                        │  fact_id         (PK)           │
                        │  streaming_date                 │
                        │  year  ──────── (partition key) │
                        │  quarter                        │
                        │  month                          │
                        │  week                           │
                        │  day_of_week                    │
  ┌──────────────┐      │  country    ◄───────────────┐   │
  │  Time dims   │      │  sport      ◄──────────┐    │   │
  │  (inline)    │      │  transaction_count      │   │   │
  │  year        │◄─────│  total_minutes_streamed  │   │  │
  │  quarter     │      │  avg_minutes_streamed    │   │  │
  │  month       │      └─────────────────────────────────┘
  │  week        │                                │    │
  │  day_of_week │         ┌──────────────────┐   │    │
  └──────────────┘         │  assets (SQLite) │───┘    │
                           │  asset_id        │        │
                           │  sport           │        │
                           │  country_id ─────┼───┐    │
                           └──────────────────┘   │    │
                                                   ▼   │
                           ┌──────────────────┐        │
                           │countries (SQLite)│────────┘
                           │  country_id      │
                           │  country         │
                           └──────────────────┘

  Staging layer: staging_transactions  (full granularity, pre-aggregation)
  Fact layer:    fact_streaming_summary (aggregated by date × country × sport)
```

---

## Prerequisites

- R 4.x with packages: `DBI`, `RMySQL`, `RSQLite`, `knitr`
- Access to a MySQL database (Aiven or compatible)

---

## Setup & Running

### 1 — Configure credentials

```bash
cp credentials_template.R credentials.R
# Edit credentials.R with your MySQL host, port, user, password, dbname
```

> `credentials.R` is gitignored and will never be committed.

### 2 — Rebuild the local SQLite database

```bash
Rscript setup_db.R
```

### 3 — Create the MySQL star schema

```bash
Rscript createStarSchema_PractII_GuptaS-MuruganandamMuraliKrishnanS.R
```

### 4 — Run the ETL pipeline

```bash
Rscript loadAnalyticsDB_PractII_GuptaS-MuruganandamMuraliKrishnanS.R
```

### 5 — Generate the analysis report

Open `BusinessAnalysis_PractII_GuptaS-MuruganandamMuraliKrishnanS.Rmd` in RStudio and click **Knit**.

---

## Project Structure

```
├── credentials_template.R          # Copy → credentials.R, fill in values
├── setup_db.R                      # Rebuilds SQLite from source CSVs
├── createStarSchema_PractII_...R   # Creates MySQL staging + fact tables
├── loadAnalyticsDB_PractII_...R    # ETL pipeline
├── BusinessAnalysis_PractII_...Rmd # Analysis and visualizations
├── BusinessAnalysis_PractII_...html# Pre-rendered report
├── export_sqlite_to_csv.R          # One-time utility: SQLite → CSVs
├── sandbox_exploration.R           # Exploratory scratch file
├── Practicum-II.Rproj              # RStudio project file
└── data/
    ├── assets.csv                  # Sport dimension source
    ├── countries.csv               # Country dimension source
    └── new-streaming-transactions-98732.csv
```

---

## Notes

- `data/subscribersDB.sqlitedb` is gitignored (116MB binary). Run `setup_db.R` to recreate it locally from the committed CSVs.
- `credentials.R` is gitignored. Never commit database credentials.