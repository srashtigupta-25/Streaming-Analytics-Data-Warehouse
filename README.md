# Streaming Analytics Data Warehouse

**Authors:** Srashti Gupta, Sai Venkatesh Varun Muruganandam Murali Krishnan  
**Course:** Practicum II — Fall 2025

## Project Overview

End-to-end streaming analytics pipeline for SportsTV. Ingests raw transaction data from a local SQLite database and a CSV file, transforms it into a star schema in a cloud MySQL database, and produces a full business analysis report.

**Architecture:**  
`SQLite (lookup data)` + `CSV (transactions)` → `ETL (R)` → `MySQL Star Schema (cloud)` → `R Markdown Report`

---

## Prerequisites

- R 4.x with packages: `DBI`, `RMySQL`, `RSQLite`, `knitr`
- Access to a MySQL database (Aiven or other)
- The following files must exist before running (included in repo):
  - `data/assets.csv`
  - `data/countries.csv`
  - `data/new-streaming-transactions-98732.csv`

---

## Setup Instructions

### Step 1 — Configure credentials

Copy the template and fill in your MySQL connection details:

```bash
cp credentials_template.R credentials.R
```

Edit `credentials.R` with your actual values:

```r
MYSQL_HOST     <- "your-host-here"
MYSQL_PORT     <- 00000
MYSQL_USER     <- "your-user-here"
MYSQL_PASSWORD <- "your-password-here"
MYSQL_DBNAME   <- "your-dbname-here"
```

> `credentials.R` is gitignored and will never be committed.

### Step 2 — Rebuild the local SQLite database

```bash
Rscript setup_db.R
```

This recreates `data/subscribersDB.sqlitedb` from `data/assets.csv` and `data/countries.csv`.

### Step 3 — Create the MySQL star schema

```bash
Rscript createStarSchema_PractII_GuptaS-MuruganandamMuraliKrishnanS.R
```

### Step 4 — Run the ETL pipeline

```bash
Rscript loadAnalyticsDB_PractII_GuptaS-MuruganandamMuraliKrishnanS.R
```

This loads ~98k transactions from CSV, enriches with country/sport from SQLite, and aggregates into the MySQL fact table.

### Step 5 — Open the analysis report

Open `BusinessAnalysis_PractII_GuptaS-MuruganandamMuraliKrishnanS.Rmd` in RStudio and click **Knit**.

---

## Project Structure

```
├── credentials_template.R         # Copy to credentials.R and fill in your values
├── setup_db.R                     # Rebuilds local SQLite from source CSVs
├── createStarSchema_PractII_...R  # Creates MySQL staging + fact tables
├── loadAnalyticsDB_PractII_...R   # ETL: SQLite + CSV → MySQL
├── BusinessAnalysis_PractII_...Rmd # Analysis and visualizations
├── BusinessAnalysis_PractII_...html # Pre-rendered report output
├── sandbox_exploration.R          # Exploratory/dev scratch file
├── export_sqlite_to_csv.R         # One-time utility: exports SQLite → CSVs
├── Practicum-II.Rproj             # RStudio project file
├── data/
│   ├── assets.csv                 # Asset lookup data (sport mapping)
│   ├── countries.csv              # Country lookup data
│   └── new-streaming-transactions-98732.csv  # Raw streaming transactions
└── .gitignore                     # Excludes credentials.R and .sqlitedb
```

---

## Data Model

**Staging table:** `staging_transactions` — raw enriched transaction records  
**Fact table:** `fact_streaming_summary` — aggregated by date × country × sport, partitioned by year

---

## Notes

- `data/subscribersDB.sqlitedb` is excluded from git (116MB). Run `setup_db.R` to recreate it locally.
- `credentials.R` is excluded from git. Never commit your database credentials.
- The HTML report is included as a pre-rendered reference output.