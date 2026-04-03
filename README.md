# Streaming Analytics Data Warehouse

**Authors:** Srashti Gupta, Sai Venkatesh Varun Muruganandam Murali Krishnan  
**Course:** Practicum II — Fall 2025

## Setup Instructions

### 1. Configure credentials
Copy the template and fill in your MySQL credentials:
```
cp credentials_template.R credentials.R
# Edit credentials.R with your actual values
```

### 2. Rebuild the local SQLite database
```
Rscript setup_db.R
```

### 3. Run the pipeline in order
```
Rscript createStarSchema_PractII_GuptaS-MuruganandamMuraliKrishnanS.R
Rscript loadAnalyticsDB_PractII_GuptaS-MuruganandamMuraliKrishnanS.R
```

### 4. Open the analysis
Open `BusinessAnalysis_PractII_GuptaS-MuruganandamMuraliKrishnanS.Rmd` in RStudio and knit.

## Project Structure
- `createStarSchema.R` — creates MySQL star schema tables
- `loadAnalyticsDB.R` — ETL: SQLite + CSV → MySQL
- `BusinessAnalysis.Rmd` — analysis and visualizations
- `setup_db.R` — rebuilds local SQLite from source CSVs
- `data/assets.csv` — asset lookup data
- `data/countries.csv` — country lookup data
- `data/new-streaming-transactions-98732.csv` — streaming transactions