# ============================================================================
# Program: createStarSchema.PractII.GuptaS-MuruganandamMuraliKrishnanS.R
# Authors: Srashti Gupta, Sai Venkatesh Varun Muruganandam Murali Krishnan
# Semester: Fall 2025
# Purpose: Create star schema for SportsTV streaming analytics datamart
# Date: December 2, 2025
# ============================================================================

# Clear environment
rm(list = ls())

# ============================================================================
# Load Required Libraries
# ============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(RMySQL)
})

# ============================================================================
# Connect to MySQL Cloud Database
# ============================================================================

# Load credentials (gitignored file)
source("credentials.R")

# MySQL connection parameters
mysql_con <- dbConnect(
  RMySQL::MySQL(),
  host     = MYSQL_HOST,
  port     = MYSQL_PORT,
  user     = MYSQL_USER,
  password = MYSQL_PASSWORD,
  dbname   = MYSQL_DBNAME
)

cat("Connected to MySQL database.\n\n")

# ============================================================================
# Drop Existing Tables
# ============================================================================

cat("Dropping existing tables (if they exist)...\n")

dbExecute(mysql_con, "DROP TABLE IF EXISTS fact_streaming_summary")
dbExecute(mysql_con, "DROP TABLE IF EXISTS staging_transactions")

cat("Existing tables dropped.\n\n")

# ============================================================================
# Create Staging Table
# ============================================================================

cat("Creating staging_transactions table...\n")

staging_sql <- "
CREATE TABLE staging_transactions (
  transaction_id VARCHAR(50) PRIMARY KEY,
  subscriber_id VARCHAR(50) NOT NULL,
  user_id VARCHAR(50),
  asset_id VARCHAR(50) NOT NULL,
  streaming_date DATE NOT NULL,
  streaming_start_time TIME,
  minutes_streamed INT NOT NULL,
  device_type VARCHAR(50),
  quality_streamed VARCHAR(20),
  completed BOOLEAN,
  country VARCHAR(50),
  sport VARCHAR(50),
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  week INT NOT NULL,
  day_of_week INT NOT NULL,
  
  INDEX idx_date (streaming_date),
  INDEX idx_country (country),
  INDEX idx_sport (sport),
  INDEX idx_subscriber (subscriber_id),
  INDEX idx_asset (asset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"

dbExecute(mysql_con, staging_sql)
cat("staging_transactions table created.\n\n")

# ============================================================================
# Create Fact Table (Partitioned)
# ============================================================================

cat("Creating fact_streaming_summary table with partitions...\n")

fact_sql <- "
CREATE TABLE fact_streaming_summary (
  fact_id INT AUTO_INCREMENT,
  streaming_date DATE NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  week INT NOT NULL,
  day_of_week INT NOT NULL,
  country VARCHAR(50) NOT NULL,
  sport VARCHAR(50) NOT NULL,
  transaction_count INT NOT NULL DEFAULT 0,
  total_minutes_streamed BIGINT NOT NULL DEFAULT 0,
  avg_minutes_streamed DECIMAL(10,2),
  
  -- Primary Key must include the partition key (year)
  PRIMARY KEY (fact_id, year),
  
  -- Unique Keys must also include the partition key
  UNIQUE KEY unique_fact (streaming_date, country, sport, year),
  
  INDEX idx_date (streaming_date),
  INDEX idx_year_month (year, month),
  INDEX idx_year_quarter (year, quarter),
  INDEX idx_country (country),
  INDEX idx_sport (sport),
  INDEX idx_country_sport (country, sport),
  INDEX idx_date_country (streaming_date, country),
  INDEX idx_date_sport (streaming_date, sport)
) 
PARTITION BY RANGE (year) (
    PARTITION p_old VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
)
"

dbExecute(mysql_con, fact_sql)
cat("fact_streaming_summary table created.\n\n")

# ============================================================================
# Verify Tables Created
# ============================================================================

cat("Verifying table creation...\n")

# List all tables
all_tables <- dbListTables(mysql_con)

if("staging_transactions" %in% all_tables && "fact_streaming_summary" %in% all_tables) {
  cat("Both tables created successfully.\n\n")
} else {
  cat("Error: Tables not found.\n\n")
}

# Show table structures
cat("Table structures:\n\n")

cat("staging_transactions:\n")
staging_structure <- suppressWarnings(dbGetQuery(mysql_con, "DESCRIBE staging_transactions"))
print(staging_structure[, c("Field", "Type", "Null", "Key")])
cat("\n")

cat("fact_streaming_summary:\n")
fact_structure <- suppressWarnings(dbGetQuery(mysql_con, "DESCRIBE fact_streaming_summary"))
print(fact_structure[, c("Field", "Type", "Null", "Key")])
cat("\n")

# ============================================================================
# Close Connection
# ============================================================================

dbDisconnect(mysql_con)
cat("Star schema creation complete.\n")