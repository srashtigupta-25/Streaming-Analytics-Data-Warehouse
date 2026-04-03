# ============================================================================
# Program: loadAnalyticsDB.PractII.GuptaS-MuruganandamMuraliKrishnanS.R
# Authors: Srashti Gupta, Sai Venkatesh
# Semester: Fall 2025
# Purpose: ETL pipeline to load and transform data into analytics datamart
# Date: December 2, 2025
# ============================================================================

rm(list = ls())
options(warn = -1)

# ============================================================================
# SECTION 1: Load Required Libraries
# ============================================================================

cat("Loading required libraries...\n")

suppressPackageStartupMessages({
  library(DBI)
  library(RMySQL)
  library(RSQLite)
})

cat("Libraries loaded successfully.\n\n")

# ============================================================================
# SECTION 2: Connect to Databases
# ============================================================================

cat("Connecting to databases...\n")

# Connect to local SQLite
sqlite_con <- dbConnect(
  RSQLite::SQLite(),
  "data/subscribersDB.sqlitedb"
)
cat("Connected to SQLite database.\n")

# Connect to Cloud MySQL
mysql_con <- dbConnect(
  RMySQL::MySQL(),
  host     = MYSQL_HOST,
  port     = MYSQL_PORT,
  user     = MYSQL_USER,
  password = MYSQL_PASSWORD,
  dbname   = MYSQL_DBNAME
)

# Setting UTF-8 encoding - CRITICAL for special characters
dbExecute(mysql_con, "SET NAMES 'utf8mb4'")
dbExecute(mysql_con, "SET CHARACTER SET utf8mb4")

cat("Connected to MySQL database.\n\n")

# ============================================================================
# SECTION 3: Extract Lookup Data from SQLite
# ============================================================================

cat("Extracting lookup data from SQLite...\n")

asset_lookup_query <- "
  SELECT 
    a.asset_id,
    a.sport,
    c.country
  FROM assets a
  LEFT JOIN countries c ON a.country_id = c.country_id
"

asset_map <- dbGetQuery(sqlite_con, asset_lookup_query)
cat("Extracted asset → country/sport mapping:", nrow(asset_map), "records\n\n")

# ============================================================================
# SECTION 4: Clear Staging and Fact Tables
# ============================================================================

cat("Clearing existing data from tables...\n")

dbExecute(mysql_con, "DELETE FROM staging_transactions")
dbExecute(mysql_con, "DELETE FROM fact_streaming_summary")

cat("Tables cleared.\n\n")

# ============================================================================
# SECTION 5: Load CSV Data in Chunks (BATCH INSERT)
# ============================================================================

cat("Loading CSV data in chunks...\n")

csv_file <- "data/new-streaming-transactions-98732.csv"
chunk_size <- 5000 
total_rows <- 98732
num_chunks <- ceiling(total_rows / chunk_size)

total_loaded <- 0

# Process CSV file in chunks
for(chunk_num in 1:num_chunks) {
  
  skip_rows <- (chunk_num - 1) * chunk_size
  rows_to_read <- min(chunk_size, total_rows - skip_rows)
  
  # Read chunk from CSV
  if(chunk_num == 1) {
    chunk <- read.csv(
      csv_file,
      nrows = rows_to_read,
      stringsAsFactors = FALSE,
      fileEncoding = "UTF-8"
    )
  } else {
    chunk <- read.csv(
      csv_file,
      skip = skip_rows,
      nrows = rows_to_read,
      header = FALSE,
      stringsAsFactors = FALSE,
      fileEncoding = "UTF-8"
    )
    colnames(chunk) <- c("transaction_id", "subscriber_id", "user_id", "asset_id",
                         "streaming_date", "streaming_start_time", "minutes_streamed",
                         "device_type", "quality_streamed", "completed")
  }
  
  total_loaded <- total_loaded + nrow(chunk)
  
  # ============================================================================
  # SECTION 6: Enrich Chunk with Country and Sport
  # ============================================================================
  
  # Match using Asset ID
  match_indices <- match(chunk$asset_id, asset_map$asset_id)
  
  chunk$country <- asset_map$country[match_indices]
  chunk$sport <- asset_map$sport[match_indices]

  chunk$country[is.na(chunk$country)] <- "Unknown"
  chunk$sport[is.na(chunk$sport)] <- "Unknown"
  
  # ============================================================================
  # SECTION 7: Parse Dates and Extract Time Dimensions
  # ============================================================================
  
  chunk$streaming_date <- as.Date(chunk$streaming_date)
  chunk$year <- as.integer(format(chunk$streaming_date, "%Y"))
  chunk$quarter <- as.integer((as.integer(format(chunk$streaming_date, "%m")) - 1) %/% 3 + 1)
  chunk$month <- as.integer(format(chunk$streaming_date, "%m"))
  chunk$week <- as.integer(format(chunk$streaming_date, "%U"))
  chunk$day_of_week <- as.integer(as.POSIXlt(chunk$streaming_date)$wday) + 1L
  
  # ============================================================================
  # SECTION 8: Convert Boolean and Build BATCH INSERT 
  # ============================================================================
  
  # Type Conversion
  chunk$completed <- as.integer(ifelse(chunk$completed == "TRUE" | chunk$completed == TRUE, 1, 0))
  chunk$minutes_streamed <- as.integer(chunk$minutes_streamed)

  if(nrow(chunk) > 0) {
    tryCatch({
      
      # Pre-format columns for SQL 
      v_txn      <- gsub("'", "''", as.character(chunk$transaction_id))
      v_sub      <- gsub("'", "''", as.character(chunk$subscriber_id))
      v_user     <- gsub("'", "''", as.character(chunk$user_id))
      v_asset    <- gsub("'", "''", as.character(chunk$asset_id))
      v_date     <- format(chunk$streaming_date, "%Y-%m-%d")
      v_time     <- gsub("'", "''", as.character(chunk$streaming_start_time))
      v_dev      <- gsub("'", "''", as.character(chunk$device_type))
      v_qual     <- gsub("'", "''", as.character(chunk$quality_streamed))
      v_country  <- gsub("'", "''", as.character(chunk$country))
      v_sport    <- gsub("'", "''", as.character(chunk$sport))
      
      # Build value tuples 
      values <- sprintf(
        "('%s','%s','%s','%s','%s','%s',%d,'%s','%s',%d,'%s','%s',%d,%d,%d,%d,%d)",
        v_txn, v_sub, v_user, v_asset, v_date, v_time, chunk$minutes_streamed,
        v_dev, v_qual, chunk$completed, v_country, v_sport, 
        chunk$year, chunk$quarter, chunk$month, chunk$week, chunk$day_of_week
      )
      
      # Construct final SQL
      sql <- paste0(
        "INSERT IGNORE INTO staging_transactions ",
        "(transaction_id, subscriber_id, user_id, asset_id, streaming_date, ",
        "streaming_start_time, minutes_streamed, device_type, quality_streamed, ",
        "completed, country, sport, year, quarter, month, week, day_of_week) ",
        "VALUES ",
        paste(values, collapse = ",")
      )
      
      # Execute batch insert
      dbExecute(mysql_con, sql)
      
      cat(sprintf("  Chunk %d/%d inserted (%d rows)\n", chunk_num, num_chunks, nrow(chunk)))
      
    }, error = function(e) {
      cat("\nERROR in chunk", chunk_num, ":", e$message, "\n")
    
    })
  }
}

cat("\nCSV loading complete.\n\n")

# ============================================================================
# SECTION 9: Validate Staging Data
# ============================================================================

cat("Validating staging data...\n")

staging_count <- dbGetQuery(mysql_con, "SELECT COUNT(*) as count FROM staging_transactions")
cat("Staging table row count:", staging_count$count, "\n")

unknown_country_count <- dbGetQuery(mysql_con, 
                                    "SELECT COUNT(*) as count FROM staging_transactions WHERE country = 'Unknown'")
cat("  Rows with Unknown country:", unknown_country_count$count, "\n")

date_range <- dbGetQuery(mysql_con,
                         "SELECT MIN(streaming_date) as min_date, MAX(streaming_date) as max_date FROM staging_transactions")
cat("  Date range:", date_range$min_date, "to", date_range$max_date, "\n\n")

# ============================================================================
# SECTION 10: Aggregate Data into Fact Table
# ============================================================================

cat("Aggregating data into fact table...\n")

aggregate_sql <- "
  INSERT INTO fact_streaming_summary 
    (streaming_date, year, quarter, month, week, day_of_week, 
     country, sport, transaction_count, total_minutes_streamed, avg_minutes_streamed)
  SELECT 
    streaming_date,
    year,
    quarter,
    month,
    week,
    day_of_week,
    country,
    sport,
    COUNT(*) as transaction_count,
    SUM(minutes_streamed) as total_minutes_streamed,
    AVG(minutes_streamed) as avg_minutes_streamed
  FROM staging_transactions
  GROUP BY streaming_date, year, quarter, month, week, day_of_week, country, sport
"

dbExecute(mysql_con, aggregate_sql)

cat("Data aggregated into fact table.\n\n")

# ============================================================================
# SECTION 11: Validate Fact Table
# ============================================================================

cat("Validating fact table...\n")

fact_count <- dbGetQuery(mysql_con, "SELECT COUNT(*) as count FROM fact_streaming_summary")
cat("Fact table row count:", fact_count$count, "\n")

total_transactions_fact <- dbGetQuery(mysql_con,
                                      "SELECT SUM(transaction_count) as total FROM fact_streaming_summary")
cat("  Total transactions in fact table:", total_transactions_fact$total, "\n")

# ============================================================================
# SECTION 12: Close Database Connections
# ============================================================================

dbDisconnect(sqlite_con)
dbDisconnect(mysql_con)

cat(rep("=", 70), "\n", sep = "")
cat("ETL PIPELINE COMPLETE\n")
cat(rep("=", 70), "\n\n", sep = "")
options(warn = 0)