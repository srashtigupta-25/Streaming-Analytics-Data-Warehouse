# ============================================================================
# Program: sandbox_exploration.R
# Author: Gupta S
# Semester: Fall 2025
# Purpose: Part A - Explore SQLite database and test MySQL connection
#          This is a sandbox file for exploration and testing only
# ============================================================================

# Clear environment
rm(list = ls())

# ============================================================================
# SECTION 1: Load Required Libraries
# ============================================================================

cat("Loading required libraries...\n")

# Install packages if not already installed
required_packages <- c("DBI", "RSQLite", "RMySQL")

for(pkg in required_packages) {
  if(!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

cat("Libraries loaded successfully!\n\n")

# ============================================================================
# SECTION 2: Connect to SQLite Database
# ============================================================================

cat("=======================================================================\n")
cat("EXPLORING SQLite DATABASE: subscribersDB.sqlitedb\n")
cat("=======================================================================\n\n")

# Use relative path from project root
sqlite_path <- "data/subscribersDB.sqlitedb"

# Check if file exists
if(!file.exists(sqlite_path)) {
  cat("ERROR: Cannot find subscribersDB.sqlitedb file!\n")
  cat("Current working directory:", getwd(), "\n")
  cat("Looking for file at:", sqlite_path, "\n")
  stop("SQLite database file not found")
}

# Connect to SQLite
sqlite_con <- dbConnect(
  RMySQL::MySQL(),
  host     = "MYSQL_HOST",   # set in credentials.R
  port     = 22334,
  user     = "avnadmin",
  password = "MYSQL_PASSWORD", # set in credentials.R
  dbname   = "defaultdb"
)
cat("Successfully connected to SQLite database!\n\n")

# ============================================================================
# SECTION 3: Database Overview
# ============================================================================

cat("=======================================================================\n")
cat("DATABASE OVERVIEW\n")
cat("=======================================================================\n\n")

# Get all tables
tables <- dbListTables(sqlite_con)
cat("Number of tables found:", length(tables), "\n")
cat("Tables:", paste(tables, collapse = ", "), "\n\n")

# Get total row count across all tables
total_rows <- 0
for(table_name in tables) {
  row_count <- dbGetQuery(sqlite_con, paste0("SELECT COUNT(*) as count FROM ", table_name))
  total_rows <- total_rows + row_count$count
}
cat("Total rows across all tables:", format(total_rows, big.mark = ","), "\n\n")

# ============================================================================
# SECTION 4: Detailed Table Analysis
# ============================================================================

cat("=======================================================================\n")
cat("DETAILED TABLE STRUCTURES\n")
cat("=======================================================================\n\n")

for(table_name in tables) {
  cat("-----------------------------------------------------------------------\n")
  cat("TABLE:", toupper(table_name), "\n")
  cat("-----------------------------------------------------------------------\n")
  
  # Get table structure
  table_info <- dbGetQuery(sqlite_con, paste0("PRAGMA table_info(", table_name, ")"))
  
  # Get row count
  row_count <- dbGetQuery(sqlite_con, paste0("SELECT COUNT(*) as count FROM ", table_name))
  cat("Row count:", format(row_count$count, big.mark = ","), "\n\n")
  
  # Print columns
  cat("Columns:\n")
  for(i in 1:nrow(table_info)) {
    col_info <- table_info[i,]
    pk_marker <- ifelse(col_info$pk == 1, " (PRIMARY KEY)", "")
    cat(sprintf("  - %-25s %s%s\n", col_info$name, col_info$type, pk_marker))
  }
  cat("\n")
  
  # Get sample data (first 3 rows)
  cat("Sample data (first 3 rows):\n")
  sample_data <- dbGetQuery(sqlite_con, paste0("SELECT * FROM ", table_name, " LIMIT 3"))
  print(sample_data)
  cat("\n")
}

# ============================================================================
# SECTION 5: Critical Columns for CSV Integration
# ============================================================================

cat("=======================================================================\n")
cat("KEY COLUMNS FOR CSV INTEGRATION\n")
cat("=======================================================================\n\n")

# Find subscriber-related columns
cat("SUBSCRIBER COLUMNS (for linking CSV.subscriber_id):\n")
for(table_name in tables) {
  cols <- dbListFields(sqlite_con, table_name)
  subscriber_cols <- cols[grepl("subscriber", cols, ignore.case = TRUE)]
  if(length(subscriber_cols) > 0) {
    cat(sprintf("  %-20s → %s\n", table_name, paste(subscriber_cols, collapse = ", ")))
  }
}
cat("\n")

# Find asset-related columns
cat("ASSET COLUMNS (for linking CSV.asset_id):\n")
for(table_name in tables) {
  cols <- dbListFields(sqlite_con, table_name)
  asset_cols <- cols[grepl("asset", cols, ignore.case = TRUE)]
  if(length(asset_cols) > 0) {
    cat(sprintf("  %-20s → %s\n", table_name, paste(asset_cols, collapse = ", ")))
  }
}
cat("\n")

# Find country-related columns
cat("COUNTRY COLUMNS (needed for analytics):\n")
for(table_name in tables) {
  cols <- dbListFields(sqlite_con, table_name)
  country_cols <- cols[grepl("country", cols, ignore.case = TRUE)]
  if(length(country_cols) > 0) {
    cat(sprintf("  %-20s → %s\n", table_name, paste(country_cols, collapse = ", ")))
  }
}
cat("\n")

# Find sport-related columns
cat("SPORT COLUMNS (needed for analytics):\n")
for(table_name in tables) {
  cols <- dbListFields(sqlite_con, table_name)
  sport_cols <- cols[grepl("sport", cols, ignore.case = TRUE)]
  if(length(sport_cols) > 0) {
    cat(sprintf("  %-20s → %s\n", table_name, paste(sport_cols, collapse = ", ")))
  }
}
cat("\n")

# ============================================================================
# SECTION 6: Data Integration Analysis
# ============================================================================

cat("=======================================================================\n")
cat("DATA INTEGRATION FINDINGS\n")
cat("=======================================================================\n\n")

cat("ANSWER 1: Getting COUNTRY for each subscriber\n")
cat("-----------------------------------------------------------------------\n")
cat("Table path: subscribers → postal2city → cities → countries\n")
cat("JOIN logic:\n")
cat("  subscribers.postal_code = postal2city.postal_code\n")
cat("  postal2city.city_id = cities.city_id\n")
cat("  cities.country_id = countries.country_id\n")
cat("Result: countries.country (text field)\n\n")

# Test the join to verify
test_country <- dbGetQuery(sqlite_con, "
  SELECT s.subscriber_id, co.country
  FROM subscribers s
  JOIN postal2city p2c ON s.postal_code = p2c.postal_code
  JOIN cities c ON p2c.city_id = c.city_id
  JOIN countries co ON c.country_id = co.country_id
  LIMIT 5
")
cat("Sample country mapping:\n")
print(test_country)
cat("\n")

cat("ANSWER 2: Getting SPORT for each asset\n")
cat("-----------------------------------------------------------------------\n")
cat("Table: assets (contains sport column directly)\n")
cat("JOIN logic:\n")
cat("  CSV.asset_id = assets.asset_id\n")
cat("Result: assets.sport (text field)\n\n")

# Test the sport lookup
test_sport <- dbGetQuery(sqlite_con, "
  SELECT asset_id, sport
  FROM assets
  LIMIT 5
")
cat("Sample sport mapping:\n")
print(test_sport)
cat("\n")

# Get unique countries
countries_list <- dbGetQuery(sqlite_con, "SELECT DISTINCT country FROM countries ORDER BY country")
cat("ANSWER 3: Available countries (", nrow(countries_list), " total):\n", sep = "")
cat("  ", paste(countries_list$country, collapse = ", "), "\n\n", sep = "")

# Get unique sports
sports_list <- dbGetQuery(sqlite_con, "SELECT DISTINCT sport FROM assets WHERE sport IS NOT NULL ORDER BY sport")
cat("ANSWER 4: Available sports (", nrow(sports_list), " total):\n", sep = "")
cat("  ", paste(sports_list$sport, collapse = ", "), "\n\n", sep = "")

# ============================================================================
# SECTION 7: Data Quality Check
# ============================================================================

cat("=======================================================================\n")
cat("DATA QUALITY ASSESSMENT\n")
cat("=======================================================================\n\n")

# Check for subscribers without country mapping
orphan_subscribers <- dbGetQuery(sqlite_con, "
  SELECT COUNT(*) as count
  FROM subscribers s
  LEFT JOIN postal2city p2c ON s.postal_code = p2c.postal_code
  WHERE p2c.postal_code IS NULL
")
cat("Subscribers without country mapping:", orphan_subscribers$count, "\n")
if(orphan_subscribers$count > 0) {
  cat("  WARNING: Some subscribers cannot be mapped to countries\n")
} else {
  cat("  All subscribers can be mapped to countries\n")
}
cat("\n")

# Check for NULL sports in assets
null_sports <- dbGetQuery(sqlite_con, "
  SELECT COUNT(*) as count
  FROM assets
  WHERE sport IS NULL
")
cat("Assets with NULL sport:", null_sports$count, "\n")
if(null_sports$count > 0) {
  cat("  WARNING: Some assets do not have sport assigned\n")
} else {
  cat("  All assets have sport assigned\n")
}
cat("\n")

# Check date range in streaming_txns (for reference)
date_range <- dbGetQuery(sqlite_con, "
  SELECT 
    MIN(streaming_date) as min_date,
    MAX(streaming_date) as max_date
  FROM streaming_txns
")
cat("Date range in SQLite streaming_txns table:\n")
cat("  Earliest:", date_range$min_date, "\n")
cat("  Latest:", date_range$max_date, "\n\n")

# ============================================================================
# SECTION 8: ETL Strategy Summary
# ============================================================================

cat("=======================================================================\n")
cat("ETL STRATEGY FOR PART C\n")
cat("=======================================================================\n\n")

cat("Step 1: Create lookup tables in R memory\n")
cat("  - Extract subscriber_id → country mapping (4-table join)\n")
cat("  - Extract asset_id → sport mapping (direct from assets)\n")
cat("  Total memory needed: ~", format(198194 * 50 / 1024^2, digits = 2), " MB\n\n", sep = "")

cat("Step 2: Load CSV in chunks (5,000 rows per batch)\n")
cat("  - Total batches needed: ~", ceiling(98732 / 5000), "\n", sep = "")
cat("  - Enrich each chunk with country and sport using match()\n")
cat("  - Parse dates and extract time dimensions\n")
cat("  - Build batch INSERT statements\n\n")

cat("Step 3: Load into MySQL staging table\n")
cat("  - Insert enriched transaction data\n")
cat("  - Validate data quality (check for NULLs)\n\n")

cat("Step 4: Aggregate into fact table\n")
cat("  - GROUP BY date, country, sport\n")
cat("  - Calculate transaction_count, total_minutes, avg_minutes\n")
cat("  - Insert into fact_streaming_summary\n\n")

# Close SQLite connection
dbDisconnect(sqlite_con)
cat("SQLite connection closed.\n\n")

# ============================================================================
# SECTION 9: Test MySQL Cloud Connection
# ============================================================================

cat("=======================================================================\n")
cat("TESTING MySQL CLOUD CONNECTION (Aiven)\n")
cat("=======================================================================\n\n")

# MySQL connection parameters
mysql_host <- "MYSQL_HOST"
mysql_port <- 22334
mysql_user <- "avnadmin"
mysql_password <- "MYSQL_PASSWORD"
mysql_dbname <- "defaultdb"
db_cert <- NULL  #Add your certificate here



cat("Connection parameters:\n")
cat("  Host:", mysql_host, "\n")
cat("  Port:", mysql_port, "\n")
cat("  Database:", mysql_dbname, "\n\n")

# Try to connect
tryCatch({
  mysql_con <- dbConnect(
    RMySQL::MySQL(),
    host = mysql_host,
    port = mysql_port,
    user = mysql_user,
    password = mysql_password,
    dbname = mysql_dbname,
    sslmode = "require",
    sslcert = db_cert
  )
  
  cat("Successfully connected to MySQL!\n\n")
  
  # Check MySQL version
  version <- dbGetQuery(mysql_con, "SELECT VERSION() as version")
  cat("MySQL Version:", version$version, "\n\n")
  
  # List existing tables
  existing_tables <- dbListTables(mysql_con)
  cat("Existing tables in database (", length(existing_tables), "):\n", sep = "")
  if(length(existing_tables) > 0) {
    for(tbl in existing_tables) {
      cat("  -", tbl, "\n")
    }
  } else {
    cat("  (No tables found - database is empty)\n")
  }
  cat("\n")
  
  # ============================================================================
  # SECTION 10: Test MySQL Operations
  # ============================================================================
  
  cat("=======================================================================\n")
  cat("TESTING MySQL CRUD OPERATIONS\n")
  cat("=======================================================================\n\n")
  
  # Drop test table if exists
  dbExecute(mysql_con, "DROP TABLE IF EXISTS test_table")
  
  # Create test table
  cat("Creating test table...\n")
  dbExecute(mysql_con, "
    CREATE TABLE test_table (
      id INT PRIMARY KEY,
      name VARCHAR(100),
      value DECIMAL(10,2)
    )
  ")
  
  # Insert sample data
  cat("Inserting sample data...\n")
  dbExecute(mysql_con, "
    INSERT INTO test_table (id, name, value) VALUES
    (1, 'Test Record 1', 100.50),
    (2, 'Test Record 2', 200.75),
    (3, 'Test Record 3', 300.25)
  ")
  
  # Query data back
  cat("Querying data...\n")
  test_data <- dbGetQuery(mysql_con, "SELECT * FROM test_table ORDER BY id")
  print(test_data)
  cat("\n")
  
  # Drop test table
  cat("Cleaning up...\n")
  dbExecute(mysql_con, "DROP TABLE test_table")
  
  cat("\nAll MySQL operations successful!\n\n")
  
  # Close MySQL connection
  dbDisconnect(mysql_con)
  cat("MySQL connection closed.\n\n")
  
}, error = function(e) {
  cat("ERROR connecting to MySQL:\n")
  cat("  ", e$message, "\n\n", sep = "")
  cat("Troubleshooting:\n")
  cat("  1. Check if Aiven service is running\n")
  cat("  2. Verify credentials are correct\n")
  cat("  3. Check firewall settings (port 22334)\n")
  cat("  4. Verify IP whitelist in Aiven console\n\n")
})

# ============================================================================
# SECTION 11: Summary
# ============================================================================

cat("=======================================================================\n")
cat("PART A EXPLORATION COMPLETE\n")
cat("=======================================================================\n\n")

cat("SQLite database structure documented\n")
cat("Join paths identified:\n")
cat("    - CSV → subscribers → postal2city → cities → countries (COUNTRY)\n")
cat("    - CSV → assets (SPORT)\n")
cat("MySQL connection verified\n")
cat("Data quality assessed\n")
cat("ETL strategy defined\n\n")
