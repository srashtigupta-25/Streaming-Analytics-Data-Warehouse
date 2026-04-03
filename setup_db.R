# setup_db.R
# Run this ONCE before running loadAnalyticsDB.R
# Rebuilds the local SQLite database from source CSV files

library(DBI)
library(RSQLite)

if (!dir.exists("data")) dir.create("data")

con <- dbConnect(RSQLite::SQLite(), "data/subscribersDB.sqlitedb")

assets    <- read.csv("data/assets.csv",    stringsAsFactors = FALSE)
countries <- read.csv("data/countries.csv", stringsAsFactors = FALSE)

dbWriteTable(con, "assets",    assets,    overwrite = TRUE)
dbWriteTable(con, "countries", countries, overwrite = TRUE)

dbDisconnect(con)
cat("SQLite DB rebuilt at data/subscribersDB.sqlitedb\n")