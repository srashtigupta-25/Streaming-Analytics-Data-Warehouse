library(DBI)
library(RSQLite)

con <- dbConnect(RSQLite::SQLite(), "data/subscribersDB.sqlitedb")

# See what tables exist
print(dbListTables(con))

# Export the two tables loadAnalyticsDB.R needs
write.csv(dbGetQuery(con, "SELECT * FROM assets"),    "data/assets.csv",    row.names = FALSE)
write.csv(dbGetQuery(con, "SELECT * FROM countries"), "data/countries.csv", row.names = FALSE)

dbDisconnect(con)
cat("Done! Check data/ folder for assets.csv and countries.csv\n")