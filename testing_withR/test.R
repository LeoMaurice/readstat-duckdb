library(DBI)
library(duckdb)

# Chemin vers l'extension compilée
extension_path <- "./build/debug/readstat_duckdb.duckdb_extension"

# Initialiser une connexion DuckDB
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# Charger l'extension
dbExecute(con, paste0("LOAD '", extension_path, "';"))

# Vérifier si l'extension est chargée (optionnel)
extensions <- dbGetQuery(con, "SELECT * FROM duckdb_extensions();")
print(extensions)
