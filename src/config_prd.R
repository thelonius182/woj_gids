config <- readr::read_yaml("config_prd.yaml")

db_host <- keyring::key_get(service = paste0("sql-wp", pm_db_type, "_host"))
db_user <- keyring::key_get(service = paste0("sql-wp", pm_db_type, "_user"))
db_password <- keyring::key_get(service = paste0("sql-wp", pm_db_type, "_pwd"))
db_name <- keyring::key_get(service = paste0("sql-wp", pm_db_type, "_db"))
