config <- yaml::read_yaml("config_prd.yaml")

db_host <- keyring::key_get(service = "sql-wpprd_host")
db_user <- keyring::key_get(service = "sql-wpprd_user")
db_password <- keyring::key_get(service = "sql-wpprd_pwd")
db_name <- keyring::key_get(service = "sql-wpprd_db")
