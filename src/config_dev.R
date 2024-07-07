config <- yaml::read_yaml("config_dev.yaml")

woj_gids_creds_dev <- readr::read_rds(config$db_dev_creds)
db_host <- woj_gids_creds_dev$db_host
db_user <- woj_gids_creds_dev$db_user
db_password <- woj_gids_creds_dev$db_password
db_name <- woj_gids_creds_dev$db_name
