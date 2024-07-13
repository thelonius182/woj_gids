message("using PRD-config")

pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs, uuid, RPostgres,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger, conflicted)

conflicts_prefer(dplyr::lag, dplyr::lead, dplyr::filter, lubridate::minutes, .quiet = T)

source("src/functions.R", encoding = "UTF-8")
config <- read_yaml("config_prd.yaml")

db_host <- key_get(service = "sql-wpprd_host")
db_user <- key_get(service = "sql-wpprd_user")
db_password <- key_get(service = "sql-wpprd_pwd")
db_name <- key_get(service = "sql-wpprd_db")

lg_ini <- flog.appender(appender.file(config$log_file), name = config$log_slug)
git_info <- salsa_git_version(getwd())
flog.info(">>> START", name = config$log_slug)
flog.info(sprintf("git-branch: %s", git_info$git_branch), name = config$log_slug)
flog.info(sprintf("  commited: %s", git_info$ts), name = config$log_slug)
flog.info(sprintf("        by: %s", git_info$by), name = config$log_slug)
flog.info(sprintf("local repo: %s", git_info$path), name = config$log_slug)
flog.info(sprintf("  using db: %s", config$wpdb_env), name = config$log_slug)
