# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create a WoJ "schedule week" & "playlist week"
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# INIT ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger)

source("src/functions.R", encoding = "UTF-8")
config <- read_yaml("config.yaml")
lg_ini <- flog.appender(appender.file(config$log_file), "wojsch")
flog.info("= = = = = START - WoJ Schedules, version 2024-05-10 15:31 = = = = =", name = "wojsch")

# run validations first ----
source("src/woj_validations.R", encoding = "UTF-8")

if (exists("salsa_source_error")) {
  flog.error("Wordpress for WoJ has issues, see file woj_schedules.log (desktop)", name = "wojsch")
} else {
  source("src/create_schedules_v2.R", encoding = "UTF-8")
}

# source("src/woj_publish_posts.R", encoding = "UTF-8")
flog.info("= = = = = STOP = = = = =", name = "wojsch")
