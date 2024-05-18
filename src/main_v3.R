# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create a WoJ "schedule week" & "playlist week"
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# INIT ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs, uuid,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger)

source("src/functions.R", encoding = "UTF-8")
config <- read_yaml("config.yaml")
lg_ini <- flog.appender(appender.file(config$log_file), "wojsch")
flog.info("= = = = = START - WoJ Schedules, version 2024-05-10 15:31 = = = = =", name = "wojsch")

# say Hello to Gmail
gm_auth_configure(path = config$email_auth_path)
gm_auth(cache = ".secret", email = config$email_from)

# run validations first ----
# on errors, the script creates a variable called 'salsa_source_error'
source("src/woj_validations.R", encoding = "UTF-8")

if (exists("salsa_source_error")) {
  flog.error("Wordpress for WoJ has issues, see file woj_schedules.log (desktop)", name = "wojsch")
} else {
  source("src/create_schedules_v2.R", encoding = "UTF-8")
}


email <- gm_mime() |> 
  gm_to(c(config$email_to_A, config$email_to_B)) |> 
  gm_from(config$email_from) |> 
  gm_subject("Test Email from R") |> 
  gm_text_body("This is a test email sent from R using gmailr with service account authentication.")

# Send the email
rtn <- gm_send_message(email)

# source("src/woj_publish_posts.R", encoding = "UTF-8")
flog.info("= = = = = STOP = = = = =", name = "wojsch")
