# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create WoJ schedules and tracklists
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# INIT ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs, uuid,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger)

# enter main control loop
repeat {
  
  source("src/functions.R", encoding = "UTF-8")
  config <- read_yaml("config.yaml")
  lg_ini <- flog.appender(appender.file(config$log_file), "wojsch")
  flog.info("= = = = = START - WoJ Schedules, version 2024-05-19.1 = = = = =", name = "wojsch")
  
  # say Hello to Gmail
  n_errors <- tryCatch(
    {
      gm_auth_configure(path = config$email_auth_path)
      gm_auth(cache = ".secret", email = config$email_from)
      0L
    },
    error = function(e1) {
      flog.error(sprintf("Accessing Gmail failed - can't report results. Msg = %s", conditionMessage(e1)), 
                 name = "wojsch")
      return(1L)
    }
  )
  
  if (n_errors > 0) {
    break
  }
  
  # run validations ----
  source("src/woj_validations.R", encoding = "UTF-8")
  
  if (exists("salsa_source_error")) {
    break
  } 
  
  # create schedules ----
  source("src/create_schedules_v2.R", encoding = "UTF-8")
  
  if (exists("salsa_source_error")) {
    break
  } 

  # upload pgm guide ----
  source("src/upload_pgm_guide.R", encoding = "UTF-8")
  
  if (exists("salsa_source_error")) {
    break
  } 
  
  # publish the draft posts ----
  source("src/woj_publish_posts.R", encoding = "UTF-8")
  
  if (exists("salsa_source_error")) {
    break
  } 
  
  # add ML-tracklists ----
  source("src/add_ml_tracklists_to_wp.R", encoding = "UTF-8")
  
  # exit from main control loop
  break
}

if (exists("salsa_source_error")) {
  report_msg <- "WoJ-weektaak kon niet voltooid worden - zie 'woj_schedules.log' (Nipper/Desktop)."
} else {
  report_msg <- "WoJ-weektaak is voltooid, geen bijzonderheden."
}

# report the result ----
woj_task_report <- gm_mime() |> 
  # gm_to(c(config$email_to_A, config$email_to_B)) |> 
  gm_to(config$email_to_A) |> 
  gm_from(config$email_from) |> 
  gm_subject("WorldOfJazz Weektaak") |> 
  gm_text_body(report_msg)

rtn <- gm_send_message(woj_task_report)

flog.info("= = = = = STOP = = = = =", name = "wojsch")
