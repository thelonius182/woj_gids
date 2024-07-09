# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create WoJ schedules, WP-posts and tracklists
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# INIT ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs, uuid, gmailr, RPostgres,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger, conflicted)

conflicts_prefer(dplyr::lag, dplyr::lead, dplyr::filter, lubridate::minutes, .quiet = T)
source("src/functions.R", encoding = "UTF-8")
lg_ini <- flog.appender(appender.file(config$log_file), "wojsch")
git_info <- salsa_git_version(getwd())
flog.info(">>> START", name = "wojsch")
flog.info(sprintf("git-branch: %s", git_info$git_branch), name = "wojsch")
flog.info(sprintf("  commited: %s", git_info$ts), name = "wojsch")
flog.info(sprintf("        by: %s", git_info$by), name = "wojsch")
flog.info(sprintf("local repo: %s", git_info$path), name = "wojsch")
flog.info(sprintf("  using db: %s", config$wpdb_env), name = "wojsch")

# signal to 'add_ml_tracklists_to_wp' it will be called from 'main'. 
# That will tell it to create a new week, instead of refreshing the most recent week
salsa_source_main <- T

# main control loop
repeat {
  
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
    flog.info("<<< STOP", name = "wojsch")
    stop("Accessing Gmail failed")
  }
  
  # create time series ----
  # cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
  cz_week_slots <- slot_sequence_wk(new_week = T)
  cz_week_start <- cz_week_slots$slot_ts[1]
  
  # run validations ----
  source("src/woj_validations.R", encoding = "UTF-8")
  
  if (exists("salsa_source_error")) {
    break
  } 
  
  # create schedules ----
  source("src/create_schedules.R", encoding = "UTF-8")
  
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
  report_msg <- "Taak kon niet voltooid worden - zie 'woj_schedules.log' (Nipper/Desktop)."
} else {
  report_msg <- "Taak voltooid, geen bijzonderheden."
}

# report the result ----
woj_task_report <- gm_mime() |> 
  # gm_to(c(config$email_to_A, config$email_to_B)) |> 
  gm_to(config$email_to_A) |> 
  gm_from(config$email_from) |> 
  gm_subject("WorldOfJazz Weektaak") |> 
  gm_text_body(report_msg)

rtn <- gm_send_message(woj_task_report)

flog.info("<<< STOP
", name = "wojsch")
