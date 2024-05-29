# Load necessary packages
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs, uuid, gmailr,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger)

source("src/functions.R", encoding = "UTF-8")
config <- read_yaml("config.yaml")
lg_ini <- flog.appender(appender.file(config$log_file), "wojsch")
flog.info("= = = = = START - upload Programme Guide = = = = = = = = = = =", name = "wojsch")

# init flag variable, to be removed if the script finishes successfully
salsa_source_error <- T

# enter main control loop
repeat {
  
  # update import-hash ----
  wp_conn <- get_wp_conn(config$wpdb_env)
  
  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", config$wpdb_env), name = "wojsch")
    break
  }
  
  new_import_hash <- UUIDgenerate(use.time = T, output = "string")
  sql_stmt <- sprintf("update wp_options set option_value = '%s' 
                       where option_name = 'import_program_items_hash'", new_import_hash)
  sql_sts <- dbExecute(wp_conn, sql_stmt)
  sql_sts <- dbDisconnect(wp_conn)
  
  # use the latest pgm guide uploaded
  qfns <- dir_ls(path = "C:/cz_salsa/gidsweek_uploaden/", 
                 type = "file",
                 regexp = "WJ_gidsweek_\\d{4}.*[.]json$") |> sort(decreasing = T)
  json_data <- fromJSON(qfns[1])
  
  # upload one by one
  keys <- names(json_data)
  
  for (k1 in keys) {
    query_string <- list_to_query(json_data[[k1]])
    url_with_query <- paste0(config$wp_endpoint, 
                             "?action=import-program-item&", 
                             query_string,
                             "&import-hash=",
                             new_import_hash)
    
    flog.info(query_string, name = "wojsch")
    
    # Send the request
    # response <- GET(url_with_query)
    # 
    # # Check the response
    # if (status_code(response) != 200L || content(response, "text") != "success") {
    #   flog.error(sprintf("this post failed: %s", k1), name = "wojsch")
    # }
  }
  
  # exit cleanly from main control loop
  rm(salsa_source_error)
  break
}

flog.info("= = = = = STOP = = = = = = = = = = = = = = = = = = = = = = = =", name = "wojsch")
