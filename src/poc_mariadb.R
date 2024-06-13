# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Add non-stop ML-tracklists to WP-posts
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs, RPostgres, DBI,
               stringr, yaml, readr, rio, keyring, jsonlite, futile.logger)

config <- read_yaml("config.yaml")
source("src/functions.R", encoding = "UTF-8")

qfn_log <- path_join(c("C:", "Users", "nipper", "Logs", "woj_schedules.log"))
lg_ini <- flog.appender(appender.file(qfn_log), "wojsch")
flog.info("= = = = = = = adding/refreshing non-stop tracklists = = = = = =", name = "wojsch")

# create time series? ----
# only when called stand-alone
if (!exists("salsa_source_main")) {
  cz_week_slots <- slot_sequence_wk(new_week = F)
  cz_week_start <- cz_week_slots$slot_ts[1]
}

# main control loop
repeat {
  
  # coonect to wordpress-DB ----
  wp_conn <- get_wp_ds(config$wpdb_env)
  
  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", config$wpdb_env), name = "wojsch")
    break
  }
  
  # connect to mAirList-DB ----
  mal_conn <- get_mal_ds()
  
  if (typeof(mal_conn) != "S4") {
    flog.error("connecting to mAirList-DB failed", name = "wojsch")
    break
  }
  
  query <- "SELECT pg_encoding_to_char(encoding) AS encoding
          FROM pg_database
          WHERE datname = 'mairlist7';"
  mal_enc <- dbGetQuery(mal_conn, query)
  
  qry <- "
     select distinct it.title as track_title
     from playlist pl
     join items it on it.idx = pl.item
     left join item_attributes ia on ia.item = pl.item
     where it.storage not in (10, 11, 17) -- no Bumpers, no Semi-live's
       and ia.value = 'The Brasil project';"
  
  ml_tracks <- dbGetQuery(mal_conn, qry)
  
  ml_tracks.1 <- ml_tracks |> mutate(track_title = iconv(track_title, from = "UTF-8", to = "CP850"))
  
  sqlstmt <- "show variables like 'character_set_client'"
  result <- dbGetQuery(conn = wp_conn, statement = sqlstmt)
  
  dbCreateTable(wp_conn, name = "salsa_maltest", ml_tracks.1)
  dbAppendTable(wp_conn, name = "salsa_maltest", ml_tracks.1)
  
  dbDisconnect(wp_conn)
  dbDisconnect(mal_conn)
  # leave main control loop
  break
}