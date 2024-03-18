get_wp_conn <- function(pm_db_type = "prd") {
  
  if (pm_db_type == "prd") {
    db_host <- key_get(service = paste0("sql-wp", pm_db_type, "_host"))
    db_user <- key_get(service = paste0("sql-wp", pm_db_type, "_user"))
    db_password <- key_get(service = paste0("sql-wp", pm_db_type, "_pwd"))
    db_name <- key_get(service = paste0("sql-wp", pm_db_type, "_db"))
  } else {
    woj_gids_creds_dev <- read_rds(config$db_dev_creds)
    db_host <- woj_gids_creds_dev$db_host
    db_user <- woj_gids_creds_dev$db_user
    db_password <- woj_gids_creds_dev$db_password
    db_name <- woj_gids_creds_dev$db_name
  }
  
  db_port <- 3306
  # flog.appender(appender.file("/Users/nipper/Logs/nipper.log"), name = "nipperlog")
  
  grh_conn <- tryCatch( 
    {
      dbConnect(drv = MySQL(), user = db_user, password = db_password,
                dbname = db_name, host = db_host, port = db_port)
    },
    error = function(cond) {
      cat("Wordpress database connection failed (dev: is PuTTY running?)")
      # flog.error("Wordpress database onbereikbaar (dev: check PuTTY)", name = "nipperlog")
      return("connection-error")
    }
  )
  
  return(grh_conn)
}

get_replay <- function(pm_broadcast, pm_ts, pm_cycle, pm_parent, pm_live) {
  
  sql_stm <- qry_prv_post |> 
    str_replace("@title", pm_broadcast) |> 
    str_replace_all("@ymd_woj_slot", pm_ts) |> 
    str_replace("@day_of_week", pm_ts) |> 
    str_replace("@hour", pm_ts)
}

