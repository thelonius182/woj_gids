
flog.info("publishing...", name = "wojsch")

# init ----
# init flag variable, to be removed if the script finishes successfully
salsa_source_error <- T

# start main control loop
repeat {
  
  # connect to wordpress-DB ----
  wp_conn <- get_wp_conn(config$wpdb_env)
  
  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", config$wpdb_env), name = "wojsch")
    break
  }
  
  # publish! ----
  sql_path <- "C:/Users/nipper/Documents/cz_queries"
  loc <- locale(encoding = "UTF-8")
  sql_stm.1 <- read_file(path_join(c(sql_path, "woj_set_names_on_originals.sql")), locale = loc)
  sql_stm.2 <- read_file(path_join(c(sql_path, "woj_set_names_on_replays.sql")), locale = loc)
  sql_stm.3 <- read_file(path_join(c(sql_path, "woj_publish_all_drafts.sql")), locale = loc)
  
  sql_stmts <- list("SET sql_mode = 'NO_BACKSLASH_ESCAPES';", 
                    "set @dtm_van = '2018-01-01 00:00:00';", 
                    "set @dtm_tem = '2100-01-01 00:00:00';", 
                    sql_stm.1, sql_stm.2, sql_stm.3)
  
  n_errors <- tryCatch(
    {
      for (s1 in sql_stmts) {
        dbExecute(wp_conn, s1)
      }
      
      0L
    }, 
    error = function(e1) {
      flog.error(sprintf("SQL-statement to publish the posts failed: %s", conditionMessage(e1)), 
                 name = "wojsch")
      return(1L)
    },
    finally = dbDisconnect(wp_conn)
  )
  
  if (n_errors > 0) {
    break
  }
  
  flog.info("publishing completed", name = "wojsch")
  
  # exit cleanly from main control loop
  rm(salsa_source_error)
  break
}
