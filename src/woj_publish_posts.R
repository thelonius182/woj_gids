
flog.info("publishing...", name = "wojsch")

# init ----
# init flag variable: when sourcing this script fails, notify the caller by creating this variable.
# So, make sure it doesn't exist at the start
if (exists("salsa_source_error")) rm(salsa_source_error)

# start main control loop
repeat {
  
  # connect to wordpress-DB ----
  cur_db_type <- "prd"
  wp_conn <- get_wp_conn(pm_db_type = cur_db_type)
  
  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", cur_db_type), name = "wojsch")
    salsa_source_error <- T
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
      flog.error(sprintf("SQL-statement to publish the posts failed: %s", conditionMessage(e1)), name = "wojsch")
      return(1L)
    },
    finally = dbDisconnect(wp_conn)
  )
  
  if (n_errors == 1) {
    salsa_source_error <- T
    
  } else {
    flog.info("publishing completed", name = "wojsch")
  }
  
  # stop main control loop
  break
}
