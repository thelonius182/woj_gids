
flog.info("running validations", name = "wojsch")

# init ----
# init flag variable: when sourcing this script fails, notify the caller by not removing this variable.
salsa_source_error <- T

# enter main control loop
repeat {
  
  # connect to wordpress-DB ----
  cur_db_type <- "prd"
  wp_conn <- get_wp_conn(config$wpdb_env)
  
  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", cur_db_type), name = "wojsch")
    break
  }

  # DRAFTS ----
  qry <- read_file("C:/Users/nipper/Documents/cz_queries/woj_check_drafts.sql")
  val_A <- dbGetQuery(wp_conn, qry)
  
  if (val_A$n_drafts > 0) {
    flog.error("WordPress still has 'draft'-posts; review & publish those manually first", name = "wojsch")
    break
  }
  
  # OVERLAP/REVERSE NL ----
  qry <- "SELECT 
        	po1.id AS pgmID,
            DATE_FORMAT(po1.post_date, '%Y-%m-%d %H:%i') AS pgmStart,
            pm1.meta_value AS pgmStop,
            po1.post_status AS pgmSts,
            CASE WHEN tr1.term_taxonomy_id = 4 THEN 'EN'
                 ELSE 'NL'
            END AS pgmTaal 
        FROM wp_posts po1
                JOIN
            wp_postmeta pm1 ON pm1.post_id = po1.id
                JOIN
            wp_term_relationships tr1 ON tr1.object_id = po1.id
        WHERE po1.post_date >= '2018-01-01 00:00:00'
          AND pm1.meta_key = 'pr_metadata_uitzenddatum_end'
          AND pm1.meta_value <= '2100-01-01 00:00:00'
          AND tr1.term_taxonomy_id = 5
          AND po1.post_type = 'programma_woj'
          AND po1.post_status IN ('draft' , 'publish')
        ;"
  val_B1 <- dbGetQuery(wp_conn, qry)
  val_B1.1 <- val_B1 |> mutate(reverse = pgmStop <= pgmStart,
                               overlap = pgmStart < lag(pgmStop)) |> filter(reverse|overlap)
  if (nrow(val_B1.1) > 0) {
    flog.info("Wordpress has overlapping or reversed slot times (NL)", name = "wojsch")
    break
  }
  
  # OVERLAP/REVERSE EN ----
  qry <- "SELECT 
        	po1.id AS pgmID,
            DATE_FORMAT(po1.post_date, '%Y-%m-%d %H:%i') AS pgmStart,
            pm1.meta_value AS pgmStop,
            po1.post_status AS pgmSts,
            CASE WHEN tr1.term_taxonomy_id = 4 THEN 'EN'
                 ELSE 'NL'
            END AS pgmTaal 
        FROM wp_posts po1
                JOIN
            wp_postmeta pm1 ON pm1.post_id = po1.id
                JOIN
            wp_term_relationships tr1 ON tr1.object_id = po1.id
        WHERE po1.post_date >= '2018-01-01 00:00:00'
          AND pm1.meta_key = 'pr_metadata_uitzenddatum_end'
          AND pm1.meta_value <= '2100-01-01 00:00:00'
          AND tr1.term_taxonomy_id = 4
          AND po1.post_type = 'programma_woj'
          AND po1.post_status IN ('draft' , 'publish')
        ;"
  val_C1 <- dbGetQuery(wp_conn, qry)
  val_C1.1 <- val_C1 |> mutate(reverse = pgmStop <= pgmStart,
                               overlap = pgmStart < lag(pgmStop)) |> filter(reverse|overlap)
  if (nrow(val_C1.1) > 0) {
    flog.info("Wordpress has overlapping or reversed slot times (EN)", name = "wojsch")
    break
  }
  
  # DANGLERS ----
  qry <- read_file("C:/Users/nipper/Documents/cz_queries/woj_bungelherhalingen.sql")
  val_D <- dbGetQuery(wp_conn, qry)
  
  if (nrow(val_D) > 0) {
    flog.info("WordPress tries to do replays of non-existing originals", name = "wojsch")
    break
  }
  
  # PREMATURES ----
  qry <- read_file("C:/Users/nipper/Documents/cz_queries/woj_premature_posts.sql")
  qry.1 <- str_replace(qry, "¶ts", as.character(cz_week_start))
  val_E <- dbGetQuery(wp_conn, qry.1)
  
  if (nrow(val_E) > 0) {
    flog.info("WordPress already has one or more posts in the coming woj-week", name = "wojsch")
    break
  }
  
  # REDUCERS ----
  qry <- "drop table if exists salsa_replacements;"
  valF.1 <- dbExecute(wp_conn, qry)
  
  qry <- "create table salsa_replacements as SELECT 
         pm1.post_id as pgmID1, 
         pm1.meta_value as replaceID1, 
         pm2.post_id as pgmID2, 
         pm2.meta_value as replaceID2
         FROM wp_postmeta pm1
         JOIN wp_postmeta pm2 ON CAST(pm1.meta_value AS UNSIGNED) = pm2.post_id
         WHERE pm1.meta_key = 'pr_metadata_orig'
           AND pm2.meta_key = 'pr_metadata_orig'
           AND LENGTH(TRIM(pm1.meta_value)) > 0
           AND LENGTH(TRIM(pm2.meta_value)) > 0
         order by 4
         ;"
  valF.2 <- dbExecute(wp_conn, qry)
  
  qry <- "update wp_postmeta pm1 join salsa_replacements rp1 on rp1.pgmID1 = pm1.post_id
         set pm1.meta_value = rp1.replaceID2 where pm1.meta_key = 'pr_metadata_orig'
         ;"
  valF.3 <- dbExecute(wp_conn, qry)
  
  flog.info("validations completed", name = "wojsch")
  dbDisconnect(wp_conn)
  
  # exit cleanly from main control loop
  rm(salsa_source_error)
  break
}
