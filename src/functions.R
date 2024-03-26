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

get_rewind_ts_old <- function(pm_rec_id) {
  
  # set current rewind
  cur_rew <- cz_week_irr_rew |> filter(rec_id == pm_rec_id)
  
  # set sql parameters
  sq_title_NL <- cur_rew$tit_nl
  sq_slot_ts <- cur_rew$slot_ts
  sq_int_parent_day <- which(r2sql_wday == str_sub(cur_rew$parent, 1, 2))
  sq_parent_hour <- parse_number(cur_rew$parent)

  # prep query  
  sql_stm <- qry_rewind_post |> 
    str_replace("@title_NL", sq_title_NL) |> 
    str_replace_all("@slot_ts", format(sq_slot_ts, "%Y-%m-%d %H:00")) |> 
    str_replace("@int_parent_day", as.character(sq_int_parent_day)) |> 
    str_replace("@parent_hour", as.character(sq_parent_hour)) |> 
    str_replace_all('"', "")
  
  # excute
  dbGetQuery(wp_conn, sql_stm)
}

# woj-slots that precede the universe one need another, earlier replay post.
# The same goes for woj-slots that replay live universe slots 
fetch_rewind_ts <- function(pm_tit_nl, pm_slot_ts, pm_parent) {
  
  # set sql parameters
  sq_int_parent_day <- which(r2sql_wday == str_sub(pm_parent, 1, 2)) |> as.character()
  sq_parent_hour <- parse_number(pm_parent) |> as.character()
  pm_tit_nl <- if_else(pm_tit_nl == "¡Mambo!", "Mambo", pm_tit_nl)

  # prep query  
  sql_stm <- qry_rewind_post |> 
    str_replace("@title_NL", pm_tit_nl) |> 
    str_replace_all("@slot_ts", format(pm_slot_ts, "%Y-%m-%d %H:00")) |> 
    str_replace("@int_parent_day", sq_int_parent_day) |> 
    str_replace("@parent_hour", sq_parent_hour)
  
  # excute
  dbGetQuery(wp_conn, sql_stm)
}

# compare two slot-id's
slot_delta <- function(slot_label_a, slot_label_b, pm_live) {
  
  # mark non-universe slots
  if (is.na(slot_label_b)) return(list(delta = 99, regular_rewind = F))
  
  # see which one comes first
  ts1_idx <- which(cz_week_slots$slot_label == slot_label_a)
  ts2_idx <- which(cz_week_slots$slot_label == slot_label_b)
  
  # Extract their timestamps (and set to midnight)
  ts1_ts <- cz_week_slots$slot_ts[[ts1_idx]]
  hour(ts1_ts) <- 0
  ts2_ts <- cz_week_slots$slot_ts[[ts2_idx]]
  hour(ts2_ts) <- 0
  
  # return result
  list(delta = time_length(ts1_ts %--% ts2_ts, unit = "day"), 
       regular_rewind = if_else(ts1_idx > ts2_idx & pm_live != "Y", T, F))
}

slot_sequence <- function(ts_start) {
  
  all_day_names <- c("ma", "di", "wo", "do", "vr", "za", "zo")

  seq(from = ts_start, to = ts_start + hours(7 * 24 - 1), by = "hours") |> as_tibble() |> 
    rename(slot_ts = value) |>
    mutate(day = all_day_names[wday(slot_ts, label = F, week_start = 1)],
           week_vd_mnd = 1 + (day(slot_ts) - 1) %/% 7L,
           start = hour(slot_ts),
           slot_label = paste0(day, hour(slot_ts)))
}

fmt_utc_ts <- function(some_date) {
  format(some_date, "%Y-%m-%d_%a%H-%Z%z") %>%
    str_replace("UTC", "GMT") %>%
    str_sub(1, 22)
}

woj2json <- function(pm_tib_json) {
  
  # Convert tibble to a list of named lists
  list_json <- lapply(1:nrow(pm_tib_json), function(i1) {
    as.list(pm_tib_json[i1, -1])
  })
  
  # set obj_name as the key
  names(list_json) <- pm_tib_json$obj_name
  
  toJSON(list_json, pretty = TRUE, auto_unbox = T)
}
