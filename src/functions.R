
get_wp_conn <- function(pm_db_type = "prd") {
  
  # sqlstmt <- "show variables like 'character_set_client'"
  # result <- dbGetQuery(conn = wp_conn, statement = sqlstmt)
  
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

# get_rewind_ts_depr <- function(pm_rec_id) {
#   
#   # set current rewind
#   cur_rew <- cz_week_irr_rew |> filter(rec_id == pm_rec_id)
#   
#   # set sql parameters
#   sq_title_NL <- cur_rew$tit_nl
#   sq_slot_ts <- cur_rew$slot_ts
#   sq_int_parent_day <- which(r2sql_wday == str_sub(cur_rew$parent, 1, 2))
#   sq_parent_hour <- parse_number(cur_rew$parent)
# 
#   # prep query  
#   sql_stm <- qry_rewind_post |> 
#     str_replace("@title_NL", sq_title_NL) |> 
#     str_replace_all("@slot_ts", format(sq_slot_ts, "%Y-%m-%d %H:00")) |> 
#     str_replace("@int_parent_day", as.character(sq_int_parent_day)) |> 
#     str_replace("@parent_hour", as.character(sq_parent_hour)) |> 
#     str_replace_all('"', "")
#   
#   # excute
#   dbGetQuery(wp_conn, sql_stm)
# }

# woj-slots that precede the universe one need another, earlier replay post.
# The same goes for woj-slots that replay live universe slots 
# fetch_rewind_ts_depr <- function(pm_tit_nl, pm_slot_ts, pm_parent) {
#   
#   # set sql parameters
#   sq_int_parent_day <- which(r2sql_wday == str_sub(pm_parent, 1, 2)) |> as.character()
#   sq_parent_hour <- parse_number(pm_parent) |> as.character()
#   pm_tit_nl <- if_else(pm_tit_nl == "¡Mambo!", "Mambo", pm_tit_nl)
# 
#   # prep query  
#   sql_stm <- qry_rewind_post |> 
#     str_replace("@title_NL", pm_tit_nl) |> 
#     str_replace_all("@slot_ts", format(pm_slot_ts, "%Y-%m-%d %H:00")) |> 
#     str_replace("@int_parent_day", sq_int_parent_day) |> 
#     str_replace("@parent_hour", sq_parent_hour)
#   
#   # excute
#   dbGetQuery(wp_conn, sql_stm)
# }

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

get_mal_conn <- function() {
  
  # just to prevent uploading credentials to github
  basie_creds <- read_rds("C:/Users/nipper/Documents/BasieBeats/basie.creds")
  
  result <- tryCatch( 
    dbConnect(RPostgres::Postgres(),
              dbname = 'mairlist7', 
              host = '192.168.178.91', 
              port = 5432,
              user = basie_creds$usr,
              password = basie_creds$pwd),
    error = function(cond) {
      flog.error("mAirList database unavailable", name = "bsblog")
      return("connection-error")
    }
  )
  
  return(result)
}

get_ts_rewind <- function(pm_week_start, pm_slot_ts, pm_tit_nl, pm_broadcast_type, pm_live) {
  
  # Universe-slots only
  if (pm_broadcast_type != "Universe") return(list(ts_rewind = NA_Date_, audio_src = NA_character_))
  
  # this week's Universe live broadcasts need a HiJack-file (previous week)
  if (pm_live == "Y") {
    ymd_upper_limit <- pm_week_start
  } else {
    ymd_upper_limit <- pm_slot_ts
  }
  
  # get the rewind of this broadcast closest to its slot
  cur_ts_rewind <- universe_rewinds |> 
    filter(wpdmp_slot_title == pm_tit_nl & wpdmp_slot_ts < ymd_upper_limit) |> 
    arrange(desc(wpdmp_slot_ts)) |> head(1) |> select(value = wpdmp_slot_ts)
  
  if (cur_ts_rewind$value < pm_week_start) {
    cur_audio_src <- "HiJack"
  } else {
    cur_audio_src <- "Universe"
  }
  
  return(list(ts_rewind = ymd_hms(cur_ts_rewind$value), audio_src = cur_audio_src))
}

# prep_week <- function(week_nr) {
#   week_x_init <- tib_uni_moro %>%
#     select(cz_slot = slot, contains(week_nr))
#   
#   # pivot-long to collapse week attributes into NV-pairs 
#   week_x_long <-
#     gather(
#       data = week_x_init,
#       key = slot_key,
#       value = slot_value, -cz_slot,
#       na.rm = T
#     ) %>%
#     mutate(
#       slot_key = case_when(
#         slot_key == paste0("week_", week_nr) ~ "titel",
#         slot_key == paste0("bijzonderheden_week_", week_nr) ~ "cmt mt-rooster",
#         slot_key == paste0("product_week_", week_nr) ~ "product",
#         T ~ slot_key
#       ),
#       slot_key = factor(slot_key, ordered = T, levels = cz_slot_key_levels),
#       cz_slot_day = str_sub(cz_slot, 1, 2),
#       cz_slot_day = factor(cz_slot_day, ordered = T, levels = weekday_levels),
#       cz_slot_hour = str_sub(cz_slot, 3, 4),
#       cz_slot_size = str_sub(cz_slot, 5),
#       ord_day = as.integer(week_nr)
#     ) %>%
#     select(
#       cz_slot_day,
#       ord_day,
#       cz_slot_hour,
#       cz_slot_key = slot_key,
#       cz_slot_value = slot_value,
#       cz_slot_size
#     ) %>%
#     arrange(cz_slot_day, cz_slot_hour, cz_slot_key)
#   
#   # promote slot-size to be an attribute too 
#   week_temp <- week_x_long %>%
#     select(-cz_slot_key, -cz_slot_value) %>%
#     mutate(
#       cz_slot_key = "size",
#       cz_slot_key = factor(x = cz_slot_key,
#                            ordered = T,
#                            levels = cz_slot_key_levels),
#       cz_slot_value = cz_slot_size
#     ) %>%
#     select(cz_slot_day,
#            ord_day,
#            cz_slot_hour,
#            cz_slot_key,
#            cz_slot_value,
#            cz_slot_size) %>%
#     distinct
#   
#   # final result week-x 
#   week_x <- bind_rows(week_x_long, week_temp) %>%
#     select(-cz_slot_size) %>%
#     arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)
#   
#   rm(week_x_init, week_x_long, week_temp)
#   
#   return(week_x)
# }

get_cycle <- function(cz_week_start) {
  
  ref_date_B_cycle <- ymd("2019-10-17")
  i_diff <- as.integer(cz_week_start - ref_date_B_cycle) %/% 7L
  if_else(i_diff %% 2 == 0, "B", "A")
}

get_wallclock <- function(pm_nonstop_start, pm_cum_time) {
  as_datetime(pm_nonstop_start + dseconds(pm_cum_time)) |> round_date("minute") |> 
    as.character() |> str_sub(12, 16)
}

sec2hms <- function(pm_duration_sec) {
  hms1 <- paste0("00:00:", round(pm_duration_sec, 0)) |> hms(roll = TRUE)
  sprintf("%02d:%02d:%02d", hms1@hour, hms1@minute, hms1@.Data)
}

get_czweek_start <- function(arg_ts = now(tz = "Europe/Amsterdam")) {
  
  # when running this on a Thursday between 13:00 and midnight, make it a Friday to force "next week"
  if (wday(arg_ts, week_start = 1, label = T) == "do" && hour(arg_ts) >= 13) {
    arg_ts <- arg_ts + days(1)
  }
  
  # get first Thursday 13:00 after arg_ts
  hour(arg_ts) <- 0
  minute(arg_ts) <- 0
  second(arg_ts) <- 0
  while (wday(arg_ts, week_start = 1, label = T) != "do") {
    arg_ts <- arg_ts + days(1)
  }
  
  tmp_format <- stamp("1969-07-20 17:18:19", orders = "%Y-%m0-%d %H:%M:%S", quiet = T)
  tmp_format(arg_ts + hours(13))
}

woj_pick <- function(pm_ids) {
  
  if (is.null(pm_ids[[1]])) { 
    0
  } else {
    fi_ids <- unlist(str_split(pm_ids, "[ ,]"))
    as.integer(sample(fi_ids, 1))
  }
}
