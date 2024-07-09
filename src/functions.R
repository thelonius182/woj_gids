
# TO VALIDATE CONN:
# sqlstmt <- "show variables like 'character_set_client'"
# result <- dbGetQuery(conn = wp_conn, statement = sqlstmt)
get_wp_conn <- function() {


  db_port <- 3306
  # flog.appender(appender.file("/Users/nipper/Logs/nipper.log"), name = "nipperlog")

  grh_conn <- tryCatch(
    {
      dbConnect(drv = MySQL(), user = db_user, password = db_password,
                dbname = db_name, host = db_host, port = db_port)
    },
    error = function(cond) {
      return("connection-error")
    }
  )

  return(grh_conn)
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

# build a list of all required slots for either the latest existing week or for a new one
slot_sequence_wk <- function(new_week = F) {
  
  # calculate start-ts based on the latest json file
  qfns <- dir_ls(path = "C:/cz_salsa/gidsweek_uploaden/", 
                 type = "file",
                 regexp = "WJ_gidsweek_\\d{4}.*[.]json$") |> sort(decreasing = T)
  
  seq_start_ts <- str_extract(qfns[1], pattern = "\\d{4}_\\d{2}_\\d{2}") |> ymd(quiet = T)
  hour(seq_start_ts) <- 13
  
  # add 7 days if this should be a new week
  if (new_week) seq_start_ts <- seq_start_ts + days(7)
  
  all_day_names <- c("ma", "di", "wo", "do", "vr", "za", "zo")

  seq(from = seq_start_ts, to = seq_start_ts + hours(7 * 24 - 1), by = "hours") |> as_tibble() |> 
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
  
  # WoJ replay-slots only
  if (pm_broadcast_type == "Universe") {
    tib_rewinds <- universe_rewinds
  } else if (pm_broadcast_type == "ReplayWoJ") {
    tib_rewinds <- woj_rewinds
  } else {
    return(list(ts_rewind = NA_Date_, audio_src = NA_character_))
  }
  
  # this week's WoJ live broadcasts need a HiJack-file (previous week)
  if (pm_live == "Y") {
    ymd_upper_limit <- pm_week_start
  } else {
    ymd_upper_limit <- pm_slot_ts
  }
  
  # get the rewind of this broadcast closest to its slot
  cur_ts_rewind <- tib_rewinds |> 
    filter(wpdmp_slot_title == pm_tit_nl & wpdmp_slot_ts < ymd_upper_limit) |> 
    arrange(desc(wpdmp_slot_ts)) |> head(1) |> select(value = wpdmp_slot_ts)
  
  # use a nice but arbitrary replacement if there's no rewind
  if (nrow(cur_ts_rewind) == 0) {
    cur_ts_rewind <- tibble(value = "2024-05-20 20:00:00")
  }
  
  if (cur_ts_rewind$value < pm_week_start) {
    cur_audio_src <- "HiJack"
  } else {
    cur_audio_src <- "WorldOfJazz"
  }
  
  return(list(ts_rewind = ymd_hms(cur_ts_rewind$value), audio_src = cur_audio_src))
}

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

woj_pick <- function(pm_ids) {
  
  if (is.null(pm_ids[[1]])) { 
    0
  } else {
    fi_ids <- unlist(str_split(pm_ids, "[ ,]"))
    as.integer(sample(fi_ids, 1))
  }
}

# convert a list to a query string with proper URL encoding
list_to_query <- function(lst) {
  params <- sapply(names(lst), function(key) {
    
    key_encoded = url_encode_param(key) 
    value_encoded = url_encode_param(lst[[key]])
    paste0(key_encoded, "=", value_encoded)
  })
  
  paste(params, collapse = "&")
}

# Use URLencode with reserved = TRUE to ensure all special characters are encoded
url_encode_param <- function(value) {
  URLencode(as.character(value), reserved = TRUE)
}

get_wp_ds <- function(pm_env) {
  
  if (pm_env == "prd") {
    db_env <- "wpprd_mariadb"
  } else {
    db_env <- "wpdev_mariadb"
  }

  result <- tryCatch( {
    grh_conn <- dbConnect(odbc::odbc(), db_env, timeout = 10)
  },
  error = function(cond) {
    return("connection-error")
  })
  
  return(result)
}

salsa_git_version <- function(qfn_repo) {
  
  repo <- git2r::repository(qfn_repo)
  branch <- git2r::repository_head(repo)$name
  latest_commit <- git2r::commits(repo, n = 1)[[1]]
  commit_author <- latest_commit$author$name
  commit_date <- latest_commit$author$when
  fmt_commit_date <- format(lubridate::with_tz(commit_date, tzone = "Europe/Amsterdam"), "%a %Y-%m-%d, %H:%M")
  
  return(list(git_branch = branch, ts = fmt_commit_date, by = commit_author, path = repo$path))
}
