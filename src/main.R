# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create a WoJ "schedule week" (WP-gids)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, 
               stringr, yaml, readr, rio, RMySQL, keyring)

source("src/functions.R", encoding = "UTF-8")
config <- read_yaml("config.yaml")
qry_rewind_post <- read_file("src/sql_stmt_prv_post_v3.txt")

# say "Hi" to Google
drive_auth(email = "cz.teamservice@gmail.com")
gs4_auth(token = drive_token())

# init sheet identifiers
gs_home <- "https://docs.google.com/spreadsheets/d/"
download_home <- "C:/Users/nipper/Downloads/cz_downloads/"

# load modelrooster ----
# NB - for some reason, GD gives an error when using 'read_sheet'. So, use 'drive_download' instead.
moro <- "1LdBjK5hcpl8m1ZdcZujWWcsx7nLJPY8Gt69mjN8QS9A"
gs_path <- paste0(download_home, "tib_moro.tsv")
drive_download(paste0(gs_home, moro), path = gs_path, overwrite = T)
tib_moro <- read_tsv(file = gs_path, locale = locale(encoding = "UTF-8"), col_types = "iiciiiciccccc") 

# rec_id's should be unique
tib_moro_dups <- tib_moro |> select(rec_id) |> group_by(rec_id) |> mutate(n = n()) |> filter(n > 1)

if (nrow(tib_moro_dups) > 0) {
  stop("WoJ modelrooster has duplicate rec_id's")
}

# load gidsinfo ----
gidsinfo <- "16DrvLEXi3mEa9AbSw28YBkYpCvyO1wXwBwaNi7HpBkA"
tib_gidsinfo <- read_sheet(paste0(gs_home, gidsinfo), sheet = "gids-info")
tib_gidsvertalingen <- read_sheet(paste0(gs_home, gidsinfo), sheet = "vertalingen NL-EN")

# + .any missing? ----
# make sure all broadcasts have a gidsvertaling
bcs_without_gi <- tib_moro |> select(broadcast_id, broadcast) |> distinct() |> 
  anti_join(tib_gidsinfo, by = c("broadcast_id" = "woj_bcid"))

if (nrow(bcs_without_gi) > 0) {
  stop("some woj broadcasts are missing from wp-gidsinfo")
}

# get woj gidsinfo
woj_gidsinfo <- tib_gidsinfo |> 
  select(woj_bcid, tit_nl = `titel-NL`, tit_en = `titel-EN`, prod_taak_nl = `productie-1-taak`, 
         prod_mdw = `productie-1-mdw`, genre_1_nl = `genre-1-NL`, genre_2_nl = `genre-2-NL`,
         txt_nl = `std.samenvatting-NL`, txt_en = `std.samenvatting-EN`) |> arrange(woj_bcid) |> 
  left_join(tib_gidsvertalingen, by = c("prod_taak_nl" = "item-NL")) |> 
  rename(prod_taak_en = `item-EN`) |> 
  left_join(tib_gidsvertalingen, by = c("genre_1_nl" = "item-NL")) |> 
  rename(genre_1_en = `item-EN`) |> 
  left_join(tib_gidsvertalingen, by = c("genre_2_nl" = "item-NL")) |> 
  rename(genre_2_en = `item-EN`) |> 
  select(woj_bcid, tit_nl, tit_en, prod_taak_nl, prod_taak_en, prod_mdw, 
         genre_1_nl, genre_1_en, genre_2_nl, genre_2_en, txt_nl, txt_en) |> 
  mutate(woj_bcid = as.integer(woj_bcid))

rm(tib_gidsinfo, tib_gidsvertalingen)  

# create time series ----
# cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
cz_week_start <- ymd_hm("2024-03-28 13:00")
cz_week_slots <- slot_sequence(cz_week_start)

# combine with 'modelrooster' ----
cz_week_sched.1 <- cz_week_slots |> inner_join(tib_moro, by = join_by(day, week_vd_mnd, start))

# prep rewinds ----
cz_week_sched.2 <- cz_week_sched.1 |> rowwise() |> 
  mutate(rewind = list(slot_delta(slot_label, parent))) |> unnest_wider(rewind)

# add regular rewinds ----
cz_week_sched.3 <- cz_week_sched.2 |> 
  mutate(ts_rewind = if_else(regular_rewind, 
                             update(slot_ts + days(delta), hour = parse_number(parent)), 
                             NA_Date_))
# add irregular rewinds ----
# . prep gidsinfo
rewind_gidsinfo <- woj_gidsinfo |> filter(!is.na(woj_bcid)) 
# . prep rewinds
cz_week_irr_rew <- cz_week_sched.3 |> filter(!regular_rewind & 0 <= delta & delta < 99) |> 
  select(rec_id, slot_ts, parent, broadcast_id) |> 
  inner_join(rewind_gidsinfo, by = join_by(broadcast_id == woj_bcid))

# coonect to greenhost database, to fetch the previous post
wp_conn <- get_wp_conn("dev")

if (typeof(wp_conn) != "S4") {
  stop("db-connection failed")
}

# SQL uses zo=1, ma=2, etc. R uses ma=1, di=2, etc. Prep a conversion list
r2sql_wday <- c("zo", "ma", "di", "wo", "do", "vr", "za")

cz_week_irr_rew.1 <- cz_week_irr_rew |> rowwise() |> 
  mutate(rewind_ts = list(fetch_rewind_ts(tit_nl, slot_ts, parent))) |> unnest(rewind_ts)

# integrate irregular rewinds
cz_week_sched.4 <- cz_week_sched.3 |> left_join(cz_week_irr_rew.1, by = join_by(rec_id))
