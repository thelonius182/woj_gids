# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create a WoJ "schedule week" (WP-gids)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite)

source("src/functions.R", encoding = "UTF-8")
config <- read_yaml("config.yaml")
qry_rewind_post <- read_file("src/sql_stmt_prv_post_v3.txt")

# say "Hi" to Google
drive_auth(email = "cz.teamservice@gmail.com")
gs4_auth(token = drive_token())

# init sheet identifiers
gs_home <- "https://docs.google.com/spreadsheets/d/"
download_home <- "C:/Users/nipper/Downloads/cz_downloads/"

# load WoJ modelrooster ----
# NB - for some reason, GD gives an error when using 'read_sheet'. So, use 'drive_download' instead.
moro <- "1LdBjK5hcpl8m1ZdcZujWWcsx7nLJPY8Gt69mjN8QS9A"
gs_path <- paste0(download_home, "tib_moro.tsv")
drive_download(paste0(gs_home, moro), path = gs_path, overwrite = T)
tib_moro <- read_tsv(file = gs_path, locale = locale(encoding = "UTF-8"), col_types = "iiciiiciccccc") 

#  + . check modelrooster length ----
if (sum(tib_moro$minutes) != 50400) {
  stop("WoJ modelrooster length is wrong; should be 50.400 minutes")
}

#  + . rec_id's should be unique ----
tib_moro_dups <- tib_moro |> select(rec_id) |> group_by(rec_id) |> mutate(n = n()) |> filter(n > 1)

if (nrow(tib_moro_dups) > 0) {
  stop("WoJ modelrooster has duplicate rec_id's")
}

# load gidsinfo ----
gidsinfo <- "16DrvLEXi3mEa9AbSw28YBkYpCvyO1wXwBwaNi7HpBkA"
tib_gidsinfo <- read_sheet(paste0(gs_home, gidsinfo), sheet = "gids-info")
tib_gidsvertalingen <- read_sheet(paste0(gs_home, gidsinfo), sheet = "vertalingen NL-EN")

# + .any missing? ----
# make sure all broadcasts have a translation
bcs_without_gi <- tib_moro |> select(broadcast_id, broadcast) |> distinct() |> 
  anti_join(tib_gidsinfo, by = c("broadcast_id" = "woj_bcid"))

if (nrow(bcs_without_gi) > 0) {
  stop("some woj broadcasts-id's are missing from wp-gidsinfo")
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

# create time series ----
# cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
cz_week_start <- ymd_hm("2024-03-28 13:00")
cz_week_slots <- slot_sequence(cz_week_start)

# combine with 'modelrooster' ----
cz_week_sched.1 <- cz_week_slots |> inner_join(tib_moro, by = join_by(day, week_vd_mnd, start))

#  + . check schedule length ----
if (sum(cz_week_sched.1$minutes) != 10080) {
  stop("CZ-week + modelrooster length is wrong; should be 10.080 minutes")
}

# combine with 'wp-gidsinfo' ----
cz_week_sched.2 <- cz_week_sched.1 |> inner_join(woj_gidsinfo, by = join_by(broadcast_id == woj_bcid))

#  + . check schedule length ----
if (sum(cz_week_sched.2$minutes) != 10080) {
  stop("CZ-week + gidsinfo length is wrong; should be 10.080 minutes")
}

# prepare rewinds ----
# - the list of universe broadcasts to share with woj. Use the list from nipper-studio
ns_weken <- read_rds("C:/cz_salsa/cz_exchange/nipperstudio_week.RDS")

# add the corresponding broadcast-id's from wp-gidsinfo
tib_bcid <- ns_weken |> 
  left_join(tib_gidsinfo, by = join_by(cz_slot_value == `key-modelrooster`)) |> 
  filter(!is.na(woj_bcid)) |> 
  select(woj_bcid, slot_ts = date_time, slot_key = cz_slot_value, slot_title_NL = `titel-NL`) |> 
  arrange(woj_bcid, slot_ts)

# find any updates to the nipper-studio list since its publication
wp_conn <- get_wp_conn("dev")

if (typeof(wp_conn) != "S4") {
  stop("db-connection failed")
}

qry <- "truncate table salsa_bcid_titles"
qry_result <- dbExecute(wp_conn, qry)
db_wrt_sts <- dbWriteTable(wp_conn, "salsa_bcid_titles", tib_bcid, row.names = F, overwrite = T)

qry <- "select bt.*, replace(po1.post_title, '&amp;', '&') as title_wp
from salsa_bcid_titles bt 
left join wp_posts po1 on po1.post_date = bt.slot_ts
left join wp_term_relationships tr1 ON tr1.object_id = po1.id                   
left join wp_term_taxonomy tx1 ON tx1.term_taxonomy_id = tr1.term_taxonomy_id   
where po1.post_type = 'programma' and tx1.term_taxonomy_id = 5 
--  and bt.slot_title_NL != replace(po1.post_title, '&amp;', '&')
;"
wp_slot_check <- dbGetQuery(wp_conn, qry) |> mutate(title_wp = str_replace(title_wp, "&amp;", "&"))

dbDisconnect(wp_conn)

# remember which replacements to remove
tib_bcid_del <- wp_slot_check |> filter(str_to_lower(slot_title_NL) != str_to_lower(title_wp)) |> 
  mutate(slot_ts = ymd_hms(slot_ts)) |>  select(slot_ts) |> arrange(slot_ts)

# only keep replacements in universe-shared
tib_bcid_title <- tib_bcid |> select(slot_title_NL, woj_bcid, slot_key) |> distinct()

wp_slot_check.1 <- wp_slot_check |> filter(str_to_lower(slot_title_NL) != str_to_lower(title_wp)) |> 
  left_join(tib_bcid_title, by = join_by(title_wp == slot_title_NL), 
            keep = T, relationship = "many-to-many") |> 
  filter(!is.na(slot_title_NL.y))

wp_slot_check_multiple <- wp_slot_check.1 |> group_by(title_wp) |> 
  mutate(n = n()) |> filter(n == 1) |> ungroup()

# make the replcaement
tib_bdc_new <- wp_slot_check_multiple |> 
  select(woj_bcid = woj_bcid.y, slot_ts, slot_key = slot_key.y, slot_title_NL = slot_title_NL.y) |> 
  mutate(slot_ts = ymd_hms(slot_ts))

# join the lot - this is the final version to get the replays from
universe_rewinds <- tib_bcid |> anti_join(tib_bcid_del, join_by(slot_ts)) |> bind_rows(tib_bdc_new) |> 
  arrange(woj_bcid)

# add rewinds to the schedule
cz_week_sched.3 <- cz_week_sched.2 |> rowwise() |> 
  mutate(ts_rewind = list(get_ts_rewind(cz_week_start, slot_ts, broadcast_id, broadcast_type, live))) |> 
  unnest_wider(ts_rewind)

#  + . check schedule length ----
if (sum(cz_week_sched.3$minutes) != 10080) {
  stop("CZ-week + replays length is wrong; should be 10.080 minutes")
}

# create the WoJ playlist week schema
source("src/schema_playlistweek.R", encoding = "UTF-8")

# prep json ----
# . + originals with 1 genre ----
tib_json_ori_gen1 <- cz_week_sched.5 |> filter(is.na(ts_rewind) & is.na(genre_2_nl)) |> 
  select(obj_name = slot_ts, start = slot_ts, minutes, tit_nl:txt_en, -genre_2_nl, -genre_2_en) |> 
  mutate(obj_name = fmt_utc_ts(obj_name), 
         stop = format(start + minutes(minutes), "%Y-%m-%d %H:%M"),
         start = format(start, "%Y-%m-%d %H:%M"),
         `post-type` = "programma_woj") |> 
  select(obj_name, `post-type`, start, stop, everything(), -minutes) |> 
  rename(`titel-nl` = tit_nl,
         `titel-en` = tit_en,
         `genre-1-nl` = genre_1_nl,
         `genre-1-en` = genre_1_en,
         `std.samenvatting-nl` = txt_nl,
         `std.samenvatting-en` = txt_en,
         `productie-1-taak-nl` = prod_taak_nl,
         `productie-1-taak-en` = prod_taak_en,
         `productie-1-mdw` = prod_mdw)

json_ori_gen1 <- woj2json(tib_json_ori_gen1)

# . + originals with 2 genres ----
tib_json_ori_gen2 <- cz_week_sched.5 |> filter(is.na(ts_rewind) & !is.na(genre_2_nl)) |> 
  select(obj_name = slot_ts, start = slot_ts, minutes, tit_nl:txt_en) |> 
  mutate(obj_name = fmt_utc_ts(obj_name), 
         stop = format(start + minutes(minutes), "%Y-%m-%d %H:%M"),
         start = format(start, "%Y-%m-%d %H:%M"),
         `post-type` = "programma_woj") |> 
  select(obj_name, `post-type`, start, stop, everything(), -minutes) |> 
  rename(`titel-nl` = tit_nl,
         `titel-en` = tit_en,
         `genre-1-nl` = genre_1_nl,
         `genre-1-en` = genre_1_en,
         `genre-2-nl` = genre_2_nl,
         `genre-2-en` = genre_2_en,
         `std.samenvatting-nl` = txt_nl,
         `std.samenvatting-en` = txt_en,
         `productie-1-taak-nl` = prod_taak_nl,
         `productie-1-taak-en` = prod_taak_en,
         `productie-1-mdw` = prod_mdw)

json_ori_gen2 <- woj2json(tib_json_ori_gen2)

# . + replays ----
tib_json_rep <- cz_week_sched.5 |> filter(!is.na(ts_rewind)) |> 
  select(obj_name = slot_ts, start = slot_ts, minutes, ts_rewind) |> 
  mutate(obj_name = fmt_utc_ts(obj_name), 
         stop = format(start + minutes(minutes), "%Y-%m-%d %H:%M"),
         start = format(start, "%Y-%m-%d %H:%M"),
         `post-type` = "programma_woj",
         `herhaling-van-post-type` = "programma",
         `herhaling-van` = format(ts_rewind, "%Y-%m-%d %H:%M")) |> 
  select(obj_name, `post-type`, start, stop, everything(), -minutes, -ts_rewind)

json_rep <- woj2json(tib_json_rep)

# . + join them ----
cz_week_json_qfn <- file_temp(pattern = "cz_week_json", ext = "json")
file_create(cz_week_json_qfn)
write_file(json_ori_gen1, cz_week_json_qfn, append = F)
write_file(json_ori_gen2, cz_week_json_qfn, append = T)
write_file(json_rep, cz_week_json_qfn, append = T)

# the file still has 3 intact json-objects. Remove the inner boundaries to make it a single object
temp_json_file.1 <- read_file(cz_week_json_qfn)
temp_json_file.2 <- temp_json_file.1 |> str_replace_all("[}][{]", ",")

# create final json-file ----
final_json_qfn <- paste0("WoJ_gidsweek_", format(cz_week_start, "%Y_%m_%d"), ".json")
write_file(temp_json_file.2, path_join(c("C:", "cz_salsa", "gidsweek_uploaden", final_json_qfn)), 
           append = F)
