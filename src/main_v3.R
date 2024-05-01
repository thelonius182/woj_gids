# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create a WoJ "schedule week" & "playlist week"
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger)

config <- read_yaml("config.yaml")
source("src/functions.R", encoding = "UTF-8")

qfn_log <- path_join(c("C:", "Users", "nipper", "Logs", "woj_schedules.log"))
lg_ini <- flog.appender(appender.file(qfn_log), "wojsch")
flog.info("= = = = = START - WoJ Schedules, version 2024-04-02 20:50 = = = = =", name = "wojsch")

# run validations ----
source("src/woj_validations.R", encoding = "UTF-8")

# say "Hi" to Google
hi_google <- tryCatch(
  {
    drive_auth(email = "cz.teamservice@gmail.com")
    gs4_auth(token = drive_token())
    "hi_google_ok"
  },
  error = function(cond) {
    msg <- sprintf("Failed to connect to GD: %s", cond)
    flog.error(msg, name = "wojsch")
    return("error")
  }
)

if (hi_google != "hi_google_ok") {
  stop("ended abmormally")
}

# init sheet identifiers
gs_home <- "https://docs.google.com/spreadsheets/d/"
download_home <- "C:/Users/nipper/Downloads/cz_downloads/"

# load WoJ modelrooster ----
# NB - for some reason, GD gives an error when using 'read_sheet'. So, use 'drive_download' instead.
moro <- "1LdBjK5hcpl8m1ZdcZujWWcsx7nLJPY8Gt69mjN8QS9A"
gs_path <- paste0(download_home, "tib_moro.tsv")

get_moro <- tryCatch(
  {
    drive_download(paste0(gs_home, moro), path = gs_path, overwrite = T)
    "get_moro_ok"
  },
  error = function(cond) {
    msg <- sprintf("Failed to download WoJ modelrooster: %s", cond)
    flog.error(msg, name = "wojsch")
    return("error")
  }
)

if (get_moro != "get_moro_ok") {
  stop("ended abmormally")
}

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

# load gidsinfo & Lacie-slots ----
get_gids <- tryCatch(
  {
    gidsinfo <- "16DrvLEXi3mEa9AbSw28YBkYpCvyO1wXwBwaNi7HpBkA"
    tib_gidsinfo <- read_sheet(paste0(gs_home, gidsinfo), sheet = "gids-info")
    tib_gidsvertalingen <- read_sheet(paste0(gs_home, gidsinfo), sheet = "vertalingen NL-EN")
    lacie_slots <- "1d8t8ZItwfBpdVrB9lyc83-F8NysdHRDpgnw-TJ9G2ng"
    tib_lacie_slots <- read_sheet(paste0(gs_home, lacie_slots), sheet = "woj_herhalingen_4.0") |> 
      mutate(key_ts = as.integer(bc_woj_ymd))
    "get_gids_ok"
  },
  error = function(cond) {
    msg <- sprintf("Failed to get gidsinfo/vertalingen: %s", cond)
    flog.error(msg, name = "wojsch")
    return("error")
  }
)

if (get_gids != "get_gids_ok") {
  stop("ended abmormally")
}

# + .any missing? ----
# make sure all broadcasts have program details
bcs_without_gi <- tib_moro |> select(broadcast_id, broadcast) |> distinct() |> 
  anti_join(tib_gidsinfo, by = c("broadcast_id" = "woj_bcid"))

if (nrow(bcs_without_gi) > 0) {
  stop("some woj broadcasts-id's are missing from wp-gidsinfo")
}

# get woj gidsinfo
woj_gidsinfo.1 <- tib_gidsinfo |> 
  select(woj_bcid, tit_nl = `titel-NL`, tit_en = `titel-EN`, prod_taak_nl = `productie-1-taak`, 
         prod_mdw = `productie-1-mdw`, genre_1_nl = `genre-1-NL`, genre_2_nl = `genre-2-NL`,
         txt_nl = `std.samenvatting-NL`, txt_en = `std.samenvatting-EN`, feat_img_ids) |> 
  arrange(woj_bcid) |> 
  left_join(tib_gidsvertalingen, by = c("prod_taak_nl" = "item-NL")) |> 
  rename(prod_taak_en = `item-EN`) |> 
  left_join(tib_gidsvertalingen, by = c("genre_1_nl" = "item-NL")) |> 
  rename(genre_1_en = `item-EN`) |> 
  left_join(tib_gidsvertalingen, by = c("genre_2_nl" = "item-NL")) |> 
  rename(genre_2_en = `item-EN`) |> 
  select(woj_bcid, tit_nl, tit_en, prod_taak_nl, prod_taak_en, prod_mdw, 
         genre_1_nl, genre_1_en, genre_2_nl, genre_2_en, txt_nl, txt_en, feat_img_ids) |> 
  mutate(woj_bcid = as.integer(woj_bcid)
         # feat_img_ids = if_else(is.null(feat_img_ids), list("0"), feat_img_ids)
         )

# randomly choose one of the featured images
woj_gidsinfo <- woj_gidsinfo.1 |> rowwise() |> 
  mutate(feat_img_id = woj_pick(feat_img_ids)) |> select(-feat_img_ids) |> ungroup()

# create time series ----
# cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
# cz_week_start <- ymd_hm("2024-04-04 13:00")
cz_week_start <- get_czweek_start() |> ymd_hms(quiet = T)
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

# prepare Universe rewinds ----
wp_conn <- get_wp_conn()

if (typeof(wp_conn) != "S4") {
  stop("db-connection failed")
}

qry <- "select po1.post_date as wpdmp_slot_ts, 
       replace(po1.post_title, '&amp;', '&') as wpdmp_slot_title
from wp_posts po1 
left join wp_term_relationships tr1 ON tr1.object_id = po1.id                   
left join wp_term_taxonomy tx1 ON tx1.term_taxonomy_id = tr1.term_taxonomy_id   
where length(trim(po1.post_title)) > 0 
  and po1.post_type = 'programma' 
  and tx1.term_taxonomy_id = 5 
  and po1.post_date > date_add(now(), interval -180 day)
order by po1.post_date desc
;"

fmt_slot_day <- stamp("zo19", orders = "%a%H", quiet = T)
universe_rewinds <- dbGetQuery(wp_conn, qry) |> mutate(slot_day = fmt_slot_day(ymd_hms(wpdmp_slot_ts))) |> 
  # CZ-live Wereld only
  filter(wpdmp_slot_title != "Concertzender Live" | slot_day == "vr22") |> 
  mutate(wpdmp_slot_title = str_to_lower(wpdmp_slot_title))

discon_result <- dbDisconnect(wp_conn)

# add rewinds to the schedule
cz_week_sched.3a <- cz_week_sched.2 |> rowwise() |> 
  mutate(ts_rewind = list(get_ts_rewind(cz_week_start, slot_ts, str_to_lower(tit_nl), 
                                        broadcast_type, live))) |> 
  unnest_wider(ts_rewind) |> select(slot_ts, minutes, ts_rewind, audio_src, everything()) |> ungroup() |> 
  mutate(key_ts = as.integer(slot_ts))

# join lacie-slots
cz_week_sched.3 <- cz_week_sched.3a |> left_join(tib_lacie_slots, join_by(key_ts)) |> 
  rename(audio_file = `audio file`) |> 
  mutate(audio_file = if_else(!is.na(audio_file), 
                              audio_file,
                              str_replace(bc_review, "keep |remove ", "")),
         replay = round_date(replay, "hour"),
         ts_rewind = if_else(is.na(ts_rewind) & !is.na(replay),
                             replay,
                             ts_rewind)) |> 
  select(-c(key_ts:bc_review), -bc_orig_id_new) 

#  + . check schedule length ----
if (sum(cz_week_sched.3$minutes) != 10080) {
  stop("CZ-week + replays length is wrong; should be 10.080 minutes")
}

# save it, so 'update_gids' can use it later
write_rds(cz_week_sched.3, "C:/Users/nipper/cz_rds_store/branches/cz_gdrive/wj_gidsweek.RDS")

# WoJ playlistweek ----
plw_items <- cz_week_sched.3 |> 
  filter(broadcast_type != "NonStop") |> 
  select(slot_ts, broadcast_id, tit_nl, broadcast_type, ts_rewind, 
         audio_src, mac, live_op_universe = live, audio_file) |> 
  mutate(live_op_universe = if_else(broadcast_type == "WorldOfJazz", "nvt", live_op_universe)) |>
  mutate(uitzending = format(slot_ts, "%Y-%m-%d_%a%Hu"),
         titel = tit_nl,
         universe_slot = format(ts_rewind, "%Y-%m-%d_%a%Hu"),
         bron = case_when(broadcast_type == "LaCie" ~ 
                            paste0("hernoemde herhaling op WoJ-pc: ", audio_file),
                          broadcast_type == "WorldOfJazz" ~ "originele montage op WoJ-pc",
                          broadcast_type == "Universe" & audio_src == "Universe" ~ 
                            paste0(format(ts_rewind, "%Y-%m-%d_%a%Hu"),
                                   ", originele montage op ",
                                   mac),
                          broadcast_type == "Universe" & audio_src == "HiJack" & live_op_universe == "Y" ~ 
                            paste0(format(ts_rewind, "%Y-%m-%d_%a%Hu"),
                                   ", HiJack op ",
                                   mac),
                          broadcast_type == "Universe" & audio_src == "HiJack" & live_op_universe == "N" ~ 
                            paste0(format(ts_rewind, "%Y-%m-%d_%a%Hu"),
                                   ", HiJack of originele montage op ",
                                   mac),
                          T ~ "todo"),
         gereed = F, bijzonderheden = " ") |> select(uitzending, titel, bron, gereed, bijzonderheden)

# + upload to GD ----
ss <- sheet_write(ss = "1OoQdHgpb6Yr3L7awSt2Ek5oz4TIegQq4YcFUALykB_E",
                  sheet = "plw_items",
                  data = plw_items)

flog.info("WJ-playistweek has been replaced on GD", name = "wojsch")

# + store local copy ----
woj_playlists_fn <- paste0("WoJ_playlistweek_", format(cz_week_start, "%Y_%m_%d"), ".tsv")
woj_playlists_qfn <- path_join(c("C:", "Users", "nipper", "Documents", "BasieBeats", woj_playlists_fn))
write_tsv(plw_items, woj_playlists_qfn)

# WoJ gidsweek ----
# . + prep json ----
# . + originals with 1 genre ----
tib_json_ori_gen1 <- cz_week_sched.3 |> filter(is.na(ts_rewind) & is.na(genre_2_nl)) |> 
  select(obj_name = slot_ts, start = slot_ts, minutes, tit_nl:txt_en, feat_img_id, 
         -genre_2_nl, -genre_2_en) |> 
  mutate(obj_name = fmt_utc_ts(obj_name), 
         stop = format(start + minutes(minutes), "%Y-%m-%d %H:%M"),
         start = format(start, "%Y-%m-%d %H:%M"),
         `post-type` = "programma_woj") |> 
  select(obj_name, `post-type`, feat_img_id, start, stop, everything(), -minutes) |> 
  rename(`titel-nl` = tit_nl,
         `titel-en` = tit_en,
         `genre-1-nl` = genre_1_nl,
         `genre-1-en` = genre_1_en,
         `std.samenvatting-nl` = txt_nl,
         `std.samenvatting-en` = txt_en,
         `featured-image` = feat_img_id,
         `productie-1-taak-nl` = prod_taak_nl,
         `productie-1-taak-en` = prod_taak_en,
         `productie-1-mdw` = prod_mdw)

json_ori_gen1 <- woj2json(tib_json_ori_gen1) |> 
  str_replace_all(pattern = '    "featured-image": 0,\\n', '')

# . + originals with 2 genres ----
tib_json_ori_gen2 <- cz_week_sched.3 |> filter(is.na(ts_rewind) & !is.na(genre_2_nl)) |> 
  select(obj_name = slot_ts, start = slot_ts, minutes, tit_nl:txt_en, feat_img_id) |> 
  mutate(obj_name = fmt_utc_ts(obj_name), 
         stop = format(start + minutes(minutes), "%Y-%m-%d %H:%M"),
         start = format(start, "%Y-%m-%d %H:%M"),
         `post-type` = "programma_woj") |> 
  select(obj_name, `post-type`, feat_img_id, start, stop, everything(), -minutes) |> 
  rename(`titel-nl` = tit_nl,
         `titel-en` = tit_en,
         `genre-1-nl` = genre_1_nl,
         `genre-1-en` = genre_1_en,
         `genre-2-nl` = genre_2_nl,
         `genre-2-en` = genre_2_en,
         `std.samenvatting-nl` = txt_nl,
         `std.samenvatting-en` = txt_en,
         `featured-image` = feat_img_id,
         `productie-1-taak-nl` = prod_taak_nl,
         `productie-1-taak-en` = prod_taak_en,
         `productie-1-mdw` = prod_mdw)

if (nrow(tib_json_ori_gen2) > 0) {
  json_ori_gen2 <- woj2json(tib_json_ori_gen2) |> 
    str_replace_all(pattern = '    "featured-image": 0,\\n', '')
}

# . + replays ----
tib_json_rep <- cz_week_sched.3 |> filter(!is.na(ts_rewind)) |> 
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

if (nrow(tib_json_ori_gen2) > 0) {
  write_file(json_ori_gen2, cz_week_json_qfn, append = T)
}

write_file(json_rep, cz_week_json_qfn, append = T)

# the file still has 3 intact json-objects. Remove the inner boundaries to make it a single object
temp_json_file.1 <- read_file(cz_week_json_qfn)
temp_json_file.2 <- temp_json_file.1 |> str_replace_all("[}][{]", ",")

# . + store it ----
final_json_qfn <- paste0("WJ_gidsweek_", format(cz_week_start, "%Y_%m_%d"), ".json")
write_file(temp_json_file.2, path_join(c("C:", "cz_salsa", "gidsweek_uploaden", final_json_qfn)), 
           append = F)

flog.info("WJ-gidsweek is now ready for upload to WP", name = "wojsch")
flog.info("= = = = =  FIN  = = = = = = = = = = = = = = = = = = = = = = = = = =", name = "wojsch")
