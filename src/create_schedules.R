# - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Create WoJ Audio Allocation Sheet & Programme Guide
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

# init ----
# init flag variable, to be removed if the script finishes successfully
salsa_source_error <- T

# enter main control loop
repeat {
  
  # say "Hi" to Google
  n_errors <- tryCatch(
    {
      drive_auth(email = config$email_from)
      gs4_auth(token = drive_token())
      0L
    },
    error = function(e1) {
      flog.error(sprintf("Failed to connect to GoogleDrive: %s", conditionMessage(e1)), name = "wojsch")
      return(1L)
    }
  )
  
  if (n_errors > 0) {
    break
  }
  
  # init sheet identifiers
  gs_home <- "https://docs.google.com/spreadsheets/d/"
  download_home <- "C:/Users/nipper/Downloads/cz_downloads/"
  
  # load WoJ modelrooster ----
  # NB - for some reason, GD gives an error when using 'read_sheet'. So, use 'drive_download' instead.
  moro <- config$ss_woj_modelrooster
  gs_path <- paste0(download_home, "tib_moro.tsv")
  
  n_errors <- tryCatch(
    {
      drive_download(paste0(gs_home, moro), path = gs_path, overwrite = T)
      0L
    },
    error = function(e1) {
      flog.error(sprintf("Failed to download WoJ modelrooster: %s", conditionMessage(e1)), name = "wojsch")
      return(1L)
    }
  )
  
  if (n_errors > 0) {
    break
  }
  
  tib_moro <- read_tsv(file = gs_path, locale = locale(encoding = "UTF-8"), col_types = "iiciiiciccccc") 
  
  #  + . check modelrooster length ----
  if (sum(tib_moro$minutes) != 50400) {
    flog.error("WoJ modelrooster length is wrong; should be 50.400 minutes", name = "wojsch")
    chk2 <- tib_moro |> select(day, week_vd_mnd, minutes) |> group_by(day, week_vd_mnd) |> 
      mutate(day_len = sum(minutes)) |> ungroup() |> select(-minutes) |> distinct() |> 
      filter(day_len != 1440) |> 
      mutate(item = str_flatten_comma(paste(day, as.character(week_vd_mnd)))) |> 
      select(item) |> distinct()
    flog.error(sprintf("errors (day X of week Y): %s", chk2$item), name = "wojsch")
    break
  }
  
  #  + . rec_id's should be unique ----
  tib_moro_dups <- tib_moro |> select(rec_id) |> group_by(rec_id) |> mutate(n = n()) |> filter(n > 1)
  
  if (nrow(tib_moro_dups) > 0) {
    flog.error("WoJ modelrooster has duplicate rec_id's", name = "wojsch")
    break
  }
  
  # load gidsinfo & Lacie-slots ----
  n_errors <- tryCatch(
    {
      tib_gidsinfo <- read_sheet(paste0(gs_home, config$ss_gidsinfo), sheet = "gids-info")
      tib_gidsvertalingen <- read_sheet(paste0(gs_home, config$ss_gidsinfo), sheet = "vertalingen NL-EN")
      tib_lacie_slots <- read_sheet(paste0(gs_home, config$ss_lacie), sheet = "woj_herhalingen_4.0") |> 
        mutate(key_ts = as.integer(bc_woj_ymd))
      0L
    },
    error = function(e1) {
      flog.error(sprintf("Failed to get gidsinfo/vertalingen/LaCie-slots: %s", conditionMessage(e1)), 
                 name = "wojsch")
      return(1L)
    }
  )
  
  if (n_errors > 0) {
    break
  }
  
  # + .any missing? ----
  # make sure all broadcasts have program details
  bcs_without_gi <- tib_moro |> select(broadcast_id, broadcast) |> distinct() |> 
    anti_join(tib_gidsinfo, by = c("broadcast_id" = "woj_bcid"))
  
  if (nrow(bcs_without_gi) > 0) {
    flog.error("some woj broadcasts-id's are missing from wp-gidsinfo", name = "wojsch")
    break
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
    mutate(woj_bcid = as.integer(woj_bcid),
           across(starts_with("genre"), str_to_lower))
  
  # randomly choose one of the featured images
  woj_gidsinfo.2 <- woj_gidsinfo.1 |> filter(!is.na(woj_bcid)) |> rowwise() |> 
    mutate(feat_img_id = woj_pick(feat_img_ids)) |> select(-feat_img_ids) |> ungroup()

  # get title-to-term-ids mapping ----
  # + \ connect WP-db ----  
  wp_conn <- get_wp_conn()
  
  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", cur_db_type), name = "wojsch")
    break
  }
  
  sql_stmt <- "select tr1.slug, 
                      replace(tr1.name, '&amp;', '&') as name_cln, 
                      substr(slug, -2) as lng_code, 
                      tr1.term_id as wp_title_id, 
                      tx1.parent as wp_genre_id 
               from wp_terms tr1 join wp_term_taxonomy tx1 
                                   on tx1.term_id = tr1.term_id
               where tx1.taxonomy = 'programma_genre' 
                 and tx1.parent > 0
                 and slug regexp '__[^-]+-(en|nl)$'
               order by 1;
  "
  tit2ids.1 <- dbGetQuery(wp_conn, sql_stmt)
  tit2ids.2 <- tit2ids.1 |> mutate(db_genre = str_extract(slug, "__(.*)-..$", group = 1),
                                   db_slug = str_extract(slug, "(.*)__.*$", group = 1),
                                   wp_title_id = as.integer(wp_title_id),
                                   wp_genre_id = as.integer(wp_genre_id)) |> 
    select(name_cln, db_genre, lng_code, wp_title_id, wp_genre_id)
  
  woj_gidsinfo.3 <- tibble()

  for (cur_bcid in woj_gidsinfo.2$woj_bcid) {
    
    cur_gi <- woj_gidsinfo.2 |> filter(woj_bcid == cur_bcid)
    
    # woj_id's are looked up in tit2ids.2, where genre names are extracted from wp_terms.slug. The slug has Dutch
    # genre names for both languages. Hence the 3rd parameter in woj_ids() is '-nl' even if the 4th is 'en'
    gi_row <- tribble(
      ~bcid, ~tid_n1, ~gid_n1, ~tid_n2, ~gid_n2, ~tid_e1, ~gid_e1, ~tid_e2, ~gid_e2,
      cur_bcid,
      woj_ids(cur_gi$tit_nl, cur_gi$genre_1_nl, "nl")$wp_title_id,
      woj_ids(cur_gi$tit_nl, cur_gi$genre_1_nl, "nl")$wp_genre_id,
      woj_ids(cur_gi$tit_nl, cur_gi$genre_2_nl, "nl")$wp_title_id,
      woj_ids(cur_gi$tit_nl, cur_gi$genre_2_nl, "nl")$wp_genre_id,
      woj_ids(cur_gi$tit_en, cur_gi$genre_1_nl, "en")$wp_title_id,
      woj_ids(cur_gi$tit_en, cur_gi$genre_1_nl, "en")$wp_genre_id,
      woj_ids(cur_gi$tit_en, cur_gi$genre_2_nl, "en")$wp_title_id,
      woj_ids(cur_gi$tit_en, cur_gi$genre_2_nl, "en")$wp_genre_id)
  
    woj_gidsinfo.3 <- woj_gidsinfo.3 |> bind_rows(gi_row)
  }

  woj_gidsinfo.4 <- woj_gidsinfo.2 |> inner_join(woj_gidsinfo.3, by = join_by(woj_bcid == bcid))
  
  #  + . check id's complete ----
  #  + \ primairy ----
  woj_gidsinfo_err <- woj_gidsinfo.4 |> filter(tid_n1 == -1 | gid_n1 == -1)
  
  if (nrow(woj_gidsinfo_err) > 0) {
    flog.error("Missing title/genre-1 id's", name = "wojsch")
    flog.error(sprintf("errors = %s", str_flatten_comma(woj_gidsinfo_err$tit_nl)), name = "wojsch")
    break
  }
  #  + \ secondary ----
  woj_gidsinfo_err <- woj_gidsinfo.4 |> filter(!is.na(genre_2_nl) & (tid_n2 == -1 | gid_n2 == -1))
  
  if (nrow(woj_gidsinfo_err) > 0) {
    flog.error("Missing title/genre-2 id's", name = "wojsch")
    flog.error(sprintf("errors = %s", str_flatten_comma(woj_gidsinfo_err$tit_nl)), name = "wojsch")
    break
  }
  
  # add 'modelrooster' to week ----
  cz_week_sched.1 <- cz_week_slots |> inner_join(tib_moro, by = join_by(day, week_vd_mnd, start))
  
  #  + . check schedule length ----
  if (sum(cz_week_sched.1$minutes, na.rm = T) != 10080) {
    chk1 <- cz_week_sched.1 |> select(slot_ts, start, minutes) |> 
      mutate(stop = slot_ts + minutes(minutes), sync = lead(slot_ts) == stop) |> filter(!sync) 
    flog.error("CZ-week + modelrooster length is wrong (A); should be 10.080 minutes", name = "wojsch")
    flog.error(sprintf("errors = %s", str_flatten_comma(format(chk1$slot_ts, "%Y-%m-%d_%a%Hu"))), 
               name = "wojsch")
    break
  }
  
  # combine with 'wp-gidsinfo' ----
  cz_week_sched.2 <- cz_week_sched.1 |> 
    inner_join(woj_gidsinfo.4, by = join_by(broadcast_id == woj_bcid))
  
  #  + . check schedule length ----
  if (sum(cz_week_sched.2$minutes) != 10080) {
    flog.error("CZ-week + gidsinfo length is wrong (B); should be 10.080 minutes", name = "wojsch")
    break
  }
  
  # prepare Universe rewinds ----
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
    flog.error("CZ-week + replays length is wrong (C); should be 10.080 minutes", name = "wojsch")
    break
  }
  
  # + . save it ----
  # so 'add_ml_tracklists_to_wp' can use it later
  write_rds(cz_week_sched.3, config$wj_gidsweek_backup)
  
  # WoJ Audio Allocation Sheet ----
  flog.info("create audio allocation sheet", name = "wojsch")
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
  plw_sheet_name <- format(cz_week_start, "plw_%Y-%m-%d")
  ss <- sheet_write(ss = config$ss_wj_playlistweek,
                    sheet = plw_sheet_name,
                    data = plw_items)
  
  # + create checkboxes ----
  rule_checkbox <- googlesheets4:::new(
    "DataValidationRule",
    condition = googlesheets4:::new_BooleanCondition(type = "BOOLEAN"),
    inputMessage = "Lorem ipsum dolor sit amet",
    strict = TRUE,
    showCustomUi = TRUE
  )
  
  googlesheets4:::range_add_validation(
    ss = config$ss_wj_playlistweek, 
    range = paste0(plw_sheet_name, "!D2:D"), 
    rule = rule_checkbox
  )
  
  # + store local copy ----
  woj_playlists_fn <- paste0("WoJ_playlistweek_", format(cz_week_start, "%Y_%m_%d"), ".tsv")
  woj_playlists_qfn <- path_join(c(config$home_playlistweek_backup, woj_playlists_fn))
  write_tsv(plw_items, woj_playlists_qfn)
  
  flog.info("WJ-playistweek has been backed up and replaced on GD", name = "wojsch")
  
  # WoJ Programme Guide ----
  flog.info("create programme guide", name = "wojsch")
  
  # . + prep json ----
  # . \ originals with 1 genre ----
  tib_json_ori_gen1 <- cz_week_sched.3 |> filter(is.na(ts_rewind) & is.na(genre_2_nl)) |> 
    select(slot_ts, minutes, prod_taak_nl:prod_mdw, txt_nl:gid_e2, -ends_with("2"), broadcast_id) |> 
    mutate(obj_name = fmt_utc_ts(slot_ts), 
           stop = format(slot_ts + minutes(minutes), "%Y-%m-%d %H:%M"),
           start = format(slot_ts, "%Y-%m-%d %H:%M"),
           `post-type` = "programma_woj",
           `upload-batch` = paste("bcid", broadcast_id, sep = "-")) |> 
    select(obj_name, `post-type`, feat_img_id, start, stop, everything(), 
           -minutes, -slot_ts, -broadcast_id) |> 
    rename(`titel-genre-1-nl` = tid_n1,
           `titel-genre-1-en` = tid_e1,
           `genre-1-nl` = gid_n1,
           `genre-1-en` = gid_e1,
           `samenvatting-nl` = txt_nl,
           `samenvatting-en` = txt_en,
           `featured-image` = feat_img_id,
           `productie-1-taak-nl` = prod_taak_nl,
           `productie-1-taak-en` = prod_taak_en,
           `productie-1-mdw` = prod_mdw)
  
  json_ori_gen1 <- woj2json(tib_json_ori_gen1) |> 
    str_replace_all(pattern = '    "featured-image": (0|"NA"),\\n', '')
  
  # . \ originals with 2 genres ----
  tib_json_ori_gen2 <- cz_week_sched.3 |> filter(is.na(ts_rewind) & !is.na(genre_2_nl)) |> 
    select(slot_ts, minutes, prod_taak_nl:prod_mdw, txt_nl:gid_e2, broadcast_id) |> 
    mutate(obj_name = fmt_utc_ts(slot_ts), 
           stop = format(slot_ts + minutes(minutes), "%Y-%m-%d %H:%M"),
           start = format(slot_ts, "%Y-%m-%d %H:%M"),
           `post-type` = "programma_woj",
           `upload-batch` = paste("bcid", broadcast_id, sep = "-")) |> 
    select(obj_name, `post-type`, feat_img_id, start, stop, everything(), 
           -minutes, -slot_ts, -broadcast_id) |> 
    rename(`titel-genre-1-nl` = tid_n1,
           `titel-genre-1-en` = tid_e1,
           `titel-genre-2-nl` = tid_n2,
           `titel-genre-2-en` = tid_e2,
           `genre-1-nl` = gid_n1,
           `genre-1-en` = gid_e1,
           `genre-2-nl` = gid_n2,
           `genre-2-en` = gid_e2,
           `samenvatting-nl` = txt_nl,
           `samenvatting-en` = txt_en,
           `featured-image` = feat_img_id,
           `productie-1-taak-nl` = prod_taak_nl,
           `productie-1-taak-en` = prod_taak_en,
           `productie-1-mdw` = prod_mdw)
  
  if (nrow(tib_json_ori_gen2) > 0) {
    json_ori_gen2 <- woj2json(tib_json_ori_gen2) |> 
      str_replace_all(pattern = '    "featured-image": (0|"NA"),\\n', '')
  }
  
  # . \ replays ----
  tib_json_rep <- cz_week_sched.3 |> filter(!is.na(ts_rewind)) |> 
    select(slot_ts, minutes, ts_rewind) |> 
    mutate(obj_name = fmt_utc_ts(slot_ts), 
           stop = format(slot_ts + minutes(minutes), "%Y-%m-%d %H:%M"),
           start = format(slot_ts, "%Y-%m-%d %H:%M"),
           `post-type` = "programma_woj",
           `herhaling-van-post-type` = "programma",
           `herhaling-van` = format(ts_rewind, "%Y-%m-%d %H:%M")) |> 
    select(obj_name, `post-type`, start, stop, everything(), -minutes, -ts_rewind, -slot_ts)
  
  json_rep <- woj2json(tib_json_rep)
  
  # . \ join them ----
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
  
  # . \ store it ----
  final_json_ufn <- paste0("WJ_gidsweek_", format(cz_week_start, "%Y_%m_%d"), ".json")
  write_file(temp_json_file.2, path_join(c(config$home_upload_gidsweek, final_json_ufn)), 
             append = F)
  
  flog.info("WJ-gidsweek is now ready for upload to WP", name = "wojsch")
  
  # exit cleanly from main control loop
  rm(salsa_source_error)
  break
}
  