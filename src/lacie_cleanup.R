# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Clean-up LaCie-replays
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger)

config <- read_yaml("config.yaml")
source("src/functions.R", encoding = "UTF-8")

# qfn_log <- path_join(c("C:", "Users", "nipper", "Logs", "woj_schedules.log"))
# lg_ini <- flog.appender(appender.file(qfn_log), "wojsch")
# flog.info("= = = = = START - WoJ Schedules, version 2024-04-02 20:50 = = = = =", name = "wojsch")

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
    return("hi_google_not_ok")
  }
)

if (hi_google != "hi_google_ok") {
  stop("ended abmormally")
}

# init sheet identifiers
gs_home <- "https://docs.google.com/spreadsheets/d/"
download_home <- "C:/Users/nipper/Downloads/cz_downloads/"

# load sdj-list ----
get_sdj_list <- tryCatch(
  {
    sdj_list <- "1d8t8ZItwfBpdVrB9lyc83-F8NysdHRDpgnw-TJ9G2ng"
    tib_sdj_list_a <- read_sheet(paste0(gs_home, sdj_list), sheet = "Three of a Kind")
    tib_sdj_list_b <- read_sheet(paste0(gs_home, sdj_list), sheet = "Mazen")
    tib_sdj_list_c <- read_sheet(paste0(gs_home, sdj_list), sheet = "Duke Ellington")
    tib_sdj_list_d <- read_sheet(paste0(gs_home, sdj_list), sheet = "House of Hard Bop")
    tib_sdj_list_e <- read_sheet(paste0(gs_home, sdj_list), sheet = "Vocale Jazz")
    tib_sdj_list_f <- read_sheet(paste0(gs_home, sdj_list), sheet = "Jazz Piano")
    tib_sdj_list_g <- read_sheet(paste0(gs_home, sdj_list), sheet = "Groove & Grease")
    tib_sdj_list_h <- read_sheet(paste0(gs_home, sdj_list), sheet = "Ratatouille")
    "get_sdj_ok"
  },
  error = function(cond) {
    return("get_sdj_not_ok")
  }
)

if (get_sdj_list != "get_sdj_ok") {
  stop("ended abmormally")
}

tib_sdj_list.1 <- tib_sdj_list_a |> 
  bind_rows(tib_sdj_list_b) |> 
  bind_rows(tib_sdj_list_c) |> 
  bind_rows(tib_sdj_list_d) |> 
  bind_rows(tib_sdj_list_e) |> 
  bind_rows(tib_sdj_list_f) |> 
  bind_rows(tib_sdj_list_g) |> 
  bind_rows(tib_sdj_list_h) |> filter(!is.na(Titel))

tib_sdj_list.2 <- tib_sdj_list.1 |> 
  mutate(bc_orig_chr = if_else(!is.na(`Oorspronkelijke uitzenddatum`),
                               paste0(`Oorspronkelijke uitzenddatum`, " ", str_sub(Tijd, 1, 2), ":00"),
                               "XX"))

tib_sdj_list.3 <- tib_sdj_list.2 |> 
  mutate(bc_orig_ymd = if_else(bc_orig_chr == "XX", `Oorspronkelijke uitzenddatum & tijd`, ymd_hm(bc_orig_chr, quiet = T)),
         bc_title = Titel,
         bc_orig_id = GidsID,
         bc_woj_ymd = ymd_hm(paste0(`Nieuwe uitzenddatum`, " ", str_sub(`Nieuwe tijd`, 1, 2), ":00"), quiet = T), 
         bc_woj_audio = `Nieuwe file naam (copy-paste)`,
         bc_audio_done = `Audio gedaan?`,
         bc_sched_done = `Gids gedaan?`) |> 
  select(starts_with("bc_"), -bc_orig_chr)

# load la-list ----
get_la_list <- tryCatch(
  {
    la_list <- "1esOkxVXLxTTg92yt8s5mR_VmrbPX5LFup_O3DRwUrrY"
    tib_la_list_a <- read_sheet(paste0(gs_home, la_list), sheet = "woj herhalingen 3.0")
    "get_la_ok"
  },
  error = function(cond) {
    return("get_la_not_ok")
  }
)

if (get_la_list != "get_la_ok") {
  stop("ended abmormally")
}

# combine with sdj-list ----
tib_lacie_replays <- tib_sdj_list.3 |> left_join(tib_la_list_a, join_by(bc_orig_id == pgmID))

needed_bcs <- tib_lacie_replays |> 
  mutate(bc_title = str_to_lower(bc_title),
         bc_orig_usable = if_else(!is.na(pgmStart), T, F),
         bc_review = case_when(bc_audio_done & bc_orig_usable ~ paste0("keep ", bc_woj_audio), 
                               bc_audio_done & !bc_orig_usable ~ paste0("remove ", bc_woj_audio),
                               !bc_audio_done & bc_orig_usable ~ "usable",
                               T ~ "not usable")) |> 
  select(bc_woj_ymd, bc_title, bc_orig_ymd, bc_orig_id, bc_orig_usable, bc_audio_done, bc_review)

usable_bc_origs <- needed_bcs |> filter(bc_review == "usable") |> 
  select(bc_title, bc_orig_ymd, bc_orig_id)

needed_bcs_new <- needed_bcs[1,] |> mutate(bc_orig_ymd_new = bc_orig_ymd,
                                           bc_orig_id_new = bc_orig_id) |> slice(-1) 

# for (b1 in 1:400) {
for (b1 in seq_along(needed_bcs$bc_woj_ymd)) {
  cat("b1 =", b1)
  cur_bc <- needed_bcs[b1,] |> mutate(bc_orig_ymd_new = bc_orig_ymd, bc_orig_id_new = bc_orig_id)
  cat(", title = ", cur_bc$bc_title, "usable_bc_origs = ", nrow(usable_bc_origs), "\n")
  
  cur_usables <- usable_bc_origs |> filter(bc_title == cur_bc$bc_title)
  
  replace_id <- case_when(cur_bc$bc_audio_done && cur_bc$bc_orig_usable ~ F, 
                          cur_bc$bc_audio_done && !cur_bc$bc_orig_usable ~ T,
                          !cur_bc$bc_audio_done && cur_bc$bc_orig_usable &&
                            !cur_bc$bc_orig_id %in% cur_usables$bc_orig_id ~ T,
                          T ~ T)
    
  if (replace_id) {
    cat("cur usables =", nrow(cur_usables), "\n")
    
    if (nrow(cur_usables) == 0) {
      cur_bc$bc_orig_ymd_new <- NA_Date_
      cur_bc$bc_orig_id_new <- 0
      
    } else {
      cur_bc$bc_orig_ymd_new <- cur_usables$bc_orig_ymd[1]
      cur_bc$bc_orig_id_new <- cur_usables$bc_orig_id[1]
      usable_bc_origs <- usable_bc_origs |> filter(bc_orig_id != cur_usables$bc_orig_id[1])
      cat("popped", cur_usables$bc_orig_id[1], "usable_bc_origs =", nrow(usable_bc_origs), "\n")
    }
  }
  
  needed_bcs_new <- needed_bcs_new |> add_row(cur_bc)
}

tmp_format <- stamp("19690720_zo17", orders = "%Y%m%d %a%H", quiet = T)

needed_bcs_new.1 <- needed_bcs_new |> 
  mutate(bc_review_b = case_when(bc_review == "usable" & bc_orig_id != bc_orig_id_new ~ 
                                   "used as replacement",
                                 bc_review == "usable" ~ "valid, no prepped audio yet",
                                 T ~ bc_review)) |> filter(bc_orig_id_new > 0) |> 
  select(bc_woj_ymd:bc_orig_id, bc_review = bc_review_b, bc_orig_ymd_new, bc_orig_id_new) |> 
  mutate(bc_audio_new = if_else(str_detect(bc_review, "^keep"),
                                NA_character_,
                                paste0(tmp_format(bc_woj_ymd), "_",
                                       str_replace_all(bc_title, " ", "_"),
                                       "_",
                                       bc_orig_id_new)),
         bc_audio_new = str_replace(bc_audio_new, "_&", ""))

# + upload to GD ----
ss <- sheet_write(ss = "1d8t8ZItwfBpdVrB9lyc83-F8NysdHRDpgnw-TJ9G2ng",
                  sheet = "woj_herhalingen_3.0",
                  data = needed_bcs_new.1)
