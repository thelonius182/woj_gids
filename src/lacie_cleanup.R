# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Clean-up LaCie-replays
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, fs,
               stringr, yaml, readr, rio, RMySQL, keyring, jsonlite, futile.logger)

config <- read_yaml("config.yaml")
source("src/functions.R", encoding = "UTF-8")

qfn_log <- path_join(c("C:", "Users", "nipper", "Logs", "woj_schedules.log"))
lg_ini <- flog.appender(appender.file(qfn_log), "wojsch")
flog.info("= = = = = START - WoJ Schedules, version 2024-04-02 20:50 = = = = =", name = "wojsch")

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
  bind_rows(tib_sdj_list_h) |> filter(!is.na(Titel)) |> select(-`Audio gedaan?`, -`Gids gedaan?`)

tib_sdj_list.2 <- tib_sdj_list.1 |> 
  mutate(bc_orig_chr = if_else(!is.na(`Oorspronkelijke uitzenddatum`),
                               paste0(`Oorspronkelijke uitzenddatum`, " ", str_sub(Tijd, 1, 2), ":00"),
                               "XX"))

tib_sdj_list.3 <- tib_sdj_list.2 |> 
  mutate(bc_orig_ymd = if_else(bc_orig_chr == "XX", `Oorspronkelijke uitzenddatum & tijd`, ymd_hm(bc_orig_chr, quiet = T)),
         bc_title = Titel,
         bc_orig_id = GidsID,
         bc_woj_ymd = ymd_hm(paste0(`Nieuwe uitzenddatum`, " ", str_sub(`Nieuwe tijd`, 1, 2), ":00"), quiet = T), 
         bc_woj_audio = `Nieuwe file naam (copy-paste)` |> str_replace_all(" ", "_") |> str_to_lower()) |> 
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

# check sdj-list ----
delta_list <- tib_sdj_list.3 |> anti_join(tib_la_list_a, join_by(bc_orig_ymd == pgmStart))
delta_list.2 <- tib_sdj_list.3 |> anti_join(tib_la_list_a, join_by(bc_orig_id == pgmID))
tib_lacie_replays <- tib_sdj_list.3 |> left_join(tib_la_list_a, join_by(bc_orig_id == pgmID))

# + upload to GD ----
ss <- sheet_write(ss = "1d8t8ZItwfBpdVrB9lyc83-F8NysdHRDpgnw-TJ9G2ng",
                  sheet = "woj_herhalingen_3.0",
                  data = tib_lacie_replays)

