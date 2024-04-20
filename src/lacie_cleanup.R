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
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# WoJ-replays 4.0
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
qry <- str_replace_all("SELECT distinct
    po1.post_date as bc_start, 
    pm1.meta_value as bc_stop, 
    po1.post_title, 
    tx1.description, 
    te1.slug
FROM
    wp_posts po1
        LEFT JOIN
    wp_term_relationships tr1 ON tr1.object_id = po1.id
        LEFT JOIN
    wp_term_taxonomy tx1 ON tx1.term_taxonomy_id = tr1.term_taxonomy_id
        LEFT JOIN
    wp_terms te1 ON te1.term_id = tr1.term_taxonomy_id
        LEFT JOIN
    wp_postmeta pm1 ON pm1.post_id = po1.id
WHERE
    po1.post_type = 'programma' and po1.post_date > '2011-06-01'
        AND pm1.meta_key = 'pr_metadata_uitzenddatum_end'
        AND (tx1.taxonomy = 'post_translations' AND tx1.description REGEXP '(s:2:@en@.*s:2:@nl@)|(s:2:@nl@.*s:2:@en@)'
             OR tx1.taxonomy = 'programma_genre' AND te1.slug REGEXP '__.*-(nl|en)$')
;", "@", '"')

wp_posts <- dbGetQuery(wp_conn, qry)

wp_posts.1 <- wp_posts |> arrange(bc_start, description, slug)
wp_posts.2 <- wp_posts.1 |> as_tibble() |> select(-post_title) |> distinct() |> 
  group_by(bc_start) |> summarise(n_rows = n()) |> ungroup() |> 
  filter(n_rows %in% c(3, 5, 7))

wp_posts.3 <- wp_posts.1 |> inner_join(wp_posts.2, by = join_by(bc_start)) |> 
  filter(str_detect(slug, "-nl$")) |> group_by(bc_start) |> mutate(g1 = row_number()) |> ungroup() |> 
  filter(g1 == 1) |> mutate(post_title = str_to_lower(post_title) |> str_replace_all("&amp;|&", "and" ))

wp_posts.4 <- wp_posts.3 |> 
  filter(str_detect(post_title, "mazen|jazz piano|vocal|duke|hard bop|groove|three|rata")) |> 
  filter(bc_start <= "2023-08-16 23:59:59") |> select(c(bc_start:post_title))

cur_ss <- read_sheet("1d8t8ZItwfBpdVrB9lyc83-F8NysdHRDpgnw-TJ9G2ng", sheet = "woj_herhalingen_3.0")
cur_ss <- cur_ss |> mutate(bc_orig_ymd_new = round_date(bc_orig_ymd_new, unit = "hour"))

cur_ss_unprepped <- cur_ss |> filter(!bc_prepped)

cur_ss_prepped <- cur_ss |> filter(bc_prepped) |> select(bc_orig_ymd_new) |> 
  mutate(bc_prepped = as.character(bc_orig_ymd_new))

wp_posts.5 <- wp_posts.4 |> anti_join(cur_ss_prepped, join_by(bc_start == bc_prepped)) |> 
  mutate(post_interval = interval(ymd_hms(bc_start), ymd_hms(paste0(bc_stop, ":00"))))

arch_2011 <- read_delim("C:/cz_salsa/cz_exchange/archief_tot_201106-201406.txt",
                                        delim = ":", escape_double = FALSE, col_names = FALSE,
                                        show_col_types = FALSE,
                                        trim_ws = TRUE) |> as_tibble() |> mutate(file = path_file(X1))


arch_2014 <- read_delim("C:/cz_salsa/cz_exchange/Archief_tot_201407-201706.txt",
                                        delim = ":", escape_double = FALSE, col_names = FALSE,
                                        show_col_types = FALSE,
                                        trim_ws = TRUE) |> as_tibble() |> mutate(file = path_file(X1))


arch_2017 <- read_delim("C:/cz_salsa/cz_exchange/Archief_tot_201707-202105.txt",
                                        delim = ":", escape_double = FALSE, col_names = FALSE,
                                        show_col_types = FALSE,
                                        trim_ws = TRUE) |> as_tibble() |> mutate(file = path_file(X1))


arch_2021 <- read_delim("C:/cz_salsa/cz_exchange/Archief_tot_202105-202310.txt",
                                        delim = ":", escape_double = FALSE, col_names = FALSE,
                                        show_col_types = FALSE,
                                        trim_ws = TRUE) |> as_tibble() |> 
  mutate(file = path_file(X1), file = str_replace(file, "Logbestand", ""))

arch_tot <- arch_2011 |> bind_rows(arch_2014) |> bind_rows(arch_2017) |> bind_rows(arch_2021) |> 
  mutate(fn_len = str_length(str_trim(file))) |> filter(fn_len == 17) |> select(file, X2)

arch_tot.1 <- arch_tot |> filter(X2 != "minutes") |> mutate(minutes_raw = parse_number(X2)) |> 
  filter(minutes_raw > 0 & (minutes_raw %% 60 <= 2 | minutes_raw %% 60 >= 58)) |> 
  mutate(minutes_rounded = round(minutes_raw, -1),
         bc_ymd = round_date(ymd_hm(str_replace(file, "\\.m4a", "")), unit = "hour"),
         bc_interval = interval(start = bc_ymd, end = bc_ymd + minutes(minutes_rounded))) |> 
  select(file, bc_interval)

arch_tot_intervals <- arch_tot.1$bc_interval |> as.list()

wp_posts.6 <- wp_posts.5 |> mutate(audio_exists = post_interval %within% arch_tot_intervals)
wp_posts.7 <- wp_posts.6 |> filter(audio_exists) |> select(post_title, bc_start, bc_stop) |> 
  group_by(post_title) |> mutate(bc_key = row_number()) |> ungroup() |> arrange(post_title, bc_start)

cur_ss_unprepped.1 <- cur_ss_unprepped |> mutate(bc_title = str_replace(bc_title, "&", "and")) |> 
  group_by(bc_title) |> mutate(bc_key = row_number()) |> ungroup()

cur_ss_unprepped.2 <- cur_ss_unprepped.1 |> left_join(wp_posts.7, join_by(bc_title == post_title, bc_key)) |> 
  filter(!is.na(bc_start))

cur_ss_unprepped.3 <- cur_ss_unprepped.2 |> 
  mutate(bc_review = "WoJ LaCie-replays 4.0",
         bc_orig_ymd_new = ymd_hms(bc_start),
         bc_orig_id_new = NA_real_,
         bc_audio_new = str_remove(bc_audio_new, "_\\d{6}$")) |> 
  select(-c(bc_key:bc_stop))

cur_ss_prepped.1 <- cur_ss |> filter(bc_prepped)

new_ss <- cur_ss_prepped.1 |> bind_rows(cur_ss_unprepped.3) |> arrange(bc_title, bc_woj_ymd) |> 
  select(bc_woj_ymd, bc_orig_ymd:bc_orig_ymd_new, bc_title, everything())

# + upload to GD ----
ss <- sheet_write(ss = "1d8t8ZItwfBpdVrB9lyc83-F8NysdHRDpgnw-TJ9G2ng",
                  sheet = "woj_herhalingen_4.0",
                  data = new_ss)
