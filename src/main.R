# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create a WoJ Schedule Week
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use ----
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, stringr, yaml, readr, chron, rio,
               RMySQL, keyring)

source("src/functions.R", encoding = "UTF-8")
config <- read_yaml("config.yaml")
qry_prv_post <- read_lines("src/sql_stmt_prv_post_v2.txt")

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
woj_gidsinfo <- tib_gidsinfo |> filter(!is.na(woj_bcid)) |> 
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
         genre_1_nl, genre_1_en, genre_2_nl, genre_2_en, txt_nl, txt_en) 
  
# Set new first day of gidsweek ----
giva_home <- "C:/cz_salsa/config/giva_start.txt"
giva_latest <- import(giva_home) |> mutate(latest_run = ymd(latest_run))

current_run_start <- giva_latest$latest_run + ddays(7)
# flog.info("Dit wordt de Gidsweek van %s", 
#           format(current_run_start, "%A %d %B %Y"),
#           name = "wpgidsweeklog")

# cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
# but to the schedule-template both Thursdays are the same, as the
# template is undated.
# Both Thursday parts will separate when the schedule gets 'calendarized'
current_run_stop <- current_run_start + ddays(7)

# create time series ----
cz_slot_days <- seq(from = current_run_start, to = current_run_stop, by = "days")
cz_slot_hours <- seq(0, 23, by = 1)
cz_slot_dates <- merge(cz_slot_days, chron(time = paste(cz_slot_hours, ":", 0, ":", 0)))
colnames(cz_slot_dates) <- c("slot_date", "slot_time")
cz_slot_dates$date_time <- as.POSIXct(paste(cz_slot_dates$slot_date, cz_slot_dates$slot_time), "GMT")
row.names(cz_slot_dates) <- NULL
cz_slot_dates.1 <- as_tibble(cz_slot_dates) |> select(date_time) |> 
  mutate(day = wday(date_time, label = T, abbr = T),
         cycle = 1 + (day(date_time) - 1) %/% 7L,
         start = hour(date_time)) |> 
  arrange(date_time)

# combine with modelrooster ----
cz_slot_dates.2 <- cz_slot_dates.1 |> inner_join(tib_moro)

# list runs Thursday to Thursday; make it run from 13:00 - 13:00
df_start <- min(cz_slot_dates.2$date_time)
hour(df_start) <- 13
df_stop <- max(cz_slot_dates.2$date_time)
hour(df_stop) <- 13

cz_slot_dates.3 <- cz_slot_dates.2 |> filter(date_time >= df_start & date_time < df_stop) |> 
  mutate()

wp_conn <- get_wp_conn("dev")

if (typeof(wp_conn) != "S4") {
  stop("db-connection failed")
}

qry <- "select count(*) as n from wp_posts where post_type = 'programma_woj'"
p1 <- dbGetQuery(wp_conn, qry)
