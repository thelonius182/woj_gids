# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
# Create the 
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  

# the packages to use
pacman::p_load(googledrive, googlesheets4, dplyr, tidyr, lubridate, stringr, yaml, readr, chron)

# say "Hi" to Google
drive_auth(email = "cz.teamservice@gmail.com")
gs4_auth(token = drive_token())

# init sheet identifiers
gs_home <- "https://docs.google.com/spreadsheets/d/"
download_home <- "C:/Users/nipper/Downloads/cz_downloads/"

# load modelrooster ----
# NB - for some reason, GD gives an error when using 'read_sheet'. So, use 'drive_download'.
moro <- "1LdBjK5hcpl8m1ZdcZujWWcsx7nLJPY8Gt69mjN8QS9A"
gs_path <- paste0(download_home, "moro_woj.tsv")
drive_download(paste0(gs_home, moro), path = gs_path, overwrite = T)
tib_moro <- read_tsv(file = gs_path, locale = locale(encoding = "UTF-8"), col_types = "iiciiiicccc")

# load gidsinfo ----
gidsinfo <- "16DrvLEXi3mEa9AbSw28YBkYpCvyO1wXwBwaNi7HpBkA"
tib_gidsinfo <- read_sheet(paste0(gs_home, gidsinfo), sheet = "gids-info")
tib_gidsvertalingen <- read_sheet(paste0(gs_home, gidsinfo), sheet = "vertalingen NL-EN")

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

# combine with moro ----
cz_slot_dates.2 <- cz_slot_dates.1 |> inner_join(moro_woj)

# list runs Thursday to Thursday; make it run from 13:00 - 13:00
df_start <- min(cz_slot_dates.2$date_time)
hour(df_start) <- 13
df_stop <- max(cz_slot_dates.2$date_time)
hour(df_stop) <- 13

cz_slot_dates.3 <- cz_slot_dates.2 |> filter(date_time >= df_start & date_time < df_stop)

View(moro_woj |> select(broadcast) |> distinct() |> arrange(broadcast))

