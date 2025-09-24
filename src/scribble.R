# test salsa_calandar
conflicts_prefer(dplyr::lag, dplyr::lead, dplyr::filter, lubridate::minutes, .quiet = T)

salsa_calendar <- get_salsa_calendar()

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

# init sheet identifiers
ss <- "1YlW-E2g4ZC--ugbebL9iQrwR8wKIVWaXaBARaoRKGPI"
omcat_jazz_tib <- read_sheet(ss, sheet = "omcat_jazz")

bc_week_start <- now(tzone = "Europe/Amsterdam")
bc <- omcat_jazz_tib[1,] |> inner_join(salsa_calendar,
                                       by = join_by(dag == weekday,
                                                    rang == nth_weekday_in_month,
                                                    uur == hour_local)) |> arrange(datetime_local) |>
  filter(datetime_local > bc_week_start) |> slice_head()

