# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Create WoJ schedules, WP-posts and tracklists
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

conflicts_prefer(dplyr::lag, dplyr::lead, dplyr::filter, lubridate::minutes, .quiet = T)

# signal to 'add_ml_tracklists_to_wp' it will be called from 'main'.
# That will tell it to create a new week, instead of refreshing the most recent week
salsa_source_main <- T

# main control loop
repeat {
  
  flog.info("WoJ-week - Deel 3", name = "wojsch")
  
  # create time series ----
  # cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
  cz_week_slots <- slot_sequence_wk() # use week from main_a
  cz_week_start <- cz_week_slots$slot_ts[1]

  # publish the draft posts ----
  source("src/woj_publish_posts.R", encoding = "UTF-8")

  if (exists("salsa_source_error")) {
    break
  }

  # add ML-tracklists ----
  source("src/add_ml_tracklists_to_wp.R", encoding = "UTF-8")

  # exit from main control loop
  break
}

if (exists("salsa_source_error")) {
  flog.info("Deel 3 kon niet voltooid worden - zie 'woj_schedules.log' (Nipper/Desktop).", name = "wojsch")
} else {
  flog.info("Deel 3 voltooid, geen bijzonderheden.", name = "wojsch")
}
