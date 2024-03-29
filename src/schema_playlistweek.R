pl_items <- cz_week_sched.5 |> select(slot_ts, broadcast, broadcast_type, regular_rewind, ts_rewind, mac) |> 
  mutate(uitzending = format(slot_ts, "%Y-%m-%d_%a%H"),
         titel = broadcast,
         universe_slot = format(ts_rewind, "%Y-%m-%d_%a%H"),
         bron = 
           case_when(broadcast_type == "LaCie" ~ "gids + audiofile volgens Overzicht Herhaalprogramma's",
                     broadcast_type == "NonStop" ~ "mAirlist non-stop playlistprofiel",
                     broadcast_type == "WorldOfJazz" ~ "lokale audiofile op WoJ-pc",
                     broadcast_type == "Universe" & regular_rewind ~ paste0("dezelfde als op de ",
                                                                           mac,
                                                                           " deze week: ",
                                                                           format(ts_rewind, "%Y-%m-%d_%a%H")),
                     broadcast_type == "Universe" & !regular_rewind ~ paste0("HiJack-file van de ",
                                                                           mac,
                                                                           ": ",
                                                                           format(ts_rewind, "%Y-%m-%d_%a%H")),
                     T ~ "todo")) |> 
  select(uitzending, titel, bron) |> arrange(bron, titel)

export(pl_items, "C:/Users/nipper/Documents/BasieBeats/plw_test.tsv")

ns_weken <- read_rds("C:/cz_salsa/cz_exchange/nipperstudio_week.RDS")

tib_bcid <- ns_weken |> 
  left_join(tib_gidsinfo, by = join_by(cz_slot_value == `key-modelrooster`)) |> 
  filter(!is.na(woj_bcid)) |> 
  select(woj_bcid, slot_ts = date_time, slot_key = cz_slot_value, slot_title_NL = `titel-NL`) |> 
  mutate(audio_from = if_else(slot_ts < ymd_h("2024-03-28 13"), "HiJack", "Universe")) |> 
  arrange(woj_bcid, slot_ts)
