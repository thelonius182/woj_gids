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
