pl_items <- cz_week_sched.3 |> 
  filter(broadcast_type != "NonStop") |> 
  select(slot_ts, broadcast_id, broadcast, broadcast_type, ts_rewind, audio_src, mac) |> 
  mutate(uitzending = format(slot_ts, "%Y-%m-%d_%a%Hu"),
         titel = broadcast,
         universe_slot = format(ts_rewind, "%Y-%m-%d_%a%Hu"),
         bron = 
           case_when(broadcast_type == "LaCie" ~ "zie Overzicht Herhaalprogramma's",
                     broadcast_type == "WorldOfJazz" ~ "lokaal op WorldOfJazz-pc",
                     broadcast_type == "Universe" & 
                       audio_src == "Universe" ~ paste0("volgt ",
                                                       mac,
                                                       " van deze week: ",
                                                       format(ts_rewind, "%Y-%m-%d_%a%Hu")),
                     broadcast_type == "Universe" &
                       audio_src == "HiJack" ~ paste0("HiJack-file ",
                                                      mac,
                                                      ": ",
                                                      format(ts_rewind, "%Y-%m-%d_%a%Hu")),
                     T ~ "todo")) |> 
  select(uitzending, titel, bron) # |> arrange(bron, titel)

export(pl_items, "C:/Users/nipper/Documents/BasieBeats/plw_test.tsv")


  # mutate(audio_from = if_else(slot_ts < ymd_h("2024-03-28 13"), "HiJack", "Universe")) |> 