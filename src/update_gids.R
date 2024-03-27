# coonect to mAirList-DB ----
mal_conn <- get_mal_conn()

if (typeof(mal_conn) != "S4") {
  stop("db-connection failed")
}

# get mAirList playlists
qry <- "SELECT pl.slot, pl.pos, it.title as track_title, it.artist, ia.value as album_title, pl.duration
FROM public.playlist pl 
   join items it on it.idx = pl.item
   left join item_attributes ia on ia.item = pl.item
where pl.duration > 0 and pl.slot between '2024-03-28 13:00:00' and '2024-05-01 00:00:00'
  and ia.name = 'album'
order by slot, pos
;
"

ml_playlists <- dbGetQuery(mal_conn, qry)

dbDisconnect(mal_conn)

# get the WordPress playlists
wp_playlists <- cz_week_sched.5 |> select(slot_ts, start, minutes, tit_nl, broadcast_type)

# limit to NonStop
wp_playlists.1 <- cz_week_slots |> select(slot_ts) |> left_join(wp_playlists, by = join_by(slot_ts)) |> 
  select(-start, -minutes) |>  fill(tit_nl, broadcast_type, .direction = "down") |> 
  filter(broadcast_type == "NonStop")

# let them be united!
wp_playlists.2 <- wp_playlists.1 |> inner_join(ml_playlists, by = join_by(slot_ts == slot))

# prep de gids ----
for (cur_pl in unique(ns_tracks$pl_name)) {
  
  sql_post_date <- playlist2postdate(cur_pl) %>% as.character
  
  drb_gids_pl <- drb_gids %>% filter(pl_name == cur_pl)
  
  koptekst <- drb_gids_pl %>% select(pl_name, componist) %>% distinct %>% 
    group_by(pl_name) %>% summarise(werken_van = paste(componist, collapse = ", "))
  
  # sql_gidstekst <- sprintf("Werken van %s.\n<!--more-->\n\n", koptekst$werken_van)
  sql_gidstekst <- "@HEADER\n<!--more-->\n\n"
  
  regel <- 
    '<a>
<img class="aligncenter" src="https://www.concertzender.nl/wp-content/uploads/2023/06/rata_logo.png" />
</a>
&nbsp;'
  sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
  
  regel <- '<style>td {padding: 6px; text-align: left;}</style>\n<table style="width: 100%;"><tbody>'
  sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
  
  for (q1 in 1:nrow(drb_gids_pl)) {
    regel <-
      sprintf(
        '<tr>\n<td>[track tijd="%s" text="%s %s"]\n<span>',
        drb_gids_pl$cum_tijd[q1],
        drb_gids_pl$wallclock[q1],
        drb_gids_pl$titel[q1] %>% 
          str_replace_all(pattern = '"',  "'") %>% 
          str_replace_all(pattern = "\\x5B", replacement = "(") %>% 
          str_replace_all(pattern = "\\x5D", replacement = ")") 
      )
    sql_gidstekst <- paste0(sql_gidstekst, regel)
    
    regel <- drb_gids_pl$componist[q1]
    sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
    
    regel <- sprintf('%s</span></td>\n</tr>',
                     drb_gids_pl$uitvoerenden[q1])
    sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
  }
  
  # + muw logo ----
  regel <- 
    '</tbody>\n</table>
&nbsp;
<a href="https://www.muziekweb.nl">
<img class="aligncenter" src="https://www.concertzender.nl/wp-content/uploads/2023/06/dank_muw_logo.png" />
</a>
&nbsp;
&nbsp;'
  
  sql_gidstekst <- paste0(sql_gidstekst, regel, "\n") %>% str_replace_all("[']", "&#39;")
  
  upd_stmt01 <- sprintf(
    "select id from wp_posts where post_date = '%s' and post_type = 'programma' order by 1;",
    sql_post_date
  )
  dsSql01 <- dbGetQuery(ns_con, upd_stmt01)
  
  # + gidskop bepalen ----
  hdr_nl_df <- bum.3 %>% 
    mutate(hdr_key = sub(".*\\.\\d{3}_(.*)", "\\1", pl_name, perl=TRUE)) %>% 
    left_join(gd_wp_gidsinfo_header_NL) %>% 
    filter(pl_name == cur_pl)
  
  hdr_en_df <- bum.3 %>% 
    mutate(hdr_key = sub(".*\\.\\d{3}_(.*)", "\\1", pl_name, perl=TRUE)) %>% 
    left_join(gd_wp_gidsinfo_header_EN) %>% 
    filter(pl_name == cur_pl)
  
  for (u1 in 1:nrow(dsSql01)) {
    
    if (u1 == 1) {
      sql_gidstekst1 <- sql_gidstekst %>% str_replace("@HEADER", hdr_nl_df$hdr_txt)
    } else {
      sql_gidstekst1 <- sql_gidstekst %>% str_replace("@HEADER", hdr_en_df$hdr_txt)
    }
    
    upd_stmt02 <- sprintf(
      "update wp_posts set post_content = '%s' where id = %s;",
      sql_gidstekst1,
      as.character(dsSql01$id[u1])
    )
    
    dbExecute(ns_con, upd_stmt02)
  }
  
  flog.info("Gids bijgewerkt: %s", cur_pl, name = "nsbe_log")
  
  # + replace replay-post? ---- 
  #   only if a recycle episode is available
  recycle_pl <- get_replay_playlist(cur_pl)
  
  if (recycle_pl == cur_pl) {
    flog.info("Kringloopherhaling is nog niet beschikbaar", name = "nsbe_log")
    next
  }
  
  cur_pl_date <- playlist2postdate(cur_pl)
  replay_date <- cur_pl_date + days(7L)
  recycle_pl_date <- playlist2postdate(recycle_pl)
  
  tmp_log <- sprintf("Kringloopherhaling: op %s klinkt die van %s", 
                     replay_date, recycle_pl_date)
  flog.info(tmp_log, name = "nsbe_log")
  
  # + . get replay_date's post-id ----
  upd_stmt06 <- sprintf(
    "select min(id) as min_id from wp_posts where post_date = '%s' and post_type = 'programma';",
    replay_date)
  
  replay_pgm_id <- dbGetQuery(ns_con, upd_stmt06)
  
  # + . get recycle-post's id ----
  upd_stmt04 <- sprintf(
    "select min(id) as min_id from wp_posts where post_date = '%s' and post_type = 'programma';",
    recycle_pl_date)
  
  recycle_pgm_id <- dbGetQuery(ns_con, upd_stmt04) 
  
  # + . update post-id's NL/EN ----
  for (r1 in 1:2) {
    recycle_pgm_id_chr <- as.character(recycle_pgm_id$min_id + r1 - 1L)
    replay_pgm_id_chr <- as.character(replay_pgm_id$min_id + r1 - 1L)
    upd_stmt05 <-
      sprintf(
        "update wp_postmeta set meta_value = %s where post_id = %s and meta_key = 'pr_metadata_orig';",
        recycle_pgm_id_chr,
        replay_pgm_id_chr
      )
    
    flog.info("SQL: %s", upd_stmt05, name = "nsbe_log")
    
    dbExecute(ns_con, upd_stmt05)
  }
  
  suppressMessages(stamped_format <- stamp("20191229_zo", orders = "%Y%0m%d_%a"))
  dummy_pl <- paste0(stamped_format(cur_pl_date + days(7L)), str_sub(cur_pl, 12))
  flog.info("Gids replay bijgewerkt: %s", dummy_pl, name = "nsbe_log")
}

#_----

