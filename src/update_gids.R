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
where pl.duration > 0 and pl.slot between '2024-04-04 00:00:00' and '2024-04-12 00:00:00'
  and ia.name = 'album'
order by slot, pos
;
"

ml_playlists <- dbGetQuery(mal_conn, qry)

dbDisconnect(mal_conn)

# get the WordPress playlists
wp_playlists <- cz_week_sched.3 |> select(slot_ts, start, minutes, tit_nl, broadcast_type, txt_nl, txt_en)

# limit to NonStop
wp_playlists.1 <- cz_week_slots |> select(slot_ts) |> left_join(wp_playlists, by = join_by(slot_ts)) |> 
  fill(start, minutes, tit_nl, txt_nl, txt_en, broadcast_type, .direction = "down") |> 
  filter(broadcast_type == "NonStop")

# let them be united!
wp_playlists.2 <- wp_playlists.1 |> inner_join(ml_playlists, by = join_by(slot_ts == slot)) |> 
  mutate(pl_label = paste0(tit_nl, ", ", format(slot_ts, "%Y-%m-%d"))) |> 
  group_by(pl_label) |> mutate(plid = row_number()) |> ungroup() 

wp_playlists.3 <- wp_playlists.2 |> filter(plid == 1)

# prep de gids ----
for (cur_pl_label in wp_playlists.3$pl_label) {
  
  cur_pl <- wp_playlists.2 |> filter(pl_label == cur_pl_label) |> 
    mutate(cum_time = round(cumsum(duration), 0),
           wall_clock = get_wallclock(slot_ts, cum_time)) |> 
    filter(cum_time < 60 * minutes)
  
  sql_post_date <- cur_pl$slot_ts[[1]] |> as.character()
  
  koptekst <- cur_pl |> select(txt_nl, txt_en) |> distinct()
  
  sql_gidstekst <- "@HEADER\n<!--more-->\n\n"

  regel <- '<style>td {padding: 6px; text-align: left;}</style>\n<table style="width: 100%;"><tbody>'
  sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
  
  for (q1 in 1:nrow(cur_pl)) {
    regel <-
      sprintf(
        '<tr>\n<td>[track tijd="%s" text="%s %s"]\n<span>',
        sec2hms(cur_pl$cum_time[q1]),
        cur_pl$wall_clock[q1],
        cur_pl$track_title[q1] |> 
          str_replace_all(pattern = '"',  "'") %>% 
          str_replace_all(pattern = "\\x5B", replacement = "(") %>% 
          str_replace_all(pattern = "\\x5D", replacement = ")") 
      )
    sql_gidstekst <- paste0(sql_gidstekst, regel)
    
    regel <- cur_pl$artist[q1]
    sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
    
    regel <- sprintf('%s</span></td>\n</tr>',
                     cur_pl$album_title[q1])
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
    "select id from wp_posts where post_date = '%s' and post_type = 'programma_woj' order by 1;",
    sql_post_date
  )
  dsSql01 <- dbGetQuery(wp_conn, upd_stmt01)
  
  # + gidskop bepalen ----
  hdr_nl_df <- cur_pl$txt_nl[[1]]
  hdr_en_df <- cur_pl$txt_en[[1]]

  for (u1 in 1:nrow(dsSql01)) {
    
    if (u1 == 1) {
      sql_gidstekst1 <- sql_gidstekst |> str_replace("@HEADER", hdr_nl_df)
    } else {
      sql_gidstekst1 <- sql_gidstekst |> str_replace("@HEADER", hdr_en_df)
    }
    
    upd_stmt02 <- sprintf(
      "update wp_posts set post_content = '%s' where id = %s;",
      str_replace_all(sql_gidstekst1, "'", "&apos;"),
      as.character(dsSql01$id[u1])
    )
    
    dbExecute(wp_conn, upd_stmt02)
  }
  
  flog.info("Gids bijgewerkt: %s", cur_pl, name = "nsbe_log")
  
}
