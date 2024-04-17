# coonect to wordpress-DB ----
wp_conn <- get_wp_conn()

if (typeof(wp_conn) != "S4") {
  stop("wordpress db-connection failed")
}

# coonect to mAirList-DB ----
mal_conn <- get_mal_conn()

if (typeof(mal_conn) != "S4") {
  stop("mairlist db-connection failed")
}

# get the week to build
qry <- "
select max(po1.post_date) as ymd_post
from wp_posts po1
    left join wp_postmeta pm1 on pm1.post_id = po1.id
where po1.post_date > '2024-03-01' 
  and DAYOFWEEK(po1.post_date) = 5 
  and hour(po1.post_date) = 13 
  and po1.post_type = 'programma'
  and pm1.meta_key = 'pr_metadata_orig'
  and pm1.meta_value = ''
;"

prv_week_start <- dbGetQuery(wp_conn, qry) |> ymd_hms(quiet = T) 
cur_week_start <- prv_week_start + days(7)
hour(cur_week_start) <- 0
cur_week_stop <- cur_week_start + days(8)
tmp_format <- stamp("1969-07-20 17:18:19", orders = "%Y-%m-%d %H:%M:%S", quiet = T)
sq_cur_week_start = tmp_format(cur_week_start)
sq_cur_week_start = "2024-04-18 00:00:00"
sq_cur_week_stop = tmp_format(cur_week_stop)
sq_cur_week_stop = "2024-04-26 00:00:00"

# get mAirList playlist tracks
qry <- sprintf("
select pl.slot
,  pl.pos
,  it.title as track_title
,  it.artist
,  ia.name as attr_name
,  ia.value as album_title
,  pl.duration
from playlist pl
join items it on it.idx = pl.item
left join item_attributes ia on ia.item = pl.item
where pl.slot between '%s' and '%s'
	and it.storage not in (10, 11, 17) -- no Bumpers, no Semi-live's
order by pl.slot, pl.pos asc;",
sq_cur_week_start, sq_cur_week_stop
)

ml_tracks <- dbGetQuery(mal_conn, qry)

dbDisconnect(mal_conn)

ml_tracks.1 <- ml_tracks |> mutate(attr_name = str_to_lower(attr_name))

# not all tracks have an album name; add an atrificial one to those
ml_tracks_w_album <- ml_tracks.1 |> filter(attr_name == "album")

track_keys_w_album <- ml_tracks_w_album |>select(slot, pos) |> distinct()

ml_tracks_wo_album <- ml_tracks.1 |> anti_join(track_keys_w_album, join_by(slot, pos)) |> 
  group_by(slot, pos) |> mutate(grp = row_number()) |> ungroup() |> filter(grp == 1) |> 
  mutate(album_title = "-") |> select(-grp)

ml_tracks.2 <- ml_tracks_w_album |> bind_rows(ml_tracks_wo_album) |> select(-attr_name) |> 
  arrange(slot, pos)

# get the WordPress playlists
wp_playlists <- cz_week_sched.3 |> select(slot_ts, start, minutes, tit_nl, broadcast_type, txt_nl, txt_en)

# limit to NonStop
wp_playlists.1 <- cz_week_slots |> select(slot_ts) |> left_join(wp_playlists, by = join_by(slot_ts)) |> 
  fill(start, minutes, tit_nl, txt_nl, txt_en, broadcast_type, .direction = "down") |> 
  filter(broadcast_type == "NonStop")

# let them be united!
wp_playlists.2 <- wp_playlists.1 |> inner_join(ml_tracks.2, by = join_by(slot_ts == slot)) |> 
  mutate(pl_label = paste0(tit_nl, ", ", format(slot_ts, "%Y-%m-%d")),
         artist = if_else(str_length(str_trim(artist)) == 0, " ", artist)) |> 
  group_by(pl_label) |> mutate(plid = row_number()) |> ungroup() 

wp_playlists.3 <- wp_playlists.2 |> filter(plid == 1)

# prep de gids ----
for (cur_pl_label in wp_playlists.3$pl_label) {
  
  cur_pl.1 <- wp_playlists.2 |> filter(pl_label == cur_pl_label)
  min_slot_ts <- min(cur_pl.1$slot_ts)
  cur_pl <- cur_pl.1 |> 
    mutate(cum_time = round(cumsum(duration), 0),
           wall_clock = get_wallclock(min_slot_ts, cum_time)) |> 
    filter(cum_time < 60 * (minutes - 2))
  
  sql_post_date <- cur_pl$slot_ts[[1]] |> as.character()
  
  koptekst <- cur_pl |> select(txt_nl, txt_en) |> distinct()
  
  sql_gidstekst <- "@HEADER\n<!--more-->\n\n"

  regel <- '<style>td {padding: 6px; text-align: left;} .album-span {color: #999999;}</style>\n<table style="width: 100%;"><tbody>'
  sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
  
  for (q1 in 1:nrow(cur_pl)) {
    
    regel <-
      sprintf(
        '<tr>\n<td>%s <strong>%s</strong> - %s\n<span  class="album-span">',
        # sec2hms(cur_pl$cum_time[q1]),
        cur_pl$wall_clock[q1],
        cur_pl$track_title[q1], 
        cur_pl$artist[q1] |> 
          str_replace_all(pattern = '"',  "'") %>% 
          str_replace_all(pattern = "\\x5B", replacement = "(") %>% 
          str_replace_all(pattern = "\\x5D", replacement = ")") 
      )
    sql_gidstekst <- paste0(sql_gidstekst, regel)
    
    # regel <- cur_pl$artist[q1]
    # sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
    
    regel <- sprintf('Album: %s</span></td>\n</tr>',
                     cur_pl$album_title[q1])
    sql_gidstekst <- paste0(sql_gidstekst, regel, "\n")
  }
  
  # + muw logo ----
  regel <- 
    '</tbody>\n</table>
&nbsp;
<a href="https://www.muziekweb.nl">
<img class="aligncenter" src="https://www.concertzender.nl/wp-content/uploads/2024/04/muw_bengu_logo_small.png" />
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
      "update wp_posts set post_content = convert(cast(convert('%s' using latin1) as binary) using utf8mb4) 
      where id = %i;",
      str_replace_all(sql_gidstekst1, "'", "&apos;"),
      dsSql01$id[u1]
    )
    
    upd_result <- dbExecute(wp_conn, upd_stmt02)
  }
  
  # flog.info("Gids bijgewerkt: %s", cur_pl, name = "nsbe_log")
  
}

discon_result <- dbDisconnect(wp_conn)
