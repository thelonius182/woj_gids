# qry <- "select date_format(po1.post_date, '%Y-%m-%d %H:%i:%s') as slot_ts, 
qry <- "select po1.post_date as wpdmp_slot_ts, 
       replace(po1.post_title, '&amp;', '&') as wpdmp_slot_title
from wp_posts po1 
left join wp_term_relationships tr1 ON tr1.object_id = po1.id                   
left join wp_term_taxonomy tx1 ON tx1.term_taxonomy_id = tr1.term_taxonomy_id   
where length(trim(po1.post_title)) > 0 
  and po1.post_type = 'programma' 
  and tx1.term_taxonomy_id = 5 
  and po1.post_date > date_add(now(), interval -180 day)
order by po1.post_date desc
;"

wp_dump <- dbGetQuery(wp_conn, qry)
