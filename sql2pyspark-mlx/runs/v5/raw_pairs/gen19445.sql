select d.device, e.page, count(*) as rows_total, min(e.duration_sec) as min_duration_sec from web_events e join sessions d on e.session_id = d.id group by d.device, e.page
