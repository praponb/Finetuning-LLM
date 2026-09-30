select event_type, page, sum(duration_sec) as total_duration_sec from web_events group by event_type, page having sum(duration_sec) < 2200 order by total_duration_sec desc
