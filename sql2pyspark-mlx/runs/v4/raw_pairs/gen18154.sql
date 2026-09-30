select hour(event_time) as event_hour, avg(duration_sec) as avg_duration_sec from web_events group by event_hour having avg(duration_sec) > 270 order by avg_duration_sec desc, event_hour limit 3;
