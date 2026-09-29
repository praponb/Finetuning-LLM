select id, event_time, duration_sec, lag(duration_sec, 1) over (order by event_time, id) as prev_duration_sec from web_events where session_id in (25, 26, 29, 33) order by prev_duration_sec desc, id
