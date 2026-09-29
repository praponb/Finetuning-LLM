select id, event_time, date_trunc('month', event_time) as event_time_period from web_events where session_id in (11, 17, 29, 30);
