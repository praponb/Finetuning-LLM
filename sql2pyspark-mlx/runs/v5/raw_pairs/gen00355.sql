select id, session_id, cast(event_time as date) as event_time_day, to_date(event_time) as event_time_date from web_events where duration_sec < 458;
