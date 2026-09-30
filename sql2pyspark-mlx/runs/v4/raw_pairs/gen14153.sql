select id, duration_sec, session_id, hour(event_time) as event_time_hour, substring(event_type, 1, 2) as event_type_prefix from web_events where event_type not in ('purchase', 'search')
