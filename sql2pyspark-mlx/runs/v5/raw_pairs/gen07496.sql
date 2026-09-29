select session_id, substring(event_type, 1, 4) as event_type_prefix from web_events where duration_sec < 187 and duration_sec between 136 and 179 order by event_type_prefix desc;
