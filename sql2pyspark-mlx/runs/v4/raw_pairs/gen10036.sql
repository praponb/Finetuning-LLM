select session_id, duration_sec from web_events where page = '/cart' except select session_id, duration_sec from web_events where session_id in (14, 15, 26, 27)
