select max(event_time) as latest_event_time from web_events where duration_sec < 429 or duration_sec > 179
