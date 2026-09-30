select sessions.started_at, web_events.session_id, web_events.event_time from sessions left outer join web_events on web_events.session_id = sessions.id where web_events.duration_sec < 416;
