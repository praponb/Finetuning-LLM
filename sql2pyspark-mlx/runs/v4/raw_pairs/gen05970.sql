SELECT we.event_time, we.page, s.started_at FROM web_events we FULL OUTER JOIN sessions s ON we.session_id = s.id ORDER BY we.event_time;
