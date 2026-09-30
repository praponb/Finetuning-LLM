SELECT we.event_type, we.duration_sec, s.started_at, s.browser FROM web_events we INNER JOIN sessions s ON we.session_id = s.id;
