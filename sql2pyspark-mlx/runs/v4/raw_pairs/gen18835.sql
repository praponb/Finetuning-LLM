SELECT we.session_id, we.event_type, we.page, s.browser FROM web_events we INNER JOIN sessions s ON we.session_id = s.id WHERE we.duration_sec BETWEEN 145 AND 410 ORDER BY we.event_type DESC;
