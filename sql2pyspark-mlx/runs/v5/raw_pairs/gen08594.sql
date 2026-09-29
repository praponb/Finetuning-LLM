SELECT we.duration_sec, we.event_time, we.page, s.country, s.device FROM web_events we INNER JOIN sessions s ON we.session_id = s.id ORDER BY country
