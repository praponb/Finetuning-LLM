SELECT we.id, we.page, we.duration_sec, s.started_at FROM web_events we JOIN sessions s ON we.session_id = s.id WHERE s.customer_id = 32 AND HOUR(s.started_at) BETWEEN 7 AND 11 ORDER BY s.started_at;
