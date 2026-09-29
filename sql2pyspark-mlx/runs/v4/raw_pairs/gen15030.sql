SELECT we.duration_sec, s.started_at, s.device FROM web_events we JOIN sessions s ON s.id = we.session_id ORDER BY 3 DESC, started_at
