SELECT we.id, we.duration_sec
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE HOUR(started_at) BETWEEN 10 AND 13);
