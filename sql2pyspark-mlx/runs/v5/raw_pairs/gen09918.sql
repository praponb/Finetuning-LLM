SELECT we.id, we.duration_sec
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE customer_id IN (5, 8, 9, 23))
ORDER BY we.id, duration_sec;
