SELECT *
FROM web_events
WHERE duration_sec = 461 AND duration_sec < 131
ORDER BY session_id ASC, id ASC
LIMIT 5;
