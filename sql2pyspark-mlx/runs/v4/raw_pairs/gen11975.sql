SELECT session_id, SUM(duration_sec) AS total_duration_sec, MAX(duration_sec) AS max_duration_sec
FROM web_events
GROUP BY session_id
ORDER BY total_duration_sec DESC, session_id
LIMIT 10;
