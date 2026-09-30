SELECT
    s.device,
    COUNT(we.id) AS web_event_count,
    MIN(we.duration_sec) AS min_duration_sec,
    COUNT(we.duration_sec) AS duration_sec_count
FROM sessions s
JOIN web_events we ON s.id = we.session_id
GROUP BY s.device
ORDER BY device ASC
LIMIT 5;
