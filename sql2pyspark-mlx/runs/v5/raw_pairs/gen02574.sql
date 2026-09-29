SELECT
    s.country,
    COUNT(we.id) AS web_event_count,
    SUM(we.duration_sec) AS total_duration_sec,
    MAX(we.duration_sec) AS max_duration_sec
FROM sessions s
JOIN web_events we ON we.session_id = s.id
GROUP BY s.country
HAVING SUM(we.duration_sec) > 2600
ORDER BY total_duration_sec, country
LIMIT 20;
