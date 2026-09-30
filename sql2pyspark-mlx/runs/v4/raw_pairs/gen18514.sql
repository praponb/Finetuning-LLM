SELECT
    s.id,
    COUNT(we.id) AS web_event_count,
    MAX(we.duration_sec) AS max_duration_sec,
    MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
GROUP BY s.id;
