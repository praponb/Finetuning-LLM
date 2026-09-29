SELECT s.device, COUNT(we.id) AS web_event_count, COUNT(DISTINCT we.duration_sec) AS unique_event_types
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
GROUP BY s.device
ORDER BY device DESC, web_event_count DESC
