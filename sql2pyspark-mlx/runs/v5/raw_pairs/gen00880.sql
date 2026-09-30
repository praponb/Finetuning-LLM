SELECT event_type, COUNT(*) AS total_count, COUNT(DISTINCT duration_sec) AS unique_sessions
FROM web_events
WHERE session_id IN (2, 31)
GROUP BY event_type
ORDER BY total_count DESC, event_type;
