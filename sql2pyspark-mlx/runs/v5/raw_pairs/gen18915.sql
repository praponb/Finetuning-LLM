SELECT event_type, session_id, MAX(event_time) AS last_event_time
FROM web_events
WHERE page IS NOT NULL
GROUP BY ROLLUP(event_type, session_id)
ORDER BY 1
