SELECT DATE_TRUNC('MONTH', event_time) AS event_month, event_type, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
WHERE duration_sec < 265 AND session_id IN (11, 17, 25, 27) AND event_time < '2025-04-01'
GROUP BY DATE_TRUNC('MONTH', event_time), event_type
