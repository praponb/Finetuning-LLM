SELECT
    CASE WHEN duration_sec >= 455 THEN 'high' WHEN duration_sec >= 136 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
    session_id,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    MAX(event_time) AS latest_event_time
FROM web_events
GROUP BY CASE WHEN duration_sec >= 455 THEN 'high' WHEN duration_sec >= 136 THEN 'mid' ELSE 'low' END, session_id
ORDER BY latest_event_time
