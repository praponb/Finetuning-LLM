SELECT event_type, CASE WHEN duration_sec >= 511 THEN 'high' WHEN duration_sec >= 156 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MAX(event_time) AS latest_event_time
FROM web_events
GROUP BY event_type, CASE WHEN duration_sec >= 511 THEN 'high' WHEN duration_sec >= 156 THEN 'mid' ELSE 'low' END
ORDER BY event_type DESC, duration_sec_bucket
LIMIT 3;
