SELECT CASE WHEN duration_sec >= 403 THEN 'high' WHEN duration_sec >= 231 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MIN(event_time) AS first_event_time, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
GROUP BY duration_sec_bucket
