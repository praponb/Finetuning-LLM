SELECT event_type, CASE WHEN duration_sec >= 366 THEN 'high' WHEN duration_sec >= 196 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE duration_sec > 410
GROUP BY event_type, duration_sec_bucket
HAVING min_duration_sec < 330;
