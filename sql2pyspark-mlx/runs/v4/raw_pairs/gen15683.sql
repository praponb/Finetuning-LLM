SELECT duration_sec_bucket, avg_duration_sec, first_event_time
FROM (
    SELECT CASE WHEN duration_sec >= 530 THEN 'high' WHEN duration_sec >= 85 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, MIN(event_time) AS first_event_time
    FROM web_events
    GROUP BY CASE WHEN duration_sec >= 530 THEN 'high' WHEN duration_sec >= 85 THEN 'mid' ELSE 'low' END
) AS sub
WHERE avg_duration_sec > 31
ORDER BY duration_sec_bucket DESC
LIMIT 50
