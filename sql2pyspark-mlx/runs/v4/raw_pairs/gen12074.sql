SELECT
    event_type,
    CASE WHEN duration_sec >= 499 THEN 'high' WHEN duration_sec >= 109 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
    AVG(duration_sec) AS avg_duration_sec,
    MIN(event_time) AS first_event_time
FROM web_events
WHERE duration_sec BETWEEN 420 AND 428
GROUP BY event_type, CASE WHEN duration_sec >= 499 THEN 'high' WHEN duration_sec >= 109 THEN 'mid' ELSE 'low' END
ORDER BY avg_duration_sec;
