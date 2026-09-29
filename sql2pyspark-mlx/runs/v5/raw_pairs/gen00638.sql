SELECT CASE WHEN duration_sec >= 487 THEN 'high' WHEN duration_sec >= 183 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MIN(duration_sec) AS min_duration_sec, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
GROUP BY CASE WHEN duration_sec >= 487 THEN 'high' WHEN duration_sec >= 183 THEN 'mid' ELSE 'low' END
ORDER BY avg_duration_sec;
