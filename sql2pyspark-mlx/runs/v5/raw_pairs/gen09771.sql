SELECT duration_sec_bucket, search_duration_sec
FROM (
    SELECT CASE WHEN duration_sec >= 389 THEN 'high' WHEN duration_sec >= 123 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
    FROM web_events
    WHERE duration_sec <= 461
    GROUP BY CASE WHEN duration_sec >= 389 THEN 'high' WHEN duration_sec >= 123 THEN 'mid' ELSE 'low' END
) AS t
WHERE search_duration_sec >= 32
ORDER BY duration_sec_bucket ASC
LIMIT 5
