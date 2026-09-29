SELECT
    event_type,
    CASE WHEN duration_sec >= 401 THEN 'high' WHEN duration_sec >= 201 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
    page,
    MIN(event_time) AS min_event_time,
    MAX(event_time) AS last_event_time,
    COUNT(DISTINCT duration_sec) AS unique_pages
FROM web_events
WHERE duration_sec BETWEEN 178 AND 466
GROUP BY event_type, duration_sec_bucket, page
HAVING SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) > 37
ORDER BY unique_pages DESC, event_type, duration_sec_bucket, page
LIMIT 50;
