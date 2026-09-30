SELECT page, COUNT(CASE WHEN page = '/checkout' THEN 1 END) checkout_count, MAX(duration_sec) max_duration_sec
FROM web_events
WHERE TO_DATE(event_time) >= '2023-12-01' AND session_id IN (24, 26, 39)
GROUP BY page
HAVING max_duration_sec >= 170
ORDER BY page DESC
