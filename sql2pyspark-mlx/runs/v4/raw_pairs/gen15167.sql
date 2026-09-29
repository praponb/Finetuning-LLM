SELECT CASE WHEN duration_sec >= 431 THEN 'high' WHEN duration_sec >= 71 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, ROUND((100 * SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_page_view
FROM web_events
WHERE TO_DATE(event_time) >= '2023-06-01' AND event_type IN ('page_view', 'search', 'add_to_cart')
GROUP BY 1
HAVING pct_page_view >= 65
ORDER BY pct_page_view DESC, duration_sec_bucket
LIMIT 50
