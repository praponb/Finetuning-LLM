SELECT event_type, ROUND((100 * SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_product
FROM web_events
WHERE duration_sec != 340
GROUP BY 1;
