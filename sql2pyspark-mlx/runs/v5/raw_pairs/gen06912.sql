SELECT
    CASE WHEN duration_sec >= 499 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_view_rows,
    COUNT(*) AS event_count,
    GROUPING_ID() AS grouping_id
FROM web_events
GROUP BY CUBE (CASE WHEN duration_sec >= 499 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING add_to_cart_count < 1000
ORDER BY duration_sec_tier DESC
