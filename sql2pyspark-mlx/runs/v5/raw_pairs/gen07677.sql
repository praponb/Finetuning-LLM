SELECT
    page,
    CASE WHEN duration_sec >= 534 THEN 'high' WHEN duration_sec >= 71 THEN 'mid' ELSE 'low' END duration_sec_bucket,
    DATE_FORMAT(event_time, 'yyyy-MM') event_month,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) event_type_values,
    ROUND((100 * SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_page_view
FROM web_events
WHERE page = '/product' AND event_type IN ('page_view', 'search', 'add_to_cart')
GROUP BY page, CASE WHEN duration_sec >= 534 THEN 'high' WHEN duration_sec >= 71 THEN 'mid' ELSE 'low' END, DATE_FORMAT(event_time, 'yyyy-MM');
