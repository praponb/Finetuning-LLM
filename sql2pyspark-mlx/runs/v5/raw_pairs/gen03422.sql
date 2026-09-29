SELECT
    MONTH(event_time) AS event_month,
    event_type,
    COUNT(DISTINCT duration_sec) AS unique_event_types,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
WHERE page IN ('/cart', '/search', '/checkout') OR event_type IN ('search', 'purchase', 'add_to_cart')
GROUP BY 1, 2;
