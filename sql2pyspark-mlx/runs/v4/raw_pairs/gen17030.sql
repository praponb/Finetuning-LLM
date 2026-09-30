SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    AVG(duration_sec) FILTER (WHERE page = '/product') AS product_avg_duration_sec,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes
FROM web_events
GROUP BY 1
ORDER BY span_minutes;
