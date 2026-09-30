SELECT
    DATE_TRUNC('MONTH', event_time) event_month,
    event_type,
    page,
    SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) home_duration_sec,
    COUNT(CASE WHEN page = '/search' THEN 1 END) search_count
FROM web_events
GROUP BY DATE_TRUNC('MONTH', event_time), event_type, page
HAVING SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) > 35
ORDER BY search_count DESC;
