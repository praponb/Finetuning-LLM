SELECT
    YEAR(event_time) AS event_year,
    MAX(duration_sec) AS max_duration_sec,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
    MAX(event_time) AS latest_event_time
FROM web_events
WHERE HOUR(event_time) BETWEEN 12 AND 20 AND (page IN ('/search', '/product', '/checkout') OR event_type NOT IN ('page_view', 'purchase', 'click'))
GROUP BY YEAR(event_time)
