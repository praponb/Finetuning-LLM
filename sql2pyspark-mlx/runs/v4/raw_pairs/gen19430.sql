SELECT
    event_type,
    MIN(duration_sec) AS min_duration_sec,
    SUM(CASE WHEN event_type = 'page_view' THEN duration_sec ELSE 0 END) AS page_view_duration_sec,
    COUNT(duration_sec) AS duration_sec_count
FROM web_events
GROUP BY event_type
