SELECT
    id,
    duration_sec,
    event_type,
    TRIM(event_type) AS clean_event_type,
    CASE WHEN page IN ('/cart', '/checkout') THEN 'Tier 1' ELSE 'Other' END AS page_group
FROM web_events
WHERE TO_DATE(event_time) >= '2025-07-25' AND page = '/product'
ORDER BY event_type;
