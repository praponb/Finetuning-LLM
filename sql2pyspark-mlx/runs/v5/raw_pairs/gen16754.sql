SELECT
    id,
    duration_sec,
    event_time,
    LENGTH(page) AS page_length,
    CASE WHEN page IN ('/cart', '/home') THEN 'Tier 1' ELSE 'Other' END AS page_group
FROM web_events
ORDER BY 3 DESC, id ASC
LIMIT 5;
