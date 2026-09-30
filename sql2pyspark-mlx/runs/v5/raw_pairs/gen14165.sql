SELECT
    event_type,
    CASE WHEN page IN ('/checkout', '/product') THEN 'tier_1' ELSE 'other' END AS page_group,
    session_id,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values,
    SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
FROM web_events
WHERE session_id IN (16, 29, 35, 38) AND duration_sec >= 346 AND event_type != 'add_to_cart'
GROUP BY 1, 2, 3
ORDER BY page_group, search_duration_sec
