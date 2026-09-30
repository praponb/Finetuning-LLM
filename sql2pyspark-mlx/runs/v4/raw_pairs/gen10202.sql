SELECT
    s.device,
    s.country,
    SORT_ARRAY(COLLECT_SET(we.page)) page_list,
    SUM(CASE WHEN we.page = '/product' THEN 1 ELSE 0 END) product_count
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY s.device, s.country;
