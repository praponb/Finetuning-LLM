SELECT
    event_type,
    page,
    product_count,
    page_list
FROM (
    SELECT
        event_type,
        page,
        COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
        SORT_ARRAY(COLLECT_SET(page)) AS page_list
    FROM web_events
    WHERE session_id = 24
    GROUP BY event_type, page
) AS t
WHERE product_count < 7
ORDER BY page, event_type;
