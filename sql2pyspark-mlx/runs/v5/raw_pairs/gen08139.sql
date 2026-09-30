SELECT
    page,
    SUM(CASE WHEN page = '/cart' THEN duration_sec ELSE 0 END) cart_duration_sec,
    SORT_ARRAY(COLLECT_SET(page)) page_list,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) event_type_values
FROM web_events
WHERE session_id = 34 AND session_id IN (17, 23)
GROUP BY page
ORDER BY event_type_values;
