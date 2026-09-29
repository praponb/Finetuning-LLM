SELECT SORT_ARRAY(COLLECT_SET(page)) AS page_list, COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count, SUM(duration_sec) AS total_duration_sec
FROM web_events
ORDER BY checkout_count DESC, total_duration_sec DESC;
