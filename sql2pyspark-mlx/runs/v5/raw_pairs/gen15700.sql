SELECT YEAR(event_time) AS event_year, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM web_events
GROUP BY YEAR(event_time)
HAVING SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) >= 33;
