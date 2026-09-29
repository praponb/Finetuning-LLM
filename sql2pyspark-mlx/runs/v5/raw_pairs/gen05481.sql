SELECT c.country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.page))) AS page_values, SORT_ARRAY(COLLECT_SET(we.event_type)) AS event_type_list
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.country
ORDER BY c.country, page_values
LIMIT 3
