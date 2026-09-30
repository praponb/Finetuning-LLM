SELECT s.country, SORT_ARRAY(COLLECT_SET(we.event_type)) AS event_type_list
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id
WHERE we.session_id IN (7, 22)
GROUP BY s.country
ORDER BY s.country DESC
