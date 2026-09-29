SELECT s.customer_id, SORT_ARRAY(COLLECT_SET(we.event_type)) AS event_type_list, ROUND(AVG(we.duration_sec), 2) AS avg_duration_sec
FROM web_events we
LEFT OUTER JOIN sessions s ON s.id = we.session_id
GROUP BY s.customer_id
