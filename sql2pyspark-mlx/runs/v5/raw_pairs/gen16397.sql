SELECT s.device, SORT_ARRAY(COLLECT_SET(we.page)) AS page_list, MAX(we.event_time) AS last_event_time
FROM web_events we
JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id
WHERE s.browser = 'Firefox'
GROUP BY s.device
ORDER BY last_event_time ASC
