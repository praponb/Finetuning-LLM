SELECT sessions.browser, SUM(CASE WHEN web_events.event_type = 'click' THEN 1 ELSE 0 END) AS click_count, MAX(web_events.duration_sec) AS max_duration_sec
FROM web_events
INNER JOIN sessions ON sessions.id = web_events.session_id
LEFT JOIN customers ON sessions.customer_id = customers.id
GROUP BY sessions.browser
ORDER BY click_count DESC;
