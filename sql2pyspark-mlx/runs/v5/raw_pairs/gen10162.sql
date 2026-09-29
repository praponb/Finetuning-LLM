SELECT sessions.device, ROUND(STDDEV(web_events.duration_sec), 2) AS stddev_duration_sec, ROUND((100 * SUM(CASE WHEN web_events.event_type = 'click' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_click
FROM web_events
JOIN sessions ON web_events.session_id = sessions.id
JOIN customers ON sessions.customer_id = customers.id
GROUP BY sessions.device
ORDER BY pct_click DESC;
