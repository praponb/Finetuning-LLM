SELECT
    web_events.id,
    web_events.event_time,
    web_events.session_id,
    sessions.device,
    sessions.browser,
    customers.email
FROM web_events
LEFT JOIN sessions ON sessions.id = web_events.session_id
INNER JOIN customers ON customers.id = sessions.customer_id
WHERE customers.is_active = TRUE AND sessions.device NOT IN ('tablet', 'desktop')
ORDER BY sessions.device;
