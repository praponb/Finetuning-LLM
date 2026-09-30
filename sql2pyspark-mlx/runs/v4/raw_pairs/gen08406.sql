SELECT
    web_events.event_time,
    web_events.page,
    sessions.browser,
    customers.name,
    customers.city
FROM web_events
JOIN sessions ON web_events.session_id = sessions.id
RIGHT JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.country IN ('UK', 'US', 'JP');
