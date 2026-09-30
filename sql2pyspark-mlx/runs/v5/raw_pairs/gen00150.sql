SELECT
    web_events.id,
    web_events.session_id,
    web_events.event_time,
    sessions.customer_id,
    sessions.browser,
    customers.signup_date,
    customers.credit_limit
FROM web_events
LEFT JOIN sessions ON sessions.id = web_events.session_id
LEFT JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.country = 'US' AND web_events.duration_sec BETWEEN 212 AND 294
