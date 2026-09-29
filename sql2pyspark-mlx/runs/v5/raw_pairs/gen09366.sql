SELECT
    web_events.id,
    web_events.session_id,
    web_events.page,
    sessions.device,
    sessions.browser,
    customers.country,
    customers.credit_limit
FROM web_events
LEFT OUTER JOIN sessions ON web_events.session_id = sessions.id
JOIN customers ON sessions.customer_id = customers.id
WHERE customers.signup_date BETWEEN '2023-11-01' AND '2025-10-13';
