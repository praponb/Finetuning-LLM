SELECT
    sessions.id,
    sessions.browser,
    sessions.customer_id,
    sessions.started_at,
    customers.name,
    customers.country
FROM sessions
LEFT JOIN customers ON sessions.customer_id = customers.id
WHERE customers.country = 'DE' AND sessions.browser IN ('Firefox', 'Safari')
ORDER BY sessions.id DESC, sessions.started_at
