SELECT
    sessions.browser,
    sessions.started_at,
    customers.email,
    customers.is_active
FROM sessions
INNER JOIN customers ON customers.id = sessions.customer_id
WHERE LENGTH(customers.name) > 5 AND customers.credit_limit < 12300
ORDER BY sessions.browser
