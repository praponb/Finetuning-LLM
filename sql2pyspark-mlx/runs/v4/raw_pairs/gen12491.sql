SELECT
    sessions.id,
    sessions.customer_id,
    customers.age,
    customers.is_active,
    customers.country,
    CONCAT_WS('/', sessions.device, sessions.browser) AS device_browser_key
FROM sessions
JOIN customers ON customers.id = sessions.customer_id
WHERE customers.country = 'TH';
