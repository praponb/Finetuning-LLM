SELECT sessions.id, sessions.country, sessions.device
FROM sessions
WHERE sessions.customer_id IN (SELECT id FROM customers WHERE age <= 55)
ORDER BY country, id DESC
