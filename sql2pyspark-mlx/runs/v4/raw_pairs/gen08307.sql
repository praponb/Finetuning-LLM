SELECT c.id, c.name, c.age
FROM customers c
WHERE c.is_active AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.end_date <= '2025-08-23')
ORDER BY name ASC
