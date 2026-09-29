SELECT c.id, c.segment, c.email
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee BETWEEN 80 AND 230)
