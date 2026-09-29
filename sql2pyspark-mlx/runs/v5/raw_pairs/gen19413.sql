SELECT s.id, s.country
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE city = 'Bangkok');
