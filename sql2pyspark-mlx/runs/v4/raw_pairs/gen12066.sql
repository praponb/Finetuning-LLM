SELECT o.id, o.channel
FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE name IS NOT NULL);
