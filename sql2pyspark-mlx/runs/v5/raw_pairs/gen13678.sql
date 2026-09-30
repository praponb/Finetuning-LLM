SELECT o.id, o.channel, o.amount
FROM orders o
WHERE o.store_id IS NOT NULL AND o.customer_id IN (SELECT id FROM customers WHERE signup_date >= '2025-03-01');
