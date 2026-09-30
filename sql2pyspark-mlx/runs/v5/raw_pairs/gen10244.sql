SELECT o.id, o.customer_id
FROM orders o
WHERE o.amount BETWEEN 1120 AND 1350 AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.method = 'paypal')
