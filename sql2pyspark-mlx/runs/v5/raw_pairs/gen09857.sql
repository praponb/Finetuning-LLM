SELECT o.id, o.customer_id, c.age
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.channel IN ('mobile', 'web', 'store');
