SELECT p.order_id, o.status, c.credit_limit
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
WHERE o.channel = 'partner'
