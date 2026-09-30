SELECT p.amount, p.status, p.method, o.discount FROM payments p LEFT OUTER JOIN orders o ON o.id = p.order_id WHERE p.amount IS NOT NULL AND o.customer_id = 12
