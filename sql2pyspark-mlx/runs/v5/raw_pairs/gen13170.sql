SELECT p.order_id, o.amount, o.discount FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE p.status IN ('SUCCESS', 'REFUNDED', 'FAILED') AND p.status <> 'FAILED'
