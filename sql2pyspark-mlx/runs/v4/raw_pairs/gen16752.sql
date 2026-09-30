SELECT p.id, p.amount, o.channel, c.is_active FROM payments p INNER JOIN orders o ON p.order_id = o.id FULL OUTER JOIN customers c ON o.customer_id = c.id;
