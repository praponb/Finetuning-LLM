SELECT p.id, p.order_id, p.amount, o.discount, o.status, o.customer_id FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE o.customer_id IN (2, 11, 37, 40);
