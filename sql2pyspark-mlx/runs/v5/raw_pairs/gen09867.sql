SELECT p.id, p.status, p.order_id, o.customer_id FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id;
