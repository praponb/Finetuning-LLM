SELECT p.paid_at, o.customer_id FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE o.discount <= 0.37;
