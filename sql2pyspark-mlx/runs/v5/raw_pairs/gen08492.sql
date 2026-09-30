SELECT p.id, p.method, p.amount
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE MONTH(order_date) IN (3, 4, 12));
