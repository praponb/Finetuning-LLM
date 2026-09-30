SELECT p.id, p.amount, p.status
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE store_id IN (3, 5, 20));
