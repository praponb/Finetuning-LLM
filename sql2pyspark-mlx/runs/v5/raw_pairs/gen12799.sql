SELECT p.id, p.order_id, p.method
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee >= 19);
