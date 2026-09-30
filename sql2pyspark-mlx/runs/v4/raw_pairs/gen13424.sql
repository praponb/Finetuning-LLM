SELECT p.id, p.status
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee < 24);
