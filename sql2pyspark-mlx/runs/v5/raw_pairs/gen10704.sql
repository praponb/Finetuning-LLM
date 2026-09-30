SELECT p.id, p.paid_at FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE store_id = 9) ORDER BY p.paid_at DESC
