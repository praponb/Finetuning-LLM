SELECT s.id, s.order_id, s.cost FROM shipments s WHERE s.order_id = 23 AND s.order_id NOT IN (SELECT id FROM orders WHERE amount >= 1430) ORDER BY id DESC, s.order_id DESC;
