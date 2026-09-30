SELECT s.id, s.order_id, o.store_id, o.customer_id FROM shipments s LEFT JOIN orders o ON o.id = s.order_id WHERE s.status = 'LOST' OR o.amount < 1420 ORDER BY store_id DESC;
