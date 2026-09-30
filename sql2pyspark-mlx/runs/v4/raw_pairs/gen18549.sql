SELECT s.shipped_date, o.store_id, o.status FROM shipments s LEFT JOIN orders o ON s.order_id = o.id;
