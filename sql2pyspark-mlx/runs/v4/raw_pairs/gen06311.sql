SELECT s.id, s.status, s.delivered_date, o.store_id FROM shipments s LEFT JOIN orders o ON o.id = s.order_id;
