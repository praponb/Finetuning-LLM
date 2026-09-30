SELECT s.id, s.carrier, o.order_date, o.shipping_fee, o.store_id FROM shipments s LEFT OUTER JOIN orders o ON o.id = s.order_id WHERE o.store_id IN (1, 7, 21, 37);
