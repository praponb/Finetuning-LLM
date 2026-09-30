SELECT o.id, o.order_date, o.shipping_fee FROM orders o WHERE EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.shipped_date > '2023-08-01');
