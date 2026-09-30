SELECT o.id, o.customer_id, o.store_id FROM orders o WHERE MONTH(o.order_date) IN (4, 7, 8) AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id) ORDER BY id DESC, o.customer_id DESC;
