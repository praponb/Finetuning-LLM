SELECT o.id, o.customer_id, o.order_date
FROM orders o
WHERE o.customer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price < 170);
