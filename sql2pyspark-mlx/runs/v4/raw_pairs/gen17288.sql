SELECT o.shipping_fee, o.order_date, o.amount, oi.unit_price FROM orders o INNER JOIN order_items oi ON o.id = oi.order_id WHERE o.shipping_fee > 15 ORDER BY o.order_date DESC
