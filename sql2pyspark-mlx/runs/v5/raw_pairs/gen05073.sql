SELECT o.order_date, o.amount, o.status, oi.quantity FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id ORDER BY o.status;
