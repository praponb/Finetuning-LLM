SELECT o.amount, oi.product_id, oi.unit_price, oi.quantity FROM orders o LEFT OUTER JOIN order_items oi ON o.id = oi.order_id WHERE oi.unit_price IS NULL
