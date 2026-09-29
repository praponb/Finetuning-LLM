SELECT oi.id, oi.quantity, o.shipping_fee, o.store_id, c.country FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.id INNER JOIN customers c ON o.customer_id = c.id ORDER BY shipping_fee DESC
