SELECT oi.quantity, o.shipping_fee, p.cost, p.launch_date FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.id RIGHT JOIN products p ON oi.product_id = p.id ORDER BY o.shipping_fee DESC;
