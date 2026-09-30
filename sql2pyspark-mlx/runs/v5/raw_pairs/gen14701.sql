SELECT oi.quantity, o.channel FROM order_items oi LEFT JOIN orders o ON o.id = oi.order_id WHERE o.shipping_fee BETWEEN 14 AND 26 AND oi.product_id IS NOT NULL;
