SELECT oi.id, oi.product_id, o.shipping_fee
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
ORDER BY oi.id DESC, o.shipping_fee DESC
