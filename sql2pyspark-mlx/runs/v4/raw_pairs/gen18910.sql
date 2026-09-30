SELECT
    oi.id,
    oi.order_id,
    oi.quantity,
    oi.product_id
FROM order_items oi
WHERE oi.quantity > 6 AND oi.order_id IN (SELECT id FROM orders WHERE shipping_fee >= 32)
