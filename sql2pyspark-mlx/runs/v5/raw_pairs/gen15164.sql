SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    oi.quantity
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE order_date >= '2025-10-30')
