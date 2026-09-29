SELECT
    o.store_id,
    o.customer_id,
    oi.product_id,
    oi.order_id
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
WHERE oi.order_id IN (10, 16, 24, 31) OR o.store_id IS NOT NULL
