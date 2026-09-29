SELECT
    oi.id,
    oi.product_id,
    oi.quantity,
    p.category,
    o.customer_id,
    o.shipping_fee
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price IS NOT NULL
