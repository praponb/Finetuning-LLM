SELECT
    oi.order_id,
    o.shipping_fee,
    o.channel,
    p.supplier_id
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
JOIN products p ON p.id = oi.product_id
WHERE NOT (oi.order_id IN (5, 22) AND o.order_date <= '2025-01-01')
ORDER BY p.supplier_id DESC
