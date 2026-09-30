SELECT
    oi.id,
    oi.unit_price,
    o.store_id,
    o.shipping_fee,
    o.status
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price >= 220 AND o.store_id IS NULL;
