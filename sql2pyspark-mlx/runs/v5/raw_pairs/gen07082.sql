SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    o.channel,
    o.order_date,
    o.store_id
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price < 280
ORDER BY oi.id DESC
