SELECT
    oi.id,
    oi.order_id,
    oi.unit_price,
    p.cost,
    o.discount,
    o.channel
FROM order_items oi
FULL OUTER JOIN products p ON p.id = oi.product_id
JOIN orders o ON oi.order_id = o.id
ORDER BY cost, channel
