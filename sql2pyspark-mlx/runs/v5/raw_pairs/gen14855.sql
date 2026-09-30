SELECT
    o.status,
    o.shipping_fee,
    o.channel,
    oi.unit_price
FROM orders o
INNER JOIN order_items oi ON oi.order_id = o.id
WHERE oi.unit_price IS NOT NULL
ORDER BY o.shipping_fee ASC;
