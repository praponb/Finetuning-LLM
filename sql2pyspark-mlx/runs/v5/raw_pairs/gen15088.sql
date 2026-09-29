SELECT
    oi.order_id,
    oi.product_id,
    o.channel,
    s.sqft,
    s.store_name
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN stores s ON o.store_id = s.id
WHERE o.amount >= 920 AND oi.product_id IN (11, 26, 36, 39)
ORDER BY o.channel;
