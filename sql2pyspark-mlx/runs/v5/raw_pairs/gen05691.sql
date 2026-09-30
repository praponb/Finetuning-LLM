SELECT
    o.id,
    o.channel,
    COUNT(oi.id) AS order_item_count,
    MAX(oi.unit_price) - MIN(oi.unit_price) AS unit_price_range
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
GROUP BY o.id, o.channel
ORDER BY channel DESC, id
LIMIT 10;
