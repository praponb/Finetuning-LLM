SELECT
    o.channel,
    COUNT(*) AS rows_total,
    MAX(src.quantity) AS max_quantity,
    MIN(src.unit_price) AS min_unit_price
FROM order_items src
LEFT JOIN orders o ON src.order_id = o.id
GROUP BY ROLLUP (o.channel)
