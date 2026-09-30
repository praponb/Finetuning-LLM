SELECT
    o.channel,
    COUNT(*) AS num_rows,
    MAX(e.unit_price) AS max_unit_price,
    MIN(e.quantity) AS min_quantity
FROM order_items e
INNER JOIN orders o ON e.order_id = o.id
GROUP BY ROLLUP (o.channel)
ORDER BY min_quantity ASC;
