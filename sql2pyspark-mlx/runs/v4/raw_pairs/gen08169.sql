SELECT
    order_id,
    AVG(unit_price) AS avg_unit_price,
    SUM(unit_price) AS total_unit_price,
    MIN(unit_price) AS min_unit_price
FROM order_items
GROUP BY order_id
HAVING min_unit_price > 370
ORDER BY total_unit_price DESC, order_id
LIMIT 20
