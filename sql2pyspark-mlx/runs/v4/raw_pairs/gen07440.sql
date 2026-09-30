SELECT
    order_id,
    MIN(unit_price) AS min_unit_price,
    AVG(unit_price) AS avg_unit_price,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
GROUP BY order_id
HAVING SUM(unit_price) >= 1100
ORDER BY avg_unit_price, unit_price_range DESC;
