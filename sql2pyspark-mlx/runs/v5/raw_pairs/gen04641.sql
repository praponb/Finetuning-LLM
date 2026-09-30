SELECT
    order_id,
    MAX(unit_price) AS max_unit_price,
    COUNT(DISTINCT quantity) AS unique_orders,
    MIN(unit_price) AS min_unit_price
FROM order_items
WHERE unit_price < 110
GROUP BY order_id
ORDER BY order_id DESC
LIMIT 50
