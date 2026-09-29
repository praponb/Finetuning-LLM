SELECT
    order_id,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    MIN(quantity) AS min_quantity,
    MAX(quantity) AS max_quantity
FROM order_items
WHERE (order_id IN (1, 8, 13, 30) AND unit_price < 100) OR order_id = 32
GROUP BY order_id
ORDER BY order_id
