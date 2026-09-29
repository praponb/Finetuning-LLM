SELECT
    product_id,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    SUM(quantity) AS total_quantity,
    AVG(quantity) AS avg_quantity
FROM order_items
WHERE (order_id IN (5, 36) AND product_id IS NULL) OR order_id = 12
GROUP BY product_id
HAVING COUNT(unit_price) >= 5;
