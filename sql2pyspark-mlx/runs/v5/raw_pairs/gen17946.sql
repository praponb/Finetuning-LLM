SELECT
    order_id,
    SUM(unit_price) AS total_unit_price,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE quantity BETWEEN 3 AND 5 AND unit_price < 140
GROUP BY 1
