SELECT
    order_id,
    product_id,
    AVG(unit_price) AS avg_unit_price,
    SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE product_id = 14 AND order_id IN (9, 25, 39)
GROUP BY order_id, product_id
HAVING revenue < 52
ORDER BY product_id
