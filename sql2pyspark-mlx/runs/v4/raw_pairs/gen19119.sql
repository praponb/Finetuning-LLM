SELECT product_id, unique_orders, avg_quantity
FROM (
    SELECT product_id, COUNT(DISTINCT unit_price) AS unique_orders, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY product_id
) AS agg
WHERE unique_orders >= 10
ORDER BY product_id DESC;
