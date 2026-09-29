SELECT *
FROM (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM inventory
    GROUP BY product_id
) AS x
WHERE total_quantity > 770
ORDER BY total_quantity
