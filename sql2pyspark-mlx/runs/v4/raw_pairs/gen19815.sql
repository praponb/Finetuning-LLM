SELECT *
FROM (
    SELECT store_id, MIN(quantity) AS min_quantity, MIN(last_restock) AS first_last_restock
    FROM inventory
    WHERE product_id IN (5, 8, 9)
    GROUP BY store_id
) AS t
WHERE min_quantity >= 100;
