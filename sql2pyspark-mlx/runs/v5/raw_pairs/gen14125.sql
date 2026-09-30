WITH RecentInventory AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (1, 2, 3, 10) OR product_id IN (2, 7)
),
summary AS (
    SELECT store_id, AVG(quantity) avg_quantity
    FROM RecentInventory
    GROUP BY store_id
)
SELECT store_id, avg_quantity
FROM summary
WHERE avg_quantity > 220
