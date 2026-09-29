WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id = 4
),
summary AS (
    SELECT product_id, SUM(quantity) AS total_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM valid_inventory
    GROUP BY product_id
)
SELECT product_id, total_quantity, stddev_quantity
FROM summary
ORDER BY stddev_quantity DESC, product_id ASC
LIMIT 3;
