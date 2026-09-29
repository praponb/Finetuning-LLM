WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE last_restock BETWEEN '2023-07-23' AND '2024-07-29' AND store_id = 4
),
summary AS (
    SELECT store_id, CASE WHEN quantity >= 192 THEN 'high' WHEN quantity >= 120 THEN 'mid' ELSE 'low' END quantity_bucket, COUNT(DISTINCT quantity) unique_products
    FROM recent_inventory
    GROUP BY store_id, CASE WHEN quantity >= 192 THEN 'high' WHEN quantity >= 120 THEN 'mid' ELSE 'low' END
)
SELECT store_id, unique_products
FROM summary;
