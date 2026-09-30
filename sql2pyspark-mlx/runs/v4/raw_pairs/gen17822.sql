WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (2, 5, 8, 10) OR last_restock > '2025-02-01'
),
summary AS (
    SELECT CASE WHEN quantity >= 243 THEN 'high' WHEN quantity >= 107 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(quantity) AS min_quantity
    FROM recent_inventory
    GROUP BY CASE WHEN quantity >= 243 THEN 'high' WHEN quantity >= 107 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE min_quantity > 70;
