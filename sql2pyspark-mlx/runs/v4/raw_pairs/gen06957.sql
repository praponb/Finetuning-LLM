WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE quantity BETWEEN 169 AND 235 AND quantity BETWEEN 66 AND 156
),
quantity_bucket_summary AS (
    SELECT CASE WHEN quantity >= 192 THEN 'high' WHEN quantity >= 116 THEN 'mid' ELSE 'low' END AS quantity_bucket, ROUND(STDDEV(quantity), 2) AS stddev_quantity, MIN(last_restock) AS min_last_restock
    FROM valid_inventory
    GROUP BY CASE WHEN quantity >= 192 THEN 'high' WHEN quantity >= 116 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM quantity_bucket_summary
ORDER BY min_last_restock DESC;
